package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/elainabialkowski/dispatcher/jobs"
	_ "github.com/godror/godror"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Dispatcher struct {
	running  map[string]*Dispatched
	queued   chan *Dispatched
	incoming chan *struct {
		jobNumber uint
		job       jobs.Job
		params    []string
	}
	maxWorkers int
	wg         *sync.WaitGroup
}

type Dispatched struct {
	id        string
	status    string
	start     time.Time
	end       time.Time
	err       error
	jobNumber uint
	job       jobs.Job
	params    []string
}

func main() {
	dispatcher := Dispatcher{
		running: make(map[string]*Dispatched),
		queued:  make(chan *Dispatched),
		incoming: make(chan *struct {
			jobNumber uint
			job       jobs.Job
			params    []string
		}),
		maxWorkers: 10,
		wg:         &sync.WaitGroup{},
	}
	err := dispatcher.Run()
	if err != nil {
		log.Printf("Dispatcher exited with error: %s", err.Error())
	}
}

func (d Dispatcher) Run() error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Interrupt handler channel initiated\n")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	db, err := sqlx.Open("godror", os.Getenv("DB_CONN_URI"))
	if err != nil {
		return err
	}

	log.Printf("DB connection established\n")

	log.Printf("Generating Worker pool")

	for i := 0; i < d.maxWorkers; i++ {
		d.wg.Add(1)
		go func(ctx context.Context) {
			for job := range d.queued {

				job.status = "running"
				job.start = time.Now()

				if err := job.job.Run(ctx, job.params, db); err != nil {
					job.status = "error"
					job.err = err
					log.Printf("Job %d failed: %s", job.jobNumber, err.Error())
				} else {
					job.status = "complete"
					job.err = nil
					log.Printf("Job %d completed without error", job.jobNumber)
				}

				job.end = time.Now()
			}
			d.wg.Done()
		}(ctx)
	}

	log.Printf("Starting dispatcher watchdog")

	func(ctx context.Context) {
		for {
			select {
			case <-ticker.C:
				var jobNumbers []uint
				db.Get(&jobNumbers,
					`
						select jobNumber
						from bop.TS_control
						where bop.PTS_control.isReady(jobNumber) = 'YES'
						ORDER BY prioritym start_time, jobnumber;
					`,
				)

				for _, v := range jobNumbers {
					var x struct {
						schema   string
						command  string
						priority string
						user     string
					}
					db.Get(
						&x,
						`
						select bop.PTS_control.get_schema(:jobNumber)
							 bop.PTS_control.get_command(:jobNumber)
							 bop.PTS_control.get_priority(:jobNumber)
							 bop.PTS_control.get_user(:jobNumber)
						from dual;
						`,
						v,
					)

					d.incoming <- &struct {
						jobNumber uint
						job       jobs.Job
						params    []string
					}{
						jobNumber: v,
						job:       jobs.JobRegistry[strings.Fields(x.command)[0]],
						params:    strings.Fields(x.command)[1:],
					}
				}

			case job := <-d.incoming:
				log.Printf("Job %d received", job.jobNumber)
				uuid, _ := uuid.NewRandom()
				d.running[uuid.String()] = &Dispatched{
					id:        uuid.String(),
					status:    "pending",
					job:       job.job,
					params:    job.params,
					jobNumber: job.jobNumber,
					err:       nil,
				}
				d.queued <- d.running[uuid.String()]
				jobs.UpdateStatus(ctx, db, job.jobNumber, "P")
				log.Printf("Job %d dispatched", job.jobNumber)
			case <-signals:
				cancel()
			case <-ctx.Done():
				close(d.queued)
				d.wg.Wait()
				return
			}
		}
	}(ctx)

	return nil

}
