package daemon

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Dispatcher struct {
	running    map[string]*Dispatched
	queued     chan *Dispatched
	incoming   chan Job
	maxWorkers int
	wg         *sync.WaitGroup
}

type Dispatched struct {
	id     string
	status string
	start  time.Time
	end    time.Time
	err    error
	job    Job
}

func (d Dispatcher) Run() error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < d.maxWorkers; i++ {
		d.wg.Add(1)
		go func(ctx context.Context) {
			for job := range d.queued {

				job.status = "running"
				job.start = time.Now()

				if err := job.job.Run(); err != nil {
					job.status = "error"
					job.err = err
				}

				job.status = "complete"
				job.end = time.Now()
			}
			d.wg.Done()
		}(ctx)
	}

	go func(ctx context.Context) {
		for {
			select {
			case job := <-d.incoming:
				if job.Validate() != nil {
					uuid, _ := uuid.NewRandom()
					d.running[uuid.String()] = &Dispatched{
						id:     uuid.String(),
						status: "pending",
						job:    job,
						err:    nil,
					}
					d.queued <- d.running[uuid.String()]
				}
			case <-signals:
				cancel()
			case <-ctx.Done():
				close(d.queued)
				d.wg.Wait()
			}
		}
	}(ctx)

	return nil

}
