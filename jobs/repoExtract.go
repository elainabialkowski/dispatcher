package jobs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"
)

type RepoExtract struct {
	Id         uint
	UserId     string
	TransferId string
}

func (job RepoExtract) Run(ctx context.Context, params []string, db *sqlx.DB) error {
	job.populate(params)
	_, err := db.ExecContext(ctx, `begin bop.PTB_lt_in_process_repo.loop_broker_list(:jobNumber, :transferId); end;`, job.Id, job.TransferId)
	if err != nil {
		addLog(
			ctx, db, job.Id, "Repo Extract ",
			fmt.Sprintf(`begin bop.PTB_lt_in_process_repo.loop_broker_list(%d, %s); end;`, job.Id, job.TransferId),
			"E", err.Error(),
		)
		UpdateStatus(ctx, db, job.Id, "E")
		return err
	}

	addLog(
		ctx, db, job.Id, "BP30 Classify",
		fmt.Sprintf(`begin bop.PTB_lt_in_process_repo.loop_broker_list(%d, %s); end;`, job.Id, job.TransferId),
		"C", "Success!",
	)
	UpdateStatus(ctx, db, job.Id, "C")
	return nil
}

func (job RepoExtract) populate(params []string) {
	id64, _ := strconv.ParseUint(params[0], 10, 32)
	job.Id = uint(id64)
	job.UserId = params[1]
	job.UserId = params[2]
}
