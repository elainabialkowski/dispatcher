package jobs

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type AutoRetirements struct {
	Id     uint
	UserId string
	Month  uint
	AppId  uint
}

func (job AutoRetirements) Run(ctx context.Context, params string, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, `begin bop.PP_auto_ret.gen_auto_retirement(:jobNumber, TO_DATE(:month, 'YYYYMM'), :appId); end;`, job.Id, job.Month, job.AppId)
	if err != nil {
		addLog(
			ctx, db, job.Id, "Automatic Retirements",
			fmt.Sprintf(`begin bop.PP_auto_ret.gen_auto_retirement(%d, TO_DATE(%d, 'YYYYMM'), %d); end;`, job.Id, job.Month, job.AppId),
			"E", err.Error(),
		)
		UpdateStatus(ctx, db, job.Id, "E")
		return err
	}

	addLog(
		ctx, db, job.Id, "Automatic Retirements",
		fmt.Sprintf(`begin bop.PP_auto_ret.gen_auto_retirement(%d, TO_DATE(%d, 'YYYYMM'), %d); end;`, job.Id, job.Month, job.AppId),
		"C", "Success!",
	)
	UpdateStatus(ctx, db, job.Id, "C")
	return nil
}
