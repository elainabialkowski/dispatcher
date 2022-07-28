package jobs

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type BP30Classify struct {
	Id     uint
	UserId string
}

func (job BP30Classify) Run(ctx context.Context, params string, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, `begin bop.PTB_trans_BP30.set_bopkey_instr(:jobNumber); end;`, job.Id)
	if err != nil {
		addLog(
			ctx, db, job.Id, "BP30 Classify",
			fmt.Sprintf(`begin bop.PTB_trans_BP30.set_bopkey_instr(%d) end;`, job.Id),
			"E", err.Error(),
		)
		UpdateStatus(ctx, db, job.Id, "E")
		return err
	}

	addLog(
		ctx, db, job.Id, "BP30 Classify",
		fmt.Sprintf(`begin bop.PTB_trans_BP30.set_bopkey_instr(%d) end;`, job.Id),
		"C", "Success!",
	)
	UpdateStatus(ctx, db, job.Id, "C")
	return nil

}
