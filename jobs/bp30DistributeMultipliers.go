package jobs

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type BP30DistributeMultipliers struct {
	Id uint
}

func (job BP30DistributeMultipliers) Run(ctx context.Context, params string, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, `begin bop.PTB_trans_BP30.distribute_multipliers(:jobNumber); end;`, job.Id)
	if err != nil {
		addLog(
			ctx, db, job.Id, "BP30 Classify",
			fmt.Sprintf(`begin bop.PTB_trans_BP30.distribute_multipliers(%d); end;`, job.Id),
			"E", err.Error(),
		)
		UpdateStatus(ctx, db, job.Id, "E")
		return err
	}

	addLog(
		ctx, db, job.Id, "BP30 Classify",
		fmt.Sprintf(`begin bop.PTB_trans_BP30.distribute_multipliers(%d); end;`, job.Id),
		"C", "Success!",
	)
	UpdateStatus(ctx, db, job.Id, "C")
	return nil
}
