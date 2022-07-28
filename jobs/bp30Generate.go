package jobs

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type BP30Generate struct {
	Id uint
}

func (job BP30Generate) Run(ctx context.Context, params string, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, `begin bop.PTB_trans_BP30.calc_par(:jobNumber); end;`, job.Id)
	if err != nil {
		addLog(
			ctx, db, job.Id, "BP30 Classify",
			fmt.Sprintf(`begin bop.PTB_trans_BP30.calc_par(%d) end;`, job.Id),
			"E", err.Error(),
		)
		updateStatus(ctx, db, job.Id, "E")
		return err
	}

	addLog(
		ctx, db, job.Id, "BP30 Classify",
		fmt.Sprintf(`begin bop.PTB_trans_BP30.calc_par(%d) end;`, job.Id),
		"C", "Success!",
	)
	updateStatus(ctx, db, job.Id, "C")
	return nil
}
