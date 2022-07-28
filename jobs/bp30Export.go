package jobs

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

type BP30Export struct {
	Id      uint
	Command string
	Out     string
}

func (job BP30Export) Run(ctx context.Context, params string, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, `begin bop.pts_lt_bp30.aggregate(:jobNumber); end;`, job.Id)
	if err != nil {
		addLog(
			ctx, db, job.Id, "BP30 Export",
			`select TO_NUMBER(TO_CHAR(bop.ptb_proc_mth.get_mth(bop.p_constant.APP_ID_LT)))`,
			"E", err.Error(),
		)
		updateStatus(ctx, db, job.Id, "E")
		return err
	}

	procMonth := getProcMonth(db)

	var count int
	err = db.GetContext(ctx, &count, `select count(*) from ts_lt_bp30 where proc_month = TO_DATE(:procMonth, 'YYYYMM')`, procMonth)
	if err != nil {
		addLog(
			ctx, db, job.Id, "BP30 Export",
			fmt.Sprintf(`select count(*) from ts_lt_bp30 where proc_month = TO_DATE(%d, 'YYYYMM')`, procMonth),
			"E", err.Error(),
		)
		updateStatus(ctx, db, job.Id, "E")
		return err
	}

	_, err = db.ExecContext(ctx, `start :script`, job.Command)
	if err != nil {
		addLog(
			ctx, db, job.Id, "BP30 Export",
			fmt.Sprintf(`start %s`, job.Command),
			"E", err.Error(),
		)
		updateStatus(ctx, db, job.Id, "C")
		return err
	}

	return nil
}
