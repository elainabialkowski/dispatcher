package jobs

import (
	"context"
	"os"
	"path"

	"github.com/jmoiron/sqlx"
)

type BP30Import struct {
	Id       uint
	UserId   string
	Language string
	RespId   uint
	Data     string
}

func (job BP30Import) Run(ctx context.Context, params string, db *sqlx.DB) error {

	job.Data = path.Join(os.Getenv("BOPINBOX"), "data", "in", job.Data)
	badFile, _ := os.CreateTemp("", "bad_*")
	respFile := getRespFile(db, job.Id)
	month := getProcMonth(db)

	loadData(job.Id, job.RespId, respFile.CtlFile, badFile.Name(), job.Data, respFile.Bopkey, month)
	loadData(job.Id, job.RespId, "lt_import_bad.ctl", "", badFile.Name(), respFile.Bopkey, month)

	return nil
}
