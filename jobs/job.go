package jobs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"text/template"

	"github.com/jmoiron/sqlx"
)

type Job interface {
	Run(ctx context.Context, params string, db *sqlx.DB)
}

type RespFile struct {
	CtlFile string `db:"import_ctl_name"`
	Bopkey  uint   `db:"resp_bopkey"`
}

func getRespFile(db *sqlx.DB, id uint) RespFile {
	respFileInfo := RespFile{}
	db.Get(&respFileInfo, `
		select import_ctl_name, resp_bopkey
		from bop.TL_resp_file
		where resp_file_id = :respId 
	`, id)

	return respFileInfo
}

func getProcMonth(db *sqlx.DB) uint {
	var procMonth uint
	db.Get(&procMonth, `
		select to_number(to_char(bop.ptb_proc_mth.get_mth(bop.p_contant.app_id_lt), 'YYYYMM'))
	`)
	return procMonth
}

func loadData(
	jobId uint,
	respId uint,
	ctlFile string,
	badFile string,
	dataFile string,
	bopkey uint,
	month uint,
) error {

	type CtlData struct {
		JobId    uint
		DataFile string
		BadFile  string
		RespId   uint
		BopKey   uint
		Month    uint
	}

	data := CtlData{
		JobId:    jobId,
		DataFile: dataFile,
		BadFile:  badFile,
		RespId:   respId,
		BopKey:   bopkey,
		Month:    month,
	}

	ctlTemplate, err := template.ParseFiles(ctlFile)
	if err != nil {
		return err
	}

	ctlDefinition, err := os.CreateTemp("", "*")
	if err != nil {
		return err
	}

	err = ctlTemplate.Execute(ctlDefinition, data)
	if err != nil {
		return err
	}

	return exec.Command("sqlldr", fmt.Sprintf("control=%s", ctlDefinition.Name())).Run()

}

func addLog(
	ctx context.Context,
	db *sqlx.DB,
	id uint,
	name string,
	method string,
	code string,
	msg string,
) {
	db.ExecContext(
		ctx,
		`begin bop.PTS_log.add(:tjobNumber, :tservice, :tactivity, :msg_level, :tmsgId, :tmessage); end;`,
		id, name, method, code, "", msg,
	)
}

func updateStatus(
	ctx context.Context,
	db *sqlx.DB,
	id uint,
	status string,
) {
	db.ExecContext(
		ctx,
		`begin bop.PTS_control.set_status(:jn, :status); end;`,
		id, status,
	)
}
