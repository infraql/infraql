package driver

import (
	"infraql/internal/iql/dto"
	"infraql/internal/iql/entryutil"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/querysubmit"
	"infraql/internal/iql/responsehandler"
	"infraql/internal/iql/util"
	"strings"

	log "github.com/sirupsen/logrus"
)

func ProcessDryRun(handlerCtx *handler.HandlerContext) {
	resultMap := map[string]map[string]interface{}{
		"1": {
			"query": handlerCtx.RawQuery,
		},
	}
	log.Debugln("dryrun query underway...")
	response := util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, resultMap, nil, nil, nil, nil))
	responsehandler.HandleResponse(handlerCtx, response)
}

func throwErr(err error, handlerCtx *handler.HandlerContext) {
	response := dto.NewExecutorOutput(nil, nil, nil, err)
	responsehandler.HandleResponse(handlerCtx, response)
}

func ProcessQuery(handlerCtx *handler.HandlerContext) {
	cmdString := handlerCtx.RawQuery
	tc, err := entryutil.GetTxnCounterManager(*handlerCtx)
	if err != nil {
		throwErr(err, handlerCtx)
		return
	}
	handlerCtx.TxnCounterMgr = tc
	for _, s := range strings.Split(cmdString, ";") {
		if s == "" {
			continue
		}
		handlerCtx.Query = s
		response := querysubmit.SubmitQuery(handlerCtx)
		responsehandler.HandleResponse(handlerCtx, response)
	}
}
