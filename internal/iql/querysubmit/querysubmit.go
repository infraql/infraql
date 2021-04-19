package querysubmit

import (
	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/planbuilder"

	log "github.com/sirupsen/logrus"
)

func SubmitQuery(handlerCtx *handler.HandlerContext) dto.ExecutorOutput {
	log.Debugln("SubmitQuery() invoked...")
	plan, err := planbuilder.BuildPlanFromContext(handlerCtx)
	if err != nil {
		return dto.NewExecutorOutput(nil, nil, nil, err)
	}
	pl := dto.NewBasicPrimitiveContext(
		nil,
		nil,
		handlerCtx.Outfile,
		handlerCtx.OutErrFile,
		nil,
	)
	return plan.Instructions.Execute(pl)
}
