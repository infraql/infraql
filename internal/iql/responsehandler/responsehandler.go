package responsehandler

import (
	"fmt"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/output"
	"os"

	log "github.com/sirupsen/logrus"
)

func handleEmptyWriter(outputWriter output.IOutputWriter, err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return
	}
	if outputWriter == nil {
		fmt.Fprintln(os.Stderr, "Unable to obtain output writer")
		return
	}
}

func HandleResponse(handlerCtx *handler.HandlerContext, response dto.ExecutorOutput) error {
	var outputWriter output.IOutputWriter
	var err error
	log.Infoln(fmt.Sprintf("response from query = '%v'", response.Result))
	if response.Msg != nil {
		for _, msg := range response.Msg.WorkingMessages {
			handlerCtx.Outfile.Write([]byte(msg + fmt.Sprintln("")))
		}
	}
	if response.Result != nil && response.Result.Fields != nil && response.Err == nil {
		outputWriter, err = output.GetOutputWriter(
			handlerCtx.Outfile,
			handlerCtx.OutErrFile,
			dto.OutputContext{
				RuntimeContext: handlerCtx.RuntimeContext,
				Result:         response.Result,
			},
		)
		if outputWriter == nil || err != nil {
			handleEmptyWriter(outputWriter, err)
			return err
		}
		outputWriter.Write(response.Result)
	} else if response.Err != nil {
		outputWriter, err = output.GetOutputWriter(
			handlerCtx.Outfile,
			handlerCtx.OutErrFile,
			dto.OutputContext{
				RuntimeContext: handlerCtx.RuntimeContext,
				Result:         response.Result,
			},
		)
		if outputWriter == nil || err != nil {
			handleEmptyWriter(outputWriter, err)
			return response.Err
		}
		outputWriter.WriteError(response.Err, handlerCtx.ErrorPresentation)
		return response.Err
	}
	return err
}
