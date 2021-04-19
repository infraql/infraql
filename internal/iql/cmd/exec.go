/*
Copyright © 2019 InfraQL info@infraql.io

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"bytes"
	"io"
	"os"

	"github.com/spf13/cobra"

	"infraql/internal/iql/driver"
	"infraql/internal/iql/entryutil"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/iqlerror"
	"infraql/internal/iql/writer"
)

// execCmd represents the exec command
var execCmd = &cobra.Command{
	Use:   "exec",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {

		var err error
		var rdr io.Reader
		var handlerCtx *handler.HandlerContext

		switch runtimeCtx.InfilePath {
		case "stdin":
			rdr = bytes.NewReader([]byte(args[0]))
		default:
			rdr, err = os.Open(runtimeCtx.InfilePath)
			iqlerror.PrintErrorAndExitOneIfError(err)
		}
		handlerCtx, err = entryutil.BuildHandlerContext(runtimeCtx, rdr, queryCache)
		iqlerror.PrintErrorAndExitOneIfError(err)
		iqlerror.PrintErrorAndExitOneIfNil(handlerCtx, "handler context error")
		RunCommand(handlerCtx, nil, nil)
	},
}

func getOutputFile(filename string) (*os.File, error) {
	switch filename {
	case "stdout":
		return os.Stdout, nil
	case "stderr":
		return os.Stderr, nil
	default:
		return os.Create(filename)
	}
}

func RunCommand(handlerCtx *handler.HandlerContext, outfile io.Writer, outErrFile io.Writer) {
	if outfile == nil {
		outfile, _ = getOutputFile(handlerCtx.RuntimeContext.OutfilePath)
	}
	if outErrFile == nil {
		outErrFile, _ = getOutputFile(writer.StdErrStr)
	}
	handlerCtx.Outfile = outfile
	handlerCtx.OutErrFile = outErrFile
	if handlerCtx.RuntimeContext.DryRunFlag {
		driver.ProcessDryRun(handlerCtx)
		return
	}
	driver.ProcessQuery(handlerCtx)
}
