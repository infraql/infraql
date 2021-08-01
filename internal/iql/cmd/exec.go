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
	Short: "Run one or more InfraQL commands or queries",
	Long: `Run one or more InfraQL commands or queries from the command line or from an
input file. For example:

infraql exec \
"select id, name from compute.instances where project = 'infraql-demo' and zone = 'australia-southeast1-a'" \
--keyfilepath /mnt/c/tmp/infraql-demo.json --output csv

infraql exec -i iqlscripts/listinstances.iql --keyfilepath /mnt/c/tmp/infraql-demo.json --output json

infraql exec -i iqlscripts/create-disk.iql --keyfilepath /mnt/c/tmp/infraql-demo.json
`,
	Run: func(cmd *cobra.Command, args []string) {

		var err error
		var rdr io.Reader

		switch runtimeCtx.InfilePath {
		case "stdin":
			if len(args) == 0 || args[0] == "" {
				cmd.Help()
				os.Exit(0)
			}
			rdr = bytes.NewReader([]byte(args[0]))
		default:
			rdr, err = os.Open(runtimeCtx.InfilePath)
			iqlerror.PrintErrorAndExitOneIfError(err)
		}
		sqlEngine, err := entryutil.BuildSQLEngine(runtimeCtx)
		iqlerror.PrintErrorAndExitOneIfError(err)
		handlerCtx, err := entryutil.BuildHandlerContext(runtimeCtx, rdr, queryCache, sqlEngine)
		iqlerror.PrintErrorAndExitOneIfError(err)
		iqlerror.PrintErrorAndExitOneIfNil(&handlerCtx, "Handler context error")
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

func RunCommand(handlerCtx handler.HandlerContext, outfile io.Writer, outErrFile io.Writer) {
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
