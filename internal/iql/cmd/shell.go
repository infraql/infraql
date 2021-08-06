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
	"fmt"
	"infraql/internal/iql/color"
	"infraql/internal/iql/config"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/entryutil"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/iqlerror"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/writer"
	"io"
	"runtime"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"readline"

	log "github.com/sirupsen/logrus"
)

const (
	shellLongStr string = `InfraQL Command Shell %s
Copyright (c) 2021, InfraQL Technologies. All rights reserved.
Welcome to the interactive shell for running InfraQL commands.
---`

	// Auth messages
	interactiveSuccessMsgTmpl string = `Authenticated interactively to google as %s, to change the authenticated user, use AUTH REVOKE followed by AUTH LOGIN, see https://docs.infraql.io/language-spec/auth`

	notAuthenticatedMsg string = `Not authenticated, to authenticate to a provider use the AUTH LOGIN command, see https://docs.infraql.io/language-spec/auth`

	saFileErrorMsgTmpl string = `Not authenticated, service account referenced in keyfilepath (%s) does not exist, authenticate interactively using AUTH LOGIN, for more information see https://docs.infraql.io/language-spec/auth`

	saSuccessMsgTmpl string = `Authenticated using a service account set using the keyfilepath flag (%s), for more information see https://docs.infraql.io/language-spec/auth`
)

func getShellIntroLong() string {
	return fmt.Sprintf(shellLongStr, SemVersion)
}

func usage(w io.Writer) {
	io.WriteString(w, getShellIntroLong()+"\r\n")
}

func getShellPRompt(authCtx *dto.AuthCtx, cd *color.ColorDriver) string {
	if authCtx != nil && authCtx.Active {
		switch authCtx.Type {
		case dto.AuthInteractiveStr:
			return cd.ShellColorPrint("InfraQL* >>")
		case dto.AuthServiceAccountStr:
			return cd.ShellColorPrint("InfraQL**>>")
		}
	}
	return cd.ShellColorPrint("InfraQL  >>")
}

func getIntroAuthMsg(authCtx *dto.AuthCtx, provider provider.IProvider) string {
	if authCtx != nil {
		if authCtx.Active {
			switch authCtx.Type {
			case dto.AuthInteractiveStr:
				return fmt.Sprintf(interactiveSuccessMsgTmpl, authCtx.ID)
			case dto.AuthServiceAccountStr:
				return fmt.Sprintf(saSuccessMsgTmpl, authCtx.KeyFilePath)
			}
		} else if err := provider.CheckServiceAccountFile(authCtx.KeyFilePath); authCtx.KeyFilePath != "" && err != nil {
			log.Debugln(fmt.Sprintf("authCtx.KeyFilePath = %v", authCtx.KeyFilePath))
			return fmt.Sprintf(saFileErrorMsgTmpl, authCtx.KeyFilePath)
		}
	}
	return notAuthenticatedMsg
}

func colorIsNull(runtimeCtx dto.RuntimeCtx) bool {
	return runtimeCtx.ColorScheme == dto.NullColorScheme || runtime.GOOS == "windows"
}

// shellCmd represents the shell command
var shellCmd = &cobra.Command{
	Use:   "shell",
	Short: "Interactive shell for running InfraQL commands",
	Long:  getShellIntroLong(),
	Run: func(command *cobra.Command, args []string) {

		cd := color.NewColorDriver(runtimeCtx)

		outfile, _ := writer.GetDecoratedOutputWriter(runtimeCtx.OutfilePath, cd)

		outErrFile, _ := writer.GetDecoratedOutputWriter(writer.StdErrStr, cd, cd.GetErrorColorAttributes(runtimeCtx)...)

		var sb strings.Builder
		fmt.Fprintln(outfile, "") // necesary hack to get 'square' coloring
		fmt.Fprintln(outfile, getShellIntroLong())

		sqlEngine, err := entryutil.BuildSQLEngine(runtimeCtx)
		iqlerror.PrintErrorAndExitOneIfError(err)

		handlerCtx, _ := handler.GetHandlerCtx("", runtimeCtx, queryCache, sqlEngine)
		provider, pErr := handlerCtx.GetProvider(handlerCtx.RuntimeContext.ProviderStr)
		authCtx, authErr := handlerCtx.GetAuthContext(provider.GetProviderString())
		if authErr != nil {
			fmt.Fprintln(outErrFile, fmt.Sprintf("Error setting up AUTH for provider '%s'", handlerCtx.RuntimeContext.ProviderStr))
		}
		if pErr == nil {
			provider.ShowAuth(authCtx)
		} else {
			fmt.Fprintln(outErrFile, fmt.Sprintf("Error setting up API for provider '%s'", handlerCtx.RuntimeContext.ProviderStr))
		}

		var readlineCfg *readline.Config

		if colorIsNull(handlerCtx.RuntimeContext) {
			readlineCfg = &readline.Config{
				Prompt:               getShellPRompt(authCtx, cd),
				InterruptPrompt:      "^C",
				EOFPrompt:            "exit",
				HistoryFile:          config.GetReadlineFilePath(handlerCtx.RuntimeContext),
				HistorySearchFold:    true,
				HistoryExternalWrite: true,
			}
		} else {
			readlineCfg = &readline.Config{
				Stderr:               outErrFile,
				Stdout:               outfile,
				Prompt:               getShellPRompt(authCtx, cd),
				InterruptPrompt:      "^C",
				EOFPrompt:            "exit",
				HistoryFile:          config.GetReadlineFilePath(handlerCtx.RuntimeContext),
				HistorySearchFold:    true,
				HistoryExternalWrite: true,
			}
		}

		l, err := readline.NewEx(readlineCfg)
		if err != nil {
			panic(err)
		}
		defer l.Close()

		fmt.Fprintln(
			outErrFile,
			getIntroAuthMsg(authCtx, provider),
		)

		for {
			l.SetPrompt(getShellPRompt(authCtx, cd))
			rawLine, err := l.Readline()
			if err == readline.ErrInterrupt {
				if len(rawLine) == 0 {
					break
				} else {
					continue
				}
			} else if err == io.EOF {
				break
			}

			line := strings.TrimSpace(rawLine)
			switch {
			case line == "help":
				usage(outErrFile)
			case line == "clear":
				readline.ClearScreen(l.Stdout())
			case line == "exit" || line == `\q` || line == "quit":
				goto exit
			case line == "":
			default:
				log.Debugln("you said:", strconv.Quote(line))
				inlineCommentIdx := strings.Index(line, "--")
				if inlineCommentIdx > -1 {
					line = line[:inlineCommentIdx]
				}
				semiColonIdx := strings.Index(line, ";")
				if semiColonIdx > -1 {
					line = strings.TrimSpace(line[:semiColonIdx+1])
					semiColonIdx := strings.Index(line, ";")
					sb.WriteString(" " + line[:semiColonIdx+1])
					queryToExecute := sb.String()
					handlerCtx.RawQuery = queryToExecute
					l.WriteToHistory(queryToExecute)
					RunCommand(&handlerCtx, outfile, outErrFile)
					sb.Reset()
					sb.WriteString(line[semiColonIdx+1:])
				} else {
					sb.WriteString(" " + line)
				}
			}
		}
	exit:
		if !colorIsNull(runtimeCtx) {
			cd.ResetColorScheme()
		}
		fmt.Fprintf(outfile, "")
		fmt.Fprintf(outErrFile, "")
		outfile, _ = writer.GetOutputWriter(writer.StdOutStr)
		outErrFile, _ = writer.GetOutputWriter(writer.StdErrStr)
		l.Config.Stdout = outfile
		l.Config.Stderr = outErrFile
	},
}
