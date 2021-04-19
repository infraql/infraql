package iqlerror

import (
	"fmt"
	"os"
)

func GetStatementNotSupportedError(stmtName string) error {
	return fmt.Errorf("statement type = '%s' not yet supported", stmtName)
}

func PrintErrorAndExitOneIfNil(subject interface{}, msg string) {
	if subject == nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintln(msg))
		os.Exit(1)
	}
}

func PrintErrorAndExitOneIfError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Sprintln(err.Error()))
		os.Exit(1)
	}
}
