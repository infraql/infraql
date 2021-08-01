package util

import (
	"path/filepath"
	"runtime"
)

func GetFilePathFromRepositoryRoot(relativePath string) (string, error) {
	_, filename, _, _ := runtime.Caller(0)
	curDir := filepath.Dir(filename)
	return filepath.Abs(filepath.Join(curDir, "../../..", relativePath))
}
