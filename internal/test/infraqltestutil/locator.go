package infraqltestutil

import (
	"fmt"
	"path/filepath"
	"runtime"

	"infraql/internal/iql/dto"
)

func GetRuntimeCtx(providerStr string, outputFmtStr string) (*dto.RuntimeCtx, error) {
	saKeyPath, err := GetFilePathFromRepositoryRoot("test/assets/credentials/dummy/google/dummy-sa-key.json")
	if err != nil {
		return nil, fmt.Errorf("Test failed on %s: %v", saKeyPath, err)
	}
	providerDir, err := GetFilePathFromRepositoryRoot("test/.infraql")
	if err != nil {
		return nil, fmt.Errorf("Test failed: %v", err)
	}
	return &dto.RuntimeCtx{
		ProviderStr: providerStr,
		KeyFilePath: saKeyPath,
		ProviderRootPath: providerDir,
		OutputFormat: outputFmtStr,
	}, nil
}

func GetFilePathFromRepositoryRoot(relativePath string) (string, error) {
	_, filename, _, _ := runtime.Caller(0)
	curDir := filepath.Dir(filename)
	return filepath.Abs(filepath.Join(curDir, "../../..", relativePath))
}