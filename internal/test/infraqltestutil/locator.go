package infraqltestutil

import (
	"fmt"
	"io/ioutil"

	"infraql/internal/iql/dto"
	"infraql/internal/iql/entryutil"
	"infraql/internal/iql/sqlengine"
	"infraql/internal/iql/util"
)

func GetRuntimeCtx(providerStr string, outputFmtStr string) (*dto.RuntimeCtx, error) {
	saKeyPath, err := util.GetFilePathFromRepositoryRoot("test/assets/credentials/dummy/google/dummy-sa-key.json")
	if err != nil {
		return nil, fmt.Errorf("test failed on %s: %v", saKeyPath, err)
	}
	providerDir, err := util.GetFilePathFromRepositoryRoot("test/.infraql")
	if err != nil {
		return nil, fmt.Errorf("test failed: %v", err)
	}
	dbInitFilePath, err := util.GetFilePathFromRepositoryRoot("test/db/setup.sql")
	if err != nil {
		return nil, fmt.Errorf("test failed on %s: %v", dbInitFilePath, err)
	}
	return &dto.RuntimeCtx{
		Delimiter:        ",",
		ProviderStr:      providerStr,
		LogLevelStr:      "warn",
		KeyFilePath:      saKeyPath,
		ProviderRootPath: providerDir,
		OutputFormat:     outputFmtStr,
		DbFilePath:       ":memory:",
		DbInitFilePath:   dbInitFilePath,
	}, nil
}

func getBytesFromLocalPath(path string) ([]byte, error) {
	fullPath, err := util.GetFilePathFromRepositoryRoot(path)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(fullPath)
}

func BuildSQLEngine(runtimeCtx dto.RuntimeCtx) (sqlengine.SQLEngine, error) {
	sqlEng, err := entryutil.BuildSQLEngine(runtimeCtx)
	if err != nil {
		return nil, err
	}
	googleRootDiscoveryBytes, err := getBytesFromLocalPath("test/db/google._root_.json")
	if err != nil {
		return nil, err
	}
	googleComputeDiscoveryBytes, err := getBytesFromLocalPath("test/db/google.compute.json")
	if err != nil {
		return nil, err
	}
	googleContainerDiscoveryBytes, err := getBytesFromLocalPath("test/db/google.container.json")
	if err != nil {
		return nil, err
	}
	sqlEng.Exec(`INSERT INTO "__iql__.cache.key_val"(k, v) VALUES(?, ?)`, "https://www.googleapis.com/discovery/v1/apis", googleRootDiscoveryBytes)
	if err != nil {
		return nil, err
	}
	sqlEng.Exec(`INSERT INTO "__iql__.cache.key_val"(k, v) VALUES(?, ?)`, "https://www.googleapis.com/discovery/v1/apis/compute/v1/rest", googleComputeDiscoveryBytes)
	if err != nil {
		return nil, err
	}
	sqlEng.Exec(`INSERT INTO "__iql__.cache.key_val"(k, v) VALUES(?, ?)`, "https://container.googleapis.com/$discovery/rest?version=v1", googleContainerDiscoveryBytes)
	if err != nil {
		return nil, err
	}
	return sqlEng, nil
}
