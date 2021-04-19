package config

import (
	"infraql/internal/iql/dto"
	"os"
	"path/filepath"
	"runtime"

	log "github.com/sirupsen/logrus"
)

const defaultConfigCacheDir = ".infraql"

const defaultNixConfigCacheDirFileMode uint32 = 0755

const defaultWindowsConfigCacheDirFileMode uint32 = 0777

const defaultConfigFileName = ".iqlrc"

const defaultKeyFileName = "sa-key.json"

const defaltLogLevel = "warn"

const defaltErrorPresentation = "stderr"

const googleProvider = "google"

const readlineDir = "readline"

const readlineTmpFile = "readline.tmp"

func GetGoogleProviderString() string {
	return googleProvider
}

func GetDefaultLogLevelString() string {
	return defaltLogLevel
}

func GetDefaultErrorPresentationString() string {
	return defaltErrorPresentation
}

func GetWorkingDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func GetDefaultProviderCacheRoot() string {
	return filepath.Join(GetWorkingDir(), defaultConfigCacheDir)
}

func GetDefaultConfigFilePath() string {
	return filepath.Join(GetWorkingDir(), defaultConfigFileName)
}

func GetDefaultColorScheme() string {
	if runtime.GOOS == "windows" {
		return dto.DefaultWindowsColorScheme
	}
	return dto.DefaultColorScheme
}

func GetReadlineDirPath(runtimeCtx dto.RuntimeCtx) string {
	return filepath.Join(runtimeCtx.ProviderRootPath, readlineDir)
}

func GetReadlineFilePath(runtimeCtx dto.RuntimeCtx) string {
	return filepath.Join(runtimeCtx.ProviderRootPath, readlineDir, readlineTmpFile)
}

func GetDefaultViperConfigFileName() string {
	return defaultConfigFileName
}

func GetDefaultKeyFilePath() string {
	return ""
}

func GetDefaultProviderCacheDirFileMode() uint32 {
	if runtime.GOOS == "windows" {
		return defaultWindowsConfigCacheDirFileMode
	}
	return defaultNixConfigCacheDirFileMode
}

func CreateDirIfNotExists(dirPath string, fileMode os.FileMode) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return os.Mkdir(dirPath, fileMode)
	}
	return nil
}
