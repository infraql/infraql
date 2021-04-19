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
	"infraql/internal/iql/config"
	"infraql/internal/iql/constants"
	"infraql/internal/iql/dto"
	"os"
	"path/filepath"

	"github.com/magiconair/properties"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	lrucache "vitess.io/vitess/go/cache"

	log "github.com/sirupsen/logrus"
)

var (
	BuildMajorVersion   string = ""
	BuildMinorVersion   string = ""
	BuildPatchVersion   string = ""
	BuildCommitSHA      string = ""
	BuildShortCommitSHA string = ""
	BuildDate           string = ""
	BuildPlatform       string = ""
)

var SemVersion string = fmt.Sprintf("%s.%s.%s", BuildMajorVersion, BuildMinorVersion, BuildPatchVersion)

var ( 
	runtimeCtx dto.RuntimeCtx 
	queryCache *lrucache.LRUCache
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "infraql",
	Version: SemVersion,
	Short:   "Cloud infrastructure coding using SQL",
	Long: `
	_ __  _ ___ ___  __   __  _    
	| |  \| | __| _ \/  \ /__\| |   
	| | | ' | _|| v / /\ | \/ | |_  
	|_|_|\__|_| |_|_\_||_|\_V_\___| 

Cloud infrastructure coding using SQL. For example:

SELECT * FROM google.compute.instances;`,
	Run: func(cmd *cobra.Command, args []string) {
		// in the root command is executed with no arguments, print the help message
		usagemsg := cmd.Long + "\n\n" + cmd.UsageString()
		fmt.Println(usagemsg)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.SetVersionTemplate("InfraQL v{{.Version}} " + BuildPlatform + " (" + BuildShortCommitSHA + ")\nBuildDate: " + BuildDate + "\nhttps://infraql.io\n")

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().IntVar(&runtimeCtx.APIRequestTimeout, dto.APIRequestTimeoutKey, 45, "API request timeout in seconds, 0 for no timeout.")
	rootCmd.PersistentFlags().StringVar(&runtimeCtx.ColorScheme, dto.ColorSchemeKey, config.GetDefaultColorScheme(), fmt.Sprintf("color scheme, must be one of {'%s', '%s', '%s'}; defaulted to: %s", dto.DarkColorScheme, dto.LightColorScheme, dto.NullColorScheme, config.GetDefaultColorScheme()))
	rootCmd.PersistentFlags().StringVar(&runtimeCtx.ConfigFilePath, dto.ConfigFilePathKey, config.GetDefaultConfigFilePath(), fmt.Sprintf("config file full path; defaulted into current dir as: %s", config.GetDefaultConfigFilePath()))
	rootCmd.PersistentFlags().StringVar(&runtimeCtx.ProviderRootPath, dto.ProviderRootPathKey, config.GetDefaultProviderCacheRoot(), fmt.Sprintf("config and cache root path; default is %s", config.GetDefaultProviderCacheRoot()))
	rootCmd.PersistentFlags().Uint32Var(&runtimeCtx.ProviderRootPathMode, dto.ProviderRootPathModeKey, config.GetDefaultProviderCacheDirFileMode(), fmt.Sprintf("config and cache root path; default is %d", config.GetDefaultProviderCacheDirFileMode()))
	rootCmd.PersistentFlags().StringVar(&runtimeCtx.ViperCfgFileName, dto.ViperCfgFileNameKey, config.GetDefaultViperConfigFileName(), fmt.Sprintf("config filename; default is %s", config.GetDefaultViperConfigFileName()))
	rootCmd.PersistentFlags().StringVar(&runtimeCtx.KeyFilePath, dto.KeyFilePathKey, config.GetDefaultKeyFilePath(), fmt.Sprintf("service account key filename; default is %s", config.GetDefaultKeyFilePath()))
	rootCmd.PersistentFlags().StringVar(&runtimeCtx.ProviderStr, dto.ProviderStrKey, config.GetGoogleProviderString(), fmt.Sprintf(`infra provder; default is "%s"`, config.GetGoogleProviderString()))
	rootCmd.PersistentFlags().BoolVar(&runtimeCtx.WorkOffline, dto.WorkOfflineKey, false, "Work offline, using cached data")
	rootCmd.PersistentFlags().BoolVarP(&runtimeCtx.VerboseFlag, dto.VerboseFlagKey, "v", false, "verbose flag")
	rootCmd.PersistentFlags().BoolVar(&runtimeCtx.DryRunFlag, dto.DryRunFlagKey, false, "dryrun flag; preprocessor only will run and output returned")
	rootCmd.PersistentFlags().BoolVarP(&runtimeCtx.CSVHeadersDisable, dto.CSVHeadersDisableKey, "H", false, "CSV headers disable flag")
	rootCmd.PersistentFlags().StringVarP(&runtimeCtx.OutputFormat, dto.OutputFormatKey, "o", "table", "output format, must be (json | table | csv), default table")
	rootCmd.PersistentFlags().StringVarP(&runtimeCtx.OutfilePath, dto.OutfilePathKey, "f", "stdout", "outfile into which results are written, default stdout")
	rootCmd.PersistentFlags().StringVarP(&runtimeCtx.InfilePath, dto.InfilePathKey, "i", "stdin", "input file from which queries are read, default stdin")
	rootCmd.PersistentFlags().StringVarP(&runtimeCtx.TemplateCtxFilePath, dto.TemplateCtxFilePathKey, "q", "", "context file for templating")
	rootCmd.PersistentFlags().IntVar(&runtimeCtx.QueryCacheSize, dto.QueryCacheSizeKey, constants.DefaultQueryCacheSize, "Size in number of entries of LRU cache for query plans.")
	rootCmd.PersistentFlags().StringVarP(&runtimeCtx.Delimiter, dto.DelimiterKey, "d", ",", "Delimiter for csv output. Single char only.  Ignored for all non-csv output.")
	rootCmd.PersistentFlags().IntVar(&runtimeCtx.CacheKeyCount, dto.CacheKeyCountKey, 100, "Cache initial key count.  Default 100.")
	rootCmd.PersistentFlags().IntVar(&runtimeCtx.CacheTTL, dto.CacheTTLKey, 3600, "TTL for cached metadata documents, in seconds.  Default 3600 (1hr).")
	rootCmd.PersistentFlags().BoolVar(&runtimeCtx.TestWithoutApiCalls, dto.TestWithoutApiCallsKey, false, "flag to omit api calls for testing, default false")
	rootCmd.PersistentFlags().BoolVar(&runtimeCtx.UseNonPreferredAPIs, dto.UseNonPreferredAPIsKEy, false, "flag enable non-poreferred APIs, default false")
	rootCmd.PersistentFlags().StringVar(&runtimeCtx.LogLevelStr, dto.LogLevelStrKey, config.GetDefaultLogLevelString(), fmt.Sprintf(`log level; default is "%s"`, config.GetDefaultLogLevelString()))
	rootCmd.PersistentFlags().StringVar(&runtimeCtx.ErrorPresentation, dto.ErrorPresentationKey, config.GetDefaultErrorPresentationString(), fmt.Sprintf(`error presetation; options are: {"stderr", "record"} default is "%s"`, config.GetDefaultErrorPresentationString()))

	rootCmd.PersistentFlags().MarkHidden(dto.TestWithoutApiCallsKey)
	rootCmd.PersistentFlags().MarkHidden(dto.ViperCfgFileNameKey)
	rootCmd.PersistentFlags().MarkHidden(dto.ErrorPresentationKey)

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	queryCache = lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize))

	rootCmd.AddCommand(execCmd)
	rootCmd.AddCommand(shellCmd)
	rootCmd.AddCommand(srvCmd)

}

func setLogLevel() {
	logLevel, err := log.ParseLevel(runtimeCtx.LogLevelStr)
	if err != nil {
		log.Fatal(err)
	}
	log.SetLevel(logLevel)
}

func mergeConfigFromFile(runtimeCtx *dto.RuntimeCtx, flagSet pflag.FlagSet) {
	props, err := properties.LoadFile(runtimeCtx.ConfigFilePath, properties.UTF8)
	if err == nil {
		propertiesMap := props.Map()
		for k, v := range propertiesMap {
			if flagSet.Lookup(k) != nil && !flagSet.Lookup(k).Changed {
				runtimeCtx.Set(k, v)
			}
		}
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {

	mergeConfigFromFile(&runtimeCtx, *rootCmd.PersistentFlags())

	setLogLevel()
	config.CreateDirIfNotExists(runtimeCtx.ProviderRootPath, os.FileMode(runtimeCtx.ProviderRootPathMode))
	config.CreateDirIfNotExists(filepath.Join(runtimeCtx.ProviderRootPath, runtimeCtx.ProviderStr), os.FileMode(runtimeCtx.ProviderRootPathMode))
	config.CreateDirIfNotExists(config.GetReadlineDirPath(runtimeCtx), os.FileMode(runtimeCtx.ProviderRootPathMode))
	viper.SetConfigFile(filepath.Join(runtimeCtx.ProviderRootPath, runtimeCtx.ViperCfgFileName))
	viper.AddConfigPath(runtimeCtx.ProviderRootPath)
	log.Infof("ProviderRootPath = %s, ViperCfgFileName = %s", runtimeCtx.ProviderRootPath, runtimeCtx.ViperCfgFileName)

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
