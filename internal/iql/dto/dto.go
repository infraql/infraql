package dto

import (
	"io"
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	AuthInteractiveStr        string = "interactive"
	AuthServiceAccountStr     string = "serviceaccount"
	DarkColorScheme           string = "dark"
	LightColorScheme          string = "light"
	NullColorScheme           string = "null"
	DefaultColorScheme        string = DarkColorScheme
	DefaultWindowsColorScheme string = NullColorScheme
	DryRunFlagKey             string = "dryrun"
	APIRequestTimeoutKey      string = "apirequesttimeout"
	CacheKeyCountKey          string = "cachekeycount"
	CacheTTLKey               string = "metadatattl"
	ColorSchemeKey            string = "colorscheme"
	ConfigFilePathKey         string = "configfile"
	CSVHeadersDisableKey      string = "hideheaders"
	DbEngineKey               string = "dbengine"
	DbGenerationIdKey         string = "dbgenerationid"
	DbFilePathKey             string = "dbfilepath"
	DbInitFilePathKey         string = "dbinitfilepath"
	DelimiterKey              string = "delimiter"
	ErrorPresentationKey      string = "errorpresentation"
	InfilePathKey             string = "infile"
	KeyFilePathKey            string = "keyfilepath"
	LogLevelStrKey            string = "loglevel"
	OutfilePathKey            string = "outfile"
	OutputFormatKey           string = "output"
	ProviderRootPathKey       string = "providerroot"
	ProviderRootPathModeKey   string = "providerrootfilemode"
	ProviderStrKey            string = "provider"
	QueryCacheSizeKey         string = "querycachesize"
	ReinitKey                 string = "reinit"
	TemplateCtxFilePathKey    string = "iqldata"
	TestWithoutApiCallsKey    string = "testwithoutapicalls"
	UseNonPreferredAPIsKEy    string = "usenonpreferredapis"
	VerboseFlagKey            string = "verbose"
	ViperCfgFileNameKey       string = "viperconfigfilename"
	WorkOfflineKey            string = "offline"
)

type KeyVal struct {
	K string
	V []byte
}

type BackendMessages struct {
	WorkingMessages []string
}

type AuthCtx struct {
	Scopes      []string
	Type        string
	ID          string
	KeyFilePath string
	Active      bool
}

type ExecPayload struct {
	Payload    []byte
	Header     map[string][]string
	PayloadMap map[string]interface{}
}

func GetAuthCtx(scopes []string, keyFilePath string) *AuthCtx {
	var authType string
	if keyFilePath == "" {
		authType = AuthInteractiveStr
	} else {
		authType = AuthServiceAccountStr
	}
	return &AuthCtx{
		Scopes:      scopes,
		Type:        authType,
		KeyFilePath: keyFilePath,
		Active:      false,
	}
}

type RuntimeCtx struct {
	APIRequestTimeout    int
	CacheKeyCount        int
	CacheTTL             int
	ColorScheme          string
	ConfigFilePath       string
	CSVHeadersDisable    bool
	DbEngine             string
	DbFilePath           string
	DbGenerationId       int
	DbInitFilePath       string
	Delimiter            string
	DryRunFlag           bool
	ErrorPresentation    string
	InfilePath           string
	KeyFilePath          string
	LogLevelStr          string
	OutfilePath          string
	OutputFormat         string
	ProviderRootPath     string
	ProviderRootPathMode uint32
	ProviderStr          string
	Reinit               bool
	QueryCacheSize       int
	TemplateCtxFilePath  string
	TestWithoutApiCalls  bool
	UseNonPreferredAPIs  bool
	VerboseFlag          bool
	ViperCfgFileName     string
	WorkOffline          bool
}

func setInt(iPtr *int, val string) error {
	i, err := strconv.Atoi(val)
	if err == nil {
		*iPtr = i
	}
	return err
}

func setUint32(uPtr *uint32, val string) error {
	ui, err := strconv.ParseUint(val, 10, 32)
	if err == nil {
		*uPtr = uint32(ui)
	}
	return err
}

func setBool(bPtr *bool, val string) error {
	b, err := strconv.ParseBool(val)
	if err == nil {
		*bPtr = b
	}
	return err
}

func (rc *RuntimeCtx) Set(key string, val string) error {
	var retVal error
	switch key {
	case APIRequestTimeoutKey:
		retVal = setInt(&rc.APIRequestTimeout, val)
	case CacheKeyCountKey:
		retVal = setInt(&rc.CacheKeyCount, val)
	case CacheTTLKey:
		retVal = setInt(&rc.CacheTTL, val)
	case ColorSchemeKey:
		rc.ColorScheme = val
	case ConfigFilePathKey:
		rc.ConfigFilePath = val
	case CSVHeadersDisableKey:
		retVal = setBool(&rc.CSVHeadersDisable, val)
	case DbEngineKey:
		rc.DbEngine = val
	case DbFilePathKey:
		rc.DbFilePath = val
	case DbGenerationIdKey:
		retVal = setInt(&rc.DbGenerationId, val)
	case DbInitFilePathKey:
		rc.DbInitFilePath = val
	case DelimiterKey:
		rc.Delimiter = val
	case DryRunFlagKey:
		retVal = setBool(&rc.DryRunFlag, val)
	case ErrorPresentationKey:
		rc.ErrorPresentation = val
	case InfilePathKey:
		rc.InfilePath = val
	case KeyFilePathKey:
		rc.KeyFilePath = val
	case LogLevelStrKey:
		rc.LogLevelStr = val
	case OutfilePathKey:
		rc.OutfilePath = val
	case OutputFormatKey:
		rc.OutputFormat = val
	case ProviderRootPathKey:
		rc.ProviderRootPath = val
	case ProviderRootPathModeKey:
		retVal = setUint32(&rc.ProviderRootPathMode, val)
	case QueryCacheSizeKey:
		retVal = setInt(&rc.QueryCacheSize, val)
	case ReinitKey:
		retVal = setBool(&rc.Reinit, val)
	case TemplateCtxFilePathKey:
		rc.TemplateCtxFilePath = val
	case TestWithoutApiCallsKey:
		retVal = setBool(&rc.TestWithoutApiCalls, val)
	case UseNonPreferredAPIsKEy:
		retVal = setBool(&rc.UseNonPreferredAPIs, val)
	case VerboseFlagKey:
		retVal = setBool(&rc.VerboseFlag, val)
	case ViperCfgFileNameKey:
		rc.ViperCfgFileName = val
	case WorkOfflineKey:
		retVal = setBool(&rc.WorkOffline, val)
	}
	return retVal
}

type RowsDTO struct {
	RowMap      map[string]map[string]interface{}
	ColumnOrder []string
	Err         error
	RowSort     func(map[string]map[string]interface{}) []string
}

type OutputContext struct {
	RuntimeContext RuntimeCtx
	Result         *sqltypes.Result
}

type PrepareResultSetDTO struct {
	OutputBody  map[string]interface{}
	Msg         *BackendMessages
	RowMap      map[string]map[string]interface{}
	ColumnOrder []string
	RowSort     func(map[string]map[string]interface{}) []string
	Err         error
}

func NewPrepareResultSetDTO(
	body map[string]interface{},
	rowMap map[string]map[string]interface{},
	columnOrder []string,
	rowSort func(map[string]map[string]interface{}) []string,
	err error,
	msg *BackendMessages,
) PrepareResultSetDTO {
	return PrepareResultSetDTO{
		OutputBody:  body,
		RowMap:      rowMap,
		ColumnOrder: columnOrder,
		RowSort:     rowSort,
		Err:         err,
		Msg:         msg,
	}
}

type ExecutorOutput struct {
	Result     *sqltypes.Result
	OutputBody map[string]interface{}
	Msg        *BackendMessages
	Err        error
}

func NewExecutorOutput(result *sqltypes.Result, body map[string]interface{}, msg *BackendMessages, err error) ExecutorOutput {
	return ExecutorOutput{
		Result:     result,
		OutputBody: body,
		Msg:        msg,
		Err:        err,
	}
}

type BasicPrimitiveContext struct {
	body              map[string]interface{}
	authCtx           *AuthCtx
	writer            io.Writer
	errWriter         io.Writer
	commentDirectives sqlparser.CommentDirectives
}

func NewBasicPrimitiveContext(body map[string]interface{}, authCtx *AuthCtx, writer io.Writer, errWriter io.Writer, commentDirectives sqlparser.CommentDirectives) *BasicPrimitiveContext {
	return &BasicPrimitiveContext{
		body:              body,
		authCtx:           authCtx,
		writer:            writer,
		errWriter:         errWriter,
		commentDirectives: commentDirectives,
	}
}

func (bpp *BasicPrimitiveContext) GetBody() map[string]interface{} {
	return bpp.body
}

func (bpp *BasicPrimitiveContext) GetAuthContext() *AuthCtx {
	return bpp.authCtx
}

func (bpp *BasicPrimitiveContext) GetWriter() io.Writer {
	return bpp.writer
}

func (bpp *BasicPrimitiveContext) GetErrWriter() io.Writer {
	return bpp.errWriter
}

func (bpp *BasicPrimitiveContext) GetCommentDirectives() sqlparser.CommentDirectives {
	return bpp.commentDirectives
}
