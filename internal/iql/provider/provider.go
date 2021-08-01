package provider

import (
	"fmt"
	"infraql/internal/iql/cache"
	"infraql/internal/iql/config"
	"infraql/internal/iql/constants"
	"infraql/internal/iql/discovery"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/googlediscovery"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/methodselect"
	"infraql/internal/iql/sqlengine"
	"net/http"
	"path/filepath"
)

const (
	ambiguousServiceErrorMessage string = "More than one service exists with this name, please use the id in the object name, or unset the --usenonpreferredapis flag"
	googleProviderName           string = "google"
	SchemaDelimiter              string = googlediscovery.SchemaDelimiter
)

var DummyAuth bool = false

type ProviderParam struct {
	Id     string
	Type   string
	Format string
}

func GetSupportedProviders(extended bool) map[string]map[string]interface{} {
	retVal := make(map[string]map[string]interface{})
	if extended {
		retVal[googleProviderName] = getGoogleMapExtended()
	} else {
		retVal[googleProviderName] = getGoogleMap()
	}
	return retVal
}

type IProvider interface {
	Auth(authCtx *dto.AuthCtx, authTypeRequested string, enforceRevokeFirst bool) (*http.Client, error)

	AuthRevoke(authCtx *dto.AuthCtx) error

	CheckServiceAccountFile(credentialFile string) error

	DescribeResource(serviceName string, resourceName string, runtimeCtx dto.RuntimeCtx, extended bool, full bool) (*metadata.Schema, []string, error)

	EnhanceMetadataFilter(string, func(iqlmodel.ITable) (iqlmodel.ITable, error), map[string]bool) (func(iqlmodel.ITable) (iqlmodel.ITable, error), error)

	GenerateHTTPRestInstruction(httpContext httpexec.IHttpContext) (httpexec.IHttpContext, error)

	GetCurrentService() string

	GetDefaultKeyForSelectItems() string

	GetDefaultKeyForDeleteItems() string

	GetLikeableColumns(string) []string

	GetMethodForAction(serviceName string, resourceName string, iqlAction string, runtimeCtx dto.RuntimeCtx) (*metadata.Method, string, error)

	GetMethodSelector() methodselect.IMethodSelector

	GetProviderServices() (map[string]metadata.Service, error)

	GetProviderString() string

	GetProviderServicesRedacted(runtimeCtx dto.RuntimeCtx, extended bool) (map[string]metadata.Service, []string, error)

	GetResource(serviceKey string, resourceKey string, runtimeCtx dto.RuntimeCtx) (*metadata.Resource, error)

	GetResourcesMap(serviceKey string, runtimeCtx dto.RuntimeCtx) (map[string]metadata.Resource, error)

	GetResourcesRedacted(currentService string, runtimeCtx dto.RuntimeCtx, extended bool) (map[string]metadata.Resource, []string, error)

	GetServiceHandle(serviceKey string, runtimeCtx dto.RuntimeCtx) (*metadata.ServiceHandle, error)

	GetServiceHandlesMap(runtimeCtx dto.RuntimeCtx) (map[string]metadata.ServiceHandle, error)

	GetObjectSchema(serviceName string, resourceName string, schemaName string) (*metadata.Schema, error)

	GetSchemaMap(serviceName string, resourceName string) (map[string]metadata.Schema, error)

	GetVersion() string

	InferDescribeMethod(*metadata.Resource) (*metadata.Method, string, error)

	Parameterise(httpContext httpexec.IHttpContext, parameters *metadata.HttpParameters, requestSchema *metadata.Schema) (httpexec.IHttpContext, error)

	SetCurrentService(serviceKey string)

	ShowAuth(authCtx *dto.AuthCtx) (*metadata.AuthMetadata, error)

	GetDiscoveryGeneration(sqlengine.SQLEngine) (int, error)
}

func getProviderCacheDir(runtimeCtx dto.RuntimeCtx, providerName string) string {
	return filepath.Join(runtimeCtx.ProviderRootPath, providerName)
}

func getGoogleProviderCacheDir(runtimeCtx dto.RuntimeCtx) string {
	return getProviderCacheDir(runtimeCtx, googleProviderName)
}

func GetProviderFromRuntimeCtx(runtimeCtx dto.RuntimeCtx, dbEngine sqlengine.SQLEngine) (IProvider, error) {
	providerStr := runtimeCtx.ProviderStr // TODO: support multiple providers
	switch providerStr {
	case config.GetGoogleProviderString():
		return NewGoogleProvider(runtimeCtx, providerStr, dbEngine)
	}
	return nil, fmt.Errorf("provider %s not supported", providerStr)
}

func NewGoogleProvider(rtCtx dto.RuntimeCtx, providerStr string, dbEngine sqlengine.SQLEngine) (IProvider, error) {
	ttl := rtCtx.CacheTTL
	if rtCtx.WorkOffline {
		ttl = -1
	}
	methSel, err := methodselect.NewMethodSelector(googleProviderName, constants.GoogleV1String)
	if err != nil {
		return nil, err
	}

	gp := &GoogleProvider{
		runtimeCtx: rtCtx,
		discoveryAdapter: discovery.NewBasicDiscoveryAdapter(
			rtCtx.ProviderStr, // TODO: allow multiple
			constants.GoogleV1DiscoveryDoc,
			discovery.NewTTLDiscoveryStore(
				dbEngine,
				rtCtx, constants.GoogleV1ProviderCacheName,
				rtCtx.CacheKeyCount, ttl, &cache.GoogleRootDiscoveryMarshaller{},
				dbEngine, rtCtx.ProviderStr, // TODO: allow multiple
			),
			getGoogleProviderCacheDir(rtCtx),
			&rtCtx,
			googlediscovery.GoogleRootDiscoveryDocParser,
			googlediscovery.GoogleServiceDiscoveryDocParser,
			&cache.GoogleRootDiscoveryMarshaller{},
			&cache.GoogleServiceDiscoveryMarshaller{},
		),
		apiVersion:     constants.GoogleV1String,
		methodSelector: methSel,
	}
	return gp, err
}
