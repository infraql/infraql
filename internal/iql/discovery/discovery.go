package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"infraql/internal/iql/cache"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/googlediscovery"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/netutils"
	"infraql/internal/iql/sqlengine"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

const (
	ambiguousServiceErrorMessage string = "More than one service exists with this name, please use the id in the object name, or unset the --usenonpreferredapis flag"
)

type IDiscoveryStore interface {
	ProcessDiscoveryDoc(string, string, dto.RuntimeCtx, string, func([]byte, sqlengine.SQLEngine, string) (map[string]interface{}, error), cache.IMarshaller) (map[string]interface{}, error)
}

type TTLDiscoveryStore struct {
	ttlCache  cache.IKeyValCache
	sqlengine sqlengine.SQLEngine
}

type IDiscoveryAdapter interface {
	GetResourcesMap(serviceKey string) (map[string]metadata.Resource, error)
	GetSchemaMap(serviceName string, resourceName string) (map[string]metadata.Schema, error)
	GetServiceHandle(serviceKey string) (*metadata.ServiceHandle, error)
	GetServiceHandlesMap() (map[string]metadata.ServiceHandle, error)
}

type BasicDiscoveryAdapter struct {
	alias              string
	apiDiscoveryDocUrl string
	discoveryStore     IDiscoveryStore
	cacheDir           string
	runtimeCtx         *dto.RuntimeCtx
	rootDocParser      func(bytes []byte, dbEngine sqlengine.SQLEngine, alias string) (map[string]interface{}, error)
	serviceDocParser   func(bytes []byte, dbEngine sqlengine.SQLEngine, alias string) (map[string]interface{}, error)
	rootMarshaller     cache.IMarshaller
	serviceMarshaller  cache.IMarshaller
}

func NewBasicDiscoveryAdapter(
	alias string,
	apiDiscoveryDocUrl string,
	discoveryStore IDiscoveryStore,
	cacheDir string,
	runtimeCtx *dto.RuntimeCtx,
	rootDocParser func(bytes []byte, dbEngine sqlengine.SQLEngine, alias string) (map[string]interface{}, error),
	serviceDocParser func(bytes []byte, dbEngine sqlengine.SQLEngine, alias string) (map[string]interface{}, error),
	rootMarshaller cache.IMarshaller,
	serviceMarshaller cache.IMarshaller,
) IDiscoveryAdapter {
	return &BasicDiscoveryAdapter{
		alias:              alias,
		apiDiscoveryDocUrl: apiDiscoveryDocUrl,
		discoveryStore:     discoveryStore,
		cacheDir:           cacheDir,
		runtimeCtx:         runtimeCtx,
		rootDocParser:      rootDocParser,
		serviceDocParser:   serviceDocParser,
		rootMarshaller:     rootMarshaller,
		serviceMarshaller:  serviceMarshaller,
	}
}

func (adp *BasicDiscoveryAdapter) getServiceDiscoveryDoc(serviceKey string, runtimeCtx dto.RuntimeCtx) (map[string]interface{}, error) {
	component, err := adp.GetServiceHandle(serviceKey)
	if component == nil || err != nil {
		return nil, err
	}
	return adp.discoveryStore.ProcessDiscoveryDoc(component.Service.DiscoveryDoc, adp.cacheDir, runtimeCtx, fmt.Sprintf("%s.%s", adp.alias, serviceKey), adp.serviceDocParser, adp.serviceMarshaller)
}

func (adp *BasicDiscoveryAdapter) GetServiceHandlesMap() (map[string]metadata.ServiceHandle, error) {
	disDoc, err := adp.discoveryStore.ProcessDiscoveryDoc(adp.apiDiscoveryDocUrl, adp.cacheDir, *adp.runtimeCtx, adp.alias, adp.rootDocParser, adp.rootMarshaller)
	if err != nil {
		return nil, err
	}
	retVal := make(map[string]metadata.ServiceHandle)
	for k, service := range disDoc {
		handle, ok := service.(metadata.ServiceHandle)
		if !ok {
			return nil, fmt.Errorf("Service Handles corrupted, got unexpected type '%T'", service)
		}
		retVal[k] = handle
	}
	return retVal, err
}

func (adp *BasicDiscoveryAdapter) GetSchemaMap(serviceName string, resourceName string) (map[string]metadata.Schema, error) {
	svcDiscDocMap, err := adp.getServiceDiscoveryDoc(serviceName, *adp.runtimeCtx)
	cannotGetSchemaErr := fmt.Errorf("Cannot obtain object schema map for service = '%s', resource = '%s'", serviceName, resourceName)
	if err != nil {
		return nil, err
	}
	switch sch := svcDiscDocMap["schemas_parsed"].(type) {
	case map[string]metadata.Schema:
		return sch, nil
	default:
		return nil, cannotGetSchemaErr
	}
	return nil, cannotGetSchemaErr
}

func (adp *BasicDiscoveryAdapter) GetServiceHandle(serviceKey string) (*metadata.ServiceHandle, error) {
	serviceIdString := googlediscovery.TranslateServiceKeyIqlToGoogle(serviceKey)
	var foundById, foundByName metadata.ServiceHandle
	var foundByIdCount, foundByNameCount int = 0, 0
	handles, err := adp.GetServiceHandlesMap()
	if err != nil {
		return nil, err
	}
	log.Debugln(fmt.Sprintf("handles = %v", handles))
	for _, handle := range handles {
		svcMap := handle.Service
		if svcMap.ID == serviceIdString {
			foundByIdCount += 1
			foundById = handle
		}
		if adp.runtimeCtx.UseNonPreferredAPIs || (svcMap.Preferred) {
			if svcMap.Name == serviceKey {
				foundByNameCount += 1
				foundByName = handle
			}
		}
	}
	if foundByNameCount == 1 && (!adp.runtimeCtx.UseNonPreferredAPIs || foundByIdCount < 2) {
		return &foundByName, nil
	} else if foundByIdCount == 1 {
		return &foundById, nil
	}
	if foundByNameCount > 1 {
		err = errors.New(ambiguousServiceErrorMessage)
	}
	return nil, fmt.Errorf("Could not find Service: '%s' from Provider: '%s'", serviceKey, "google")
}

func (adp *BasicDiscoveryAdapter) GetResourcesMap(serviceKey string) (map[string]metadata.Resource, error) {
	component, err := adp.GetServiceHandle(serviceKey)
	if component == nil || err != nil {
		return nil, err
	}
	disDoc, err := adp.discoveryStore.ProcessDiscoveryDoc(component.Service.DiscoveryDoc, adp.cacheDir, *adp.runtimeCtx, fmt.Sprintf("%s.%s", adp.alias, serviceKey), adp.serviceDocParser, adp.serviceMarshaller)
	if err != nil {
		return nil, err
	}
	return disDoc["resources"].(map[string]metadata.Resource), err
}

func NewTTLDiscoveryStore(dbEngine sqlengine.SQLEngine, runtimeCtx dto.RuntimeCtx, cacheName string, size int, ttl int, marshaller cache.IMarshaller, sqlengine sqlengine.SQLEngine, alias string) IDiscoveryStore {
	return &TTLDiscoveryStore{
		ttlCache:  cache.NewTTLMap(dbEngine, runtimeCtx, cacheName, size, ttl, marshaller),
		sqlengine: sqlengine,
	}
}

func DownloadDiscoveryDoc(url string, runtimeCtx dto.RuntimeCtx) (io.ReadCloser, error) {
	httpClient := netutils.GetHttpClient(runtimeCtx, nil)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, getErr := httpClient.Do(req)
	if getErr != nil {
		return nil, err
	}
	if res.StatusCode >= 400 {
		return nil, fmt.Errorf("discovery doc download for '%s' failed with code = %d", url, res.StatusCode)
	}
	return res.Body, nil
}

func defaultParser(bytes []byte, dbEngine sqlengine.SQLEngine, alias string) (map[string]interface{}, error) {
	var result map[string]interface{}
	jsonErr := json.Unmarshal(bytes, &result)
	return result, jsonErr
}

func parseDiscoveryDoc(bodyBytes []byte, dbEngine sqlengine.SQLEngine, alias string, parser func([]byte, sqlengine.SQLEngine, string) (map[string]interface{}, error)) (map[string]interface{}, error) {
	result, jsonErr := parser(bodyBytes, dbEngine, alias)
	if jsonErr != nil {
		return nil, jsonErr
	}
	return result, nil
}

func processDiscoveryDoc(url string, cacheDir string, fileMode os.FileMode, runtimeCtx dto.RuntimeCtx, dbEngine sqlengine.SQLEngine, alias string, parser func([]byte, sqlengine.SQLEngine, string) (map[string]interface{}, error)) (map[string]interface{}, error) {
	body, err := DownloadDiscoveryDoc(url, runtimeCtx)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("error downloading discovery document.  Hint: check network settings, proxy config.")
	}
	defer body.Close()
	bodyBytes, readErr := ioutil.ReadAll(body)
	if readErr != nil {
		return nil, readErr
	}

	// TODO: convert to metadata
	return parseDiscoveryDoc(bodyBytes, dbEngine, alias, parser)
}

func (store *TTLDiscoveryStore) ProcessDiscoveryDoc(url string, cacheDir string, runtimeCtx dto.RuntimeCtx, alias string, parser func([]byte, sqlengine.SQLEngine, string) (map[string]interface{}, error), marshaller cache.IMarshaller) (map[string]interface{}, error) {
	if parser == nil {
		parser = defaultParser
	}
	fileMode := os.FileMode(runtimeCtx.ProviderRootPathMode)
	val := store.ttlCache.Get(url, marshaller)
	retVal := make(map[string]interface{})
	var err error
	switch rv := val.(type) {
	case map[string]metadata.ServiceHandle:
		if len(rv) > 0 {
			for k, v := range rv {
				retVal[k] = v
			}
			return retVal, nil
		}
	case map[string]metadata.Resource:
		if len(rv) > 0 {
			for k, v := range rv {
				retVal[k] = v
			}
			return retVal, nil
		}
	case map[string]interface{}:
		log.Infoln("retrieving discovery doc from cache")
		return rv, nil

	default:
		log.Infoln(fmt.Sprintf("coud not retrieve discovery doc from cache, type = %T", val))
	}
	if runtimeCtx.WorkOffline {
		retVal, err = processDiscoveryDocFromLocal(url, cacheDir, store.sqlengine, alias, parser)
		if retVal != nil && err == nil {
			log.Infoln("placing discovery doc into cache")
			store.ttlCache.Put(url, retVal, marshaller)
		} else if err != nil {
			log.Infoln(err.Error())
			err = errors.New("Provider information is not available in offline mode, run the command once without the --offline flag, then try again in offline mode")
		}
		return retVal, err
	}
	retVal, err = processDiscoveryDoc(url, cacheDir, fileMode, runtimeCtx, store.sqlengine, alias, parser)
	if err != nil {
		return nil, err
	}
	log.Infoln("placing discovery doc into cache")
	store.ttlCache.Put(url, retVal, marshaller)
	db, err := store.sqlengine.GetDB()
	if err != nil {
		return nil, err
	}
	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}
	err = txn.Commit()
	if err != nil {
		return nil, err
	}
	return retVal, err
}

func processDiscoveryDocFromLocal(url string, cacheDir string, dbEngine sqlengine.SQLEngine, alias string, parser func([]byte, sqlengine.SQLEngine, string) (map[string]interface{}, error)) (map[string]interface{}, error) {
	_, fileName := path.Split(url)
	fullPath := filepath.Join(cacheDir, fileName)
	bodyBytes, readErr := ioutil.ReadFile(fullPath)
	if readErr != nil {
		log.Infoln(fmt.Sprintf(`cannot process discovery doc with url = "%s", cacheDir = "%s", fullPath = "%s"`, url, cacheDir, fullPath))
		return nil, readErr
	}
	return parseDiscoveryDoc(bodyBytes, dbEngine, alias, parser)
}
