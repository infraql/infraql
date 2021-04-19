package cache

import (
	"encoding/json"
	"fmt"
	"infraql/internal/iql/config"
	"infraql/internal/iql/constants"
	"infraql/internal/iql/dto"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	DefaultMarshallerKey       string = "default_marshaller"
	GoogleRootMarshallerKey    string = "google_root_marshaller"
	GoogleServiceMarshallerKey string = "google_service_marshaller"
)

type IKeyValCache interface {
	Len() int
	Get(string, IMarshaller) interface{}
	Put(string, interface{}, IMarshaller)
}

type Item struct {
	Value         interface{}     `json:"-"`
	RawValue      json.RawMessage `json:"raw_value"`
	LastAccess    int64
	Marshaller    IMarshaller `json:"-"`
	MarshallerKey string
}

type TTLMap struct {
	m               map[string]*Item
	l               sync.Mutex
	cacheName       string
	cacheFileSuffix string
	runtimeCtx      dto.RuntimeCtx
}

func (m *TTLMap) persistToFile() {

	for _, v := range m.m {
		err := v.Marshaller.Marshal(v)
		if err != nil {
			log.Infoln(fmt.Sprintf("persist to file Marshal error = %s", err.Error()))
		}
	}
	blob, jsonErr := json.Marshal(m.m)
	if jsonErr != nil {
		log.Infoln(fmt.Sprintf("persist to file final Marshal error = %s", jsonErr.Error()))
	}

	cacheDir := m.getCacheDir("ttl_cache")
	fullPath := filepath.Join(cacheDir, m.getCacheFileName())

	config.CreateDirIfNotExists(cacheDir, os.FileMode(m.runtimeCtx.ProviderRootPathMode))
	fileHandle, err := os.Create(fullPath)
	if err != nil {
		log.Fatalf("cannot open cache file %s", fullPath)
	}

	fileHandle.Write(blob)
}

func (m *TTLMap) getCacheDir(relativePath string) string {
	return filepath.Join(m.runtimeCtx.ProviderRootPath, relativePath)
}

func sanitisePath(p string) string {
	return p
}

func (m *TTLMap) getCacheFileName() string {
	if m.cacheFileSuffix == "" {
		return m.cacheName
	}
	return m.cacheName + "." + m.cacheFileSuffix
}

func (m *TTLMap) restoreFromFile() error {
	fullPath := filepath.Join(m.getCacheDir("ttl_cache"), m.getCacheFileName())
	bodyBytes, readErr := ioutil.ReadFile(fullPath)
	if readErr != nil {
		return fmt.Errorf(`cannot access TTL Cache file at: "%s"`, fullPath)
	}
	im := make(map[string]*Item)
	err := json.Unmarshal(bodyBytes, &im)
	if err != nil {
		return err
	} else {
		for _, val := range im {
			marshaller, e := GetMarshaller(val.MarshallerKey)
			if e != nil {
				return e
			}
			val.Marshaller = marshaller
			jsonErr := val.Marshaller.Unmarshal(val)
			if jsonErr != nil {
				return jsonErr
			}
		}
	}
	m.m = im
	return nil
}

func NewTTLMap(runtimeCtx dto.RuntimeCtx, cacheName string, initSize int, maxTTL int, marshaller IMarshaller) IKeyValCache {
	log.Infoln(fmt.Sprintf("cache op: created new cache"))
	m := &TTLMap{
		m:               make(map[string]*Item, initSize),
		cacheName:       cacheName,
		cacheFileSuffix: constants.JsonStr,
		runtimeCtx:      runtimeCtx,
	}
	restorErr := m.restoreFromFile()
	if restorErr != nil {
		log.Infoln(restorErr.Error())
	}
	go func() {
		for now := range time.Tick(time.Second) {
			m.l.Lock()
			for k, v := range m.m {
				if (maxTTL > 0) && ((now.Unix() - v.LastAccess) > int64(maxTTL)) {
					delete(m.m, k)
					log.Infoln(fmt.Sprintf("cache op: deleted %s", k))
				}
			}
			m.l.Unlock()
		}
	}()
	return m
}

func (m *TTLMap) Len() int {
	return len(m.m)
}

func (m *TTLMap) Put(k string, v interface{}, marshaller IMarshaller) {
	if v == nil {
		log.Infoln("attempting to add nil to cache")
		return
	}
	m.l.Lock()
	log.Debugln(fmt.Sprintf("TTLMap.Put() called for k = %v, v = %v", k, v))
	it, _ := m.m[k]
	it = &Item{
		Value:         v,
		Marshaller:    marshaller,
		MarshallerKey: marshaller.GetKey(),
	}
	m.m[k] = it
	log.Infoln(fmt.Sprintf("cache op: added %s", k))
	it.LastAccess = time.Now().Unix()
	log.Infoln(fmt.Sprintf("type of interface for Put = %T", v))
	m.persistToFile()
	m.l.Unlock()
}

func (m *TTLMap) Get(k string, marshaller IMarshaller) (v interface{}) {
	m.l.Lock()
	if it, ok := m.m[k]; ok {
		v = it.Value
		log.Infoln(fmt.Sprintf("cache op: succeeded in retrieving %s", k))
		it.LastAccess = time.Now().Unix()
	} else {
		log.Infoln(fmt.Sprintf("cache op: failed to retrieve %s", k))
	}
	m.l.Unlock()
	return
}
