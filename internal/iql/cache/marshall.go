package cache

import (
	"encoding/json"
	"fmt"
	"infraql/internal/iql/metadata"
)

type IMarshaller interface {
	Unmarshal(item *Item) error
	Marshal(item *Item) error
	GetKey() string
}

type ServiceDiscoveryDocWrapper struct {
	Resources        map[string]metadata.Resource `json:"-"`
	RawResources     json.RawMessage              `json:"raw_resources"`
	Schemas          map[string]interface{}       `json:"schemas"`
	SchemaObjects    map[string]metadata.Schema   `json:"schema_objects"`
	RawSchemaObjects json.RawMessage              `json:"raw_schema_objects"`
}

func newServiceDiscoveryDocWrapper() *ServiceDiscoveryDocWrapper {
	return &ServiceDiscoveryDocWrapper{
		Resources:     make(map[string]metadata.Resource),
		Schemas:       make(map[string]interface{}),
		SchemaObjects: make(map[string]metadata.Schema),
	}
}

func GetMarshaller(key string) (IMarshaller, error) {
	switch key {
	case DefaultMarshallerKey:
		return &DefaultMarshaller{}, nil
	case GoogleRootMarshallerKey:
		return &GoogleRootDiscoveryMarshaller{}, nil
	case GoogleServiceMarshallerKey:
		return &GoogleServiceDiscoveryMarshaller{}, nil
	}
	return nil, fmt.Errorf("cannot find apt marshaller")
}

type DefaultMarshaller struct{}

func (dm *DefaultMarshaller) Unmarshal(item *Item) error {
	return json.Unmarshal(item.RawValue, &item.Value)
}

func (dm *DefaultMarshaller) Marshal(item *Item) error {
	return nil
}

func (dm *DefaultMarshaller) GetKey() string {
	return DefaultMarshallerKey
}

type GoogleRootDiscoveryMarshaller struct{}

func (dm *GoogleRootDiscoveryMarshaller) Unmarshal(item *Item) error {
	var err error
	var blob map[string]metadata.ServiceHandle
	err = json.Unmarshal(item.RawValue, &blob)
	if err != nil {
		return err
	}
	item.Value = blob
	return err
}

func (dm *GoogleRootDiscoveryMarshaller) Marshal(item *Item) error {
	var err error
	blob := make(map[string]metadata.ServiceHandle)
	value, ok := item.Value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("Cannot Marshal cache object of type: %T", item.Value)
	}
	for k, hn := range value {
		switch handle := hn.(type) {
		case metadata.ServiceHandle:
			blob[k] = handle
		default:
			return fmt.Errorf("handle type is %T", handle)
		}
	}
	item.RawValue, err = json.Marshal(blob)
	return err
}

func (dm *GoogleRootDiscoveryMarshaller) GetKey() string {
	return GoogleRootMarshallerKey
}

type GoogleServiceDiscoveryMarshaller struct{}

func (dm *GoogleServiceDiscoveryMarshaller) Unmarshal(item *Item) error {
	var err error
	wrapperBlob := newServiceDiscoveryDocWrapper()
	blob := make(map[string]interface{})
	err = json.Unmarshal(item.RawValue, wrapperBlob)
	if err != nil {
		return err
	}
	err = json.Unmarshal(wrapperBlob.RawResources, &wrapperBlob.Resources)
	if err != nil {
		return err
	}
	err = json.Unmarshal(wrapperBlob.RawSchemaObjects, &wrapperBlob.SchemaObjects)
	if err != nil {
		return err
	}
	sr := &metadata.SchemaRegistry{}
	for k, v := range wrapperBlob.SchemaObjects {
		v.Unmarshal()
		v.SchemaCentral = sr
		wrapperBlob.SchemaObjects[k] = v
		if err != nil {
			return err
		}
	}
	blob["resources"] = wrapperBlob.Resources
	blob["schemas"] = wrapperBlob.Schemas
	so := wrapperBlob.SchemaObjects
	blob["schemas_parsed"] = so
	for _, v := range blob["schemas_parsed"].(map[string]metadata.Schema) {
		v.UpdateSchemaRegistry(sr)
	}
	sr.SchemaRef = so
	item.Value = blob
	return err
}

func (dm *GoogleServiceDiscoveryMarshaller) Marshal(item *Item) error {
	var err error
	wrapperBlob := newServiceDiscoveryDocWrapper()
	resources := make(map[string]metadata.Resource)
	schemas := make(map[string]metadata.Schema)
	value, ok := item.Value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("Cannot Marshal cache object of type: %T", item.Value)
	}
	for tlk, tlv := range value {
		switch tlk {
		case "resources":
			switch tlvt := tlv.(type) {
			case map[string]metadata.Resource:
				for k, hn := range tlvt {
					resources[k] = hn
				}
			default:
				return fmt.Errorf("reseources cannot be marshaled, unexpected type = %T", tlvt)
			}
		case "schemas_parsed":
			switch tlvt := tlv.(type) {
			case map[string]metadata.Schema:
				for k, hn := range tlvt {
					schemas[k] = hn
				}
			default:
				return fmt.Errorf("schemas_parsed cannot be marshaled, unexpected type = %T", tlvt)
			}
		case "schemas":
			wrapperBlob.Schemas = tlv.(map[string]interface{})
		}
	}
	wrapperBlob.RawResources, err = json.Marshal(resources)
	if err != nil {
		return err
	}
	wrapperBlob.RawSchemaObjects, err = json.Marshal(schemas)
	if err != nil {
		return err
	}
	item.RawValue, err = json.Marshal(*wrapperBlob)
	return err
}

func (dm *GoogleServiceDiscoveryMarshaller) GetKey() string {
	return GoogleServiceMarshallerKey
}
