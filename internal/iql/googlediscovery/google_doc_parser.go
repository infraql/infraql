package googlediscovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"infraql/internal/iql/drm"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/sqlengine"
	"infraql/internal/iql/util"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	SchemaDelimiter            string = "."
	googleServiceKeyDelimiter  string = ":"
	infraqlServiceKeyDelimiter string = "__"
)

var (
	drmConfig drm.DRMConfig = drm.GetGoogleV1SQLiteConfig()
)

func TranslateServiceKeyGoogleToIql(serviceKey string) string {
	return strings.Replace(serviceKey, googleServiceKeyDelimiter, infraqlServiceKeyDelimiter, -1)
}

func TranslateServiceKeyIqlToGoogle(serviceKey string) string {
	return strings.Replace(serviceKey, infraqlServiceKeyDelimiter, googleServiceKeyDelimiter, -1)
}

func GoogleServiceDiscoveryDocParser(bytes []byte, dbEngine sqlengine.SQLEngine, prefix string) (map[string]interface{}, error) {
	fields := strings.Split(prefix, ".")
	if len(fields) != 2 {
		return nil, fmt.Errorf("improper resource prefix '%s'", prefix)
	}
	provStr := fields[0]
	svcStr := fields[1]
	var result map[string]interface{}
	jsonErr := json.Unmarshal(bytes, &result)
	discoveryGenerationId, err := dbEngine.GetCurrentDiscoveryGenerationId(prefix)
	if err != nil {
		discoveryGenerationId, err = dbEngine.GetNextDiscoveryGenerationId(prefix)
		if err != nil {
			return nil, err
		}
	}
	serviceName := result["id"].(string)
	serviceId := TranslateServiceKeyGoogleToIql(serviceName)
	baseUrl := result["baseUrl"].(string)
	resources, _ := findGoogleResourcesMaps("", result)
	keys := make(map[string]metadata.Resource)
	for k, v := range resources {
		item := v.(map[string]interface{})
		methodObj, err := extractSchemaForDescriptionGoogle(item)
		// methodObj
		if err == nil {
			var rsc metadata.Resource
			rscMap := extractResourceDescriptionGoogle(result, k, serviceId, methodObj, "")
			mm, mOk := item["methods"]
			if mOk {
				switch mmt := mm.(type) {
				case map[string]interface{}:
					for k, v := range mmt {
						switch vt := v.(type) {
						case map[string]interface{}:
							vt["name"] = k
							vt["__protocol__"] = "http"
							mmt[k] = vt
						}
					}
				}
				rscMap["methods"] = mm
			}
			bytes, marshalErr := json.Marshal(rscMap)
			if marshalErr != nil {
				return nil, marshalErr
			}
			unmarshalErr := json.Unmarshal(bytes, &rsc)
			if unmarshalErr != nil {
				return nil, unmarshalErr
			}
			rsc.BaseUrl = baseUrl
			keys[k] = rsc
		}
	}
	schemas := make(map[string]metadata.Schema)
	sReg := metadata.SchemaRegistry{SchemaRef: schemas}
	s := result["schemas"]
	if s != nil {
		sMap := s.(map[string]interface{})
		for k, _ := range sMap {
			schemaDeepMap := getSchema(result, serviceName, k, "")
			so, parseErr := parseSchema(schemaDeepMap, &sReg)
			if parseErr != nil {
				return nil, parseErr
			}
			so.ID = k
			schemas[k] = *so
		}
	}
	var tabluationsAnnotated []util.AnnotatedTabulation
	for _, v := range schemas {
		if v.IsArrayRef() {
			continue
		}
		// tableName := fmt.Sprintf("%s.%s", prefix, k)
		switch v.Type {
		case "object":
			tabulation := v.Tabulate(false)
			annTab := util.NewAnnotatedTabulation(tabulation, dto.NewHeirarchyIdentifiers(provStr, svcStr, tabulation.GetName(), ""))
			tabluationsAnnotated = append(tabluationsAnnotated, annTab)
			// create table
		case "array":
			itemsSchema, _ := v.GetItemsSchema()
			if len(itemsSchema.Properties) > 0 {
				// create "inline" table
				tabulation := v.Tabulate(false)
				annTab := util.NewAnnotatedTabulation(tabulation, dto.NewHeirarchyIdentifiers(provStr, svcStr, tabulation.GetName(), ""))
				tabluationsAnnotated = append(tabluationsAnnotated, annTab)
			}
		}
	}
	db, err := dbEngine.GetDB()
	if err != nil {
		return nil, err
	}
	txn, err := db.Begin()
	if err != nil {
		return nil, err
	}
	for _, tblt := range tabluationsAnnotated {
		// log.Infoln(fmt.Sprintf("tabulation %d = %s", i, tblt.GetName()))
		ddl := drmConfig.GenerateDDL(tblt, discoveryGenerationId)
		for _, q := range ddl {
			// log.Infoln(q)
			_, err = db.Exec(q)
			if err != nil {
				errStr := fmt.Sprintf("aborting DDL run on query = %s, err = %v", q, err)
				log.Infoln(errStr)
				txn.Rollback()
				return nil, err
			}
		}
	}
	err = txn.Commit()
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"resources":                keys,
		"schemas":                  result["schemas"],
		"schemas_parsed":           schemas,
		"tablespace":               prefix,
		"tablespace_generation_id": discoveryGenerationId,
	}, jsonErr
}

func parseSchema(schemaDeepMap map[string]interface{}, sReg *metadata.SchemaRegistry) (*metadata.Schema, error) {
	properties, propertiesOk := schemaDeepMap["properties"].(map[string]interface{})
	bytes, marshalErr := json.Marshal(schemaDeepMap)
	if marshalErr != nil {
		return nil, marshalErr
	}
	so := metadata.Schema{}
	err := json.Unmarshal(bytes, &so)
	if err != nil {
		return nil, err
	}
	so.SchemaCentral = sReg
	so.Required = getRequiredIfPresent(schemaDeepMap)
	if so.ItemsRawValue != nil {
		bytes, marshalErr := json.Marshal(so.ItemsRawValue)
		if marshalErr != nil {
			return nil, marshalErr
		}
		itemsMap := make(map[string]interface{})
		unmarshalErr := json.Unmarshal(bytes, &itemsMap)
		if unmarshalErr != nil {
			return nil, unmarshalErr
		}
		ref, isRef := itemsMap["$ref"]
		if isRef {
			so.Items = metadata.SchemaHandle{
				NamedRef: ref.(string),
			}
		} else {
			itemsObj, parseErr := parseSchema(itemsMap, sReg)
			if parseErr != nil {
				return nil, parseErr
			}
			so.Items = metadata.SchemaHandle{
				NamedRef: "",
				SchemaRef: map[string]metadata.Schema{
					"items": *itemsObj,
				},
			}
		}
	}
	if propertiesOk {
		prMap := make(map[string]metadata.SchemaHandle)
		//
		for k, property := range properties {
			prop := property.(map[string]interface{})
			ref, isRef := prop["$ref"]
			if isRef {
				prMap[k] = metadata.SchemaHandle{
					NamedRef: ref.(string),
				}
				// colDes := metadata.ColumnDescriptor{Name: k, Schema: }
			} else {
				sObj, err := parseSchema(prop, sReg)
				if err == nil {
					prMap[k] = metadata.SchemaHandle{
						NamedRef: "",
						SchemaRef: map[string]metadata.Schema{
							k: *sObj,
						},
					}
				}
			}
		}
		so.Properties = prMap
	}

	return &so, err
}

func GoogleRootDiscoveryDocParser(bytes []byte, dbEngine sqlengine.SQLEngine, prefix string) (map[string]interface{}, error) {
	var result struct {
		Items []metadata.Service `json:"items"`
	}
	jsonErr := json.Unmarshal(bytes, &result)
	if jsonErr != nil {
		return nil, jsonErr
	}
	retVal := make(map[string]interface{}, len(result.Items))
	for _, item := range result.Items {
		retVal[item.ID] = metadata.ServiceHandle{
			Service: item,
		}
	}
	return retVal, jsonErr
}

func extractResourceDescriptionGoogle(docMap map[string]interface{}, name string, serviceId string, method metadata.Method, prefix string) map[string]interface{} {
	schemaName := method.GetResponseType()
	if schemaName == "" {
		schemaName = method.GetRequestType()
		if schemaName == "" {
			return map[string]interface{}{
				"name":        name,
				"id":          serviceId + "." + name,
				"title":       name,
				"description": "",
			}
		}
	}
	os := getSchema(docMap, serviceId, schemaName, prefix)
	// deep copy to prevent schema mutations
	objSchema := make(map[string]interface{})
	for k, v := range os {
		objSchema[k] = v
	}
	properties, ok := objSchema["properties"]
	if ok {
		ps, ok := properties.(map[string]interface{})
		if !ok {
			goto outland
		}
		for k, v := range ps {
			switch v.(type) {
			case string:
				objSchema[k] = v.(string)
			case map[string]interface{}:
				d, ok := v.(map[string]interface{})[k]
				if ok {
					ds, ok := d.(string)
					if ok {
						objSchema[k] = ds
					}
				}
			}
		}
	}
outland:
	var description, title string
	desc, ok := objSchema["description"]
	if ok && desc != nil {
		switch desc.(type) {
		case string:
			description = desc.(string)
		}
	}
	t, ok := objSchema["title"]
	if ok && t != nil {
		title = t.(string)
	}
	rv := map[string]interface{}{
		"name":        name,
		"id":          serviceId + "." + name,
		"title":       title,
		"description": description,
	}
	return rv
}

func getObjectSchema(svcDiscDocMap map[string]interface{}, val map[string]interface{}, serviceName string, prefix string) map[string]interface{} {
	if title := extractTitle(val); title != "" {
		val["title"] = title
	}
	if _, ok := val["properties"]; ok {
		properties := val["properties"].(map[string]interface{})
		for k, v := range properties {
			if _, ok := v.(map[string]interface{})["$ref"]; ok {
				s := v.(map[string]interface{})
				s["id"] = k
				properties[k] = s
			}
		}
		val["properties"] = properties
	}
	return val
}

func copyMap(inMap map[string]interface{}) map[string]interface{} {
	retVal := make(map[string]interface{})
	for k, v := range inMap {
		retVal[k] = v
	}
	return retVal
}

func mergeRefMaps(refMap *map[string]interface{}, mergeMap map[string]interface{}) {
	for k, v := range mergeMap {
		if k != "$ref" {
			(*refMap)[k] = v
		}
	}
}

func prefixMerge(prefix string, suffix string, delim string) string {
	if prefix != "" {
		return prefix + delim + suffix
	}
	return suffix
}

func getArraySchema(svcDiscDocMap map[string]interface{}, val map[string]interface{}, name string, serviceName string, prefix string) map[string]interface{} {
	if title := extractTitle(val); title != "" {
		val["title"] = title
	}
	if _, ok := val["items"].(map[string]interface{})["$ref"]; ok {
		prefix = prefix + "[]"
		rv := val["items"].(map[string]interface{})
		rv["path"] = prefix
		val["items"] = rv
	}
	return val
}

func extractTitle(schema map[string]interface{}) string {
	title, ok := schema["id"]
	if ok {
		titleString, sOk := title.(string)
		if sOk {
			return titleString
		}
	}
	return ""
}

func isOutputOnly(obj map[string]interface{}) bool {
	desc, ok := obj["description"]
	if !ok {
		return false
	}
	switch d := desc.(type) {
	case string:
		return strings.Contains(d, "[Output Only]")
	}
	return false
}

func getSchema(svcDiscDocMap map[string]interface{}, serviceName string, schemaName string, prefix string) map[string]interface{} {
	retVal := svcDiscDocMap["schemas"].(map[string]interface{})[schemaName].(map[string]interface{})
	sp := retVal["properties"]
	retVal["title"] = schemaName
	retVal["__output_only__"] = isOutputOnly(retVal)
	switch schemaPropertiesMap := sp.(type) {
	case map[string]interface{}:
		for k, v := range schemaPropertiesMap {
			val := v.(map[string]interface{})
			if _, ok := val["$ref"]; ok {
				s := val
				s["__output_only__"] = isOutputOnly(s)
				schemaPropertiesMap[k] = s
			} else if val["type"] == "array" {
				s := getArraySchema(svcDiscDocMap, val, k, serviceName, prefixMerge(prefix, k, SchemaDelimiter))
				s["__output_only__"] = isOutputOnly(s)
				s["__id__"] = k
				schemaPropertiesMap[k] = s
			} else if val["type"] == "object" {
				val["__id__"] = k
				val["__output_only__"] = isOutputOnly(val)
				schemaPropertiesMap[k] = getObjectSchema(svcDiscDocMap, val, serviceName, prefixMerge(prefix, k, SchemaDelimiter))
			} else {
				p := prefixMerge(prefix, k, SchemaDelimiter)
				val["path"] = p
				val["__id__"] = k
				val["__output_only__"] = isOutputOnly(val)
				schemaPropertiesMap[k] = val
			}
		}
		retVal["properties"] = schemaPropertiesMap
	}
	return retVal
}

func extractMethodsMapGoogle(item map[string]interface{}) (map[string]metadata.Method, error) {
	errStr := "cannot find methods array"
	retVal := make(map[string]metadata.Method)
	methods, ok := item["methods"].(map[string]interface{})

	if !ok {
		return nil, errors.New(errStr)
	}

	bytes, marshalErr := json.Marshal(methods)
	if marshalErr != nil {
		return nil, marshalErr
	}
	unmarshalErr := json.Unmarshal(bytes, &retVal)
	if unmarshalErr != nil {
		return nil, unmarshalErr
	}
	for k, v := range retVal {
		v.Name = k
		v.Protocol = "http"
		retVal[k] = v
	}
	return retVal, nil
}

func extractSchemaForDescriptionGoogle(item map[string]interface{}) (metadata.Method, error) {
	errStr := "cannot find descriptive method"
	var retVal metadata.Method
	mm, err := extractMethodsMapGoogle(item)
	if err != nil {
		return retVal, err
	}
	getMathod, ok := mm["get"]
	if ok {
		return getMathod, nil
	}
	listMethod, ok := mm["list"]
	if ok {
		return extractDescriptionFromMethodGoogle(listMethod)
	}
	for _, v := range mm {
		return extractDescriptionFromMethodGoogle(v)
	}
	return retVal, errors.New(errStr)
}

func getRequestTypeIfPresent(request interface{}) metadata.SchemaType {
	retVal := metadata.SchemaType{}
	if request != nil {
		if rMap, ok := request.(map[string]interface{}); ok {
			if ref, ok := rMap["$ref"]; ok {
				if rv, ok := ref.(string); ok {
					retVal.Type = rv
				}
			}
		}
	}
	return retVal
}

func getRequiredIfPresent(item interface{}) map[string]bool {
	var retVal map[string]bool
	if item != nil {
		if rMap, ok := item.(map[string]interface{}); ok {
			if ref, ok := rMap["annotations"]; ok {
				if ann, ok := ref.(map[string]interface{}); ok {
					if req, ok := ann["required"]; ok {
						switch req := req.(type) {
						case []interface{}:
							retVal = make(map[string]bool)
							for _, s := range req {
								switch v := s.(type) {
								case string:
									retVal[v] = true
								}
							}
						}
					}
				}
			}
		}
	}
	return retVal
}

func extractDescriptionFromMethodGoogle(methodVal interface{}) (metadata.Method, error) {
	errStr := "cannot extract description from method"
	var retVal metadata.Method
	switch rt := methodVal.(type) {
	case metadata.Method:
		return rt, nil
	case map[string]interface{}:
		mMap := rt
		response, ok := mMap["response"]
		if !ok {
			return retVal, errors.New(errStr)
		}
		rMap, ok := response.(map[string]interface{})
		if !ok {
			return retVal, errors.New(errStr)
		}
		ref, ok := rMap["$ref"]
		if !ok {
			return retVal, errors.New(errStr)
		}
		rv, ok := ref.(string)
		if !ok {
			return retVal, errors.New(errStr)
		}
		retVal.ResponseType.Type = rv
		retVal.RequestType = getRequestTypeIfPresent(mMap["request"])
		return retVal, nil
	}
	return retVal, errors.New(errStr)

}

func findGoogleResourcesMaps(prefix string, resourceMap map[string]interface{}) (map[string]interface{}, error) {
	retVal := make(map[string]interface{})
	errStr := "cannot parse resources from discovery doc"
	resources, ok := resourceMap["resources"]
	if !ok {
		return nil, errors.New(errStr)
	}
	rv, ok := resources.(map[string]interface{})
	if !ok {
		return nil, errors.New(errStr)
	}
	for k, v := range rv {
		rm, ok := v.(map[string]interface{})
		if !ok {
			return nil, errors.New(errStr)
		}
		methods, ok := rm["methods"]
		if ok {
			if _, ok := methods.(map[string]interface{}); ok {
				rk := k
				if prefix != "" {
					rk = prefix + "." + k
				}
				retVal[rk] = rm
			}
		}
		subResources, ok := rm["resources"]
		if ok {
			if _, ok := subResources.(map[string]interface{}); ok {
				subPrefix := k
				if prefix != "" {
					subPrefix = prefix + "." + k
				}
				subMap, err := findGoogleResourcesMaps(subPrefix, rm)
				if err != nil {
					log.Debugln(fmt.Sprintf("err parsing sub-resource = %v", err))
					continue
				}
				for k, v := range subMap {
					retVal[k] = v
				}
			}
		}
	}
	return retVal, nil
}
