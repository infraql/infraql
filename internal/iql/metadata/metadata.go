package metadata

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"infraql/internal/iql/constants"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/iqlutil"
	"infraql/internal/iql/sqltypeutil"

	log "github.com/sirupsen/logrus"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

type Provider struct {
	Name         string
	DiscoveryDoc string
}

type ColumnDescriptor struct {
	Alias        string
	Name         string
	Schema       *Schema
	DecoratedCol string
	Val          *sqlparser.SQLVal
}

func (cd ColumnDescriptor) GetIdentifier() string {
	if cd.Alias != "" {
		return cd.Alias
	}
	return cd.Name
}

func NewColumnDescriptor(alias string, name string, decoratedCol string, schema *Schema, val *sqlparser.SQLVal) ColumnDescriptor {
	return ColumnDescriptor{Alias: alias, Name: name, DecoratedCol: decoratedCol, Schema: schema, Val: val}
}

type Tabulation struct {
	columns   []ColumnDescriptor
	name      string
	arrayType string
}

func GetTabulation(name, arrayType string) Tabulation {
	return Tabulation{name: name, arrayType: arrayType}
}

func (t *Tabulation) GetColumns() []ColumnDescriptor {
	return t.columns
}

func (t *Tabulation) PushBackColumn(col ColumnDescriptor) {
	t.columns = append(t.columns, col)
}

func (t *Tabulation) GetName() string {
	return t.name
}

type Service struct {
	ID           string `json:"ID"`
	Name         string `json:"name"`
	Title        string `json:"title"`
	Description  string `json:"description"`
	Version      string `json:"version"`
	Preferred    bool   `json:"preferred"`
	DiscoveryDoc string `json:"discoveryRestUrl"`
	DocLink      string `json:"documentationLink"`
	Type         string
}

func (svc *Service) ToMap() map[string]interface{} {
	retVal := make(map[string]interface{})
	retVal["id"] = svc.ID
	retVal["name"] = svc.Name
	retVal["title"] = svc.Title
	retVal["description"] = svc.Description
	retVal["version"] = svc.Version
	retVal["preferred"] = svc.Preferred
	retVal["discoveryRestUrl"] = svc.DiscoveryDoc
	retVal["documentationLink"] = svc.DocLink
	retVal["type"] = svc.Type
	return retVal
}

type ServiceHandle struct {
	Service   Service
	Resources map[string]Resource
}

type Resource struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	BaseUrl     string            `json:"baseUrl"`
	Methods     map[string]Method `json:"methods"`
}

func (r *Resource) ConditionIsValid(lhs string, rhs interface{}) bool {
	elem := r.ToMap(true)[lhs]
	if reflect.TypeOf(elem) == reflect.TypeOf(rhs) {
		return true
	}
	return false
}

func (sv *Service) GetName() string {
	return "metadata_service_" + sv.Name
}

func (rs *Resource) GetName() string {
	return "metadata_resource_" + rs.Name
}

func (sv *Service) FilterBy(predicate func(interface{}) (iqlmodel.ITable, error)) (iqlmodel.ITable, error) {
	return predicate(sv)
}

func (r *Resource) FilterBy(predicate func(interface{}) (iqlmodel.ITable, error)) (iqlmodel.ITable, error) {
	return predicate(r)
}

func (m *Method) FilterBy(predicate func(interface{}) (iqlmodel.ITable, error)) (iqlmodel.ITable, error) {
	return predicate(m)
}

func (sv *Service) KeyExists(lhs string) bool {
	_, ok := sv.ToMap()[lhs]
	return ok
}

func (sv *Service) GetKeyAsSqlVal(lhs string) (sqltypes.Value, error) {
	val, ok := sv.ToMap()[lhs]
	rv, err := sqltypeutil.InterfaceToSQLType(val)
	if !ok {
		return rv, fmt.Errorf("key '%s' no preset in metadata_service", lhs)
	}
	return rv, err
}

func (m *Method) GetKeyAsSqlVal(lhs string) (sqltypes.Value, error) {
	val, ok := m.ToPresentationMap(true)[lhs]
	rv, err := sqltypeutil.InterfaceToSQLType(val)
	if !ok {
		return rv, fmt.Errorf("key '%s' no preset in metadata_service", lhs)
	}
	return rv, err
}

func (rs *Resource) GetKeyAsSqlVal(lhs string) (sqltypes.Value, error) {
	val, ok := rs.ToMap(true)[lhs]
	rv, err := sqltypeutil.InterfaceToSQLType(val)
	if !ok {
		return rv, fmt.Errorf("key '%s' no preset in metadata_service", lhs)
	}
	return rv, err
}

func (sv *Service) GetKey(lhs string) (interface{}, error) {
	val, ok := sv.ToMap()[lhs]
	if !ok {
		return nil, fmt.Errorf("key '%s' no preset in metadata_service", lhs)
	}
	return val, nil
}

func (m *Method) GetKey(lhs string) (interface{}, error) {
	val, ok := m.ToPresentationMap(true)[lhs]
	if !ok {
		return nil, fmt.Errorf("key '%s' no preset in metadata_service", lhs)
	}
	return val, nil
}

func (rs *Resource) GetKey(lhs string) (interface{}, error) {
	val, ok := rs.ToMap(true)[lhs]
	if !ok {
		return nil, fmt.Errorf("key '%s' no preset in metadata_service", lhs)
	}
	return val, nil
}

func (sv *Service) MemberEquals(lhs string, rhs interface{}) bool {
	val, ok := sv.ToMap()[lhs]
	if !ok {
		return false
	}
	return val == rhs
}

func (rs *Resource) MemberEquals(lhs string, rhs interface{}) bool {
	val, ok := rs.ToMap(true)[lhs]
	if !ok {
		return false
	}
	return val == rhs
}

func (rs *Resource) KeyExists(lhs string) bool {
	_, ok := rs.ToMap(true)[lhs]
	return ok
}

func (rs *Resource) GetRequiredParameters() map[string]iqlmodel.Parameter {
	return nil
}

func (rs *Service) GetRequiredParameters() map[string]iqlmodel.Parameter {
	return nil
}

func ResourceKeyExists(key string) bool {
	rs := Resource{}
	return rs.KeyExists(key)
}

func ServiceKeyExists(key string) bool {
	sv := Service{}
	return sv.KeyExists(key)
}

func (sv *Service) ConditionIsValid(lhs string, rhs interface{}) bool {
	elem := sv.ToMap()[lhs]
	if reflect.TypeOf(elem) == reflect.TypeOf(rhs) {
		return true
	}
	return false
}

func ResourceConditionIsValid(lhs string, rhs interface{}) bool {
	rs := Resource{}
	return rs.ConditionIsValid(lhs, rhs)
}

func ServiceConditionIsValid(lhs string, rhs interface{}) bool {
	sv := Service{}
	return sv.ConditionIsValid(lhs, rhs)
}

func (rs *Resource) ToMap(extended bool) map[string]interface{} {
	retVal := make(map[string]interface{})
	retVal["id"] = rs.ID
	retVal["name"] = rs.Name
	retVal["title"] = rs.Title
	retVal["description"] = rs.Description
	return retVal
}

type SchemaType struct {
	Type   string `json:"$ref"`
	Format string `json:"__format__"`
}

func (st *SchemaType) GetFormat() string {
	if st.Format != "" {
		return st.Format
	}
	return constants.DefaulHttpBodyFormat
}

type Method struct {
	ID           string                        `json:"id"`
	Name         string                        `json:"name"`
	Path         string                        `json:"path"`
	Description  string                        `json:"description"`
	Protocol     string                        `json:"__protocol__"`
	Verb         string                        `json:"httpMethod"`
	RequestType  SchemaType                    `json:"request"`
	ResponseType SchemaType                    `json:"response"`
	Parameters   map[string]iqlmodel.Parameter `json:"parameters"`
}

func (m *Method) GetColumnOrder(extended bool) []string {
	if extended {
		return []string{
			MethodName,
			RequiredParams,
			MethodDescription,
		}
	}
	return []string{
		MethodName,
		RequiredParams,
	}
}

func (m *Method) ToPresentationMap(extended bool) map[string]interface{} {
	requiredParams := m.GetRequiredParameters()
	var requiredParamNames []string
	for s := range requiredParams {
		requiredParamNames = append(requiredParamNames, s)
	}
	retVal := map[string]interface{}{
		MethodName:     m.Name,
		RequiredParams: strings.Join(requiredParamNames, ", "),
	}
	if extended {
		retVal[MethodDescription] = m.Description
	}
	return retVal
}

func (m *Method) GetName() string {
	return "metadata_method_" + m.Path
}

func (m *Method) KeyExists(lhs string) bool {
	if lhs == MethodName {
		return true
	}
	for key, _ := range m.Parameters {
		if key == lhs {
			return true
		}
	}
	return false
}

func (m *Method) MemberEquals(lhs string, rhs interface{}) bool {
	for key, param := range m.Parameters {
		if key == lhs {
			return param.ID == rhs
		}
	}
	return false
}

func (m *Method) GetResponseType() string {
	return m.ResponseType.Type
}

func (m *Method) GetRequestType() string {
	return m.RequestType.Type
}

func (m *Method) GetRequiredParameters() map[string]iqlmodel.Parameter {
	retVal := make(map[string]iqlmodel.Parameter)
	for k, p := range m.Parameters {
		if p.Required {
			retVal[k] = p
		}
	}
	return retVal
}

func (s *Schema) ConditionIsValid(lhs string, rhs interface{}) bool {
	return iqlutil.ProviderTypeConditionIsValid(s.Type, lhs, rhs)
}

type Schema struct {
	SchemaCentral    *SchemaRegistry         `json:"-"`
	Description      string                  `json:"description"`
	ID               string                  `json:"__id__"`
	OutputOnly       bool                    `json:"__output_only__"`
	Properties       map[string]SchemaHandle `json:"properties"`
	Type             string                  `json:"type"`
	Enum             []string                `json:"enum"`
	EnumDescriptions []string                `json:"enumDescriptions"`
	Format           string                  `json:"__format__"`
	ItemsRawValue    json.RawMessage         `json:"items"`
	Items            SchemaHandle            `json:"__items__"`
	Path             string                  `json:"path"`
	Required         map[string]bool         `json:"__required__"`
}

func (s *Schema) IsIntegral() bool {
	return s.Type == "int" || s.Type == "integer"
}

func (s *Schema) IsArrayRef() bool {
	return s.Type == "array" || s.Items.NamedRef != ""
}

func (s *Schema) IsBoolean() bool {
	return s.Type == "bool" || s.Type == "boolean"
}

func (s *Schema) IsFloat() bool {
	return s.Type == "float" || s.Type == "float64"
}

func (s *Schema) IsRequired(m *Method) bool {
	req, ok := s.Required[m.ID]
	return ok && req
}

func (sc *Schema) UpdateSchemaRegistry(sr *SchemaRegistry) {
	sc.SchemaCentral = sr
	for k, v := range sc.Properties {
		if v.SchemaRef != nil {
			ss, ok := v.SchemaRef[k]
			if ok {
				sp := &ss
				sp.UpdateSchemaRegistry(sr)
				v.SchemaRef[k] = *sp
			}
		}
		ss, _ := v.GetSchema(sr)
		ss.UpdateSchemaRegistry(sr)
	}
	if sc.Items.SchemaRef != nil {
		ss, ok := sc.Items.SchemaRef["items"]
		if ok {
			sp := &ss
			sp.UpdateSchemaRegistry(sr)
			sc.Items.SchemaRef["items"] = *sp
		}
	}
	for _, v := range sc.Items.SchemaRef {
		v.UpdateSchemaRegistry(sr)
	}
}

func (sc *Schema) GetPropertySchema(key string) (*Schema, error) {
	absentErr := fmt.Errorf("property schema not present for key '%s'", key)
	sh, ok := sc.Properties[key]
	if !ok {
		return nil, absentErr
	}
	if sh.NamedRef != "" {
		subSchema, ok := sc.SchemaCentral.SchemaRef[sh.NamedRef]
		if ok {
			return &subSchema, nil
		}
	} else {
		subSchema, ok := sh.SchemaRef[key]
		if ok {
			return &subSchema, nil
		}
	}
	return nil, absentErr
}

func (sc *Schema) GetItemsSchema() (*Schema, error) {
	absentErr := fmt.Errorf("items schema not present")
	sh := sc.Items
	if sh.NamedRef != "" {
		subSchema, ok := sc.SchemaCentral.SchemaRef[sh.NamedRef]
		if ok {
			return &subSchema, nil
		}
	} else {
		subSchema, ok := sh.SchemaRef["items"]
		if ok {
			return &subSchema, nil
		}
	}
	return nil, absentErr
}

type SchemaRegistry struct {
	SchemaRef map[string]Schema
}

type SchemaHandle struct {
	NamedRef  string
	SchemaRef map[string]Schema
}

func (sh *SchemaHandle) IsEmpty() bool {
	return sh.NamedRef == "" && sh.SchemaRef == nil
}

func (sh *SchemaHandle) GetSchema(sr *SchemaRegistry) (*Schema, string) {
	if sh.NamedRef != "" {
		s := sr.SchemaRef[sh.NamedRef]
		return &s, sh.NamedRef
	}
	for _, subS := range sh.SchemaRef {
		rv := subS
		return &rv, ""
	}
	return nil, ""
}

func (schema *Schema) GetSelectListItems(key string) (*Schema, string) {
	propS := schema.Properties[key]
	itemS, _ := propS.GetSchema(schema.SchemaCentral)
	if itemS != nil {
		return itemS, key
	}
	for k, psh := range schema.Properties {
		ss, _ := psh.GetSchema(schema.SchemaCentral)
		ish := ss.Items
		iS, _ := ish.GetSchema(schema.SchemaCentral)
		if iS != nil {
			return ss, k
		}
	}
	return nil, ""
}

func (s *Schema) toFlatDescriptionMap(extended bool) map[string]interface{} {
	retVal := make(map[string]interface{})
	retVal["name"] = s.ID
	retVal["type"] = s.Type
	if extended {
		retVal["description"] = s.Description
	}
	return retVal
}

func (s *Schema) GetAllColumns() []string {
	log.Infoln(fmt.Sprintf("s = %v", *s))
	var retVal []string
	if s.Type == "object" || (s.Properties != nil && len(s.Properties) > 0) {
		for k, val := range s.Properties {
			valSchema, _ := val.GetSchema(s.SchemaCentral)
			if valSchema != nil && !valSchema.OutputOnly {
				retVal = append(retVal, k)
			}
		}
	} else if s.Type == "array" {
		if items, _ := s.Items.GetSchema(s.SchemaCentral); items != nil {
			return items.GetAllColumns()
		}
	}
	return retVal
}

func (s *Schema) Tabulate(omitColumns bool) *Tabulation {
	if s.Type == "object" || (s.Properties != nil && len(s.Properties) > 0) {
		var cols []ColumnDescriptor
		if !omitColumns {
			for k, val := range s.Properties {
				valSchema, _ := val.GetSchema(s.SchemaCentral)
				if valSchema != nil {
					col := ColumnDescriptor{Name: k, Schema: valSchema}
					cols = append(cols, col)
				}
			}
		}
		return &Tabulation{columns: cols, name: s.ID}
	} else if s.Type == "array" {
		if items, _ := s.Items.GetSchema(s.SchemaCentral); items != nil {
			return items.Tabulate(false)
		}
	}
	return nil
}

func (s *Schema) ToDescriptionMap(extended bool) map[string]interface{} {
	retVal := make(map[string]interface{})
	if s.Type == "array" {
		items, _ := s.Items.GetSchema(s.SchemaCentral)
		if items != nil {
			return items.toFlatDescriptionMap(extended)
		}
	}
	if s.Type == "object" {
		for k, v := range s.Properties {
			p, _ := v.GetSchema(s.SchemaCentral)
			if p != nil {
				pm := p.toFlatDescriptionMap(extended)
				pm["name"] = k
				retVal[k] = pm
			}
		}
		return retVal
	}
	retVal["name"] = s.ID
	retVal["type"] = s.Type
	if extended {
		retVal["description"] = s.Description
	}
	return retVal
}

func (s *Schema) Unmarshal() error {
	bytes := []byte(s.ItemsRawValue)
	if s.Items.IsEmpty() && s.ItemsRawValue != nil && bytes != nil {
		so := SchemaHandle{}
		err := json.Unmarshal(bytes, &so)
		if err != nil {
			log.Infoln(fmt.Sprintf("s.Unmarshal(): err = %s", err.Error()))
			log.Infoln(fmt.Sprintf("s.Unmarshal(): err on = '%s'", string(bytes)))
			return err
		}
		s.Items = so
	}
	for k, v := range s.Properties {
		bytes := []byte(s.ItemsRawValue)
		so := SchemaHandle{}
		res := json.Unmarshal(bytes, &so)
		if res != nil {
			log.Infoln(fmt.Sprintf("s.Unmarshal(): err = %s", res.Error()))
			return res
		}
		s.Properties[k] = v
	}
	for k, item := range s.Items.SchemaRef {
		res := item.Unmarshal()
		if res != nil {
			log.Infoln(fmt.Sprintf("s.Unmarshal(): err = %s", res.Error()))
			return res
		}
		s.Items.SchemaRef[k] = item
	}
	return nil
}

func (s *Schema) FindByPath(path string, visited map[string]bool) *Schema {
	if visited == nil {
		visited = make(map[string]bool)
	}
	log.Infoln(fmt.Sprintf("FindByPath() called with path = '%s'", path))
	if s.Path == path {
		return s
	}
	remainingPath := strings.TrimPrefix(path, s.Path)
	for k, v := range s.Properties {
		if v.NamedRef != "" {
			isVis, ok := visited[v.NamedRef]
			if isVis && ok {
				continue
			}
			visited[v.NamedRef] = true
		}
		log.Infoln(fmt.Sprintf("FindByPath() attempting to match  path = '%s' with property '%s', visited = %v", path, k, visited))
		if k == path {
			rv, _ := v.GetSchema(s.SchemaCentral)
			return rv
		}
		ss, _ := v.GetSchema(s.SchemaCentral)
		if ss != nil {
			res := ss.FindByPath(path, visited)
			if res != nil {
				return res
			}
			resRem := ss.FindByPath(remainingPath, visited)
			if resRem != nil {
				return resRem
			}
		}
	}
	if s.Items.NamedRef != "" {
		isVis, ok := visited[s.Items.NamedRef]
		if isVis && ok {
			return nil
		}
		visited[s.Items.NamedRef] = true
	}
	ss, _ := s.Items.GetSchema(s.SchemaCentral)
	if ss != nil {
		res := ss.FindByPath(path, visited)
		if res != nil {
			return res
		}
		resRem := ss.FindByPath(remainingPath, visited)
		if resRem != nil {
			return resRem
		}
	}
	return nil
}

func SchemaFromMap(sMap map[string]interface{}) Schema {
	return Schema{}
}

type MetadataStore struct {
	Store map[string]ServiceHandle
}

func (ms *MetadataStore) GetServices() ([]Service, error) {
	var retVal []Service
	for _, svc := range ms.Store {
		retVal = append(retVal, svc.Service)
	}
	return retVal, nil
}

func (ms *MetadataStore) GetResources(serviceName string) ([]*Resource, error) {
	var retVal []*Resource
	serviceHandle, ok := ms.Store[serviceName]
	if !ok {
		return nil, fmt.Errorf("cannnot find service %s", serviceName)
	}
	for _, rsc := range serviceHandle.Resources {
		retVal = append(retVal, &rsc)
	}
	return retVal, nil
}

func (ms *MetadataStore) GetResource(serviceName string, resourceName string) (*Resource, error) {
	serviceHandle, ok := ms.Store[serviceName]
	if !ok {
		return nil, fmt.Errorf("cannnot find service %s", serviceName)
	}
	rsc, ok := serviceHandle.Resources[resourceName]
	if !ok {
		return nil, fmt.Errorf("cannnot find resource %s", resourceName)
	}
	return &rsc, nil
}

type AuthMetadata struct {
	Principal string `json:"principal"`
	Type      string `json:"type"`
	Source    string `json:"source"`
}

func (am *AuthMetadata) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"principal": am.Principal,
		"type":      am.Type,
		"source":    am.Source,
	}
}

func (am *AuthMetadata) GetHeaders() []string {
	return []string{
		"principal",
		"type",
		"source",
	}
}
