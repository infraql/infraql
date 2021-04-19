package metadatavisitors

import (
	"fmt"
	"infraql/internal/iql/constants"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/iqlutil"
	"infraql/internal/iql/metadata"
	"infraql/internal/pkg/prettyprint"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/sqlparser"
)

type SchemaRequestTemplateVisitor struct {
	MaxDepth       int
	Strategy       string
	PrettyPrinter  *prettyprint.PrettyPrinter
	visitedObjects map[string]bool
}

func NewSchemaRequestTemplateVisitor(maxDepth int, strategy string, prettyPrinter *prettyprint.PrettyPrinter) *SchemaRequestTemplateVisitor {
	return &SchemaRequestTemplateVisitor{
		MaxDepth:       maxDepth,
		Strategy:       strategy,
		PrettyPrinter:  prettyPrinter,
		visitedObjects: make(map[string]bool),
	}
}

func (sv *SchemaRequestTemplateVisitor) recordSchemaVisited(schemaKey string) {
	sv.visitedObjects[schemaKey] = true
}

func (sv *SchemaRequestTemplateVisitor) isVisited(schemaKey string) bool {
	return sv.visitedObjects[schemaKey]
}

func checkAllColumnsPresent(columns sqlparser.Columns, toInclude map[string]bool) error {
	var missingColNames []string
	if columns != nil {
		for _, col := range columns {
			cName := col.GetRawVal()
			if !toInclude[cName] {
				missingColNames = append(missingColNames, cName)
			}
		}
		if len(missingColNames) > 0 {
			return fmt.Errorf("cannot find the following columns: %s", strings.Join(missingColNames, ", "))
		}
	}
	return nil
}

func getColsMap(columns sqlparser.Columns) map[string]bool {
	retVal := make(map[string]bool)
	for _, col := range columns {
		retVal[col.GetRawVal()] = true
	}
	return retVal

}

func isColIncludable(key string, columns sqlparser.Columns, colMap map[string]bool) bool {
	colOk := columns == nil
	if colOk {
		return colOk
	}
	return colMap[key]
}

func isBodyParam(paramName string) bool {
	return strings.HasPrefix(paramName, constants.RequestBodyBaseKey)
}

func ToInsertStatement(columns sqlparser.Columns, m *metadata.Method, schemaMap map[string]metadata.Schema, extended bool, prettyPrinter *prettyprint.PrettyPrinter) (string, error) {
	paramsToInclude := m.Parameters
	successfullyIncludedCols := make(map[string]bool)
	if !extended {
		paramsToInclude = m.GetRequiredParameters()
	}
	if columns != nil {
		paramsToInclude = make(map[string]iqlmodel.Parameter)
		for _, col := range columns {
			cName := col.GetRawVal()
			if !isBodyParam(cName) {
				p, ok := m.Parameters[cName]
				if !ok {
					return "", fmt.Errorf("cannot generate insert statement: column '%s' not present", cName)
				}
				paramsToInclude[cName] = p
				successfullyIncludedCols[cName] = true
			}
		}
	}
	var includedParamNames []string
	for k, _ := range paramsToInclude {
		includedParamNames = append(includedParamNames, k)
	}
	sort.Strings(includedParamNames)
	var columnList, exprList []string
	for _, s := range includedParamNames {
		columnList = append(columnList, prettyPrinter.RenderColumnName(s))
		switch m.Parameters[s].Type {
		case "string":
			exprList = append(exprList, prettyPrinter.RenderTemplateVarAndDelimit(s))
		default:
			exprList = append(exprList, prettyPrinter.RenderTemplateVarNoDelimit(s))
		}
	}

	var sch *metadata.Schema
	if m.RequestType.Type != "" {
		s, ok := schemaMap[m.RequestType.Type]
		if ok {
			sch = &s
		}
	}

	if sch == nil {
		err := checkAllColumnsPresent(columns, successfullyIncludedCols)
		return "INSERT INTO %s" + "(\n" + strings.Join(columnList, ",\n") +
			"\n)\n" + "SELECT\n" + strings.Join(exprList, ",\n") + "\n;\n", err
	}

	schemaVisitor := NewSchemaRequestTemplateVisitor(2, "", prettyPrinter)

	tVal, _ := schemaVisitor.RetrieveTemplate(sch, m.RequestType.Type, m.ID, extended)

	log.Infoln(fmt.Sprintf("tVal = %v", tVal))

	colMap := getColsMap(columns)

	if columns != nil {
		for _, c := range columns {
			cName := c.GetRawVal()
			if !isBodyParam(cName) {
				continue
			}
			cNameSuffix := strings.TrimPrefix(cName, constants.RequestBodyBaseKey)
			if v, ok := tVal[cNameSuffix]; ok {
				columnList = append(columnList, prettyPrinter.RenderColumnName(cName))
				exprList = append(exprList, v)
				successfullyIncludedCols[cName] = true
			}
		}
	} else {
		tValKeysSorted := iqlutil.GetSortedKeysStringMap(tVal)
		for _, k := range tValKeysSorted {
			v := tVal[k]
			if isColIncludable(k, columns, colMap) {
				columnList = append(columnList, prettyPrinter.RenderColumnName(constants.RequestBodyBaseKey+k))
				exprList = append(exprList, v)
			}
		}
	}

	err := checkAllColumnsPresent(columns, successfullyIncludedCols)
	retVal := "INSERT INTO %s" + "(\n" + strings.Join(columnList, ",\n") +
		"\n)\n" + "SELECT\n" + strings.Join(exprList, ",\n") + "\n;\n"
	return retVal, err
}

func (sv *SchemaRequestTemplateVisitor) RetrieveTemplate(sc *metadata.Schema, schemaKey string, methodKey string, extended bool) (map[string]string, error) {
	retVal := make(map[string]string)
	sv.recordSchemaVisited(schemaKey)
	switch sc.Type {
	case "object":
		for k, v := range sc.Properties {
			ss, idStr := v.GetSchema(sc.SchemaCentral)
			if ss != nil && (idStr == "" || !sv.isVisited(idStr)) {
				sv.recordSchemaVisited(idStr)
				rv, err := sv.retrieveTemplateVal(ss, methodKey, ".values."+constants.RequestBodyBaseKey+k)
				if err != nil {
					return nil, err
				}
				switch rvt := rv.(type) {
				case map[string]interface{}, []interface{}, string:
					bytes, err := sv.PrettyPrinter.PrintTemplatedJSON(rvt)
					if err != nil {
						return nil, err
					}
					retVal[k] = string(bytes)
				case nil:
					continue
				default:
					return nil, fmt.Errorf("error processing template key '%s' with disallowed type '%T'", k, rvt)
				}
			}
		}
		if len(retVal) == 0 {
			return nil, nil
		}
		return retVal, nil
	}
	return nil, fmt.Errorf("templating of request body only supported for object type payload")
}

func (sv *SchemaRequestTemplateVisitor) retrieveTemplateVal(sc *metadata.Schema, methodKey string, objectKey string) (interface{}, error) {
	sSplit := strings.Split(objectKey, ".")
	oKey := sSplit[len(sSplit)-1]
	oPrefix := objectKey
	if len(sSplit) > 1 {
		oPrefix = strings.TrimSuffix(objectKey, "."+oKey)
	} else {
		oPrefix = ""
	}
	templateValSuffix := oKey
	templateValName := oPrefix + "." + templateValSuffix
	if oPrefix == "" {
		templateValName = templateValSuffix
	}
	switch sc.Type {
	case "object":
		rv := make(map[string]interface{})
		for k, v := range sc.Properties {
			ss, idStr := v.GetSchema(sc.SchemaCentral)
			if ss != nil && (idStr == "" || !sv.isVisited(idStr)) {
				sv.recordSchemaVisited(idStr)
				sv, err := sv.retrieveTemplateVal(ss, methodKey, templateValName+"."+k)
				if err != nil {
					return nil, err
				}
				if sv != nil {
					rv[k] = sv
				}
			}
		}
		if len(rv) == 0 {
			return nil, nil
		}
		return rv, nil
		// bytes, err := json.Marshal(rv)
		// return string(bytes), err
	case "array":
		var arr []interface{}
		iSch, err := sc.GetItemsSchema()
		if err != nil {
			return nil, err
		}
		itemS, err := sv.retrieveTemplateVal(iSch, methodKey, templateValName+"[0]")
		arr = append(arr, itemS)
		if err != nil {
			return nil, err
		}
		return arr, nil
	case "string":
		return "\"{{ " + templateValName + " }}\"", nil
	default:
		return "{{ " + templateValName + " }}", nil
	}
	return nil, nil
}
