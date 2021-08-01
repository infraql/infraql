package planbuilder

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"infraql/internal/iql/constants"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpbuild"
	"infraql/internal/iql/iqlerror"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/iqlutil"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/parserutil"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/primitivebuilder"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/relational"
	"infraql/internal/iql/symtab"
	"infraql/internal/iql/taxonomy"
	"infraql/internal/iql/util"

	"vitess.io/vitess/go/vt/sqlparser"

	log "github.com/sirupsen/logrus"
)

func (p *primitiveGenerator) analyzeStatement(handlerCtx *handler.HandlerContext, statement sqlparser.Statement) error {
	var err error
	switch stmt := statement.(type) {
	case *sqlparser.Auth:
		return p.analyzeAuth(handlerCtx, stmt)
	case *sqlparser.AuthRevoke:
		return p.analyzeAuthRevoke(handlerCtx, stmt)
	case *sqlparser.Begin:
		return iqlerror.GetStatementNotSupportedError("TRANSACTION: BEGIN")
	case *sqlparser.Commit:
		return iqlerror.GetStatementNotSupportedError("TRANSACTION: COMMIT")
	case *sqlparser.DBDDL:
		return iqlerror.GetStatementNotSupportedError(fmt.Sprintf("unsupported: Database DDL %v", sqlparser.String(stmt)))
	case *sqlparser.DDL:
		return iqlerror.GetStatementNotSupportedError("DDL")
	case *sqlparser.Delete:
		return p.analyzeDelete(handlerCtx, stmt)
	case *sqlparser.DescribeTable:
		return p.analyzeDescribe(handlerCtx, stmt)
	case *sqlparser.Exec:
		return p.analyzeExec(handlerCtx, stmt)
	case *sqlparser.Explain:
		return iqlerror.GetStatementNotSupportedError("EXPLAIN")
	case *sqlparser.Insert:
		return p.analyzeInsert(handlerCtx, stmt)
	case *sqlparser.OtherRead, *sqlparser.OtherAdmin:
		return iqlerror.GetStatementNotSupportedError("OTHER")
	case *sqlparser.Rollback:
		return iqlerror.GetStatementNotSupportedError("TRANSACTION: ROLLBACK")
	case *sqlparser.Savepoint:
		return iqlerror.GetStatementNotSupportedError("TRANSACTION: SAVEPOINT")
	case *sqlparser.Select:
		return p.analyzeSelect(handlerCtx, stmt)
	case *sqlparser.Set:
		return iqlerror.GetStatementNotSupportedError("SET")
	case *sqlparser.SetTransaction:
		return iqlerror.GetStatementNotSupportedError("SET TRANSACTION")
	case *sqlparser.Show:
		return p.analyzeShow(handlerCtx, stmt)
	case *sqlparser.Sleep:
		return p.analyzeSleep(handlerCtx, stmt)
	case *sqlparser.SRollback:
		return iqlerror.GetStatementNotSupportedError("TRANSACTION: SROLLBACK")
	case *sqlparser.Release:
		return iqlerror.GetStatementNotSupportedError("TRANSACTION: RELEASE")
	case *sqlparser.Union:
		return iqlerror.GetStatementNotSupportedError("UNION")
	case *sqlparser.Update:
		return iqlerror.GetStatementNotSupportedError("UPDATE")
	case *sqlparser.Use:
		return p.analyzeUse(handlerCtx, stmt)
	}
	return err
}

func (p *primitiveGenerator) analyzeUse(handlerCtx *handler.HandlerContext, node *sqlparser.Use) error {
	prov, pErr := handlerCtx.GetProvider(node.DBName.GetRawVal())
	if pErr != nil {
		return pErr
	}
	p.PrimitiveBuilder.SetProvider(prov)
	return nil
}

func (p *primitiveGenerator) analyzeAuth(handlerCtx *handler.HandlerContext, node *sqlparser.Auth) error {
	provider, pErr := handlerCtx.GetProvider(node.Provider)
	if pErr != nil {
		return pErr
	}
	p.PrimitiveBuilder.SetProvider(provider)
	return nil
}

func (p *primitiveGenerator) analyzeAuthRevoke(handlerCtx *handler.HandlerContext, node *sqlparser.AuthRevoke) error {
	authCtx, authErr := handlerCtx.GetAuthContext(node.Provider)
	if authErr != nil {
		return authErr
	}
	switch strings.ToLower(authCtx.Type) {
	case dto.AuthServiceAccountStr, dto.AuthInteractiveStr:
		return nil
	}
	return fmt.Errorf(`Auth revoke for Google Failed; improper auth method: "%s" specified`, authCtx.Type)
}

func checkResource(handlerCtx *handler.HandlerContext, prov provider.IProvider, service string, resource string) (*metadata.Resource, error) {
	return prov.GetResource(service, resource, handlerCtx.RuntimeContext)
}

func checkService(handlerCtx *handler.HandlerContext, prov provider.IProvider, service string) (*metadata.ServiceHandle, error) {
	return prov.GetServiceHandle(service, handlerCtx.RuntimeContext)
}

func (pb *primitiveGenerator) assembleServiceAndResources(handlerCtx *handler.HandlerContext, prov provider.IProvider, service string) (*metadata.ServiceHandle, error) {
	svc, err := prov.GetServiceHandle(service, handlerCtx.RuntimeContext)
	if err != nil {
		return nil, err
	}
	rscMap, err := prov.GetResourcesMap(service, handlerCtx.RuntimeContext)
	if err != nil {
		return nil, err
	}
	svc.Resources = rscMap
	return svc, err
}

func (pb *primitiveGenerator) analyzeShowFilter(node *sqlparser.Show, table iqlmodel.ITable) error {
	showFilter := node.ShowTablesOpt.Filter
	if showFilter == nil {
		return nil
	}
	if showFilter.Like != "" {
		likeRegexp, err := regexp.Compile(iqlutil.TranslateLikeToRegexPattern(showFilter.Like))
		if err != nil {
			return fmt.Errorf("cannot compile like string '%s': %s", showFilter.Like, err.Error())
		}
		tableFilter := pb.PrimitiveBuilder.GetTableFilter()
		for _, col := range pb.PrimitiveBuilder.GetLikeAbleColumns() {
			tableFilter = relational.OrTableFilters(tableFilter, relational.ConstructLikePredicateFilter(col, likeRegexp, false))
		}
		pb.PrimitiveBuilder.SetTableFilter(relational.OrTableFilters(pb.PrimitiveBuilder.GetTableFilter(), tableFilter))
	} else if showFilter.Filter != nil {
		tableFilter, err := pb.traverseShowFilter(table, node, showFilter.Filter)
		if err != nil {
			return err
		}
		pb.PrimitiveBuilder.SetTableFilter(tableFilter)
	}
	return nil
}

func (pb *primitiveGenerator) traverseShowFilter(table iqlmodel.ITable, node *sqlparser.Show, filter sqlparser.Expr) (func(iqlmodel.ITable) (iqlmodel.ITable, error), error) {
	var retVal func(iqlmodel.ITable) (iqlmodel.ITable, error)
	switch filter := filter.(type) {
	case *sqlparser.ComparisonExpr:
		return pb.comparisonExprToFilterFunc(table, node, filter)
	case *sqlparser.AndExpr:
		log.Infoln("complex AND expr detected")
		lhs, lhErr := pb.traverseShowFilter(table, node, filter.Left)
		rhs, rhErr := pb.traverseShowFilter(table, node, filter.Right)
		if lhErr != nil {
			return nil, lhErr
		}
		if rhErr != nil {
			return nil, rhErr
		}
		return relational.AndTableFilters(lhs, rhs), nil
	case *sqlparser.OrExpr:
		log.Infoln("complex OR expr detected")
		lhs, lhErr := pb.traverseShowFilter(table, node, filter.Left)
		rhs, rhErr := pb.traverseShowFilter(table, node, filter.Right)
		if lhErr != nil {
			return nil, lhErr
		}
		if rhErr != nil {
			return nil, rhErr
		}
		return relational.OrTableFilters(lhs, rhs), nil
	case *sqlparser.FuncExpr:
		return nil, fmt.Errorf("unsupported constraint in metadata filter: %v", sqlparser.String(filter))
	default:
		return nil, fmt.Errorf("unsupported constraint in metadata filter: %v", sqlparser.String(filter))
	}
	return retVal, nil
}

// DEPRECATED
func (pb *primitiveGenerator) traverseWhereFilterDeprecated(table *metadata.Method, node sqlparser.Expr, schema *metadata.Schema, requiredParameters map[string]iqlmodel.Parameter) (func(iqlmodel.ITable) (iqlmodel.ITable, error), error) {
	var retVal func(iqlmodel.ITable) (iqlmodel.ITable, error)
	switch node := node.(type) {
	case *sqlparser.ComparisonExpr:
		return pb.whereComparisonExprToFilterFunc(node, table, schema, requiredParameters)
	case *sqlparser.AndExpr:
		log.Infoln("complex AND expr detected")
		lhs, lhErr := pb.traverseWhereFilterDeprecated(table, node.Left, schema, requiredParameters)
		rhs, rhErr := pb.traverseWhereFilterDeprecated(table, node.Right, schema, requiredParameters)
		if lhErr != nil {
			return nil, lhErr
		}
		if rhErr != nil {
			return nil, rhErr
		}
		return relational.AndTableFilters(lhs, rhs), nil
	case *sqlparser.OrExpr:
		log.Infoln("complex OR expr detected")
		lhs, lhErr := pb.traverseWhereFilterDeprecated(table, node.Left, schema, requiredParameters)
		rhs, rhErr := pb.traverseWhereFilterDeprecated(table, node.Right, schema, requiredParameters)
		if lhErr != nil {
			return nil, lhErr
		}
		if rhErr != nil {
			return nil, rhErr
		}
		return relational.OrTableFilters(lhs, rhs), nil
	case *sqlparser.FuncExpr:
		return nil, fmt.Errorf("unsupported constraint in metadata filter: %v", sqlparser.String(node))
	default:
		return nil, fmt.Errorf("unsupported constraint in metadata filter: %v", sqlparser.String(node))
	}
	return retVal, nil
}

func (pb *primitiveGenerator) traverseWhereFilter(node sqlparser.SQLNode, requiredParameters map[string]iqlmodel.Parameter) (sqlparser.Expr, error) {
	switch node := node.(type) {
	case *sqlparser.ComparisonExpr:
		return pb.whereComparisonExprCopyAndReWrite(node, requiredParameters)
	case *sqlparser.AndExpr:
		log.Infoln("complex AND expr detected")
		lhs, lhErr := pb.traverseWhereFilter(node.Left, requiredParameters)
		rhs, rhErr := pb.traverseWhereFilter(node.Right, requiredParameters)
		if lhErr != nil {
			return nil, lhErr
		}
		if rhErr != nil {
			return nil, rhErr
		}
		return &sqlparser.AndExpr{Left: lhs, Right: rhs}, nil
	case *sqlparser.OrExpr:
		log.Infoln("complex OR expr detected")
		lhs, lhErr := pb.traverseWhereFilter(node.Left, requiredParameters)
		rhs, rhErr := pb.traverseWhereFilter(node.Right, requiredParameters)
		if lhErr != nil {
			return nil, lhErr
		}
		if rhErr != nil {
			return nil, rhErr
		}
		return &sqlparser.OrExpr{Left: lhs, Right: rhs}, nil
	case *sqlparser.FuncExpr:
		return nil, fmt.Errorf("unsupported constraint in metadata filter: %v", sqlparser.String(node))
	case *sqlparser.IsExpr:
		return &sqlparser.IsExpr{
			Operator: node.Operator,
			Expr:     node.Expr,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported constraint in metadata filter: %v", sqlparser.String(node))
	}
	return nil, fmt.Errorf("unsupported constraint in metadata filter: %v", sqlparser.String(node))
}

func (pb *primitiveGenerator) whereComparisonExprCopyAndReWrite(expr *sqlparser.ComparisonExpr, requiredParameters map[string]iqlmodel.Parameter) (sqlparser.Expr, error) {
	qualifiedName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
	colName := qualifiedName.Name.GetRawVal()
	symTabEntry, symTabErr := pb.PrimitiveBuilder.GetSymbol(colName)
	_, requiredParamPresent := requiredParameters[colName]
	log.Infoln(fmt.Sprintf("symTabEntry = %v", symTabEntry))
	if symTabErr != nil && !requiredParamPresent {
		return nil, symTabErr
	}
	delete(requiredParameters, colName)
	if symTabErr == nil {
		if !requiredParamPresent {
			return &sqlparser.ComparisonExpr{
				Left:     expr.Left,
				Right:    expr.Right,
				Operator: expr.Operator,
				Escape:   expr.Escape,
			}, nil
		}
		paramMAtchStr := ""
		switch rhs := expr.Right.(type) {
		case *sqlparser.SQLVal:
			paramMAtchStr = string(rhs.Val)
		}
		newRhs := &sqlparser.SQLVal{
			Type: sqlparser.StrVal,
			Val:  []byte(fmt.Sprintf("%%%s", paramMAtchStr)),
		}
		return &sqlparser.ComparisonExpr{
			Left:     expr.Left,
			Right:    newRhs,
			Operator: sqlparser.LikeStr,
			Escape:   nil,
		}, nil
	}
	return &sqlparser.ComparisonExpr{
		Left:     &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte("1")},
		Right:    &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte("1")},
		Operator: expr.Operator,
		Escape:   expr.Escape,
	}, nil
}

// DEPRECATED
func (pb *primitiveGenerator) whereComparisonExprToFilterFunc(expr *sqlparser.ComparisonExpr, table *metadata.Method, schema *metadata.Schema, requiredParameters map[string]iqlmodel.Parameter) (func(iqlmodel.ITable) (iqlmodel.ITable, error), error) {
	qualifiedName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
	colName := qualifiedName.Name.GetRawVal()
	tableContainsKey := table.KeyExists(colName)
	subSchema := schema.FindByPath(colName)
	if !tableContainsKey && subSchema == nil {
		return nil, fmt.Errorf("col name = '%s' not found in resource name = '%s'", colName, table.GetName())
	}
	delete(requiredParameters, colName)
	if tableContainsKey && subSchema != nil && !subSchema.OutputOnly {
		log.Infoln(fmt.Sprintf("tableContainsKey && subSchema = %v", subSchema))
		return nil, fmt.Errorf("col name = '%s' ambiguous for resource name = '%s'", colName, table.GetName())
	}
	val, ok := expr.Right.(*sqlparser.SQLVal)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
	//StrVal is varbinary, we do not support varchar since we would have to implement all collation types
	if val.Type != sqlparser.IntVal && val.Type != sqlparser.StrVal {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
	pv, err := sqlparser.NewPlanValue(val)
	if err != nil {
		return nil, err
	}
	resolved, err := pv.ResolveValue(nil)
	log.Debugln(fmt.Sprintf("resolved = %v", resolved))
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// DEPRECATED
func (pb *primitiveGenerator) analyzeSingleTableWhere(where *sqlparser.Where, schema *metadata.Schema) error {
	remainingRequiredParameters := make(map[string]iqlmodel.Parameter)
	for _, v := range pb.PrimitiveBuilder.GetTables() {
		method, err := v.GetMethod()
		if err != nil {
			return err
		}
		requiredParameters := method.GetRequiredParameters()
		if where != nil {
			pb.traverseWhereFilterDeprecated(method, where.Expr, schema, requiredParameters)
		}
		for l, w := range requiredParameters {
			rscStr, _ := v.GetResourceStr()
			remainingRequiredParameters[fmt.Sprintf("%s.%s", rscStr, l)] = w
		}
		var colUsages []parserutil.ColumnUsageMetadata
		if where != nil {
			colUsages, err = parserutil.GetColumnUsageTypes(where.Expr)
		}
		if err != nil {
			return err
		}
		err = parserutil.CheckColUsagesAgainstTable(colUsages, method)
		if err != nil {
			return err
		}
	}
	if len(remainingRequiredParameters) > 0 {
		var keys []string
		for k := range remainingRequiredParameters {
			keys = append(keys, k)
		}
		return fmt.Errorf("Query cannot be executed, missing required parameters: { %s }", strings.Join(keys, ", "))
	}
	return nil
}

func (pb *primitiveGenerator) analyzeWhere(where *sqlparser.Where, schema *metadata.Schema) (*sqlparser.Where, error) {
	requiredParameters := make(map[string]iqlmodel.Parameter)
	remainingRequiredParameters := make(map[string]iqlmodel.Parameter)
	for _, v := range pb.PrimitiveBuilder.GetTables() {
		method, err := v.GetMethod()
		if err != nil {
			return nil, err
		}
		for k, v := range method.GetRequiredParameters() {
			_, keyExists := requiredParameters[k]
			if keyExists {
				return nil, fmt.Errorf("key already is required: %s", k)
			}
			requiredParameters[k] = v
		}
	}
	var retVal sqlparser.Expr
	var err error
	if where != nil {
		retVal, err = pb.traverseWhereFilter(where.Expr, requiredParameters)
		if err != nil {
			return nil, err
		}
	}

	for l, w := range requiredParameters {
		remainingRequiredParameters[fmt.Sprintf("%s", l)] = w
	}

	if len(remainingRequiredParameters) > 0 {
		var keys []string
		for k := range remainingRequiredParameters {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("Query cannot be executed, missing required parameters: { %s }", strings.Join(keys, ", "))
	}
	return &sqlparser.Where{Type: where.Type, Expr: retVal}, nil
}

func extractVarDefFromExec(node *sqlparser.Exec, argName string) (*sqlparser.ExecVarDef, error) {
	for _, varDef := range node.ExecVarDefs {
		if varDef.ColIdent.GetRawVal() == argName {
			return &varDef, nil
		}
	}
	return nil, fmt.Errorf("could not find variable '%s'", argName)
}

func (p *primitiveGenerator) parseComments(comments sqlparser.Comments) {
	if comments != nil {
		p.PrimitiveBuilder.SetCommentDirectives(sqlparser.ExtractCommentDirectives(comments))
		p.PrimitiveBuilder.SetAwait(p.PrimitiveBuilder.GetCommentDirectives().IsSet("AWAIT"))
	}
}

func (p *primitiveGenerator) persistHerarchyToBuilder(heirarchy *taxonomy.HeirarchyObjects, node sqlparser.SQLNode) {
	p.PrimitiveBuilder.SetTable(node, taxonomy.NewExtendedTableMetadata(heirarchy))
}

func (p *primitiveGenerator) analyzeExec(handlerCtx *handler.HandlerContext, node *sqlparser.Exec) error {
	err := p.inferHeirarchyAndPersist(handlerCtx, node)
	if err != nil {
		return err
	}
	p.parseComments(node.Comments)

	meta, err := p.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return err
	}

	method, err := meta.GetMethod()
	if err != nil {
		return err
	}

	requiredParams := method.GetRequiredParameters()

	colz, err := parserutil.GetColumnUsageTypesForExec(node)
	usageErr := parserutil.CheckColUsagesAgainstTable(colz, method)
	if usageErr != nil {
		return usageErr
	}
	for k, param := range requiredParams {
		log.Debugln(fmt.Sprintf("param = %v", param))
		_, err := extractVarDefFromExec(node, k)
		if err != nil {
			return fmt.Errorf("required param not supplied for exec: %s", err.Error())
		}
	}
	prov, err := meta.GetProvider()
	if err != nil {
		return err
	}
	svcStr, err := meta.GetServiceStr()
	if err != nil {
		return err
	}
	rStr, err := meta.GetResourceStr()
	if err != nil {
		return err
	}
	requestSchema, err := prov.GetObjectSchema(svcStr, rStr, method.RequestType.Type)
	var execPayload *dto.ExecPayload
	if node.OptExecPayload != nil {
		execPayload, err = p.parseExecPayload(node.OptExecPayload, method.RequestType.GetFormat())
		if err != nil {
			return err
		}
		err = p.analyzeSchemaVsMap(handlerCtx, requestSchema, execPayload.PayloadMap, method)
		if err != nil {
			return err
		}
	}
	sm, err := prov.GetSchemaMap(svcStr, rStr)
	if err != nil {
		return err
	}
	rsc, err := meta.GetResource()
	if err != nil {
		return err
	}
	err = p.buildRequestContext(handlerCtx, node, &meta, sm, httpbuild.NewExecContext(execPayload, rsc))
	if err != nil {
		return err
	}
	p.PrimitiveBuilder.SetTable(node, meta)
	return nil
}

func (p *primitiveGenerator) parseExecPayload(node *sqlparser.ExecVarDef, payloadType string) (*dto.ExecPayload, error) {
	var b []byte
	m := make(map[string][]string)
	var pm map[string]interface{}
	switch val := node.Val.(type) {
	case *sqlparser.SQLVal:
		b = val.Val
	default:
		return nil, fmt.Errorf("payload map of SQL type = '%T' not allowed", val)
	}
	switch payloadType {
	case constants.JsonStr:
		m["Content-Type"] = []string{"application/json"}
		err := json.Unmarshal(b, &pm)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("payload map of declared type = '%T' not allowed", payloadType)
	}
	return &dto.ExecPayload{
		Payload:    b,
		Header:     m,
		PayloadMap: pm,
	}, nil
}

func contains(slice []interface{}, elem interface{}) bool {
	for _, a := range slice {
		if a == elem {
			return true
		}
	}
	return false
}

func (p *primitiveGenerator) analyzeSchemaVsMap(handlerCtx *handler.HandlerContext, schema *metadata.Schema, payload map[string]interface{}, method *metadata.Method) error {
	requiredElements := make(map[string]bool)
	for k, v := range schema.Properties {
		if v.NamedRef != "" {
			ss := schema.SchemaCentral.SchemaRef[v.NamedRef]
			if ss.IsRequired(method) {
				requiredElements[k] = true
			}
		} else {
			ss := v.SchemaRef[k]
			if ss.IsRequired(method) {
				requiredElements[k] = true
			}
		}
	}
	for k, v := range payload {
		subSchema, ok := schema.Properties[k]
		if !ok {
			return fmt.Errorf("schema does not possess payload key '%s'", k)
		}
		var ss metadata.Schema
		if subSchema.NamedRef != "" {
			ss = schema.SchemaCentral.SchemaRef[subSchema.NamedRef]
		} else {
			ss = subSchema.SchemaRef[k]
		}
		switch val := v.(type) {
		case map[string]interface{}:
			delete(requiredElements, k)
			var err error
			err = p.analyzeSchemaVsMap(handlerCtx, &ss, val, method)
			if err != nil {
				return err
			}
		case []interface{}:
			subSchema, sErr := schema.GetPropertySchema(k)
			if sErr != nil {
				return sErr
			}
			arraySchema, itemsErr := subSchema.GetItemsSchema()
			if itemsErr != nil {
				return itemsErr
			}
			delete(requiredElements, k)
			if len(val) > 0 && val[0] != nil {
				switch item := val[0].(type) {
				case map[string]interface{}:
					err := p.analyzeSchemaVsMap(handlerCtx, arraySchema, item, method)
					if err != nil {
						return err
					}
				case string:
					if arraySchema.Type != "string" {
						return fmt.Errorf("array at key '%s' expected to contain elemenst of type 'string' but instead they are type '%T'", k, item)
					}
				default:
					return fmt.Errorf("array at key '%s' does not contain recognisable type '%T'", k, item)
				}
			}
		case string:
			if ss.Type != "string" {
				return fmt.Errorf("key '%s' expected to contain element of type 'string' but instead it is type '%T'", k, val)
			}
			delete(requiredElements, k)
		case int:
			if ss.IsIntegral() {
				delete(requiredElements, k)
				continue
			}
			return fmt.Errorf("key '%s' expected to contain element of type 'int' but instead it is type '%T'", k, val)
		case bool:
			if ss.IsBoolean() {
				delete(requiredElements, k)
				continue
			}
			return fmt.Errorf("key '%s' expected to contain element of type 'bool' but instead it is type '%T'", k, val)
		case float64:
			if ss.IsFloat() {
				delete(requiredElements, k)
				continue
			}
			return fmt.Errorf("key '%s' expected to contain element of type 'float64' but instead it is type '%T'", k, val)
		default:
			return fmt.Errorf("key '%s' of type '%T' not currently supported", k, val)
		}
	}
	if len(requiredElements) != 0 {
		var missingKeys []string
		for k, _ := range requiredElements {
			missingKeys = append(missingKeys, k)
		}
		return fmt.Errorf("required elements not included in suplied object; the following keys are missing: %s.", strings.Join(missingKeys, ", "))
	}
	return nil
}

func (p *primitiveGenerator) analyzeSelect(handlerCtx *handler.HandlerContext, node *sqlparser.Select) error {

	for i, fromExpr := range node.From {
		tbl, err := p.analyzeTableExpr(handlerCtx, fromExpr)
		if err != nil {
			return err
		}
		fromSymTab := symtab.NewHashMapTreeSymTab()
		responseSchema, err := tbl.GetItemsObjectSchema()
		if err != nil {
			return err
		}
		var leafKey interface{} = i
		switch tbl := fromExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			if tbl.As.GetRawVal() != "" {
				leafKey = tbl.As.GetRawVal()
			}
		}
		for colName, col := range responseSchema.Properties {
			colSchema, _ := col.GetSchema(responseSchema.SchemaCentral)
			if colSchema == nil {
				return fmt.Errorf("could not infer column information")
			}
			colEntry := symtab.NewSymTabEntry(
				p.PrimitiveBuilder.GetDRMConfig().GetRelationalType(colSchema.Type),
				colSchema,
			)
			fromSymTab.SetSymbol(colName, colEntry)
		}
		p.PrimitiveBuilder.SetLeaf(leafKey, fromSymTab)
	}
	if len(node.From) == 1 {
		switch ft := node.From[0].(type) {
		case *sqlparser.JoinTableExpr:
			tbl, err := p.analyzeTableExpr(handlerCtx, ft.LeftExpr)
			if err != nil {
				return err
			}
			err = p.analyzeSelectDetail(handlerCtx, node, tbl)
			if err != nil {
				return err
			}
			rhsPb := newPrimitiveGenerator(p.PrimitiveBuilder.GetAst(), handlerCtx)
			tbl, err = rhsPb.analyzeTableExpr(handlerCtx, ft.RightExpr)
			if err != nil {
				return err
			}
			err = rhsPb.analyzeSelectDetail(handlerCtx, node, tbl)
			if err != nil {
				return err
			}
			p.PrimitiveBuilder.SetBuilder(primitivebuilder.NewJoin(p.PrimitiveBuilder, rhsPb.PrimitiveBuilder, handlerCtx, nil))
			return nil
		case *sqlparser.AliasedTableExpr:
			tbl, err := p.analyzeTableExpr(handlerCtx, node.From[0])
			if err != nil {
				return err
			}
			err = p.analyzeSelectDetail(handlerCtx, node, tbl)
			if err != nil {
				return err
			}
			p.PrimitiveBuilder.SetBuilder(primitivebuilder.NewSingleSelect(p.PrimitiveBuilder, handlerCtx, *tbl, p.PrimitiveBuilder.GetInsertPreparedStatementCtx(), p.PrimitiveBuilder.GetSelectPreparedStatementCtx(), nil))
			return nil
		}
	}
	return fmt.Errorf("cannot process complex select just yet")
}

func (p *primitiveGenerator) analyzeSelectDetail(handlerCtx *handler.HandlerContext, node *sqlparser.Select, tbl *taxonomy.ExtendedTableMetadata) error {
	var err error
	valOnlyCols, nonValCols := parserutil.ExtractSelectValColumns(node)
	p.PrimitiveBuilder.SetValOnlyCols(valOnlyCols)
	svcStr, _ := tbl.GetServiceStr()
	rStr, _ := tbl.GetResourceStr()
	provStr, _ := tbl.GetProviderStr()
	if rStr == "dual" { // some bizarre artifact of vitess.io, indicates no table supplied
		tbl.IsLocallyExecutable = true
		if svcStr == "" {
			if nonValCols == 0 && node.Where == nil {
				log.Infoln("val only select looks ok")
				return nil
			}
			err = fmt.Errorf("select values inadequate: expected 0 non-val columns but got %d", nonValCols)
		}
		return err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return err
	}
	method, err := tbl.GetMethod()
	if err != nil {
		return err
	}
	schema, err := prov.GetObjectSchema(svcStr, rStr, method.ResponseType.Type)
	if err != nil {
		return err
	}
	rewrittenWhere, whereErr := p.analyzeWhere(node.Where, schema)
	if whereErr != nil {
		return whereErr
	}
	p.PrimitiveBuilder.SetWhere(rewrittenWhere)
	cols, err := parserutil.ExtractSelectColumnNames(node)
	if err != nil {
		return err
	}
	unsuitableSchemaMsg := "schema unsuitable for select query"
	log.Infoln(fmt.Sprintf("schema.ID = %v", schema.ID))
	log.Infoln(fmt.Sprintf("schema.Items = %v", schema.Items))
	log.Infoln(fmt.Sprintf("schema.Properties = %v", schema.Properties))
	var itemS *metadata.Schema
	itemS, tbl.SelectItemsKey = schema.GetSelectListItems(prov.GetDefaultKeyForSelectItems())
	if itemS == nil {
		return fmt.Errorf(unsuitableSchemaMsg)
	}
	is := itemS.Items
	itemObjS, _ := is.GetSchema(schema.SchemaCentral)
	if itemObjS == nil {
		return fmt.Errorf(unsuitableSchemaMsg)
	}
	if len(cols) == 0 {
		colNames := itemObjS.GetAllColumns()
		for _, v := range colNames {
			cols = append(cols, parserutil.NewUnaliasedColumnHandle(v))
		}
	}
	insertTabulation := itemObjS.Tabulate(false)

	hIds := dto.NewHeirarchyIdentifiers(provStr, svcStr, insertTabulation.GetName(), "")
	selectTabulation := itemObjS.Tabulate(true)
	// TODO: get rid of prefix garbage
	colPrefix := tbl.SelectItemsKey + "[]."
	annotatedInsertTabulation := util.NewAnnotatedTabulation(insertTabulation, hIds)
	tableDTO, err := p.PrimitiveBuilder.GetDRMConfig().GetCurrentTable(hIds, handlerCtx.SQLEngine)
	if err != nil {
		return err
	}

	insPsc, err := p.PrimitiveBuilder.GetDRMConfig().GenerateInsertDML(annotatedInsertTabulation, p.PrimitiveBuilder.GetTxnCounterManager(), tableDTO.GetDiscoveryID())
	if err != nil {
		return err
	}
	for _, col := range cols {
		// TODO: get rid of prefix garbage
		foundSchemaPrefixed := schema.FindByPath(colPrefix + col.Name)
		foundSchema := schema.FindByPath(col.Name)
		if foundSchema == nil {
			foundSchema = foundSchemaPrefixed
		}
		cc, ok := method.Parameters[col.Name]
		if ok && cc.ID == col.Name {
			continue
		}
		if foundSchema == nil && col.IsColumn {
			return fmt.Errorf("column = '%v' is NOT present in either:  - data returned from provider, - acceptable parameters", col)
		}
		selectTabulation.PushBackColumn(metadata.NewColumnDescriptor(col.Alias, col.Name, col.DecoratedColumn, foundSchema, col.Val))
		log.Debugln(fmt.Sprintf("foundSchemaPrefixed = '%v'", foundSchemaPrefixed))
		log.Infoln(fmt.Sprintf("rsc = %T", col))
		log.Infoln(fmt.Sprintf("schema type = %T", schema))
	}

	selPsc, err := p.PrimitiveBuilder.GetDRMConfig().GenerateSelectDML(util.NewAnnotatedTabulation(selectTabulation, hIds), insPsc.TxnCtrlCtrs, node, rewrittenWhere)
	if err != nil {
		return err
	}
	p.PrimitiveBuilder.SetInsertPreparedStatementCtx(&insPsc)
	p.PrimitiveBuilder.SetSelectPreparedStatementCtx(&selPsc)
	p.PrimitiveBuilder.SetColumnOrder(cols)
	whereNames, err := parserutil.ExtractWhereColNames(node.Where)
	if err != nil {
		return err
	}
	for _, w := range whereNames {
		_, ok := method.Parameters[w]
		if ok {
			continue
		}
		log.Infoln(fmt.Sprintf("w = '%s'", w))
		foundSchemaPrefixed := schema.FindByPath(colPrefix + w)
		foundSchema := schema.FindByPath(w)
		if foundSchemaPrefixed == nil && foundSchema == nil {
			return fmt.Errorf("SELECT Where element = '%s' is NOT present in data returned from provider", w)
		}
	}
	if err != nil {
		return err
	}
	havingNames, err := parserutil.ExtractWhereColNames(node.Having)
	if err != nil {
		return err
	}
	for _, w := range havingNames {
		_, ok := method.Parameters[w]
		if ok {
			continue
		}
		log.Infoln(fmt.Sprintf("w = '%s'", w))
		foundSchemaPrefixed := schema.FindByPath(colPrefix + w)
		foundSchema := schema.FindByPath(w)
		if foundSchemaPrefixed == nil && foundSchema == nil {
			return fmt.Errorf("SELECT HAVING element = '%s' is NOT present in data returned from provider", w)
		}
	}
	if err != nil {
		return err
	}
	sm, err := prov.GetSchemaMap(svcStr, rStr)
	if err != nil {
		return err
	}
	err = p.buildRequestContext(handlerCtx, node, tbl, sm, nil)
	if err != nil {
		return err
	}
	return nil
}

func (p *primitiveGenerator) analyzeTableExpr(handlerCtx *handler.HandlerContext, node sqlparser.TableExpr) (*taxonomy.ExtendedTableMetadata, error) {
	err := p.inferHeirarchyAndPersist(handlerCtx, node)
	if err != nil {
		return nil, err
	}
	tbl, err := p.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return nil, err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return nil, err
	}
	method, err := tbl.GetMethod()
	if err != nil {
		return nil, err
	}
	svcStr, err := tbl.GetServiceStr()
	if err != nil {
		return nil, err
	}
	rStr, err := tbl.GetResourceStr()
	if err != nil {
		return nil, err
	}
	schema, err := prov.GetObjectSchema(svcStr, rStr, method.ResponseType.Type)
	if err != nil {
		return nil, err
	}
	unsuitableSchemaMsg := "schema unsuitable for select query"
	log.Infoln(fmt.Sprintf("schema.ID = %v", schema.ID))
	log.Infoln(fmt.Sprintf("schema.Items = %v", schema.Items))
	log.Infoln(fmt.Sprintf("schema.Properties = %v", schema.Properties))
	var itemS *metadata.Schema
	itemS, tbl.SelectItemsKey = schema.GetSelectListItems(prov.GetDefaultKeyForSelectItems())
	if itemS == nil {
		return nil, fmt.Errorf(unsuitableSchemaMsg)
	}
	is := itemS.Items
	itemObjS, _ := is.GetSchema(schema.SchemaCentral)
	if itemObjS == nil {
		return nil, fmt.Errorf(unsuitableSchemaMsg)
	}
	return &tbl, nil
}

func (p *primitiveGenerator) buildRequestContext(handlerCtx *handler.HandlerContext, node sqlparser.SQLNode, meta *taxonomy.ExtendedTableMetadata, schemaMap map[string]metadata.Schema, execContext *httpbuild.ExecContext) error {
	m, err := meta.GetMethod()
	if err != nil {
		return err
	}
	switch m.Protocol {
	case "http":
		prov, err := meta.GetProvider()
		if err != nil {
			return err
		}
		httpArmoury, err := httpbuild.BuildHTTPRequestCtx(handlerCtx, node, prov, m, schemaMap, p.PrimitiveBuilder.GetInsertValOnlyRows(), execContext)
		if err != nil {
			return err
		}
		meta.HttpArmoury = httpArmoury
		return nil
	}
	return fmt.Errorf("protocol '%s' unsupported", m.Protocol)
}

func (p *primitiveGenerator) analyzeInsert(handlerCtx *handler.HandlerContext, node *sqlparser.Insert) error {
	err := p.inferHeirarchyAndPersist(handlerCtx, node)
	if err != nil {
		return err
	}
	tbl, err := p.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return err
	}
	currentService, err := tbl.GetServiceStr()
	if err != nil {
		return err
	}
	currentResource, err := tbl.GetResourceStr()
	if err != nil {
		return err
	}
	insertValOnlyRows, nonValCols, err := parserutil.ExtractInsertValColumns(node)
	if err != nil {
		return err
	}
	p.PrimitiveBuilder.SetInsertValOnlyRows(insertValOnlyRows)
	if nonValCols > 0 {
		return fmt.Errorf("insert not supported for anything but static values: found %d non-static values", nonValCols)
	}

	p.parseComments(node.Comments)

	_, err = checkResource(handlerCtx, prov, currentService, currentResource)
	if err != nil {
		return err
	}

	sm, err := prov.GetSchemaMap(currentService, currentResource)
	if err != nil {
		return err
	}

	err = p.buildRequestContext(handlerCtx, node, &tbl, sm, nil)
	if err != nil {
		return err
	}
	p.PrimitiveBuilder.SetTable(node, tbl)
	return nil
}

func (p *primitiveGenerator) inferHeirarchyAndPersist(handlerCtx *handler.HandlerContext, node sqlparser.SQLNode) error {
	heirarchy, err := taxonomy.GetHeirarchyFromStatement(handlerCtx, node)
	if err != nil {
		return err
	}
	p.persistHerarchyToBuilder(heirarchy, node)
	return err
}

func (p *primitiveGenerator) analyzeDelete(handlerCtx *handler.HandlerContext, node *sqlparser.Delete) error {
	p.parseComments(node.Comments)
	err := p.inferHeirarchyAndPersist(handlerCtx, node)
	if err != nil {
		return err
	}
	tbl, err := p.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return err
	}
	method, err := tbl.GetMethod()
	if err != nil {
		return err
	}
	currentService, err := tbl.GetServiceStr()
	if err != nil {
		return err
	}
	currentResource, err := tbl.GetResourceStr()
	if err != nil {
		return err
	}
	_, err = checkService(handlerCtx, prov, currentService)
	if err != nil {
		return err
	}
	_, err = checkResource(handlerCtx, prov, currentService, currentResource)
	if err != nil {
		return err
	}
	schema, err := prov.GetObjectSchema(currentService, currentResource, method.ResponseType.Type)
	if err != nil {
		return err
	}
	whereErr := p.analyzeSingleTableWhere(node.Where, schema)
	if whereErr != nil {
		return whereErr
	}
	colPrefix := prov.GetDefaultKeyForDeleteItems() + "[]."
	whereNames, err := parserutil.ExtractWhereColNames(node.Where)
	if err != nil {
		return err
	}
	for _, w := range whereNames {
		_, ok := method.Parameters[w]
		if ok {
			continue
		}
		log.Infoln(fmt.Sprintf("w = '%s'", w))
		foundSchemaPrefixed := schema.FindByPath(colPrefix + w)
		foundSchema := schema.FindByPath(w)
		if foundSchemaPrefixed == nil && foundSchema == nil {
			return fmt.Errorf("DELETE Where element = '%s' is NOT present in data returned from provider", w)
		}
	}
	if err != nil {
		return err
	}
	sm, err := prov.GetSchemaMap(currentService, currentResource)
	if err != nil {
		return err
	}
	err = p.buildRequestContext(handlerCtx, node, &tbl, sm, nil)
	if err != nil {
		return err
	}
	p.PrimitiveBuilder.SetTable(node, tbl)
	return err
}

func (p *primitiveGenerator) analyzeDescribe(handlerCtx *handler.HandlerContext, node *sqlparser.DescribeTable) error {
	var err error
	err = p.inferHeirarchyAndPersist(handlerCtx, node)
	if err != nil {
		return err
	}
	tbl, err := p.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return err
	}
	currentService, err := tbl.GetServiceStr()
	if err != nil {
		return err
	}
	currentResource, err := tbl.GetResourceStr()
	if err != nil {
		return err
	}
	_, err = checkService(handlerCtx, prov, currentService)
	if err != nil {
		return err
	}
	_, err = checkResource(handlerCtx, prov, currentService, currentResource)
	if err != nil {
		return err
	}
	return nil
}

func (p *primitiveGenerator) analyzeSleep(handlerCtx *handler.HandlerContext, node *sqlparser.Sleep) error {
	sleepDuration, err := parserutil.ExtractSleepDuration(node)
	if err != nil {
		return err
	}
	if sleepDuration <= 0 {
		return fmt.Errorf("sleep duration %d not allowed, must be > 0", sleepDuration)
	}
	p.PrimitiveBuilder.SetPrimitive(primitivebuilder.NewLocalPrimitive(
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
			msgs := dto.BackendMessages{
				WorkingMessages: []string{
					fmt.Sprintf("Success: slept for %d milliseconds", sleepDuration),
				},
			}
			return dto.NewExecutorOutput(nil, nil, &msgs, nil)
		},
	),
	)
	return err
}

func (p *primitiveGenerator) analyzeShow(handlerCtx *handler.HandlerContext, node *sqlparser.Show) error {
	var err error
	p.parseComments(node.Comments)
	err = p.inferProviderForShow(node, handlerCtx)
	if err != nil {
		return err
	}
	nodeTypeUpperCase := strings.ToUpper(node.Type)
	if p.PrimitiveBuilder.GetProvider() != nil {
		p.PrimitiveBuilder.SetLikeAbleColumns(p.PrimitiveBuilder.GetProvider().GetLikeableColumns(nodeTypeUpperCase))
	}
	colNames, err := parserutil.ExtractShowColNames(node.ShowTablesOpt)
	if err != nil {
		return err
	}
	colUsages, err := parserutil.ExtractShowColUsage(node.ShowTablesOpt)
	if err != nil {
		return err
	}
	switch nodeTypeUpperCase {
	case "AUTH":
		// TODO
	case "INSERT":
		err = p.inferHeirarchyAndPersist(handlerCtx, node)
		if err != nil {
			return err
		}
		tbl, err := p.PrimitiveBuilder.GetTable(node)
		if err != nil {
			return err
		}
		prov, err := tbl.GetProvider()
		if err != nil {
			return err
		}
		currentService, err := tbl.GetServiceStr()
		if err != nil {
			return err
		}
		currentResource, err := tbl.GetResourceStr()
		if err != nil {
			return err
		}
		sm, err := prov.GetSchemaMap(currentService, currentResource)
		if err != nil {
			return err
		}
		p.PrimitiveBuilder.SetInsertSchemaMap(sm)
	case "METHODS":
		err = p.inferHeirarchyAndPersist(handlerCtx, node)
		if err != nil {
			return err
		}
		tbl, err := p.PrimitiveBuilder.GetTable(node)
		if err != nil {
			return err
		}
		currentService, err := tbl.GetServiceStr()
		if err != nil {
			return err
		}
		currentResource, err := tbl.GetResourceStr()
		if err != nil {
			return err
		}
		_, err = checkResource(handlerCtx, p.PrimitiveBuilder.GetProvider(), currentService, currentResource)
		if err != nil {
			return err
		}
		if node.ShowTablesOpt != nil {
			meth := &metadata.Method{}
			err = p.analyzeShowFilter(node, meth)
			if err != nil {
				return err
			}
		}
		return nil
	case "PROVIDERS":
		// TODO
	case "RESOURCES":
		prov, err := handlerCtx.GetProvider(node.OnTable.Qualifier.GetRawVal())
		if err != nil {
			return err
		}
		p.PrimitiveBuilder.SetProvider(prov)
		_, err = p.assembleServiceAndResources(handlerCtx, p.PrimitiveBuilder.GetProvider(), node.OnTable.Name.GetRawVal())
		if err != nil {
			return err
		}
		for _, col := range colNames {
			if !metadata.ResourceKeyExists(col) {
				return fmt.Errorf("SHOW key = '%s' does NOT exist", col)
			}
		}
		for _, colUsage := range colUsages {
			if !metadata.ResourceKeyExists(colUsage.ColName.Name.GetRawVal()) {
				return fmt.Errorf("SHOW key = '%s' does NOT exist", colUsage.ColName.Name.GetRawVal())
			}
			usageErr := parserutil.CheckSqlParserTypeVsResourceColumn(colUsage)
			if usageErr != nil {
				return usageErr
			}
		}
		if node.ShowTablesOpt != nil {
			rsc := &metadata.Resource{}
			err = p.analyzeShowFilter(node, rsc)
			if err != nil {
				return err
			}
		}
	case "SERVICES":
		prov, err := handlerCtx.GetProvider(node.OnTable.Name.GetRawVal())
		if err != nil {
			return err
		}
		p.PrimitiveBuilder.SetProvider(prov)
		for _, col := range colNames {
			if !metadata.ServiceKeyExists(col) {
				return fmt.Errorf("SHOW key = '%s' does NOT exist", col)
			}
		}
		for _, colUsage := range colUsages {
			if !metadata.ServiceKeyExists(colUsage.ColName.Name.GetRawVal()) {
				return fmt.Errorf("SHOW key = '%s' does NOT exist", colUsage.ColName.Name.GetRawVal())
			}
			usageErr := parserutil.CheckSqlParserTypeVsServiceColumn(colUsage)
			if usageErr != nil {
				return usageErr
			}
		}
		if node.ShowTablesOpt != nil {
			svc := &metadata.Service{}
			err = p.analyzeShowFilter(node, svc)
			if err != nil {
				return err
			}
		}
	default:
		err = fmt.Errorf("SHOW statement not supported for '%s'", nodeTypeUpperCase)
	}
	return err
}
