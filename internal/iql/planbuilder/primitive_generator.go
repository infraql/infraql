package planbuilder

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"infraql/internal/iql/asyncmonitor"
	"infraql/internal/iql/constants"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/iqlutil"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/metadatavisitors"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/relational"
	"infraql/internal/iql/sqltypeutil"
	"infraql/internal/iql/taxonomy"
	"infraql/internal/iql/util"
	"infraql/internal/pkg/prettyprint"

	log "github.com/sirupsen/logrus"
	
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
)

type tblMap map[sqlparser.SQLNode]taxonomy.ExtendedTableMetadata

func (tm tblMap) GetTable(node sqlparser.SQLNode) (taxonomy.ExtendedTableMetadata, error) {
	tbl, ok := tm[node]
	if !ok {
		return taxonomy.ExtendedTableMetadata{}, fmt.Errorf("could not locate table metadata for AST node: %v", node)
	}
	return tbl, nil
}

type primitiveBuilder struct {
	await bool

	ast sqlparser.Statement

	builder builder

	// needed globally for non-heirarchy queries, such as "SHOW SERVICES FROM google;"
	prov                 provider.IProvider
	tableFilter          func(iqlmodel.ITable) (iqlmodel.ITable, error)
	colsVisited          map[string]bool
	likeAbleColumns      []string

	// per table	
	tables tblMap
	
	

	// per query
	columnOrder         []string
	commentDirectives   sqlparser.CommentDirectives

	// per query -- SELECT only
	insertValOnlyRows   map[int]map[int]interface{}
	valOnlyCols         map[int]map[string]interface{}

	// per query -- SHOW INSERT only
	insertSchemaMap           map[string]metadata.Schema
	
	// TODO: universally retire in favour of builder, which returns plan.IPrimitive
	primitive           plan.IPrimitive
	
}

type HTTPRestPrimitive struct {
	Provider provider.IProvider
	Executor func(pc plan.IPrimitiveCtx) dto.ExecutorOutput
}

type MetaDataPrimitive struct {
	Provider provider.IProvider
	Executor func(pc plan.IPrimitiveCtx) dto.ExecutorOutput
}

type LocalPrimitive struct {
	Executor func(pc plan.IPrimitiveCtx) dto.ExecutorOutput
}

func (pr *HTTPRestPrimitive) Execute(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
	if pr.Executor != nil {
		return pr.Executor(pc)
	}
	return dto.NewExecutorOutput(nil, nil, nil, nil)
}

func (pr *MetaDataPrimitive) Execute(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
	if pr.Executor != nil {
		return pr.Executor(pc)
	}
	return dto.NewExecutorOutput(nil, nil, nil, nil)
}

func (pr *LocalPrimitive) Execute(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
	if pr.Executor != nil {
		return pr.Executor(pc)
	}
	return dto.NewExecutorOutput(nil, nil, nil, nil)
}

func NewMetaDataPrimitive(provider provider.IProvider, executor func(pc plan.IPrimitiveCtx) dto.ExecutorOutput) *MetaDataPrimitive {
	return &MetaDataPrimitive{
		Provider: provider,
		Executor: executor,
	}
}

func NewHTTPRestPrimitive(provider provider.IProvider, executor func(pc plan.IPrimitiveCtx) dto.ExecutorOutput) *HTTPRestPrimitive {
	return &HTTPRestPrimitive{
		Provider: provider,
		Executor: executor,
	}
}

func NewLocalPrimitive(executor func(pc plan.IPrimitiveCtx) dto.ExecutorOutput) *LocalPrimitive {
	return &LocalPrimitive{
		Executor: executor,
	}
}

func newPrimitiveBuilder(ast sqlparser.Statement) *primitiveBuilder {
	return &primitiveBuilder{
		ast:                ast,
		tables:             make(map[sqlparser.SQLNode]taxonomy.ExtendedTableMetadata),
		valOnlyCols:        make(map[int]map[string]interface{}),
		insertValOnlyRows:  make(map[int]map[int]interface{}),
		colsVisited:        make(map[string]bool),
	}
}

func (pb *primitiveBuilder) comparisonExprToFilterFunc(table iqlmodel.ITable, parentNode *sqlparser.Show, expr *sqlparser.ComparisonExpr) (func(iqlmodel.ITable) (iqlmodel.ITable, error), error) {
	qualifiedName, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
	}
	if !qualifiedName.Qualifier.IsEmpty() {
		return nil, fmt.Errorf("unsupported qualifier for column: %v", sqlparser.String(qualifiedName))
	}
	colName := qualifiedName.Name.GetRawVal()
	tableContainsKey := table.KeyExists(colName)
	if !tableContainsKey {
		return nil, fmt.Errorf("col name = '%s' not found in table name = '%s'", colName, table.GetName())
	}
	_, lhsValErr := table.GetKeyAsSqlVal(colName)
	if lhsValErr != nil {
		return nil, lhsValErr
	}
	var resolved sqltypes.Value
	var rhsStr string
	switch right := expr.Right.(type) {
	case *sqlparser.SQLVal:
		if right.Type != sqlparser.IntVal && right.Type != sqlparser.StrVal {
			return nil, fmt.Errorf("unexpected: %v", sqlparser.String(expr))
		}
		pv, err := sqlparser.NewPlanValue(right)
		if err != nil {
			return nil, err
		}
		rhsStr = string(right.Val)
		resolved, err = pv.ResolveValue(nil)
		if err != nil {
			return nil, err
		}
	case sqlparser.BoolVal:
		var resErr error
		resolved, resErr = sqltypeutil.InterfaceToSQLType(right == true)
		if resErr != nil {
			return nil, resErr
		}
	default:
		return nil, fmt.Errorf("unexpected: %v", sqlparser.String(right))
	}
	var retVal func(iqlmodel.ITable) (iqlmodel.ITable, error)
	if expr.Operator == sqlparser.LikeStr || expr.Operator == sqlparser.NotLikeStr {
		likeRegexp, err := regexp.Compile(iqlutil.TranslateLikeToRegexPattern(rhsStr))
		if err != nil {
			return nil, err
		}
		retVal = relational.ConstructLikePredicateFilter(colName, likeRegexp, expr.Operator == sqlparser.NotLikeStr)
		pb.colsVisited[colName] = true
		return retVal, nil
	}
	operatorPredicate, preErr := relational.GetOperatorPredicate(expr.Operator)

	if preErr != nil {
		return nil, preErr
	}

	pb.colsVisited[colName] = true
	return relational.ConstructTablePredicateFilter(colName, resolved, operatorPredicate), nil
}

func getProviderServiceMap(item metadata.Service, extended bool) map[string]interface{} {
	var retVal map[string]interface{}
	retVal = map[string]interface{}{
		"id":    item.ID,
		"name":  item.Name,
		"title": item.Title,
	}
	if extended {
		retVal["description"] = item.Description
		retVal["version"] = item.Version
		retVal["preferred"] = item.Preferred
	}
	return retVal
}

func convertProviderServicesToMap(services map[string]metadata.Service, extended bool) map[string]map[string]interface{} {
	retVal := make(map[string]map[string]interface{})
	for k, v := range services {
		retVal[k] = getProviderServiceMap(v, extended)
	}
	return retVal
}

func filterResources(resources map[string]metadata.Resource, tableFilter func(iqlmodel.ITable) (iqlmodel.ITable, error)) (map[string]metadata.Resource, error) {
	var err error
	if tableFilter != nil {
		filteredResources := make(map[string]metadata.Resource)
		for k, rsc := range resources {
			filteredResource, filterErr := tableFilter(&rsc)
			if filterErr == nil && filteredResource != nil {
				filteredResources[k] = *(filteredResource.(*metadata.Resource))
			}
			if filterErr != nil {
				err = filterErr
			}
		}
		resources = filteredResources
	}
	return resources, err
}

func filterServices(services map[string]metadata.Service, tableFilter func(iqlmodel.ITable) (iqlmodel.ITable, error), useNonPreferredAPIs bool) (map[string]metadata.Service, error) {
	var err error
	if tableFilter != nil {
		filteredServices := make(map[string]metadata.Service)
		for k, svc := range services {
			if useNonPreferredAPIs || svc.Preferred {
				filteredService, filterErr := tableFilter(&svc)
				if filterErr == nil && filteredService != nil {
					filteredServices[k] = *(filteredService.(*metadata.Service))
				}
				if filterErr != nil {
					err = filterErr
				}
			}
		}
		services = filteredServices
	}
	return services, err
}

func filterMethods(methods map[string]metadata.Method, tableFilter func(iqlmodel.ITable) (iqlmodel.ITable, error)) (map[string]metadata.Method, error) {
	var err error
	if tableFilter != nil {
		filteredMethods := make(map[string]metadata.Method)
		for k, rsc := range methods {
			filteredMethod, filterErr := tableFilter(&rsc)
			if filterErr == nil && filteredMethod != nil {
				filteredMethods[k] = *(filteredMethod.(*metadata.Method))
			}
			if filterErr != nil {
				err = filterErr
			}
		}
		methods = filteredMethods
	}
	return methods, err
}

func (pb *primitiveBuilder) inferProviderForShow(node *sqlparser.Show, handlerCtx *handler.HandlerContext) error {
	nodeTypeUpperCase := strings.ToUpper(node.Type)
	switch nodeTypeUpperCase {
	case "AUTH":
		prov, err := handlerCtx.GetProvider(node.OnTable.Name.GetRawVal())
		if err != nil {
			return err
		}
		pb.prov = prov
	case "INSERT":
		prov, err := handlerCtx.GetProvider(node.OnTable.QualifierSecond.GetRawVal())
		if err != nil {
			return err
		}
		pb.prov = prov
		
	case "METHODS":
		prov, err := handlerCtx.GetProvider(node.OnTable.QualifierSecond.GetRawVal())
		if err != nil {
			return err
		}
		pb.prov = prov
	case "PROVIDERS":
		// no provider, might create some dummy object dunno
	case "RESOURCES":
		prov, err := handlerCtx.GetProvider(node.OnTable.Qualifier.GetRawVal())
		if err != nil {
			return err
		}
		pb.prov = prov
	case "SERVICES":
		prov, err := handlerCtx.GetProvider(node.OnTable.Name.GetRawVal())
		if err != nil {
			return err
		}
		pb.prov = prov
	default:
		return fmt.Errorf("unsuported node type: '%s'", node.Type)
	}
	return nil
}

func (pb *primitiveBuilder) showInstructionExecutor(node *sqlparser.Show, handlerCtx *handler.HandlerContext) dto.ExecutorOutput {
	extended := strings.TrimSpace(strings.ToUpper(node.Extended)) == "EXTENDED"
	nodeTypeUpperCase := strings.ToUpper(node.Type)
	var keys map[string]map[string]interface{}
	var columnOrder []string
	var err error
	var filter func(interface{}) (iqlmodel.ITable, error)
	log.Infoln(fmt.Sprintf("filter type = %T", filter))
	switch nodeTypeUpperCase {
	case "AUTH":
		log.Infoln(fmt.Sprintf("Show For node.Type = '%s'", node.Type))
		if err == nil {
			authCtx, err := handlerCtx.GetAuthContext(pb.prov.GetProviderString())
			if err == nil {
				var authMeta *metadata.AuthMetadata
				authMeta, err = pb.prov.ShowAuth(authCtx)
				if err == nil {
					keys = map[string]map[string]interface{}{
						"1": authMeta.ToMap(),
					}
					columnOrder = authMeta.GetHeaders()
				}
			}
		}
	case "INSERT":
		ppCtx := prettyprint.NewPrettyPrintContext(
			handlerCtx.RuntimeContext.OutputFormat == constants.PrettyTextStr,
			constants.DefaultPrettyPrintIndent,
			constants.DefaultPrettyPrintBaseIndent,
			"'",
		)
		tbl, err := pb.tables.GetTable(node)
		if err != nil {
			return util.GenerateSimpleErroneousOutput(err)
		}
		meth, err := tbl.GetMethod()
		if err != nil {
			rsc, _ := tbl.GetResourceStr()
			return util.GenerateSimpleErroneousOutput(fmt.Errorf("error creating insert statement for %s: %s", rsc, err.Error()))
		}
		pp := prettyprint.NewPrettyPrinter(ppCtx)
		insertStmt, err := metadatavisitors.ToInsertStatement(node.Columns, meth, pb.insertSchemaMap, extended, pp)
		tableName, _  := tbl.GetTableName()
		if err != nil {
			return util.GenerateSimpleErroneousOutput(fmt.Errorf("error creating insert statement for %s: %s", tableName, err.Error()))
		}
		stmtStr := fmt.Sprintf(insertStmt, tableName)
		keys = map[string]map[string]interface{}{
			"1": map[string]interface{}{
				"insert_statement": stmtStr,
			},
		}
	case "METHODS":
		var rsc *metadata.Resource
		rsc, err = pb.prov.GetResource(node.OnTable.Qualifier.GetRawVal(), node.OnTable.Name.GetRawVal(), handlerCtx.RuntimeContext)
		methods := rsc.Methods
		tbl, err := pb.tables.GetTable(node.OnTable)
		var filter func(iqlmodel.ITable) (iqlmodel.ITable, error)
		if err != nil {
			log.Infoln(fmt.Sprintf("table and therefore filter not found for AST, shall procede nil filter"))
		} else {
			filter = tbl.TableFilter
		}
		methods, err = filterMethods(methods, filter)
		if err != nil {
			return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, keys, columnOrder, nil, err, nil))
		}
		methodKeys := make(map[string]map[string]interface{})
		var i int = 0
		for _, method := range methods {
			methMap := method.ToPresentationMap(extended)
			methodKeys[strconv.Itoa(i)] = methMap
			i++
			columnOrder = method.GetColumnOrder(extended)
		}
		keys = methodKeys
	case "PROVIDERS":
		keys = provider.GetSupportedProviders(extended)
	case "RESOURCES":
		svcName := node.OnTable.Name.GetRawVal()
		if svcName == "" {
			return prepareErroneousResultSet(keys, columnOrder, fmt.Errorf("no service designated from which to resolve resources"))
		}
		var resources map[string]metadata.Resource
		resources, columnOrder, err = pb.prov.GetResourcesRedacted(svcName, handlerCtx.RuntimeContext, extended)
		tbl, err := pb.tables.GetTable(node.OnTable)
		var filter func(iqlmodel.ITable) (iqlmodel.ITable, error)
		if err != nil {
			log.Infoln(fmt.Sprintf("table and therefore filter not found for AST, shall procede nil filter"))
		} else {
			filter = tbl.TableFilter
		}
		resources, err = filterResources(resources, filter)
		if err != nil {
			return prepareErroneousResultSet(keys, columnOrder, err)
		}
		keys = make(map[string]map[string]interface{})
		for k, v := range resources {
			keys[k] = v.ToMap(extended)
		}
	case "SERVICES":
		log.Infoln(fmt.Sprintf("Show For node.Type = '%s': Displaying services for provider = '%s'", node.Type, pb.prov.GetProviderString()))
		var services map[string]metadata.Service
		services, columnOrder, err = pb.prov.GetProviderServicesRedacted(handlerCtx.RuntimeContext, extended)
		services, err = filterServices(services, pb.tableFilter, handlerCtx.RuntimeContext.UseNonPreferredAPIs)
		if err != nil {
			return prepareErroneousResultSet(keys, columnOrder, err)
		}
		keys = convertProviderServicesToMap(services, extended)
	}
	return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, keys, columnOrder, nil, err, nil))
}

func prepareErroneousResultSet(rowMap map[string]map[string]interface{}, columnOrder []string, err error) dto.ExecutorOutput {
	return util.PrepareResultSet(
		dto.NewPrepareResultSetDTO(
			nil,
			rowMap,
			columnOrder,
			nil,
			err,
			nil,
		),
	)
}

func (pb *primitiveBuilder) describeInstructionExecutor(prov provider.IProvider,serviceName string, resourceName string, handlerCtx *handler.HandlerContext, extended bool, full bool) dto.ExecutorOutput {
	var schema *metadata.Schema
	schema, columnOrder, err := prov.DescribeResource(serviceName, resourceName, handlerCtx.RuntimeContext, extended, full)
	descriptionMap := schema.ToDescriptionMap(extended)
	keys := make(map[string]map[string]interface{})
	for k, v := range descriptionMap {
		switch val := v.(type) {
		case map[string]interface{}:
			keys[k] = val
		}
	}
	return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, keys, columnOrder, util.DescribeRowSort, err, nil))
}

func extractStringFromMap(m map[string]interface{}, k string) string {
	var retVal string
	p, ok := m[k]
	if ok {
		s, ok := p.(string)
		if ok {
			retVal = s
		}
	}
	return retVal
}

func (pb *primitiveBuilder) selectExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Select, rowSort func(map[string]map[string]interface{}) []string) (plan.IPrimitive, error) {
	if pb.builder == nil {
		return nil, fmt.Errorf("builder not created for select, cannot proceed")
	}
	err := pb.builder.Build()
	if err != nil {
		return nil, err
	}
	return pb.builder.GetPrimitive(), nil
}

func (pb *primitiveBuilder) insertExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Insert, rowSort func(map[string]map[string]interface{}) []string) (plan.IPrimitive, error) {
	tbl, err := pb.tables.GetTable(node)
	if err != nil {
		return nil, err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return nil, err
	}
	insertPrimitive := NewHTTPRestPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			var err error
			response, apiErr := httpApiCall(*handlerCtx, prov, tbl.HttpArmoury.Context)
			if apiErr != nil {
				return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, nil, nil, rowSort, apiErr, nil))
			}
			target, err := httpexec.ProcessHttpResponse(response)
			if err != nil {
				return util.PrepareResultSet(dto.NewPrepareResultSetDTO(
					nil,
					nil,
					nil,
					nil,
					err,
					nil,
				))
			}
			log.Infoln(fmt.Sprintf("target = %v", target))
			items, ok := target[prov.GetDefaultKeyForSelectItems()]
			keys := make(map[string]map[string]interface{})
			if ok {
				iArr, ok := items.([]interface{})
				if ok && len(iArr) > 0 {
					for i := range iArr {
						item, ok := iArr[i].(map[string]interface{})
						if ok {
							keys[strconv.Itoa(i)] = item
						}
					}
				}
			}
			msgs := dto.BackendMessages{}
			if err == nil {
				msgs.WorkingMessages = generateSuccessMessagesFromHeirarchy(tbl)
			}
			return dto.NewExecutorOutput(nil, target, &msgs, err)
		})
	if !pb.await {
		return insertPrimitive, nil
	}
	return pb.composeAsyncMonitor(handlerCtx, insertPrimitive, tbl)
}

func (pb *primitiveBuilder) localSelectExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Select, rowSort func(map[string]map[string]interface{}) []string) (plan.IPrimitive, error) {
	return NewLocalPrimitive(
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			var columnOrder []string
			keys := make(map[string]map[string]interface{})
			row := make(map[string]interface{})
			for idx := range pb.valOnlyCols {
				col := pb.valOnlyCols[idx]
				if col != nil {
					var alias string
					var val interface{}
					for k, v := range col {
						alias = k
						val = v
						break
					}
					if alias == "" {
						alias = "val_" + strconv.Itoa(idx)
					}
					row[alias] = val
					columnOrder = append(columnOrder, alias)
				}
			}
			keys["0"] = row
			return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, keys, columnOrder, rowSort, nil, nil))
		}), nil
}

func (pb *primitiveBuilder) deleteExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Delete) (plan.IPrimitive, error) {
	tbl, err := pb.tables.GetTable(node)
	if err != nil {
		return nil, err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return nil, err
	} 
	deletePrimitive := NewHTTPRestPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			var err error
			response, apiErr := httpApiCall(*handlerCtx, prov, tbl.HttpArmoury.Context)
			if apiErr != nil {
				return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, nil, nil, nil, apiErr, nil))
			}
			target, err := httpexec.ProcessHttpResponse(response)
			if err != nil {
				return util.PrepareResultSet(dto.NewPrepareResultSetDTO(
					nil,
					nil,
					nil,
					nil,
					err,
					nil,
				))
			}
			log.Infoln(fmt.Sprintf("target = %v", target))
			items, ok := target[prov.GetDefaultKeyForDeleteItems()]
			keys := make(map[string]map[string]interface{})
			if ok {
				iArr, ok := items.([]interface{})
				if ok && len(iArr) > 0 {
					for i := range iArr {
						item, ok := iArr[i].(map[string]interface{})
						if ok {
							keys[strconv.Itoa(i)] = item
						}
					}
				}
			}
			msgs := dto.BackendMessages{}
			if err == nil {
				msgs.WorkingMessages = generateSuccessMessagesFromHeirarchy(tbl)
			}
			return pb.generateResultIfNeededfunc(keys, target, &msgs, err)
		})
	if !pb.await {
		return deletePrimitive, nil
	}
	return pb.composeAsyncMonitor(handlerCtx, deletePrimitive, tbl)
}

func generateSuccessMessagesFromHeirarchy(meta taxonomy.ExtendedTableMetadata) []string {
	successMsgs := []string{
		"The operation completed successfully",
	}
	m, methodErr := meta.GetMethod()
	prov, err := meta.GetProvider()
	if methodErr == nil && err == nil && m != nil && prov != nil && prov.GetProviderString() == "google" {
		if m.Name == "get" || m.Name == "list" || m.Name == "aggregatedList" {
			successMsgs = []string{
				"The operation completed successfully, consider using a SELECT statement if you are performing an operation that returns data, see https://help.infraql.io/SELECT.html for more information",
			}
		}
	}
	return successMsgs
}

func (pb *primitiveBuilder) generateResultIfNeededfunc(resultMap map[string]map[string]interface{}, body map[string]interface{}, msg *dto.BackendMessages, err error) dto.ExecutorOutput {
	if pb.commentDirectives != nil && pb.commentDirectives.IsSet("SHOWRESULTS") {
		return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, resultMap, nil, nil, nil, nil))
	}
	return dto.NewExecutorOutput(nil, body, msg, err)
}

func (pb *primitiveBuilder) execExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Exec) (plan.IPrimitive, error) {
	var target map[string]interface{}
	tbl, err := pb.tables.GetTable(node)
	if err != nil {
		return nil, err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return nil, err
	}
	execPrimitive := NewHTTPRestPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			var err error
			var columnOrder []string
			response, apiErr := httpApiCall(*handlerCtx, prov, tbl.HttpArmoury.Context)
			if apiErr != nil {
				return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, nil, nil, nil, apiErr, nil))
			}
			target, err = httpexec.ProcessHttpResponse(response)
			if err != nil {
				return util.PrepareResultSet(dto.NewPrepareResultSetDTO(
					nil,
					nil,
					nil,
					nil,
					err,
					nil,
				))
			}
			log.Infoln(fmt.Sprintf("target = %v", target))
			items, ok := target[prov.GetDefaultKeyForSelectItems()]
			keys := make(map[string]map[string]interface{})
			if ok {
				iArr, ok := items.([]interface{})
				if ok && len(iArr) > 0 {
					for i := range iArr {
						item, ok := iArr[i].(map[string]interface{})
						if ok {
							keys[strconv.Itoa(i)] = item
						}
					}
				}
			} else {
				keys["0"] = target
			}
			// optional data return pattern to be included in grammar subsequently
			// return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, keys, columnOrder, nil, err, nil))
			log.Debugln(fmt.Sprintf("keys = %v", keys))
			log.Debugln(fmt.Sprintf("columnOrder = %v", columnOrder))
			msgs := dto.BackendMessages{}
			if err == nil {
				msgs.WorkingMessages = generateSuccessMessagesFromHeirarchy(tbl)
			}
			return pb.generateResultIfNeededfunc(keys, target, &msgs, err)
		})
	if !pb.await {
		return execPrimitive, nil
	}
	return pb.composeAsyncMonitor(handlerCtx, execPrimitive, tbl)
}

func (pb *primitiveBuilder) composeAsyncMonitor(handlerCtx *handler.HandlerContext, precursor plan.IPrimitive, meta taxonomy.ExtendedTableMetadata) (plan.IPrimitive, error) {
	prov, err := meta.GetProvider()
	if err != nil {
		return nil, err
	}
	asm, err := asyncmonitor.NewAsyncMonitor(prov)
	if err != nil {
		return nil, err
	}
	authCtx, err := handlerCtx.GetAuthContext(prov.GetProviderString())
	if err != nil {
		return nil, err
	}
	pl := dto.NewBasicPrimitiveContext(
		nil,
		authCtx,
		handlerCtx.Outfile,
		handlerCtx.OutErrFile,
		pb.commentDirectives,
	)
	primitive, err := asm.GetMonitorPrimitive(meta.HeirarchyObjects, precursor, pl)
	if err != nil {
		return nil, err
	}
	return primitive, err
}
