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
	"infraql/internal/iql/httpmiddleware"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/iqlutil"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/metadatavisitors"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/primitivebuilder"
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

type primitiveGenerator struct {
	PrimitiveBuilder *primitivebuilder.PrimitiveBuilder
}

func newPrimitiveGenerator(ast sqlparser.Statement) *primitiveGenerator {
	return &primitiveGenerator{
		PrimitiveBuilder: primitivebuilder.NewPrimitiveBuilder(ast),
	}
}

func (pb *primitiveGenerator) comparisonExprToFilterFunc(table iqlmodel.ITable, parentNode *sqlparser.Show, expr *sqlparser.ComparisonExpr) (func(iqlmodel.ITable) (iqlmodel.ITable, error), error) {
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
		pb.PrimitiveBuilder.SetColVisited(colName, true)
		return retVal, nil
	}
	operatorPredicate, preErr := relational.GetOperatorPredicate(expr.Operator)

	if preErr != nil {
		return nil, preErr
	}

	pb.PrimitiveBuilder.SetColVisited(colName, true)
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

func (pb *primitiveGenerator) inferProviderForShow(node *sqlparser.Show, handlerCtx *handler.HandlerContext) error {
	nodeTypeUpperCase := strings.ToUpper(node.Type)
	switch nodeTypeUpperCase {
	case "AUTH":
		prov, err := handlerCtx.GetProvider(node.OnTable.Name.GetRawVal())
		if err != nil {
			return err
		}
		pb.PrimitiveBuilder.SetProvider(prov)
	case "INSERT":
		prov, err := handlerCtx.GetProvider(node.OnTable.QualifierSecond.GetRawVal())
		if err != nil {
			return err
		}
		pb.PrimitiveBuilder.SetProvider(prov)

	case "METHODS":
		prov, err := handlerCtx.GetProvider(node.OnTable.QualifierSecond.GetRawVal())
		if err != nil {
			return err
		}
		pb.PrimitiveBuilder.SetProvider(prov)
	case "PROVIDERS":
		// no provider, might create some dummy object dunno
	case "RESOURCES":
		prov, err := handlerCtx.GetProvider(node.OnTable.Qualifier.GetRawVal())
		if err != nil {
			return err
		}
		pb.PrimitiveBuilder.SetProvider(prov)
	case "SERVICES":
		prov, err := handlerCtx.GetProvider(node.OnTable.Name.GetRawVal())
		if err != nil {
			return err
		}
		pb.PrimitiveBuilder.SetProvider(prov)
	default:
		return fmt.Errorf("unsuported node type: '%s'", node.Type)
	}
	return nil
}

func (pb *primitiveGenerator) showInstructionExecutor(node *sqlparser.Show, handlerCtx *handler.HandlerContext) dto.ExecutorOutput {
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
			authCtx, err := handlerCtx.GetAuthContext(pb.PrimitiveBuilder.GetProvider().GetProviderString())
			if err == nil {
				var authMeta *metadata.AuthMetadata
				authMeta, err = pb.PrimitiveBuilder.GetProvider().ShowAuth(authCtx)
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
		tbl, err := pb.PrimitiveBuilder.GetTable(node)
		if err != nil {
			return util.GenerateSimpleErroneousOutput(err)
		}
		meth, err := tbl.GetMethod()
		if err != nil {
			rsc, _ := tbl.GetResourceStr()
			return util.GenerateSimpleErroneousOutput(fmt.Errorf("error creating insert statement for %s: %s", rsc, err.Error()))
		}
		pp := prettyprint.NewPrettyPrinter(ppCtx)
		requiredOnly := pb.PrimitiveBuilder.GetCommentDirectives() != nil && pb.PrimitiveBuilder.GetCommentDirectives().IsSet("REQUIRED")
		insertStmt, err := metadatavisitors.ToInsertStatement(node.Columns, meth, pb.PrimitiveBuilder.GetInsertSchemaMap(), extended, pp, requiredOnly)
		tableName, _ := tbl.GetTableName()
		if err != nil {
			return util.GenerateSimpleErroneousOutput(fmt.Errorf("error creating insert statement for %s: %s", tableName, err.Error()))
		}
		stmtStr := fmt.Sprintf(insertStmt, tableName)
		keys = map[string]map[string]interface{}{
			"1": {
				"insert_statement": stmtStr,
			},
		}
	case "METHODS":
		var rsc *metadata.Resource
		rsc, err = pb.PrimitiveBuilder.GetProvider().GetResource(node.OnTable.Qualifier.GetRawVal(), node.OnTable.Name.GetRawVal(), handlerCtx.RuntimeContext)
		methods := rsc.Methods
		tbl, err := pb.PrimitiveBuilder.GetTable(node.OnTable)
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
		resources, columnOrder, err = pb.PrimitiveBuilder.GetProvider().GetResourcesRedacted(svcName, handlerCtx.RuntimeContext, extended)
		var filter func(iqlmodel.ITable) (iqlmodel.ITable, error)
		if err != nil {
			log.Infoln(fmt.Sprintf("table and therefore filter not found for AST, shall procede nil filter"))
		} else {
			filter = pb.PrimitiveBuilder.GetTableFilter()
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
		log.Infoln(fmt.Sprintf("Show For node.Type = '%s': Displaying services for provider = '%s'", node.Type, pb.PrimitiveBuilder.GetProvider().GetProviderString()))
		var services map[string]metadata.Service
		services, columnOrder, err = pb.PrimitiveBuilder.GetProvider().GetProviderServicesRedacted(handlerCtx.RuntimeContext, extended)
		services, err = filterServices(services, pb.PrimitiveBuilder.GetTableFilter(), handlerCtx.RuntimeContext.UseNonPreferredAPIs)
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

func (pb *primitiveGenerator) describeInstructionExecutor(prov provider.IProvider, serviceName string, resourceName string, handlerCtx *handler.HandlerContext, extended bool, full bool) dto.ExecutorOutput {
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

func (pb *primitiveGenerator) selectExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Select, rowSort func(map[string]map[string]interface{}) []string) (plan.IPrimitive, error) {
	if pb.PrimitiveBuilder.GetBuilder() == nil {
		return nil, fmt.Errorf("builder not created for select, cannot proceed")
	}
	err := pb.PrimitiveBuilder.GetBuilder().Build()
	if err != nil {
		return nil, err
	}
	return pb.PrimitiveBuilder.GetBuilder().GetPrimitive(), nil
}

func (pb *primitiveGenerator) insertExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Insert, rowSort func(map[string]map[string]interface{}) []string) (plan.IPrimitive, error) {
	tbl, err := pb.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return nil, err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return nil, err
	}
	insertPrimitive := primitivebuilder.NewHTTPRestPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			var err error
			response, apiErr := httpmiddleware.HttpApiCall(*handlerCtx, prov, tbl.HttpArmoury.Context)
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
	if !pb.PrimitiveBuilder.IsAwait() {
		return insertPrimitive, nil
	}
	return pb.composeAsyncMonitor(handlerCtx, insertPrimitive, tbl)
}

func (pb *primitiveGenerator) localSelectExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Select, rowSort func(map[string]map[string]interface{}) []string) (plan.IPrimitive, error) {
	return primitivebuilder.NewLocalPrimitive(
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			var columnOrder []string
			keys := make(map[string]map[string]interface{})
			row := make(map[string]interface{})
			for idx := range pb.PrimitiveBuilder.GetValOnlyColKeys() {
				col := pb.PrimitiveBuilder.GetValOnlyCol(idx)
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

func (pb *primitiveGenerator) deleteExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Delete) (plan.IPrimitive, error) {
	tbl, err := pb.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return nil, err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return nil, err
	}
	deletePrimitive := primitivebuilder.NewHTTPRestPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			var err error
			response, apiErr := httpmiddleware.HttpApiCall(*handlerCtx, prov, tbl.HttpArmoury.Context)
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
	if !pb.PrimitiveBuilder.IsAwait() {
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

func (pb *primitiveGenerator) generateResultIfNeededfunc(resultMap map[string]map[string]interface{}, body map[string]interface{}, msg *dto.BackendMessages, err error) dto.ExecutorOutput {
	if pb.PrimitiveBuilder.GetCommentDirectives() != nil && pb.PrimitiveBuilder.GetCommentDirectives().IsSet("SHOWRESULTS") {
		return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, resultMap, nil, nil, nil, nil))
	}
	return dto.NewExecutorOutput(nil, body, msg, err)
}

func (pb *primitiveGenerator) execExecutor(handlerCtx *handler.HandlerContext, node *sqlparser.Exec) (plan.IPrimitive, error) {
	var target map[string]interface{}
	tbl, err := pb.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return nil, err
	}
	prov, err := tbl.GetProvider()
	if err != nil {
		return nil, err
	}
	execPrimitive := primitivebuilder.NewHTTPRestPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			var err error
			var columnOrder []string
			response, apiErr := httpmiddleware.HttpApiCall(*handlerCtx, prov, tbl.HttpArmoury.Context)
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
	if !pb.PrimitiveBuilder.IsAwait() {
		return execPrimitive, nil
	}
	return pb.composeAsyncMonitor(handlerCtx, execPrimitive, tbl)
}

func (pb *primitiveGenerator) composeAsyncMonitor(handlerCtx *handler.HandlerContext, precursor plan.IPrimitive, meta taxonomy.ExtendedTableMetadata) (plan.IPrimitive, error) {
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
		pb.PrimitiveBuilder.GetCommentDirectives(),
	)
	primitive, err := asm.GetMonitorPrimitive(meta.HeirarchyObjects, precursor, pl)
	if err != nil {
		return nil, err
	}
	return primitive, err
}
