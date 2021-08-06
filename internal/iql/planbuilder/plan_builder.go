package planbuilder

import (
	"fmt"
	"infraql/internal/iql/astvisit"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/iqlerror"
	"infraql/internal/iql/parse"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/primitivebuilder"
	"infraql/internal/iql/util"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	log "github.com/sirupsen/logrus"
)

func createInstructionFor(handlerCtx *handler.HandlerContext, stmt sqlparser.Statement) (plan.IPrimitive, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Auth:
		return handleAuth(handlerCtx, stmt)
	case *sqlparser.AuthRevoke:
		return handleAuthRevoke(handlerCtx, stmt)
	case *sqlparser.Begin:
		return nil, iqlerror.GetStatementNotSupportedError("TRANSACTION: BEGIN")
	case *sqlparser.Commit:
		return nil, iqlerror.GetStatementNotSupportedError("TRANSACTION: COMMIT")
	case *sqlparser.DBDDL:
		return nil, iqlerror.GetStatementNotSupportedError(fmt.Sprintf("unsupported: Database DDL %v", sqlparser.String(stmt)))
	case *sqlparser.DDL:
		return nil, iqlerror.GetStatementNotSupportedError("DDL")
	case *sqlparser.Delete:
		return handleDelete(handlerCtx, stmt)
	case *sqlparser.DescribeTable:
		return handleDescribe(handlerCtx, stmt)
	case *sqlparser.Exec:
		return handleExec(handlerCtx, stmt)
	case *sqlparser.Explain:
		return nil, iqlerror.GetStatementNotSupportedError("EXPLAIN")
	case *sqlparser.Insert:
		return handleInsert(handlerCtx, stmt)
	case *sqlparser.OtherRead, *sqlparser.OtherAdmin:
		return nil, iqlerror.GetStatementNotSupportedError("OTHER")
	case *sqlparser.Rollback:
		return nil, iqlerror.GetStatementNotSupportedError("TRANSACTION: ROLLBACK")
	case *sqlparser.Savepoint:
		return nil, iqlerror.GetStatementNotSupportedError("TRANSACTION: SAVEPOINT")
	case *sqlparser.Select:
		return handleSelect(handlerCtx, stmt)
	case *sqlparser.Set:
		return nil, iqlerror.GetStatementNotSupportedError("SET")
	case *sqlparser.SetTransaction:
		return nil, iqlerror.GetStatementNotSupportedError("SET TRANSACTION")
	case *sqlparser.Show:
		return handleShow(handlerCtx, stmt)
	case *sqlparser.Sleep:
		return handleSleep(handlerCtx, stmt)
	case *sqlparser.SRollback:
		return nil, iqlerror.GetStatementNotSupportedError("TRANSACTION: SROLLBACK")
	case *sqlparser.Release:
		return nil, iqlerror.GetStatementNotSupportedError("TRANSACTION: RELEASE")
	case *sqlparser.Union:
		return nil, iqlerror.GetStatementNotSupportedError("UNION")
	case *sqlparser.Update:
		return nil, iqlerror.GetStatementNotSupportedError("UPDATE")
	case *sqlparser.Use:
		return handleUse(handlerCtx, stmt)
	}
	return nil, iqlerror.GetStatementNotSupportedError(fmt.Sprintf("BUG: unexpected statement type: %T", stmt))
}

func handleAuth(handlerCtx *handler.HandlerContext, node *sqlparser.Auth) (plan.IPrimitive, error) {
	primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
	prov, err := handlerCtx.GetProvider(node.Provider)
	if err != nil {
		return nil, err
	}
	err = primitiveGenerator.analyzeStatement(handlerCtx, node)
	if err != nil {
		log.Debugln(fmt.Sprintf("err = %s", err.Error()))
		return nil, err
	}
	authCtx, authErr := handlerCtx.GetAuthContext(node.Provider)
	if authErr != nil {
		return nil, authErr
	}
	return primitivebuilder.NewMetaDataPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			authType := strings.ToLower(node.Type)
			if node.KeyFilePath != "" {
				authCtx.KeyFilePath = node.KeyFilePath
			}
			_, err := prov.Auth(authCtx, authType, true)
			return dto.NewExecutorOutput(nil, nil, nil, err)
		}), nil
}

func handleAuthRevoke(handlerCtx *handler.HandlerContext, node *sqlparser.AuthRevoke) (plan.IPrimitive, error) {
	primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
	err := primitiveGenerator.analyzeStatement(handlerCtx, node)
	if err != nil {
		return nil, err
	}
	prov, err := handlerCtx.GetProvider(node.Provider)
	if err != nil {
		return nil, err
	}
	authCtx, authErr := handlerCtx.GetAuthContext(node.Provider)
	if authErr != nil {
		return nil, authErr
	}
	return primitivebuilder.NewMetaDataPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			return dto.NewExecutorOutput(nil, nil, nil, prov.AuthRevoke(authCtx))
		}), nil
}

func handleDescribe(handlerCtx *handler.HandlerContext, node *sqlparser.DescribeTable) (plan.IPrimitive, error) {
	primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
	err := primitiveGenerator.analyzeStatement(handlerCtx, node)
	if err != nil {
		return nil, err
	}
	md, err := primitiveGenerator.PrimitiveBuilder.GetTable(node)
	if err != nil {
		return nil, err
	}
	prov, err := md.GetProvider()
	if err != nil {
		return nil, err
	}
	var extended bool = strings.TrimSpace(strings.ToUpper(node.Extended)) == "EXTENDED"
	var full bool = strings.TrimSpace(strings.ToUpper(node.Full)) == "FULL"
	svcStr, err := md.GetServiceStr()
	if err != nil {
		return nil, err
	}
	rStr, err := md.GetResourceStr()
	if err != nil {
		return nil, err
	}
	return primitivebuilder.NewMetaDataPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			return primitiveGenerator.describeInstructionExecutor(prov, svcStr, rStr, handlerCtx, extended, full)
		}), nil
}

func handleSelect(handlerCtx *handler.HandlerContext, node *sqlparser.Select) (plan.IPrimitive, error) {
	if !handlerCtx.RuntimeContext.TestWithoutApiCalls {
		primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
		err := primitiveGenerator.analyzeStatement(handlerCtx, node)
		if err != nil {
			return nil, err
		}
		isLocallyExecutable := true
		for _, val := range primitiveGenerator.PrimitiveBuilder.GetTables() {
			isLocallyExecutable = isLocallyExecutable && val.IsLocallyExecutable
		}
		if isLocallyExecutable {
			return primitiveGenerator.localSelectExecutor(handlerCtx, node, util.DefaultRowSort)
		}
		return primitiveGenerator.selectExecutor(handlerCtx, node, util.DefaultRowSort)
	}
	return primitivebuilder.NewLocalPrimitive(nil), nil
}

func handleDelete(handlerCtx *handler.HandlerContext, node *sqlparser.Delete) (plan.IPrimitive, error) {
	if !handlerCtx.RuntimeContext.TestWithoutApiCalls {
		primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
		err := primitiveGenerator.analyzeStatement(handlerCtx, node)
		if err != nil {
			return nil, err
		}
		return primitiveGenerator.deleteExecutor(handlerCtx, node)
	} else {
		return primitivebuilder.NewHTTPRestPrimitive(nil, nil, nil, nil), nil
	}
	return nil, nil
}

func handleInsert(handlerCtx *handler.HandlerContext, node *sqlparser.Insert) (plan.IPrimitive, error) {
	if !handlerCtx.RuntimeContext.TestWithoutApiCalls {
		primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
		err := primitiveGenerator.analyzeStatement(handlerCtx, node)
		if err != nil {
			return nil, err
		}
		return primitiveGenerator.insertExecutor(handlerCtx, node, util.DefaultRowSort)
	} else {
		return primitivebuilder.NewHTTPRestPrimitive(nil, nil, nil, nil), nil
	}
	return nil, nil
}

func handleExec(handlerCtx *handler.HandlerContext, node *sqlparser.Exec) (plan.IPrimitive, error) {
	if !handlerCtx.RuntimeContext.TestWithoutApiCalls {
		primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
		err := primitiveGenerator.analyzeStatement(handlerCtx, node)
		if err != nil {
			return nil, err
		}
		return primitiveGenerator.execExecutor(handlerCtx, node)
	}
	return primitivebuilder.NewHTTPRestPrimitive(nil, nil, nil, nil), nil
}

func handleShow(handlerCtx *handler.HandlerContext, node *sqlparser.Show) (plan.IPrimitive, error) {
	primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
	err := primitiveGenerator.analyzeStatement(handlerCtx, node)
	if err != nil {
		return nil, err
	}
	return primitivebuilder.NewMetaDataPrimitive(
		primitiveGenerator.PrimitiveBuilder.GetProvider(),
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			return primitiveGenerator.showInstructionExecutor(node, handlerCtx)
		}), nil
}

func handleSleep(handlerCtx *handler.HandlerContext, node *sqlparser.Sleep) (plan.IPrimitive, error) {
	primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
	err := primitiveGenerator.analyzeStatement(handlerCtx, node)
	if err != nil {
		return nil, err
	}
	return primitiveGenerator.PrimitiveBuilder.GetPrimitive(), nil
}

func handleUse(handlerCtx *handler.HandlerContext, node *sqlparser.Use) (plan.IPrimitive, error) {
	primitiveGenerator := newPrimitiveGenerator(node, handlerCtx)
	err := primitiveGenerator.analyzeStatement(handlerCtx, node)
	if err != nil {
		return nil, err
	}
	return primitivebuilder.NewMetaDataPrimitive(
		primitiveGenerator.PrimitiveBuilder.GetProvider(),
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			handlerCtx.CurrentProvider = node.DBName.GetRawVal()
			return dto.NewExecutorOutput(nil, nil, nil, nil)
		}), nil
}

func createErroneousPlan(handlerCtx *handler.HandlerContext, qPlan *plan.Plan, rowSort func(map[string]map[string]interface{}) []string, err error) (*plan.Plan, error) {
	qPlan.Instructions = primitivebuilder.NewLocalPrimitive(func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
		return util.PrepareResultSet(
			dto.PrepareResultSetDTO{
				OutputBody:  nil,
				Msg:         nil,
				RowMap:      nil,
				ColumnOrder: nil,
				RowSort:     rowSort,
				Err:         err,
			},
		)
	})
	return qPlan, err
}

func BuildPlanFromContext(handlerCtx *handler.HandlerContext) (*plan.Plan, error) {
	planKey := handlerCtx.Query
	if qp, ok := handlerCtx.LRUCache.Get(planKey); ok {
		log.Infoln("retrieving query plan from cache")
		pl, ok := qp.(*plan.Plan)
		if ok {
			pl.Instructions.SetTxnId(handlerCtx.TxnCounterMgr.GetNextTxnId())
			return pl, nil
		}
		return qp.(*plan.Plan), nil
	}
	qPlan := &plan.Plan{
		Original: handlerCtx.RawQuery,
	}
	var err error
	var rowSort func(map[string]map[string]interface{}) []string
	var statement sqlparser.Statement
	statement, err = parse.ParseQuery(handlerCtx.Query)
	if err != nil {
		return createErroneousPlan(handlerCtx, qPlan, rowSort, err)
	}
	s := sqlparser.String(statement)
	result, err := sqlparser.RewriteAST(statement)
	if err != nil {
		return createErroneousPlan(handlerCtx, qPlan, rowSort, err)
	}
	vis := astvisit.NewDRMAstVisitor("iql_query_id", false)
	statement.Accept(vis)
	provStrSlice := astvisit.ExtractProviderStrings(result.AST)
	for _, p := range provStrSlice {
		_, err := handlerCtx.GetProvider(p)
		if err != nil {
			return nil, err
		}
	}
	log.Infoln("Recovered query: " + s)
	log.Infoln("Recovered query from vis: " + vis.GetRewrittenQuery())
	if err != nil {
		return createErroneousPlan(handlerCtx, qPlan, rowSort, err)
	}
	statementType := sqlparser.ASTToStatementType(result.AST)
	if err != nil {
		return createErroneousPlan(handlerCtx, qPlan, rowSort, err)
	}
	qPlan.Type = statementType

	instructions, createInstructionError := createInstructionFor(handlerCtx, result.AST)
	if createInstructionError != nil {
		err = createInstructionError
	}

	qPlan.Instructions = instructions

	if instructions != nil {
		handlerCtx.LRUCache.Set(planKey, qPlan)
	}

	return qPlan, err
}
