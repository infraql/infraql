package planbuilder

import (

	"fmt"
	"strconv"
	
	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/taxonomy"
	"infraql/internal/iql/util"


	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"

	log "github.com/sirupsen/logrus"
)

type builder interface{

	Build() error

	GetPrimitive() plan.IPrimitive
}

type singleSelect struct {
	primitiveBuilder *primitiveBuilder
	handlerCtx       *handler.HandlerContext
	tableMeta        taxonomy.ExtendedTableMetadata
	rowSort func(map[string]map[string]interface{}) []string
}

type join struct {
	lhsPb, rhsPb     *primitiveBuilder
	lhs, rhs         builder
	handlerCtx       *handler.HandlerContext
	rowSort func(map[string]map[string]interface{}) []string
}


func newSingleSelect(pb *primitiveBuilder, handlerCtx *handler.HandlerContext, tableMeta taxonomy.ExtendedTableMetadata, rowSort func(map[string]map[string]interface{}) []string) *singleSelect {
	return &singleSelect{
		primitiveBuilder: pb,
		handlerCtx: handlerCtx,
		tableMeta: tableMeta,
		rowSort: rowSort,
	}
}

func newJoin(lhsPb *primitiveBuilder, rhsPb *primitiveBuilder, handlerCtx *handler.HandlerContext, rowSort func(map[string]map[string]interface{}) []string) *join {
	return &join{
		lhsPb: lhsPb,
		rhsPb: rhsPb,
		handlerCtx: handlerCtx,
		rowSort: rowSort,
	}
}

func (ss *singleSelect) Build() error {
	return nil
}

func (ss *singleSelect) GetPrimitive() plan.IPrimitive {
	prov, err := ss.tableMeta.GetProvider()
	return NewHTTPRestPrimitive(
		prov,
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
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
			response, apiErr := httpApiCall(*(ss.handlerCtx), prov, ss.tableMeta.HttpArmoury.Context)
			if apiErr != nil {
				return util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, nil, nil, ss.rowSort, apiErr, nil))
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
			items, ok := target[ss.tableMeta.SelectItemsKey]
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
			rv := util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, keys, ss.primitiveBuilder.columnOrder, ss.rowSort, err, nil))
			if rv.Result == nil && err == nil {
				rv.Result = &sqltypes.Result{
					Fields: make([]*querypb.Field, len(ss.primitiveBuilder.columnOrder)),
				}
				for f := range rv.Result.Fields {
					rv.Result.Fields[f] = &querypb.Field{
						Name: ss.primitiveBuilder.columnOrder[f],
					}
				}
			}
			return rv
		})
}

func (j *join) Build() error {
	return nil
}

func (j *join) GetPrimitive() plan.IPrimitive {
	return NewLocalPrimitive(
		func (pc plan.IPrimitiveCtx) dto.ExecutorOutput {
		  return util.GenerateSimpleErroneousOutput(fmt.Errorf("joins not yet supported"))
		},
	)
}
