package primitivebuilder

import (
	"fmt"
	"strconv"

	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/httpmiddleware"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/taxonomy"
	"infraql/internal/iql/util"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"

	log "github.com/sirupsen/logrus"
)

type Builder interface {
	Build() error

	GetPrimitive() plan.IPrimitive
}

type SingleSelect struct {
	primitiveBuilder *PrimitiveBuilder
	handlerCtx       *handler.HandlerContext
	tableMeta        taxonomy.ExtendedTableMetadata
	rowSort          func(map[string]map[string]interface{}) []string
}

type Join struct {
	lhsPb, rhsPb *PrimitiveBuilder
	lhs, rhs     Builder
	handlerCtx   *handler.HandlerContext
	rowSort      func(map[string]map[string]interface{}) []string
}

func NewSingleSelect(pb *PrimitiveBuilder, handlerCtx *handler.HandlerContext, tableMeta taxonomy.ExtendedTableMetadata, rowSort func(map[string]map[string]interface{}) []string) *SingleSelect {
	return &SingleSelect{
		primitiveBuilder: pb,
		handlerCtx:       handlerCtx,
		tableMeta:        tableMeta,
		rowSort:          rowSort,
	}
}

func NewJoin(lhsPb *PrimitiveBuilder, rhsPb *PrimitiveBuilder, handlerCtx *handler.HandlerContext, rowSort func(map[string]map[string]interface{}) []string) *Join {
	return &Join{
		lhsPb:      lhsPb,
		rhsPb:      rhsPb,
		handlerCtx: handlerCtx,
		rowSort:    rowSort,
	}
}

func (ss *SingleSelect) Build() error {
	return nil
}

func (ss *SingleSelect) GetPrimitive() plan.IPrimitive {
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
			response, apiErr := httpmiddleware.HttpApiCall(*(ss.handlerCtx), prov, ss.tableMeta.HttpArmoury.Context)
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
			rv := util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, keys, ss.primitiveBuilder.GetColumnOrder(), ss.rowSort, err, nil))
			if rv.Result == nil && err == nil {
				rv.Result = &sqltypes.Result{
					Fields: make([]*querypb.Field, len(ss.primitiveBuilder.GetColumnOrder())),
				}
				for f := range rv.Result.Fields {
					rv.Result.Fields[f] = &querypb.Field{
						Name: ss.primitiveBuilder.GetColumnOrder()[f],
					}
				}
			}
			return rv
		})
}

func (j *Join) Build() error {
	return nil
}

func (j *Join) GetPrimitive() plan.IPrimitive {
	return NewLocalPrimitive(
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			return util.GenerateSimpleErroneousOutput(fmt.Errorf("joins not yet supported"))
		},
	)
}
