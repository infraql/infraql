package primitivebuilder

import (
	"fmt"
	"strconv"

	"infraql/internal/iql/drm"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/httpmiddleware"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/taxonomy"
	"infraql/internal/iql/util"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"

	log "github.com/sirupsen/logrus"
)

type Builder interface {
	Build() error

	GetQuery() string

	GetPrimitive() plan.IPrimitive
}

type SingleSelect struct {
	primitiveBuilder           *PrimitiveBuilder
	primitive                  plan.IPrimitive
	query                      string
	handlerCtx                 *handler.HandlerContext
	tableMeta                  taxonomy.ExtendedTableMetadata
	tabulation                 metadata.Tabulation
	drmCfg                     drm.DRMConfig
	insertPreparedStatementCtx *drm.PreparedStatementCtx
	selectPreparedStatementCtx *drm.PreparedStatementCtx
	generationId               int
	txnId                      int
	insertId                   int
	rowSort                    func(map[string]map[string]interface{}) []string
}

type Join struct {
	lhsPb, rhsPb *PrimitiveBuilder
	lhs, rhs     Builder
	handlerCtx   *handler.HandlerContext
	rowSort      func(map[string]map[string]interface{}) []string
}

func NewSingleSelect(pb *PrimitiveBuilder, handlerCtx *handler.HandlerContext, tableMeta taxonomy.ExtendedTableMetadata, insertCtx *drm.PreparedStatementCtx, selectCtx *drm.PreparedStatementCtx, rowSort func(map[string]map[string]interface{}) []string) *SingleSelect {
	return &SingleSelect{
		primitiveBuilder:           pb,
		handlerCtx:                 handlerCtx,
		tableMeta:                  tableMeta,
		rowSort:                    rowSort,
		drmCfg:                     handlerCtx.DrmConfig,
		insertPreparedStatementCtx: insertCtx,
		selectPreparedStatementCtx: selectCtx,
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
	prov, err := ss.tableMeta.GetProvider()
	if err != nil {
		return err
	}
	ex := func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
		defer ss.handlerCtx.SQLEngine.GCCollectObsolete(ss.insertPreparedStatementCtx.TxnCtrlCtrs)
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
		altKeys := make(map[string]map[string]interface{})
		if ok {
			iArr, ok := items.([]interface{})
			if ok && len(iArr) > 0 {
				_, err = ss.handlerCtx.SQLEngine.Exec(ss.insertPreparedStatementCtx.GetGCHousekeepingQueries())
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

				for i := range iArr {
					item, ok := iArr[i].(map[string]interface{})
					if ok {

						log.Infoln(fmt.Sprintf("running insert with control parameters: %v", ss.insertPreparedStatementCtx.TxnCtrlCtrs))
						r, err := ss.drmCfg.ExecuteInsertDML(ss.handlerCtx.SQLEngine, ss.insertPreparedStatementCtx, item)
						log.Infoln(fmt.Sprintf("insert result = %v, error = %v", r, err))
						keys[strconv.Itoa(i)] = item
					}
				}
			}
		}
		log.Infoln(fmt.Sprintf("running select with control parameters: %v", ss.selectPreparedStatementCtx.TxnCtrlCtrs))
		r, sqlErr := ss.drmCfg.QueryDML(ss.handlerCtx.SQLEngine, ss.selectPreparedStatementCtx, nil)
		log.Infoln(fmt.Sprintf("select result = %v, error = %v", r, sqlErr))

		i := 0
		var keyArr []string
		var ifArr []interface{}
		for i < len(ss.selectPreparedStatementCtx.NonControlColumns) {
			x := ss.selectPreparedStatementCtx.NonControlColumns[i]
			y := ss.drmCfg.GetGolangValue(x.GetType())
			ifArr = append(ifArr, y)
			keyArr = append(keyArr, x.Column.GetIdentifier())
			i++
		}
		if r != nil {
			i := 0
			for r.Next() {
				errScan := r.Scan(ifArr...)
				if errScan != nil {
					log.Infoln(fmt.Sprintf("%v", errScan))
				}
				for ord, val := range ifArr {
					log.Infoln(fmt.Sprintf("col #%d '%s':  %v  type: %T", ord, ss.selectPreparedStatementCtx.NonControlColumns[ord].GetName(), val, val))
				}
				im := make(map[string]interface{})
				for ord, key := range keyArr {
					val := ifArr[ord]
					ev := ss.drmCfg.ExtractFromGolangValue(val)
					im[key] = ev
				}
				altKeys[strconv.Itoa(i)] = im
				i++
			}
		}
		for ord, val := range altKeys {
			log.Infoln(fmt.Sprintf("row #%s:  %v  type: %T", ord, val, val))
		}
		var cNames []string
		for _, v := range ss.selectPreparedStatementCtx.NonControlColumns {
			cNames = append(cNames, v.Column.GetIdentifier())
		}
		rv := util.PrepareResultSet(dto.NewPrepareResultSetDTO(nil, altKeys, cNames, ss.rowSort, err, nil))
		if rv.Result == nil && err == nil {
			rv.Result = &sqltypes.Result{
				Fields: make([]*querypb.Field, len(ss.selectPreparedStatementCtx.NonControlColumns)),
			}
			for f := range rv.Result.Fields {
				rv.Result.Fields[f] = &querypb.Field{
					Name: cNames[f],
				}
			}
		}
		return rv
	}
	prep := func() *drm.PreparedStatementCtx {
		return ss.selectPreparedStatementCtx
	}
	ss.primitive = NewHTTPRestPrimitive(
		prov,
		ex,
		prep,
	)
	return nil
}

func (ss *SingleSelect) GetPrimitive() plan.IPrimitive {
	return ss.primitive
}

func (ss *SingleSelect) GetQuery() string {
	return ss.query
}

func (j *Join) Build() error {
	return nil
}

func (j *Join) GetQuery() string {
	return ""
}

func (j *Join) GetPrimitive() plan.IPrimitive {
	return NewLocalPrimitive(
		func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			return util.GenerateSimpleErroneousOutput(fmt.Errorf("joins not yet supported"))
		},
	)
}
