package drm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"infraql/internal/iql/astvisit"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/parserutil"
	"infraql/internal/iql/sqlengine"
	"infraql/internal/iql/util"
	"infraql/internal/pkg/txncounter"
	"reflect"
	"strings"

	log "github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	gen_id_col_name string = "iql_generation_id"
	ssn_id_col_name string = "iql_session_id"
	txn_id_col_name string = "iql_txn_id"
	ins_id_col_name string = "iql_insert_id"
)

type DRM interface {
	DRMConfig
}

type DRMCoupling struct {
	RelationalType string
	GolangKind     reflect.Kind
}

type ColumnMetadata struct {
	Coupling DRMCoupling
	Column   metadata.ColumnDescriptor
}

func (cd ColumnMetadata) GetName() string {
	return cd.Column.Name
}

func (cd ColumnMetadata) GetType() string {
	if cd.Column.Schema != nil {
		return cd.Column.Schema.Type
	}
	return parserutil.ExtractStringRepresentationOfValueColumn(cd.Column.Val)
}

func (cd ColumnMetadata) getTypeFromVal() string {
	switch cd.Column.Val.Type {
	case sqlparser.BitVal, sqlparser.HexNum, sqlparser.HexVal, sqlparser.StrVal:
		return "string"
	case sqlparser.FloatVal:
		return "float"
	case sqlparser.IntVal:
		return "int"
	default:
		return "string"
	}
}

func NewColDescriptor(col metadata.ColumnDescriptor, relTypeStr string) ColumnMetadata {
	return ColumnMetadata{
		Coupling: DRMCoupling{RelationalType: relTypeStr, GolangKind: reflect.String},
		Column:   col,
	}
}

type PreparedStatementCtx struct {
	Query                   string
	GenIdControlColName     string
	SessionIdControlColName string
	TableNames              []string
	TxnIdControlColName     string
	InsIdControlColName     string
	NonControlColumns       []ColumnMetadata
	TxnCtrlCtrs             *dto.TxnControlCounters
}

func (ps PreparedStatementCtx) GetGCHousekeepingQueries() string {
	templateQuery := `INSERT INTO "__iql__.control.gc.txn_table_x_ref" (iql_generation_id, iql_session_id, iql_transaction_id, table_name) values(%d, %d, %d, '%s')`
	var housekeepingQueries []string
	for _, table := range ps.TableNames {
		housekeepingQueries = append(housekeepingQueries, fmt.Sprintf(templateQuery, ps.TxnCtrlCtrs.GenId, ps.TxnCtrlCtrs.SessionId, ps.TxnCtrlCtrs.TxnId, table))
	}
	return strings.Join(housekeepingQueries, "; ")
}

type DRMConfig interface {
	ExtractFromGolangValue(interface{}) interface{}
	GetCurrentTable(*dto.HeirarchyIdentifiers, sqlengine.SQLEngine) (dto.DBTable, error)
	GetRelationalType(string) string
	GenerateDDL(util.AnnotatedTabulation, int) []string
	GetGolangValue(string) interface{}
	GenerateInsertDML(util.AnnotatedTabulation, *txncounter.TxnCounterManager, int) (PreparedStatementCtx, error)
	GenerateSelectDML(util.AnnotatedTabulation, *dto.TxnControlCounters, sqlparser.SQLNode, *sqlparser.Where) (PreparedStatementCtx, error)
	ExecuteInsertDML(sqlengine.SQLEngine, *PreparedStatementCtx, map[string]interface{}) (sql.Result, error)
	QueryDML(sqlengine.SQLEngine, *PreparedStatementCtx, map[string]interface{}) (*sql.Rows, error)
}

type StaticDRMConfig struct {
	typeMappings          map[string]DRMCoupling
	defaultRelationalType string
	defaultGolangKind     reflect.Kind
	defaultGolangValue    interface{}
}

func (dc *StaticDRMConfig) getDefaultGolangValue() interface{} {
	return &sql.NullString{}
}

func (dc *StaticDRMConfig) getDefaultGolangKind() reflect.Kind {
	return dc.defaultGolangKind
}

func (dc *StaticDRMConfig) GetRelationalType(discoType string) string {
	rv, ok := dc.typeMappings[discoType]
	if ok {
		return rv.RelationalType
	}
	return dc.defaultRelationalType
}

func (dc *StaticDRMConfig) GetGolangValue(discoType string) interface{} {
	rv, ok := dc.typeMappings[discoType]
	if !ok {
		return dc.getDefaultGolangValue()
	}
	switch rv.GolangKind {
	case reflect.String:
		return &sql.NullString{}
	case reflect.Array:
		return &sql.NullString{}
	case reflect.Bool:
		return &sql.NullBool{}
	case reflect.Map:
		return &sql.NullString{}
	case reflect.Int:
		return &sql.NullInt64{}
	}
	return dc.getDefaultGolangValue()
}

func (dc *StaticDRMConfig) ExtractFromGolangValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	var retVal interface{}
	switch v := val.(type) {
	case *sql.NullString:
		retVal, _ = (*v).Value()
	case *sql.NullBool:
		retVal, _ = (*v).Value()
	case *sql.NullInt64:
		retVal, _ = (*v).Value()
	}
	return retVal
}

func (dc *StaticDRMConfig) GetGolangKind(discoType string) reflect.Kind {
	rv, ok := dc.typeMappings[discoType]
	if !ok {
		return dc.getDefaultGolangKind()
	}
	return rv.GolangKind
}

// switch v := reflect.ValueOf(v); v.Kind()

func (dc *StaticDRMConfig) getGenerationControlColumn() string {
	return gen_id_col_name
}

func (dc *StaticDRMConfig) getSessionControlColumn() string {
	return ssn_id_col_name
}

func (dc *StaticDRMConfig) getTxnControlColumn() string {
	return txn_id_col_name
}

func (dc *StaticDRMConfig) getInsControlColumn() string {
	return ins_id_col_name
}

func (dc *StaticDRMConfig) GetCurrentTable(tableHeirarchyIDs *dto.HeirarchyIdentifiers, dbEngine sqlengine.SQLEngine) (dto.DBTable, error) {
	return dbEngine.GetCurrentTable(tableHeirarchyIDs)
}

func (dc *StaticDRMConfig) getTableName(hIds *dto.HeirarchyIdentifiers, discoveryGenerationID int) string {
	return fmt.Sprintf("%s.generation_%d", hIds.GetTableName(), discoveryGenerationID)
}

func (dc *StaticDRMConfig) inferTableName(hIds *dto.HeirarchyIdentifiers, discoveryGenerationID int) string {
	return dc.getTableName(hIds, discoveryGenerationID)
}

func (dc *StaticDRMConfig) generateDropTableStatement(hIds *dto.HeirarchyIdentifiers, discoveryGenerationID int) string {
	return fmt.Sprintf(`drop table if exists "%s"`, dc.getTableName(hIds, discoveryGenerationID))
}

func (dc *StaticDRMConfig) GenerateDDL(tabAnn util.AnnotatedTabulation, discoveryGenerationID int) []string {
	var colDefs, retVal []string
	var rv strings.Builder
	tableName := dc.getTableName(tabAnn.GetHeirarchyIdentifiers(), discoveryGenerationID)
	rv.WriteString(fmt.Sprintf(`create table if not exists "%s" ( `, tableName))
	colDefs = append(colDefs, fmt.Sprintf(`"iql_%s_id" INTEGER PRIMARY KEY AUTOINCREMENT`, tableName))
	genIdColName := dc.getGenerationControlColumn()
	sessionIdColName := dc.getSessionControlColumn()
	txnIdColName := dc.getTxnControlColumn()
	insIdColName := dc.getInsControlColumn()
	colDefs = append(colDefs, fmt.Sprintf(`"%s" INTEGER `, genIdColName))
	colDefs = append(colDefs, fmt.Sprintf(`"%s" INTEGER `, sessionIdColName))
	colDefs = append(colDefs, fmt.Sprintf(`"%s" INTEGER `, txnIdColName))
	colDefs = append(colDefs, fmt.Sprintf(`"%s" INTEGER `, insIdColName))
	for _, col := range tabAnn.GetTabulation().GetColumns() {
		var b strings.Builder
		b.WriteString(`"` + col.Name + `" `)
		b.WriteString(dc.GetRelationalType(col.Schema.Type))
		colDefs = append(colDefs, b.String())
	}
	rv.WriteString(strings.Join(colDefs, " , "))
	rv.WriteString(" ) ")
	retVal = append(retVal, dc.generateDropTableStatement(tabAnn.GetHeirarchyIdentifiers(), discoveryGenerationID))
	retVal = append(retVal, rv.String())
	retVal = append(retVal, fmt.Sprintf(`create index if not exists "idx_%s_%s" on "%s" ( "%s" ) `, strings.ReplaceAll(tableName, ".", "_"), genIdColName, tableName, genIdColName))
	retVal = append(retVal, fmt.Sprintf(`create index if not exists "idx_%s_%s" on "%s" ( "%s" ) `, strings.ReplaceAll(tableName, ".", "_"), sessionIdColName, tableName, sessionIdColName))
	retVal = append(retVal, fmt.Sprintf(`create index if not exists "idx_%s_%s" on "%s" ( "%s" ) `, strings.ReplaceAll(tableName, ".", "_"), txnIdColName, tableName, txnIdColName))
	retVal = append(retVal, fmt.Sprintf(`create index if not exists "idx_%s_%s" on "%s" ( "%s" ) `, strings.ReplaceAll(tableName, ".", "_"), insIdColName, tableName, insIdColName))
	return retVal
}

func (dc *StaticDRMConfig) GenerateInsertDML(tabAnnotated util.AnnotatedTabulation, txnCtrMgr *txncounter.TxnCounterManager, discoveryGenerationID int) (PreparedStatementCtx, error) {
	// log.Infoln(fmt.Sprintf("%v", tabulation))
	var q strings.Builder
	var quotedColNames, vals []string
	var columns []ColumnMetadata
	tableName := dc.inferTableName(tabAnnotated.GetHeirarchyIdentifiers(), discoveryGenerationID)
	q.WriteString(fmt.Sprintf(`INSERT INTO "%s" `, tableName))
	genIdColName := dc.getGenerationControlColumn()
	sessionIdColName := dc.getSessionControlColumn()
	txnIdColName := dc.getTxnControlColumn()
	insIdColName := dc.getInsControlColumn()
	quotedColNames = append(quotedColNames, `"`+genIdColName+`" `)
	quotedColNames = append(quotedColNames, `"`+sessionIdColName+`" `)
	quotedColNames = append(quotedColNames, `"`+txnIdColName+`" `)
	quotedColNames = append(quotedColNames, `"`+insIdColName+`" `)
	vals = append(vals, "?")
	vals = append(vals, "?")
	vals = append(vals, "?")
	vals = append(vals, "?")
	for _, col := range tabAnnotated.GetTabulation().GetColumns() {
		columns = append(columns, NewColDescriptor(col, dc.GetRelationalType(col.Schema.Type)))
		quotedColNames = append(quotedColNames, `"`+col.Name+`" `)
		vals = append(vals, "?")
	}
	q.WriteString(fmt.Sprintf(" (%s) ", strings.Join(quotedColNames, ", ")))
	q.WriteString(fmt.Sprintf(" VALUES (%s) ", strings.Join(vals, ", ")))
	return PreparedStatementCtx{
			Query:                   q.String(),
			GenIdControlColName:     genIdColName,
			SessionIdControlColName: sessionIdColName,
			TableNames:              []string{tableName},
			TxnIdControlColName:     txnIdColName,
			InsIdControlColName:     insIdColName,
			NonControlColumns:       columns,
			TxnCtrlCtrs: &dto.TxnControlCounters{
				GenId:                 txnCtrMgr.GetCurrentGenerationId(),
				SessionId:             txnCtrMgr.GetCurrentSessionId(),
				TxnId:                 txnCtrMgr.GetNextTxnId(),
				InsertId:              txnCtrMgr.GetNextInsertId(),
				DiscoveryGenerationId: discoveryGenerationID,
			},
		},
		nil
}

func (dc *StaticDRMConfig) GenerateSelectDML(tabAnnotated util.AnnotatedTabulation, txnCtrlCtrs *dto.TxnControlCounters, node sqlparser.SQLNode, rewrittenWhere *sqlparser.Where) (PreparedStatementCtx, error) {
	var q strings.Builder
	var quotedColNames, quotedWhereColNames []string
	var columns []ColumnMetadata
	// var vals []interface{}
	for _, col := range tabAnnotated.GetTabulation().GetColumns() {
		var typeStr string
		if col.Schema != nil {
			typeStr = dc.GetRelationalType(col.Schema.Type)
		} else {
			if col.Val != nil {
				switch col.Val.Type {
				case sqlparser.BitVal:
				}
			}
		}
		columns = append(columns, NewColDescriptor(col, typeStr))
		var colEntry strings.Builder
		if col.DecoratedCol == "" {
			colEntry.WriteString(fmt.Sprintf(`"%s" `, col.Name))
			if col.Alias != "" {
				colEntry.WriteString(fmt.Sprintf(` AS "%s"`, col.Alias))
			}
		} else {
			colEntry.WriteString(fmt.Sprintf("%s ", col.DecoratedCol))
		}
		quotedColNames = append(quotedColNames, fmt.Sprintf("%s ", colEntry.String()))

	}
	genIdColName := dc.getGenerationControlColumn()
	sessionIDColName := dc.getSessionControlColumn()
	txnIdColName := dc.getTxnControlColumn()
	insIdColName := dc.getInsControlColumn()
	quotedWhereColNames = append(quotedWhereColNames, `"`+genIdColName+`" `)
	quotedWhereColNames = append(quotedWhereColNames, `"`+txnIdColName+`" `)
	quotedWhereColNames = append(quotedWhereColNames, `"`+insIdColName+`" `)
	q.WriteString(fmt.Sprintf(`SELECT %s FROM "%s" WHERE `, strings.Join(quotedColNames, ", "), dc.getTableName(tabAnnotated.GetHeirarchyIdentifiers(), txnCtrlCtrs.DiscoveryGenerationId)))
	q.WriteString(fmt.Sprintf(`( "%s" = ? AND "%s" = ? AND "%s" = ? AND "%s" = ? ) `, genIdColName, sessionIDColName, txnIdColName, insIdColName))
	if rewrittenWhere != nil {
		q.WriteString(fmt.Sprintf(" AND ( %s ) ", astvisit.GenerateModifiedWhereClause(rewrittenWhere)))
	}
	q.WriteString(astvisit.GenerateModifiedSelectSuffix(node))

	return PreparedStatementCtx{
		Query:                   q.String(),
		GenIdControlColName:     genIdColName,
		SessionIdControlColName: sessionIDColName,
		TxnIdControlColName:     txnIdColName,
		InsIdControlColName:     insIdColName,
		NonControlColumns:       columns,
		TxnCtrlCtrs:             txnCtrlCtrs,
	}, nil
}

func (dc *StaticDRMConfig) generateControlVarArgs(ctx PreparedStatementCtx) ([]interface{}, error) {
	// log.Infoln(fmt.Sprintf("%v", ctx))
	var varArgs []interface{}
	varArgs = append(varArgs, ctx.TxnCtrlCtrs.GenId)
	varArgs = append(varArgs, ctx.TxnCtrlCtrs.SessionId)
	varArgs = append(varArgs, ctx.TxnCtrlCtrs.TxnId)
	varArgs = append(varArgs, ctx.TxnCtrlCtrs.InsertId)
	return varArgs, nil
}

func (dc *StaticDRMConfig) generateVarArgs(ctx PreparedStatementCtx, payload map[string]interface{}) ([]interface{}, error) {
	log.Infoln(fmt.Sprintf("%v", payload))
	varArgs, _ := dc.generateControlVarArgs(ctx)
	for _, col := range ctx.NonControlColumns {
		va, ok := payload[col.GetName()]
		if !ok {
			varArgs = append(varArgs, nil)
			continue
			// return nil, fmt.Errorf("expected column '%s' missing", col)
		}
		switch vt := va.(type) {
		case map[string]interface{}, []interface{}:
			b, err := json.Marshal(vt)
			if err != nil {
				return nil, err
			}
			varArgs = append(varArgs, string(b))
		default:
			varArgs = append(varArgs, va)
		}
	}
	return varArgs, nil
}

func (dc *StaticDRMConfig) ExecuteInsertDML(dbEngine sqlengine.SQLEngine, ctx *PreparedStatementCtx, payload map[string]interface{}) (sql.Result, error) {
	if ctx == nil {
		return nil, fmt.Errorf("cannot execute on nil PreparedStatementContext")
	}
	log.Infoln(fmt.Sprintf("%v", ctx.Query))
	varArgs, err := dc.generateVarArgs(*ctx, payload)
	if err != nil {
		return nil, err
	}
	return dbEngine.Exec(ctx.Query, varArgs...)
}

func (dc *StaticDRMConfig) QueryDML(dbEngine sqlengine.SQLEngine, ctx *PreparedStatementCtx, payload map[string]interface{}) (*sql.Rows, error) {
	if ctx == nil {
		return nil, fmt.Errorf("cannot execute on nil PreparedStatementContext")
	}
	log.Infoln(fmt.Sprintf("%v", ctx.Query))
	var varArgs []interface{}
	var err error
	if payload != nil {
		varArgs, err = dc.generateVarArgs(*ctx, payload)
	} else {
		varArgs, err = dc.generateControlVarArgs(*ctx)
	}
	if err != nil {
		return nil, err
	}
	return dbEngine.Query(ctx.Query, varArgs...)
}

func GetGoogleV1SQLiteConfig() DRMConfig {
	return &StaticDRMConfig{
		typeMappings: map[string]DRMCoupling{
			"array":   DRMCoupling{RelationalType: "text", GolangKind: reflect.Slice},
			"boolean": DRMCoupling{RelationalType: "boolean", GolangKind: reflect.Bool},
			"int":     DRMCoupling{RelationalType: "integer", GolangKind: reflect.Int},
			"integer": DRMCoupling{RelationalType: "integer", GolangKind: reflect.Int},
			"object":  DRMCoupling{RelationalType: "text", GolangKind: reflect.Map},
			"string":  DRMCoupling{RelationalType: "text", GolangKind: reflect.String},
		},
		defaultRelationalType: "text",
		defaultGolangKind:     reflect.String,
		defaultGolangValue:    sql.NullString{}, // string is default
	}
}

type GoogleV1DRM struct {
}
