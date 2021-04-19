package parserutil

import (
	"fmt"
	"infraql/internal/iql/metadata"
	"strconv"

	log "github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	FloatBitSize int = 64
)

func GetTableNameFromTableExpr(node sqlparser.TableExpr) (sqlparser.TableName, error) {
	switch tt := node.(type) {
	case *sqlparser.AliasedTableExpr:
		tn, ok := tt.Expr.(sqlparser.TableName)
		if ok {
			return tn, nil
		}
	}
	return sqlparser.TableName{}, fmt.Errorf("table expression too colmplex")
}

func ExtractSelectColumnNames(selStmt *sqlparser.Select) ([]string, error) {
	var colNames []string
	var err error
	for _, node := range selStmt.SelectExprs {
		switch node := node.(type) {
		case *sqlparser.AliasedExpr:
			colNames = inferColNameFromExpr(node.Expr, colNames)
		case *sqlparser.StarExpr:

		}
	}
	return colNames, err
}

func ExtractInsertColumnNames(insertStmt *sqlparser.Insert) ([]string, error) {
	var colNames []string
	var err error
	for _, node := range insertStmt.Columns {
		colNames = append(colNames, node.GetRawVal())
	}
	return colNames, err
}

func ExtractAliasedValColumnData(aliasedExpr *sqlparser.AliasedExpr) (map[string]interface{}, error) {
	alias := aliasedExpr.As.GetRawVal()
	switch expr := aliasedExpr.Expr.(type) {
	case *sqlparser.SQLVal:
		switch expr.Type {
		case sqlparser.StrVal:
			return map[string]interface{}{alias: string(expr.Val)}, nil
		case sqlparser.IntVal:
			rv, err := strconv.Atoi(string(expr.Val))
			return map[string]interface{}{alias: rv}, err
		case sqlparser.FloatVal:
			rv, err := strconv.ParseFloat(string(expr.Val), FloatBitSize)
			return map[string]interface{}{alias: rv}, err
		}
	}
	return nil, fmt.Errorf("unextractable val only col")
}

func ExtractValuesColumnData(values sqlparser.Values) (map[int]map[int]interface{}, int, error) {
	retVal := make(map[int]map[int]interface{})
	var nonValCount int
	var err error
	for outerIdx, valTuple := range values {
		row := make(map[int]interface{})
		for innerIdx, expr := range valTuple {
			switch expr := expr.(type) {
			case *sqlparser.SQLVal:
				switch expr.Type {
				case sqlparser.StrVal:
					row[innerIdx] = string(expr.Val)
				case sqlparser.IntVal:
					rv, err := strconv.Atoi(string(expr.Val))
					if err != nil {
						return nil, nonValCount, fmt.Errorf("error extracting Values integer: %s", err.Error())
					}
					row[innerIdx] = rv
				case sqlparser.FloatVal:
					rv, err := strconv.ParseFloat(string(expr.Val), FloatBitSize)
					if err != nil {
						return nil, nonValCount, fmt.Errorf("error extracting Values float: %s", err.Error())
					}
					row[innerIdx] = rv
				default:
					return nil, nonValCount, fmt.Errorf("unextractable val only col of type %v", expr.Type)
				}
			}
		}
		retVal[outerIdx] = row
	}
	return retVal, 0, err
}

func ExtractSelectValColumns(selStmt *sqlparser.Select) (map[int]map[string]interface{}, int) {
	cols := make(map[int]map[string]interface{})
	var nonValCount int
	for idx, node := range selStmt.SelectExprs {
		switch node := node.(type) {
		case *sqlparser.AliasedExpr:
			switch expr := node.Expr.(type) {
			case *sqlparser.SQLVal:
				col, err := ExtractAliasedValColumnData(node)
				if err == nil {
					cols[idx] = col
				} else {
					cols[idx] = nil
					nonValCount++
				}
			case sqlparser.BoolVal:
				cols[idx] = map[string]interface{}{fmt.Sprintf("$$unaliased_col_%d", idx): expr}
			default:
				log.Infoln(fmt.Sprintf("cannot use AliasedExpr of type '%T' as a raw value", expr))
				cols[idx] = nil
				nonValCount++
			}
		default:
			log.Infoln(fmt.Sprintf("cannot use SelectExpr of type '%T' as a raw value", node))
			cols[idx] = nil
			nonValCount++
		}
	}
	return cols, nonValCount
}

func ExtractInsertValColumns(insStmt *sqlparser.Insert) (map[int]map[int]interface{}, int, error) {
	var nonValCount int
	var err error
	switch node := insStmt.Rows.(type) {
	case *sqlparser.Select:
		row, nvc := ExtractSelectValColumns(node)
		transformedRow := make(map[int]interface{})
		for k, v := range row {
			if v != nil {
				for _, c := range v {
					transformedRow[k] = c
					break
				}
			}
		}
		return map[int]map[int]interface{}{
			0: transformedRow,
		}, nvc, err
	case sqlparser.Values:
		return ExtractValuesColumnData(node)
	default:
		err = fmt.Errorf("cannot use an insert Rows value column of type '%T' as a raw value", node)
	}
	return nil, nonValCount, err
}

func ExtractWhereColNames(statement *sqlparser.Where) ([]string, error) {
	var whereNames []string
	var err error
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			whereNames = append(whereNames, node.Name.String())
		}
		return true, err
	}, statement)
	return whereNames, err
}

func ExtractShowColNames(statement *sqlparser.ShowTablesOpt) ([]string, error) {
	var whereNames []string
	var err error
	if statement == nil || statement.Filter == nil {
		return whereNames, err
	}
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			whereNames = append(whereNames, node.Name.String())
		}
		return true, err
	}, statement.Filter)
	return whereNames, err
}

func ExtractShowColUsage(statement *sqlparser.ShowTablesOpt) ([]ColumnUsageMetadata, error) {
	var colUsageSlice []ColumnUsageMetadata
	var err error
	if statement == nil || statement.Filter == nil {
		return colUsageSlice, err
	}
	return GetColumnUsageTypes(statement.Filter.Filter)
}

func ExtractSleepDuration(statement *sqlparser.Sleep) (int, error) {
	var retVal int
	if statement == nil || statement.Duration == nil {
		return retVal, fmt.Errorf("no sleep duration provided")
	}
	switch statement.Duration.Type {
	case sqlparser.IntVal:
		return strconv.Atoi(string(statement.Duration.Val))
	}
	return retVal, fmt.Errorf("sleep definition inadequate")
}

type ColumnUsageMetadata struct {
	ColName *sqlparser.ColName
	ColVal  *sqlparser.SQLVal
}

func CheckColUsagesAgainstTable(colUsages []ColumnUsageMetadata, table *metadata.Method) error {
	for _, colUsage := range colUsages {
		param, ok := table.Parameters[colUsage.ColName.Name.GetRawVal()]
		if ok {
			usageErr := CheckSqlParserTypeVsColumn(colUsage, param.ConditionIsValid)
			if usageErr != nil {
				return usageErr
			}
		}
		log.Debugln(fmt.Sprintf("colname = %v", colUsage.ColName))
	}
	return nil
}

func GetColumnUsageTypes(statement sqlparser.Expr) ([]ColumnUsageMetadata, error) {
	var colMetaSlice []ColumnUsageMetadata
	var err error
	sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.ComparisonExpr:
			colMeta := ColumnUsageMetadata{}
			switch lhs := node.Left.(type) {
			case *sqlparser.ColName:
				colMeta.ColName = lhs
			}
			switch rhs := node.Right.(type) {
			case *sqlparser.SQLVal:
				colMeta.ColVal = rhs
			}
			if colMeta.ColName != nil && colMeta.ColVal != nil {
				colMetaSlice = append(colMetaSlice, colMeta)
			}
		}
		return true, nil
	}, statement)
	return colMetaSlice, err
}

func GetColumnUsageTypesForExec(exec *sqlparser.Exec) ([]ColumnUsageMetadata, error) {
	var colMetaSlice []ColumnUsageMetadata
	for _, execVarDef := range exec.ExecVarDefs {
		colMeta := ColumnUsageMetadata{}
		colMeta.ColName = &sqlparser.ColName{Name: execVarDef.ColIdent}
		switch rhs := execVarDef.Val.(type) {
		case *sqlparser.SQLVal:
			colMeta.ColVal = rhs
		default:
			return nil, fmt.Errorf("EXEC param not supplied as valid SQLVal")
		}
		colMetaSlice = append(colMetaSlice, colMeta)
	}
	return colMetaSlice, nil
}

func inferColNameFromExpr(node sqlparser.Expr, colNames []string) []string {
	switch node := node.(type) {
	case *sqlparser.ColName:
		colNames = append(colNames, node.Name.String())
	case *sqlparser.FuncExpr:
		// As a shortcut, functions are integral types
		colNames = append(colNames, sqlparser.String(node))
	case *sqlparser.SQLVal:
		colNames = append(colNames, sqlparser.String(node))
	}
	return colNames
}

func CheckSqlParserTypeVsServiceColumn(colUsage ColumnUsageMetadata) error {
	return CheckSqlParserTypeVsColumn(colUsage, metadata.ServiceConditionIsValid)
}

func CheckSqlParserTypeVsResourceColumn(colUsage ColumnUsageMetadata) error {
	return CheckSqlParserTypeVsColumn(colUsage, metadata.ResourceConditionIsValid)
}

func CheckSqlParserTypeVsColumn(colUsage ColumnUsageMetadata, verifyCallback func(string, interface{}) bool) error {
	switch colUsage.ColVal.Type {
	case sqlparser.StrVal:
		if !verifyCallback(colUsage.ColName.Name.String(), "") {
			return fmt.Errorf("SHOW key = '%s' does NOT match SQL type '%s'", colUsage.ColName.Name.String(), "StrVal")
		}
	case sqlparser.IntVal:
		if !verifyCallback(colUsage.ColName.Name.String(), 11) {
			return fmt.Errorf("SHOW key = '%s' does NOT match SQL type '%s'", colUsage.ColName.Name.String(), "IntVal")
		}
	case sqlparser.FloatVal:
		if !verifyCallback(colUsage.ColName.Name.String(), 3.33) {
			return fmt.Errorf("SHOW key = '%s' does NOT match SQL type '%s'", colUsage.ColName.Name.String(), "FloatVal")
		}
	case sqlparser.HexNum:
		if !verifyCallback(colUsage.ColName.Name.String(), 0x11) {
			return fmt.Errorf("SHOW key = '%s' does NOT match SQL type '%s'", colUsage.ColName.Name.String(), "HexNum")
		}
	case sqlparser.HexVal:
		return fmt.Errorf("SHOW key = '%s' does NOT match SQL type '%s'", colUsage.ColName.Name.String(), "HexVal")
	case sqlparser.ValArg:
		return fmt.Errorf("SHOW key = '%s' does NOT match SQL type '%s'", colUsage.ColName.Name.String(), "ValArg")
	case sqlparser.BitVal:
		return fmt.Errorf("SHOW key = '%s' does NOT match SQL type '%s'", colUsage.ColName.Name.String(), "BitVal")
	}
	return nil
}

func ExtractTableNameFromTableExpr(tableExpr sqlparser.TableExpr) (*sqlparser.TableName, error) {
	switch table := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableExpr := table.Expr.(type) {
		case sqlparser.TableName:
			return &tableExpr, nil
		default:
			return nil, fmt.Errorf("could not extract table name from AliasedTableExpr of type %T", tableExpr)
		}
	default:
		return nil, fmt.Errorf("could not extract table name from TableExpr of type %T", table)
	}
	return nil, fmt.Errorf("could not extract table name from TableExpr")
}

func ExtractSingleTableFromTableExprs(tableExprs sqlparser.TableExprs) (*sqlparser.TableName, error) {
	for _, t := range tableExprs {
		log.Infoln(fmt.Sprintf("t = %v", t))
		return ExtractTableNameFromTableExpr(t)
	}
	return nil, fmt.Errorf("could not extract table name from TableExprs")
}
