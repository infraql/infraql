package astvisit

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

func GenerateModifiedSelectSuffix(node sqlparser.SQLNode) string {
	v := NewDRMAstVisitor("", false)
	switch node := node.(type) {
	case *sqlparser.Select:
		var options string
		addIf := func(b bool, s string) {
			if b {
				options += s
			}
		}
		addIf(node.Distinct, sqlparser.DistinctStr)
		if node.Cache != nil {
			if *node.Cache {
				options += sqlparser.SQLCacheStr
			} else {
				options += sqlparser.SQLNoCacheStr
			}
		}
		addIf(node.StraightJoinHint, sqlparser.StraightJoinHint)
		addIf(node.SQLCalcFoundRows, sqlparser.SQLCalcFoundRowsStr)

		var groupByStr, havingStr, orderByStr, limitStr string
		if node.GroupBy != nil {
			node.GroupBy.Accept(v)
			groupByStr = v.GetRewrittenQuery()
		}
		if node.Having != nil {
			node.Having.Accept(v)
			havingStr = v.GetRewrittenQuery()
		}
		if node.OrderBy != nil {
			node.OrderBy.Accept(v)
			orderByStr = v.GetRewrittenQuery()
		}
		if node.Limit != nil {
			node.Limit.Accept(v)
			orderByStr = v.GetRewrittenQuery()
		}
		rq := fmt.Sprintf("%v%v%v%v%s",
			groupByStr, havingStr, orderByStr,
			limitStr, node.Lock)
		v.rewrittenQuery = rq
	}
	return v.GetRewrittenQuery()
}

func GenerateModifiedWhereClause(node *sqlparser.Where) string {
	v := NewDRMAstVisitor("", false)
	var whereStr string
	if node != nil && node.Expr != nil {
		node.Expr.Accept(v)
		whereStr = v.GetRewrittenQuery()
	} else {
		return "true"
	}
	v.rewrittenQuery = whereStr
	return v.GetRewrittenQuery()
}

func ExtractProviderStrings(node sqlparser.SQLNode) []string {
	v := NewDRMAstVisitor("", true)
	node.Accept(v)
	return v.GetProviderStrings()
}
