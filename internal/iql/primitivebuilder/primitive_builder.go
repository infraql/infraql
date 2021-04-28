package primitivebuilder

import (
	"fmt"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/taxonomy"

	"vitess.io/vitess/go/vt/sqlparser"
)

type TblMap map[sqlparser.SQLNode]taxonomy.ExtendedTableMetadata

func (tm TblMap) GetTable(node sqlparser.SQLNode) (taxonomy.ExtendedTableMetadata, error) {
	tbl, ok := tm[node]
	if !ok {
		return taxonomy.ExtendedTableMetadata{}, fmt.Errorf("could not locate table metadata for AST node: %v", node)
	}
	return tbl, nil
}

func (tm TblMap) SetTable(node sqlparser.SQLNode, table taxonomy.ExtendedTableMetadata) {
	tm[node] = table
}

type PrimitiveBuilder struct {
	await bool

	ast sqlparser.Statement

	builder Builder

	// needed globally for non-heirarchy queries, such as "SHOW SERVICES FROM google;"
	prov            provider.IProvider
	tableFilter     func(iqlmodel.ITable) (iqlmodel.ITable, error)
	colsVisited     map[string]bool
	likeAbleColumns []string

	// per table
	tables TblMap

	// per query
	columnOrder       []string
	commentDirectives sqlparser.CommentDirectives

	// per query -- SELECT only
	insertValOnlyRows map[int]map[int]interface{}
	valOnlyCols       map[int]map[string]interface{}

	// per query -- SHOW INSERT only
	insertSchemaMap map[string]metadata.Schema

	// TODO: universally retire in favour of builder, which returns plan.IPrimitive
	primitive plan.IPrimitive
}

func (pb *PrimitiveBuilder) GetAst() sqlparser.Statement {
	return pb.ast
}

func (pb *PrimitiveBuilder) GetInsertSchemaMap() map[string]metadata.Schema {
	return pb.insertSchemaMap
}

func (pb *PrimitiveBuilder) SetInsertSchemaMap(m map[string]metadata.Schema) {
	pb.insertSchemaMap = m
}

func (pb *PrimitiveBuilder) GetInsertValOnlyRows() map[int]map[int]interface{} {
	return pb.insertValOnlyRows
}

func (pb *PrimitiveBuilder) SetInsertValOnlyRows(m map[int]map[int]interface{}) {
	pb.insertValOnlyRows = m
}

func (pb *PrimitiveBuilder) GetColumnOrder() []string {
	return pb.columnOrder
}

func (pb *PrimitiveBuilder) SetColumnOrder(co []string) {
	pb.columnOrder = co
}

func (pb *PrimitiveBuilder) GetPrimitive() plan.IPrimitive {
	return pb.primitive
}

func (pb *PrimitiveBuilder) SetPrimitive(primitive plan.IPrimitive) {
	pb.primitive = primitive
}

func (pb *PrimitiveBuilder) GetCommentDirectives() sqlparser.CommentDirectives {
	return pb.commentDirectives
}

func (pb *PrimitiveBuilder) SetCommentDirectives(dirs sqlparser.CommentDirectives) {
	pb.commentDirectives = dirs
}

func (pb *PrimitiveBuilder) GetLikeAbleColumns() []string {
	return pb.likeAbleColumns
}

func (pb *PrimitiveBuilder) SetLikeAbleColumns(cols []string) {
	pb.likeAbleColumns = cols
}

func (pb *PrimitiveBuilder) GetValOnlyColKeys() []int {
	keys := make([]int, 0, len(pb.valOnlyCols))
	for k := range pb.valOnlyCols {
		keys = append(keys, k)
	}
	return keys
}

func (pb *PrimitiveBuilder) GetValOnlyCol(key int) map[string]interface{} {
	return pb.valOnlyCols[key]
}

func (pb *PrimitiveBuilder) SetValOnlyCols(m map[int]map[string]interface{}) {
	pb.valOnlyCols = m
}

func (pb *PrimitiveBuilder) SetColVisited(colname string, isVisited bool) {
	pb.colsVisited[colname] = isVisited
}

func (pb *PrimitiveBuilder) GetTableFilter() func(iqlmodel.ITable) (iqlmodel.ITable, error) {
	return pb.tableFilter
}

func (pb *PrimitiveBuilder) SetTableFilter(tableFilter func(iqlmodel.ITable) (iqlmodel.ITable, error)) {
	pb.tableFilter = tableFilter
}

func (pb *PrimitiveBuilder) GetProvider() provider.IProvider {
	return pb.prov
}

func (pb *PrimitiveBuilder) SetProvider(prov provider.IProvider) {
	pb.prov = prov
}

func (pb *PrimitiveBuilder) GetBuilder() Builder {
	return pb.builder
}

func (pb *PrimitiveBuilder) SetBuilder(builder Builder) {
	pb.builder = builder
}

func (pb *PrimitiveBuilder) IsAwait() bool {
	return pb.await
}

func (pb *PrimitiveBuilder) SetAwait(await bool) {
	pb.await = await
}

func (pb PrimitiveBuilder) GetTable(node sqlparser.SQLNode) (taxonomy.ExtendedTableMetadata, error) {
	return pb.tables.GetTable(node)
}

func (pb PrimitiveBuilder) SetTable(node sqlparser.SQLNode, table taxonomy.ExtendedTableMetadata) {
	pb.tables.SetTable(node, table)
}

func (pb PrimitiveBuilder) GetTables() TblMap {
	return pb.tables
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

func NewPrimitiveBuilder(ast sqlparser.Statement) *PrimitiveBuilder {
	return &PrimitiveBuilder{
		ast:               ast,
		tables:            make(map[sqlparser.SQLNode]taxonomy.ExtendedTableMetadata),
		valOnlyCols:       make(map[int]map[string]interface{}),
		insertValOnlyRows: make(map[int]map[int]interface{}),
		colsVisited:       make(map[string]bool),
	}
}
