package taxonomy

import (
	"fmt"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpbuild"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/parserutil"
	"infraql/internal/iql/provider"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type TblMap map[sqlparser.SQLNode]ExtendedTableMetadata

func (tm TblMap) GetTable(node sqlparser.SQLNode) (ExtendedTableMetadata, error) {
	tbl, ok := tm[node]
	if !ok {
		return ExtendedTableMetadata{}, fmt.Errorf("could not locate table metadata for AST node: %v", node)
	}
	return tbl, nil
}

func (tm TblMap) SetTable(node sqlparser.SQLNode, table ExtendedTableMetadata) {
	tm[node] = table
}

type ExtendedTableMetadata struct {
	TableFilter         func(iqlmodel.ITable) (iqlmodel.ITable, error)
	ColsVisited         map[string]bool
	HeirarchyObjects    *HeirarchyObjects
	RequiredParameters  map[string]iqlmodel.Parameter
	IsLocallyExecutable bool
	HttpArmoury         *httpbuild.HTTPArmoury
	SelectItemsKey      string
}

func (ex ExtendedTableMetadata) GetProvider() (provider.IProvider, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.Provider == nil {
		return nil, fmt.Errorf("cannot resolve Provider")
	}
	return ex.HeirarchyObjects.Provider, nil
}

func (ex ExtendedTableMetadata) GetServiceHandle() (*metadata.ServiceHandle, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.ServiceHdl == nil {
		return nil, fmt.Errorf("cannot resolve ServiceHandle")
	}
	return ex.HeirarchyObjects.ServiceHdl, nil
}

func (ex ExtendedTableMetadata) GetResource() (*metadata.Resource, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.Resource == nil {
		return nil, fmt.Errorf("cannot resolve Resource")
	}
	return ex.HeirarchyObjects.Resource, nil
}

func (ex ExtendedTableMetadata) GetMethod() (*metadata.Method, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.Method == nil {
		return nil, fmt.Errorf("cannot resolve Method")
	}
	return ex.HeirarchyObjects.Method, nil
}

func (ex ExtendedTableMetadata) GetServiceStr() (string, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.HeirarchyIds.ServiceStr == "" {
		return "", fmt.Errorf("cannot resolve ServiceStr")
	}
	return ex.HeirarchyObjects.HeirarchyIds.ServiceStr, nil
}

func (ex ExtendedTableMetadata) GetResourceStr() (string, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.HeirarchyIds.ResourceStr == "" {
		return "", fmt.Errorf("cannot resolve ResourceStr")
	}
	return ex.HeirarchyObjects.HeirarchyIds.ResourceStr, nil
}

func (ex ExtendedTableMetadata) GetProviderStr() (string, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.HeirarchyIds.ProviderStr == "" {
		return "", fmt.Errorf("cannot resolve ProviderStr")
	}
	return ex.HeirarchyObjects.HeirarchyIds.ProviderStr, nil
}

func (ex ExtendedTableMetadata) GetMethodStr() (string, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.HeirarchyIds.MethodStr == "" {
		return "", fmt.Errorf("cannot resolve MethodStr")
	}
	return ex.HeirarchyObjects.HeirarchyIds.MethodStr, nil
}

func (ex ExtendedTableMetadata) GetHTTPArmoury() (*httpbuild.HTTPArmoury, error) {
	return ex.HttpArmoury, nil
}

func (ex ExtendedTableMetadata) GetTableName() (string, error) {
	if ex.HeirarchyObjects == nil || ex.HeirarchyObjects.HeirarchyIds.GetTableName() == "" {
		return "", fmt.Errorf("cannot resolve TableName")
	}
	return ex.HeirarchyObjects.HeirarchyIds.GetTableName(), nil
}

func (ex ExtendedTableMetadata) GetItemsObjectSchema() (*metadata.Schema, error) {
	return ex.HeirarchyObjects.GetItemsObjectSchema()
}

func NewExtendedTableMetadata(heirarchyObjects *HeirarchyObjects) ExtendedTableMetadata {
	return ExtendedTableMetadata{
		ColsVisited:        make(map[string]bool),
		RequiredParameters: make(map[string]iqlmodel.Parameter),
		HeirarchyObjects:   heirarchyObjects,
	}
}

type HeirarchyObjects struct {
	HeirarchyIds dto.HeirarchyIdentifiers
	Provider     provider.IProvider
	ServiceHdl   *metadata.ServiceHandle
	Resource     *metadata.Resource
	Method       *metadata.Method
}

func (ho *HeirarchyObjects) GetTableName() string {
	return ho.HeirarchyIds.GetTableName()
}

func (ho *HeirarchyObjects) GetObjectSchema() (*metadata.Schema, error) {
	return ho.getObjectSchema()
}

func (ho *HeirarchyObjects) getObjectSchema() (*metadata.Schema, error) {
	return ho.Provider.GetObjectSchema(ho.HeirarchyIds.ServiceStr, ho.HeirarchyIds.ResourceStr, ho.Method.ResponseType.Type)
}

func (ho *HeirarchyObjects) GetItemsObjectSchema() (*metadata.Schema, error) {
	responseObj, err := ho.getObjectSchema()
	if err != nil {
		return nil, err
	}
	itemS, _ := responseObj.GetSelectListItems(ho.Provider.GetDefaultKeyForSelectItems())
	if itemS == nil {
		return nil, fmt.Errorf("could not locate dml aggregate object for response type '%v'", responseObj.ID)
	}
	is := itemS.Items
	itemObjS, _ := is.GetSchema(itemS.SchemaCentral)
	if itemObjS == nil {
		return nil, fmt.Errorf("could not locate dml object for response type '%v'", responseObj.ID)
	}
	return itemObjS, nil
}

func GetHeirarchyIDs(handlerCtx *handler.HandlerContext, node sqlparser.SQLNode) (*dto.HeirarchyIdentifiers, error) {
	return getHids(handlerCtx, node)
}

func getHids(handlerCtx *handler.HandlerContext, node sqlparser.SQLNode) (*dto.HeirarchyIdentifiers, error) {
	var hIds *dto.HeirarchyIdentifiers
	switch n := node.(type) {
	case *sqlparser.Exec:
		hIds = dto.ResolveMethodTerminalHeirarchyIdentifiers(n.MethodName)
	case *sqlparser.Select:
		currentSvcRsc, err := sqlparser.TableFromStatement(handlerCtx.Query)
		if err != nil {
			return nil, err
		}
		hIds = dto.ResolveResourceTerminalHeirarchyIdentifiers(currentSvcRsc)
	case sqlparser.TableName:
		hIds = dto.ResolveResourceTerminalHeirarchyIdentifiers(n)
	case *sqlparser.AliasedTableExpr:
		return getHids(handlerCtx, n.Expr)
	case *sqlparser.DescribeTable:
		return getHids(handlerCtx, n.Table)
	case *sqlparser.Show:
		switch strings.ToUpper(n.Type) {
		case "INSERT":
			hIds = dto.ResolveResourceTerminalHeirarchyIdentifiers(n.OnTable)
		case "METHODS":
			hIds = dto.ResolveResourceTerminalHeirarchyIdentifiers(n.OnTable)
		default:
			return nil, fmt.Errorf("cannot resolve taxonomy for SHOW statement of type = '%s'", strings.ToUpper(n.Type))
		}
	case *sqlparser.Insert:
		hIds = dto.ResolveResourceTerminalHeirarchyIdentifiers(n.Table)
	case *sqlparser.Delete:
		currentSvcRsc, err := parserutil.ExtractSingleTableFromTableExprs(n.TableExprs)
		if err != nil {
			return nil, err
		}
		hIds = dto.ResolveResourceTerminalHeirarchyIdentifiers(*currentSvcRsc)
	default:
		return nil, fmt.Errorf("cannot resolve taxonomy")
	}
	return hIds, nil
}

func GetHeirarchyFromStatement(handlerCtx *handler.HandlerContext, node sqlparser.SQLNode) (*HeirarchyObjects, error) {
	var hIds *dto.HeirarchyIdentifiers
	hIds, err := getHids(handlerCtx, node)
	methodRequired := true
	var methodAction string
	switch n := node.(type) {
	case *sqlparser.Exec:
	case *sqlparser.Select:
		methodAction = "select"
	case *sqlparser.DescribeTable:

	case sqlparser.TableName:
	case *sqlparser.AliasedTableExpr:
		return GetHeirarchyFromStatement(handlerCtx, n.Expr)
	case *sqlparser.Show:
		switch strings.ToUpper(n.Type) {
		case "INSERT":
			methodAction = "insert"
		case "METHODS":
			methodRequired = false
		default:
			return nil, fmt.Errorf("cannot resolve taxonomy for SHOW statement of type = '%s'", strings.ToUpper(n.Type))
		}
	case *sqlparser.Insert:
		methodAction = "insert"
	case *sqlparser.Delete:
		methodAction = "delete"
	default:
		return nil, fmt.Errorf("cannot resolve taxonomy")
	}
	retVal := HeirarchyObjects{
		HeirarchyIds: *hIds,
	}
	prov, err := handlerCtx.GetProvider(hIds.ProviderStr)
	retVal.Provider = prov
	if err != nil {
		return nil, err
	}
	svcHdl, err := prov.GetServiceHandle(hIds.ServiceStr, handlerCtx.RuntimeContext)
	if err != nil {
		return nil, err
	}
	retVal.ServiceHdl = svcHdl
	rsc, err := prov.GetResource(hIds.ServiceStr, hIds.ResourceStr, handlerCtx.RuntimeContext)
	if err != nil {
		return nil, err
	}
	retVal.Resource = rsc
	method, methodPresent := rsc.Methods[hIds.MethodStr]
	if !methodPresent && methodRequired {
		switch node.(type) {
		case *sqlparser.DescribeTable:
			m, mStr, err := prov.InferDescribeMethod(rsc)
			if err != nil {
				return nil, err
			}
			retVal.Method = m
			retVal.HeirarchyIds.MethodStr = mStr
			return &retVal, nil
		}
		if methodAction == "" {
			methodAction = "select"
		}
		meth, methStr, err := prov.GetMethodForAction(retVal.HeirarchyIds.ServiceStr, retVal.HeirarchyIds.ResourceStr, methodAction, handlerCtx.RuntimeContext)
		if err != nil {
			return nil, fmt.Errorf("could not find method in taxonomy: %s", err.Error())
		}
		method = *meth
		retVal.HeirarchyIds.MethodStr = methStr
	}
	if methodRequired {
		retVal.Method = &method
	}
	return &retVal, nil
}
