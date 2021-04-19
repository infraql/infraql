package taxonomy

import (
	"fmt"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpbuild"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/parserutil"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/iqlutil"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type ExtendedTableMetadata struct {
	TableFilter          func(iqlmodel.ITable) (iqlmodel.ITable, error)
	ColsVisited          map[string]bool
	HeirarchyObjects     *HeirarchyObjects
	RequiredParameters   map[string]iqlmodel.Parameter
	IsLocallyExecutable  bool
	HttpArmoury          *httpbuild.HTTPArmoury
	SelectItemsKey       string
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

func NewExtendedTableMetadata(heirarchyObjects *HeirarchyObjects) ExtendedTableMetadata {
	return ExtendedTableMetadata{
		ColsVisited:        make(map[string]bool),
		RequiredParameters: make(map[string]iqlmodel.Parameter),
		HeirarchyObjects:   heirarchyObjects,
	}
}

type HeirarchyIdentifiers struct {
	ProviderStr string
	ServiceStr  string
	ResourceStr string
	MethodStr   string
}

type HeirarchyObjects struct {
	HeirarchyIds HeirarchyIdentifiers
	Provider     provider.IProvider
	ServiceHdl   *metadata.ServiceHandle
	Resource     *metadata.Resource
	Method       *metadata.Method
}

func ResolveExecHeirarchyIdentifiers(node sqlparser.TableName) *HeirarchyIdentifiers {
	return resolveMethodTerminalHeirarchyIdentifiers(node)
}

func (hi *HeirarchyIdentifiers) GetTableName() string {
	if hi.ProviderStr != "" {
		return fmt.Sprintf("%s.%s.%s", hi.ProviderStr, hi.ServiceStr, hi.ResourceStr)
	}
	return fmt.Sprintf("%s.%s", hi.ServiceStr, hi.ResourceStr)
}

func (ho *HeirarchyObjects) GetTableName() string {
	return ho.HeirarchyIds.GetTableName()
}

func resolveMethodTerminalHeirarchyIdentifiers(node sqlparser.TableName) *HeirarchyIdentifiers {
	var retVal HeirarchyIdentifiers
	// all will default to empty string
	retVal.ProviderStr = iqlutil.SanitisePossibleTickEscapedTerm(node.QualifierThird.String())
	retVal.ServiceStr = iqlutil.SanitisePossibleTickEscapedTerm(node.QualifierSecond.String())
	retVal.ResourceStr = iqlutil.SanitisePossibleTickEscapedTerm(node.Qualifier.String())
	retVal.MethodStr = iqlutil.SanitisePossibleTickEscapedTerm(node.Name.String())
	return &retVal
}

func resolveResourceTerminalHeirarchyIdentifiers(node sqlparser.TableName) *HeirarchyIdentifiers {
	var retVal HeirarchyIdentifiers
	// all will default to empty string
	retVal.ProviderStr = iqlutil.SanitisePossibleTickEscapedTerm(node.QualifierSecond.String())
	retVal.ServiceStr = iqlutil.SanitisePossibleTickEscapedTerm(node.Qualifier.String())
	retVal.ResourceStr = iqlutil.SanitisePossibleTickEscapedTerm(node.Name.String())
	return &retVal
}

func ResolveHeirarchyIDsFromResourceTerminalTable(node sqlparser.TableName) HeirarchyIdentifiers {
	return *resolveResourceTerminalHeirarchyIdentifiers(node)
}

func GetHeirarchyFromStatement(handlerCtx *handler.HandlerContext, node sqlparser.SQLNode) (*HeirarchyObjects, error) {
	var hIds *HeirarchyIdentifiers
	var methodAction string
	switch n := node.(type) {
	case *sqlparser.Exec:
		hIds = resolveMethodTerminalHeirarchyIdentifiers(n.MethodName)
	case *sqlparser.Select:
		currentSvcRsc, err := sqlparser.TableFromStatement(handlerCtx.Query)
		if err != nil {
			return nil, err
		}
		hIds = resolveResourceTerminalHeirarchyIdentifiers(currentSvcRsc)
		methodAction = "select"
	case sqlparser.TableName:
		hIds = resolveResourceTerminalHeirarchyIdentifiers(n)
	case *sqlparser.AliasedTableExpr:
		return GetHeirarchyFromStatement(handlerCtx, n.Expr)
	case *sqlparser.Show:
		switch strings.ToUpper(n.Type) {
		case "INSERT":
			hIds = resolveResourceTerminalHeirarchyIdentifiers(n.OnTable)
			methodAction = "insert"
		default:
			return nil, fmt.Errorf("cannot resolve taxonomy for SHOW statement of type = '%s'", strings.ToUpper(n.Type))
		}
	case *sqlparser.Insert:
		hIds = resolveResourceTerminalHeirarchyIdentifiers(n.Table)
		methodAction = "insert"
	case *sqlparser.Delete:
		currentSvcRsc, err := parserutil.ExtractSingleTableFromTableExprs(n.TableExprs)
		if err != nil {
			return nil, err
		}
		hIds = resolveResourceTerminalHeirarchyIdentifiers(*currentSvcRsc)
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
	if !methodPresent {
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
	retVal.Method = &method
	return &retVal, nil
}
