package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"infraql/internal/iql/constants"
	"infraql/internal/iql/discovery"
	"infraql/internal/iql/dto"
	sdk "infraql/internal/iql/google_sdk"
	"infraql/internal/iql/googlediscovery"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/iqlmodel"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/methodselect"
	"infraql/internal/iql/relational"
	"infraql/internal/iql/sqlengine"
	"infraql/internal/iql/sqltypeutil"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	log "github.com/sirupsen/logrus"
)

type googleServiceAccount struct {
	Email      string `json:"client_email"`
	PrivateKey string `json:"private_key"`
}

func getGoogleMap() map[string]interface{} {
	googleMap := map[string]interface{}{
		"name": googleProviderName,
	}
	return googleMap
}

func getGoogleMapExtended() map[string]interface{} {
	return getGoogleMap()
}

type GoogleProvider struct {
	runtimeCtx       dto.RuntimeCtx
	currentService   string
	discoveryAdapter discovery.IDiscoveryAdapter
	apiVersion       string
	methodSelector   methodselect.IMethodSelector
}

func (gp *GoogleProvider) GetDefaultKeyForSelectItems() string {
	return "items"
}

func (gp *GoogleProvider) GetDiscoveryGeneration(dbEngine sqlengine.SQLEngine) (int, error) {
	return dbEngine.GetCurrentDiscoveryGenerationId(gp.GetProviderString())
}

func (gp *GoogleProvider) GetDefaultKeyForDeleteItems() string {
	return "items"
}

func (gp *GoogleProvider) GetMethodSelector() methodselect.IMethodSelector {
	return gp.methodSelector
}

func (gp *GoogleProvider) GetVersion() string {
	return gp.apiVersion
}

func (gp *GoogleProvider) GetServiceHandlesMap(runtimeCtx dto.RuntimeCtx) (map[string]metadata.ServiceHandle, error) {
	return gp.discoveryAdapter.GetServiceHandlesMap()
}

func (gp *GoogleProvider) GetServiceHandle(serviceKey string, runtimeCtx dto.RuntimeCtx) (*metadata.ServiceHandle, error) {
	return gp.discoveryAdapter.GetServiceHandle(serviceKey)
}

func (gp *GoogleProvider) inferAuthType(authCtx dto.AuthCtx, authTypeRequested string) string {
	switch strings.ToLower(authTypeRequested) {
	case dto.AuthServiceAccountStr:
		return dto.AuthServiceAccountStr
	case dto.AuthInteractiveStr:
		return dto.AuthInteractiveStr
	}
	if authCtx.KeyFilePath != "" {
		return dto.AuthServiceAccountStr
	}
	return dto.AuthInteractiveStr
}

func (gp *GoogleProvider) Auth(authCtx *dto.AuthCtx, authTypeRequested string, enforceRevokeFirst bool) (*http.Client, error) {
	switch gp.inferAuthType(*authCtx, authTypeRequested) {
	case dto.AuthServiceAccountStr:
		return gp.keyFileAuth(authCtx)
	case dto.AuthInteractiveStr:
		return gp.oAuth(authCtx, enforceRevokeFirst)
	}
	return nil, fmt.Errorf("Could not infer auth type")
}

func (gp *GoogleProvider) AuthRevoke(authCtx *dto.AuthCtx) error {
	switch strings.ToLower(authCtx.Type) {
	case dto.AuthServiceAccountStr:
		return errors.New(constants.ServiceAccountRevokeErrStr)
	case dto.AuthInteractiveStr:
		err := sdk.RevokeGoogleAuth()
		if err == nil {
			deactivateAuth(authCtx)
		}
		return err
	}
	return fmt.Errorf(`Auth revoke for Google Failed; improper auth method: "%s" speciied`, authCtx.Type)
}

func (gp *GoogleProvider) GetMethodForAction(serviceName string, resourceName string, iqlAction string, runtimeCtx dto.RuntimeCtx) (*metadata.Method, string, error) {
	rsc, err := gp.GetResource(serviceName, resourceName, runtimeCtx)
	if err != nil {
		return nil, "", err
	}
	return gp.methodSelector.GetMethodForAction(rsc, iqlAction)
}

func (gp *GoogleProvider) InferDescribeMethod(rsc *metadata.Resource) (*metadata.Method, string, error) {
	if rsc == nil {
		return nil, "", fmt.Errorf("cannot infer describe method from nil resource")
	}
	var method metadata.Method
	m, methodPresent := rsc.Methods["get"]
	if methodPresent {
		method = m
		return &method, "get", nil
	}
	m, methodPresent = rsc.Methods["aggregatedList"]
	if methodPresent {
		method = m
		return &method, "aggregatedList", nil
	}
	m, methodPresent = rsc.Methods["list"]
	if methodPresent {
		method = m
		return &method, "list", nil
	}
	var ms []string
	for k, v := range rsc.Methods {
		ms = append(ms, k)
		if strings.HasPrefix(k, "get") {
			method = v
			return &method, k, nil
		}
		if strings.HasPrefix(k, "list") {
			method = v
			return &method, k, nil
		}
	}
	return nil, "", fmt.Errorf("SELECT not supported for this resource, use SHOW METHODS to view available operations for the resource and then invoke a supported method using the EXEC command")
}

func (gp *GoogleProvider) retrieveSchemaMap(serviceName string, resourceName string) (map[string]metadata.Schema, error) {
	return gp.discoveryAdapter.GetSchemaMap(serviceName, resourceName)
}

func (gp *GoogleProvider) GetSchemaMap(serviceName string, resourceName string) (map[string]metadata.Schema, error) {
	return gp.discoveryAdapter.GetSchemaMap(serviceName, resourceName)
}

func (gp *GoogleProvider) GetObjectSchema(serviceName string, resourceName string, schemaName string) (*metadata.Schema, error) {
	sm, err := gp.retrieveSchemaMap(serviceName, resourceName)
	if err != nil {
		return nil, err
	}
	s := sm[schemaName]
	return &s, nil
}

type transport struct {
	token               []byte
	underlyingTransport http.RoundTripper
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add(
		"Authorization",
		fmt.Sprintf("Bearer %s", string(t.token)),
	)
	return t.underlyingTransport.RoundTrip(req)
}

func activateAuth(authCtx *dto.AuthCtx, principal string, authType string) {
	authCtx.Active = true
	authCtx.Type = authType
	if principal != "" {
		authCtx.ID = principal
	}
}

func deactivateAuth(authCtx *dto.AuthCtx) {
	authCtx.Active = false
}

func (gp *GoogleProvider) ShowAuth(authCtx *dto.AuthCtx) (*metadata.AuthMetadata, error) {
	var err error
	var retVal *metadata.AuthMetadata
	var authObj metadata.AuthMetadata
	switch gp.inferAuthType(*authCtx, authCtx.Type) {
	case dto.AuthServiceAccountStr:
		var sa googleServiceAccount
		sa, err = parseServiceAccountFile(authCtx.KeyFilePath)
		if err == nil {
			authObj = metadata.AuthMetadata{
				Principal: sa.Email,
				Type:      strings.ToUpper(dto.AuthServiceAccountStr),
				Source:    authCtx.KeyFilePath,
			}
			retVal = &authObj
			activateAuth(authCtx, sa.Email, dto.AuthServiceAccountStr)
		}
	case dto.AuthInteractiveStr:
		principal, sdkErr := sdk.GetCurrentAuthUser()
		if sdkErr == nil {
			principalStr := string(principal)
			if principalStr != "" {
				authObj = metadata.AuthMetadata{
					Principal: principalStr,
					Type:      strings.ToUpper(dto.AuthInteractiveStr),
					Source:    "OAuth",
				}
				retVal = &authObj
				activateAuth(authCtx, principalStr, dto.AuthInteractiveStr)
			} else {
				err = errors.New(constants.NotAuthenticatedShowStr)
			}
		} else {
			log.Infoln(sdkErr)
			err = errors.New(constants.NotAuthenticatedShowStr)
		}
	default:
		err = errors.New(constants.NotAuthenticatedShowStr)
	}
	return retVal, err
}

func (gp *GoogleProvider) oAuth(authCtx *dto.AuthCtx, enforceRevokeFirst bool) (*http.Client, error) {
	var err error
	var tokenBytes []byte
	var client *http.Client
	tokenBytes, err = sdk.GetAccessToken()
	if enforceRevokeFirst && authCtx.Type == dto.AuthInteractiveStr && err == nil {
		return nil, fmt.Errorf(constants.OAuthInteractiveAuthErrStr)
	}
	if err != nil {
		err = sdk.OAuthToGoogle()
		if err == nil {
			tokenBytes, err = sdk.GetAccessToken()
		}
	}
	if err == nil {
		activateAuth(authCtx, "", dto.AuthInteractiveStr)
		client = &http.Client{
			Transport: &transport{
				token:               tokenBytes,
				underlyingTransport: http.DefaultTransport,
			},
		}
	}
	return client, err
}

func (gp *GoogleProvider) keyFileAuth(authCtx *dto.AuthCtx) (*http.Client, error) {
	scopes := authCtx.Scopes
	if scopes == nil {
		scopes = []string{
			"https://www.googleapis.com/auth/cloud-platform",
		}
	}
	return serviceAccount(authCtx, scopes)
}

func (gp *GoogleProvider) getServiceType(service metadata.Service) string {
	specialServiceNamesMap := map[string]bool{
		"storage": true,
		"compute": true,
		"dns":     true,
		"sql":     true,
	}
	nameIsSpecial, ok := specialServiceNamesMap[service.Name]
	cloudRegex := regexp.MustCompile(`(^https://.*cloud\.google\.com|^https://firebase\.google\.com)`)
	if service.Preferred && (cloudRegex.MatchString(service.DocLink) || (ok && nameIsSpecial)) {
		return "cloud"
	}
	return "developer"
}

func (gp *GoogleProvider) GetLikeableColumns(tableName string) []string {
	var retVal []string
	switch tableName {
	case "SERVICES":
		return []string{
			"id",
			"name",
		}
	case "RESOURCES":
		return []string{
			"id",
			"name",
		}
	case "METHODS":
		return []string{
			"id",
			"name",
		}
	case "PROVIDERS":
		return []string{
			"name",
		}
	}
	return retVal
}

func (gp *GoogleProvider) EnhanceMetadataFilter(metadataType string, metadataFilter func(iqlmodel.ITable) (iqlmodel.ITable, error), colsVisited map[string]bool) (func(iqlmodel.ITable) (iqlmodel.ITable, error), error) {
	typeVisited, typeOk := colsVisited["type"]
	preferredVisited, preferredOk := colsVisited["preferred"]
	sqlTrue, sqlTrueErr := sqltypeutil.InterfaceToSQLType(true)
	sqlCloudStr, sqlCloudStrErr := sqltypeutil.InterfaceToSQLType("cloud")
	equalsOperator, operatorErr := relational.GetOperatorPredicate("=")
	if sqlTrueErr != nil || sqlCloudStrErr != nil || operatorErr != nil {
		return nil, fmt.Errorf("typing and operator system broken!!!")
	}
	switch metadataType {
	case "service":
		if typeOk && typeVisited && preferredOk && preferredVisited {
			return metadataFilter, nil
		}
		if typeOk && typeVisited {
			return relational.AndTableFilters(
				metadataFilter,
				relational.ConstructTablePredicateFilter("preferred", sqlTrue, equalsOperator),
			), nil
		}
		if preferredOk && preferredVisited {
			return relational.AndTableFilters(
				metadataFilter,
				relational.ConstructTablePredicateFilter("type", sqlCloudStr, equalsOperator),
			), nil
		}
		return relational.AndTableFilters(
			relational.AndTableFilters(
				metadataFilter,
				relational.ConstructTablePredicateFilter("cloud", sqlCloudStr, equalsOperator),
			),
			relational.ConstructTablePredicateFilter("preferred", sqlTrue, equalsOperator),
		), nil
	}
	return metadataFilter, nil
}

func (gp *GoogleProvider) GetProviderServices() (map[string]metadata.Service, error) {
	retVal := make(map[string]metadata.Service)
	disDoc, err := gp.discoveryAdapter.GetServiceHandlesMap()
	if err != nil {
		return nil, err
	}
	for _, item := range disDoc {
		item.Service.Type = gp.getServiceType(item.Service)
		retVal[googlediscovery.TranslateServiceKeyGoogleToIql(item.Service.ID)] = item.Service
	}
	return retVal, nil
}

func (gp *GoogleProvider) GetProviderServicesRedacted(runtimeCtx dto.RuntimeCtx, extended bool) (map[string]metadata.Service, []string, error) {
	services, err := gp.GetProviderServices()
	if err != nil {
		return nil, nil, err
	}
	retVal := make(map[string]metadata.Service)
	for key, item := range services {
		item.ID = key
		retVal[key] = item
	}
	return retVal, gp.getServicesHeader(extended), err
}

func (gp *GoogleProvider) getDescribeHeader(extended bool) []string {
	var retVal []string
	if extended {
		retVal = []string{
			"name",
			"type",
			"description",
		}
	} else {
		retVal = []string{
			"name",
			"type",
		}
	}
	return retVal
}

func (gp *GoogleProvider) getServicesHeader(extended bool) []string {
	var retVal []string
	if extended {
		retVal = []string{
			"id",
			"name",
			"title",
			"description",
			"version",
			"preferred",
		}
	} else {
		retVal = []string{
			"id",
			"name",
			"title",
		}
	}
	return retVal
}

func (gp *GoogleProvider) getResourcesHeader(extended bool) []string {
	var retVal []string
	if extended {
		retVal = []string{
			"name",
			"id",
			"title",
			"description",
		}
	} else {
		retVal = []string{
			"name",
			"id",
			"title",
		}
	}
	return retVal
}

func (gp *GoogleProvider) GetResourcesRedacted(currentService string, runtimeCtx dto.RuntimeCtx, extended bool) (map[string]metadata.Resource, []string, error) {
	svcDiscDocMap, err := gp.discoveryAdapter.GetResourcesMap(currentService)
	headers := gp.getResourcesHeader(extended)
	return svcDiscDocMap, headers, err
}

func (gp *GoogleProvider) DescribeResource(serviceName string, resourceName string, runtimeCtx dto.RuntimeCtx, extended bool, full bool) (*metadata.Schema, []string, error) {
	header := gp.getDescribeHeader(extended)
	canonicalError := fmt.Errorf("can't find DESCRIBE schema for service '%s' resource '%s'", serviceName, resourceName)

	describeErr := fmt.Errorf("Error generating DESCRIBE for service = '%s' and resource = '%s'", serviceName, resourceName)
	rescources, err := gp.discoveryAdapter.GetResourcesMap(serviceName)
	if err != nil {
		return nil, nil, canonicalError
	}
	rsc, ok := rescources[resourceName]
	if !ok {
		return nil, header, describeErr
	}
	m, _, err := gp.InferDescribeMethod(&rsc)
	if err != nil {
		return nil, nil, canonicalError
	}
	schemaName := m.ResponseType.Type
	sm, err := gp.discoveryAdapter.GetSchemaMap(serviceName, resourceName)
	if err != nil {
		return nil, nil, err
	}
	retVal, ok := sm[schemaName]
	if !ok {
		return nil, nil, fmt.Errorf("can't find schema '%s'", schemaName)
	}
	return &retVal, header, err
}

func parseServiceAccountFile(credentialFile string) (googleServiceAccount, error) {
	b, err := ioutil.ReadFile(credentialFile)
	var c googleServiceAccount
	if err != nil {
		return c, errors.New(constants.ServiceAccountPathErrStr)
	}
	return c, json.Unmarshal(b, &c)
}

func (gp *GoogleProvider) CheckServiceAccountFile(credentialFile string) error {
	_, err := parseServiceAccountFile(credentialFile)
	return err
}

func serviceAccount(authCtx *dto.AuthCtx, scopes []string) (*http.Client, error) {
	credentialFile := authCtx.KeyFilePath
	b, err := ioutil.ReadFile(credentialFile)
	if err != nil {
		return nil, errors.New(constants.ServiceAccountPathErrStr)
	}
	config, errToken := google.JWTConfigFromJSON(b, scopes...)
	if errToken != nil {
		return nil, errToken
	}
	activateAuth(authCtx, "", dto.AuthServiceAccountStr)
	if DummyAuth {
		return http.DefaultClient, nil
	}
	return config.Client(oauth2.NoContext), nil
}

func (gp *GoogleProvider) GenerateHTTPRestInstruction(httpContext httpexec.IHttpContext) (httpexec.IHttpContext, error) {
	return httpContext, nil
}

func (gp *GoogleProvider) Parameterise(httpContext httpexec.IHttpContext, parameters *metadata.HttpParameters, requestSchema *metadata.Schema) (httpexec.IHttpContext, error) {
	visited := make(map[string]bool)
	args := make([]string, len(parameters.PathParams)*2)
	var sb strings.Builder
	var queryParams []string
	i := 0
	for k, v := range parameters.PathParams {
		if strings.Contains(httpContext.GetTemplateUrl(), "{"+k+"}") {
			args[i] = "{" + k + "}"
			args[i+1] = fmt.Sprint(v)
			i += 2
			visited[k] = true
			continue
		}
		if strings.Contains(httpContext.GetTemplateUrl(), "{+"+k+"}") {
			args[i] = "{+" + k + "}"
			args[i+1] = fmt.Sprint(v)
			i += 2
			visited[k] = true
			continue
		}
	}
	if len(parameters.QueryParams) > 0 {
		sb.WriteString("?")
	}
	for k, v := range parameters.QueryParams {
		vStr, vOk := v.(string)
		if isVisited, kExists := visited[k]; !kExists || (!isVisited && vOk) {
			queryParams = append(queryParams, k+"="+vStr)
			visited[k] = true
		}
	}
	sb.WriteString(strings.Join(queryParams, "&"))
	httpContext.SetUrl(strings.NewReplacer(args...).Replace(httpContext.GetTemplateUrl()) + sb.String())
	return httpContext, nil
}

func (gp *GoogleProvider) SetCurrentService(serviceKey string) {
	gp.currentService = serviceKey

}

func (gp *GoogleProvider) GetCurrentService() string {
	return gp.currentService
}

func (gp *GoogleProvider) getPathParams(httpContext httpexec.IHttpContext) map[string]bool {
	re := regexp.MustCompile(`\{([^\{\}]+)\}`)
	keys := re.FindAllString(httpContext.GetTemplateUrl(), -1)
	retVal := make(map[string]bool, len(keys))
	for _, k := range keys {
		retVal[strings.Trim(k, "{}")] = true
	}
	return retVal
}

func (gp *GoogleProvider) GetResourcesMap(serviceKey string, runtimeCtx dto.RuntimeCtx) (map[string]metadata.Resource, error) {
	return gp.discoveryAdapter.GetResourcesMap(serviceKey)
}

func (gp *GoogleProvider) GetResource(serviceKey string, resourceKey string, runtimeCtx dto.RuntimeCtx) (*metadata.Resource, error) {
	rm, err := gp.GetResourcesMap(serviceKey, runtimeCtx)
	retVal, ok := rm[resourceKey]
	if !ok {
		return nil, fmt.Errorf("Could not obtain resource '%s' from service '%s'", resourceKey, serviceKey)
	}
	return &retVal, err
}

func (gp *GoogleProvider) GetProviderString() string {
	return googleProviderName
}
