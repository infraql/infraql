package httpmiddleware

import (
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/provider"
	"net/http"
)

func HttpApiCall(handlerCtx handler.HandlerContext, prov provider.IProvider, requestCtx httpexec.IHttpContext) (*http.Response, error) {
	authCtx, authErr := handlerCtx.GetAuthContext(prov.GetProviderString())
	if authErr != nil {
		return nil, authErr
	}
	httpClient, httpClientErr := prov.Auth(authCtx, authCtx.Type, false)
	if httpClientErr != nil {
		return nil, httpClientErr
	}
	return httpexec.HTTPApiCall(httpClient, requestCtx)
}
