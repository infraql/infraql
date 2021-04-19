package httpexec

import (
	"encoding/json"
	"fmt"
	"infraql/internal/iql/util"
	"io"
	"net/http"
)

type IHttpContext interface {
	GetHeaders() http.Header
	GetMethod() string
	GetTemplateUrl() string
	GetUrl() string
	SetHeader(string, string)
	SetHeaders(http.Header)
	SetMethod(string)
	SetUrl(string)
	SetBody(io.Reader)
	GetBody() io.Reader
}

type BasicHttpContext struct {
	method      string
	templateUrl string
	url         string
	headers     http.Header
	body        io.Reader
}

func CreateTemplatedHttpContext(method string, templateUrl string, headers http.Header) IHttpContext {
	return &BasicHttpContext{
		method:      method,
		templateUrl: templateUrl,
		headers:     headers,
	}
}

func CreateNonTemplatedHttpContext(method string, url string, headers http.Header) IHttpContext {
	return &BasicHttpContext{
		method:  method,
		url:     url,
		headers: headers,
	}
}

func (bc *BasicHttpContext) GetMethod() string {
	return bc.method
}

func (bc *BasicHttpContext) GetHeaders() http.Header {
	return bc.headers
}

func (bc *BasicHttpContext) GetUrl() string {
	return bc.url
}

func (bc *BasicHttpContext) SetBody(body io.Reader) {
	bc.body = body
}

func (bc *BasicHttpContext) GetBody() io.Reader {
	return bc.body
}

func (bc *BasicHttpContext) GetTemplateUrl() string {
	return bc.templateUrl
}

func (bc *BasicHttpContext) SetMethod(method string) {
	bc.method = method
}

func (bc *BasicHttpContext) SetUrl(url string) {
	bc.url = url
}

func (bc *BasicHttpContext) SetHeaders(headers http.Header) {
	bc.headers = headers
}

func (bc *BasicHttpContext) SetHeader(k string, v string) {
	if headerVals, ok := bc.headers[k]; ok {
		bc.headers[k] = append(headerVals, v)
	}
	bc.headers[k] = []string{v}
}

func HTTPApiCall(httpClient *http.Client, requestCtx IHttpContext) (*http.Response, error) {
	req, requestErr := http.NewRequest(requestCtx.GetMethod(), requestCtx.GetUrl(), requestCtx.GetBody())
	for k, v := range requestCtx.GetHeaders() {
		for i := range v {
			req.Header.Set(k, v[i])
		}
	}
	if requestErr != nil {
		return nil, requestErr
	}
	response, reponseErr := httpClient.Do(req)
	if reponseErr != nil {
		return nil, reponseErr
	}
	return response, nil
}

func ProcessHttpResponse(response *http.Response) (map[string]interface{}, error) {
	body := response.Body
	if body != nil {
		defer body.Close()
	}
	var target map[string]interface{}
	err := json.NewDecoder(body).Decode(&target)
	if err == nil && response.StatusCode >= 400 {
		err = fmt.Errorf(fmt.Sprintf("HTTP response error: %s", string(util.InterfaceToBytes(target, true))))
	}
	return target, err
}
