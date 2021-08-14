package httpexec

import (
	"encoding/json"
	"fmt"
	"infraql/internal/iql/util"
	"io"
	"net/http"
	"net/url"
)

type IHttpContext interface {
	GetHeaders() http.Header
	GetMethod() string
	GetTemplateUrl() string
	GetUrl() (string, error)
	SetHeader(string, string)
	SetQueryParam(string, string)
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
	queryParams map[string]string
}

func CreateTemplatedHttpContext(method string, templateUrl string, headers http.Header) IHttpContext {
	return &BasicHttpContext{
		method:      method,
		templateUrl: templateUrl,
		headers:     headers,
		queryParams: make(map[string]string),
	}
}

func CreateNonTemplatedHttpContext(method string, url string, headers http.Header) IHttpContext {
	return &BasicHttpContext{
		method:      method,
		url:         url,
		headers:     headers,
		queryParams: make(map[string]string),
	}
}

func (bc *BasicHttpContext) GetMethod() string {
	return bc.method
}

func (bc *BasicHttpContext) GetHeaders() http.Header {
	return bc.headers
}

func (bc *BasicHttpContext) GetUrl() (string, error) {
	urlObj, err := url.Parse(bc.url)
	if err != nil {
		return "", err
	}
	q := urlObj.Query()
	for k, v := range bc.queryParams {
		q.Set(k, v)
	}
	urlObj.RawQuery = q.Encode()
	return urlObj.String(), nil
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

func (bc *BasicHttpContext) SetQueryParam(k string, v string) {
	bc.queryParams[k] = v
}

func HTTPApiCall(httpClient *http.Client, requestCtx IHttpContext) (*http.Response, error) {
	urlStr, err := requestCtx.GetUrl()
	if err != nil {
		return nil, err
	}
	req, requestErr := http.NewRequest(requestCtx.GetMethod(), urlStr, requestCtx.GetBody())
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
	if err == io.EOF {
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return map[string]interface{}{"result": "The Operation Completed Successfully"}, nil
		}
	}
	return target, err
}
