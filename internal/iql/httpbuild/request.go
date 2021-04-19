package httpbuild

import (

	"fmt"
	"bytes"
	"net/http"
	"encoding/json"

	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/metadata"
	"infraql/internal/iql/parserutil"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/util"

	"vitess.io/vitess/go/vt/sqlparser"

	log "github.com/sirupsen/logrus"
)

type ExecContext struct {
	ExecPayload *dto.ExecPayload
	Resource    *metadata.Resource
}

func NewExecContext(payload *dto.ExecPayload, rsc *metadata.Resource) *ExecContext {
	return &ExecContext{
		ExecPayload: payload,
		Resource: rsc,
	}
}

type HTTPArmoury struct {
	Header http.Header
	Parameters *metadata.HttpParameters
	Context httpexec.IHttpContext
	BodyBytes []byte
	RequestSchema *metadata.Schema
	ResponseSchema *metadata.Schema
}

func NewHTTPArmoury() HTTPArmoury {
	return HTTPArmoury{
		Header: make(http.Header),
	}
}


func BuildHTTPRequestCtx(handlerCtx *handler.HandlerContext, node sqlparser.SQLNode, prov provider.IProvider, m *metadata.Method, schemaMap map[string]metadata.Schema, insertValOnlyRows map[int]map[int]interface{}, execContext *ExecContext) (*HTTPArmoury, error) {
	var err error
	if m.Protocol != "http" {
		return nil, nil
	}
	httpArmoury := NewHTTPArmoury()
	requestSchema, ok := schemaMap[m.RequestType.Type]
	if ok {
		httpArmoury.RequestSchema = &requestSchema
	} else {
		log.Infoln(fmt.Sprintf("cannot locate schema for response type = '%s'", m.RequestType.Type))
	}
	responseSchema, ok := schemaMap[m.ResponseType.Type]
	if ok {
		httpArmoury.ResponseSchema = &responseSchema
	} else {
		log.Infoln(fmt.Sprintf("cannot locate schema for response type = '%s'", m.ResponseType.Type))
	}
	if err != nil {
		return nil, err
	}
	paramMap, err := util.ExtractSQLNodeParams(node, insertValOnlyRows)
	if err != nil {
		return nil, err
	}
	httpArmoury.Parameters, err = metadata.SplitHttpParameters(paramMap, m, httpArmoury.RequestSchema, httpArmoury.ResponseSchema)
	if err != nil {
		return nil, err
	}
	if execContext != nil &&  execContext.ExecPayload != nil {
		httpArmoury.BodyBytes = execContext.ExecPayload.Payload
		for k, v := range execContext.ExecPayload.Header {
			httpArmoury.Header[k] = v
		}
	}
	if httpArmoury.Parameters.RequestBody != nil && len(httpArmoury.Parameters.RequestBody) != 0 {
		b, err := json.Marshal(httpArmoury.Parameters.RequestBody)
		if err != nil {
			return nil, err
		}
		httpArmoury.BodyBytes = b
		httpArmoury.Header["Content-Type"] = []string{"application/json"}
	}
	var baseRequestCtx httpexec.IHttpContext
	switch node := node.(type) {
	case *sqlparser.Delete:
		baseRequestCtx, err = getDeleteRequestCtx(handlerCtx, prov, node, m)
	case *sqlparser.Exec:
		baseRequestCtx, err = getExecRequestCtx(execContext.Resource, m)
	case *sqlparser.Insert:
		baseRequestCtx, err = getInsertRequestCtx(handlerCtx, prov, node, m)
	case *sqlparser.Select:
		baseRequestCtx, err = getSelectRequestCtx(handlerCtx, prov, node, m)
	default:
		return nil, fmt.Errorf("cannot create http primitive for sql node of type %T", node)
	}
	if err != nil {
		return nil, err
	}
	httpArmoury.Context, err = prov.Parameterise(baseRequestCtx, httpArmoury.Parameters, httpArmoury.RequestSchema)
	if httpArmoury.BodyBytes != nil && httpArmoury.Header != nil && len(httpArmoury.Header) > 0 {
		httpArmoury.Context.SetBody(bytes.NewReader(httpArmoury.BodyBytes))
		httpArmoury.Context.SetHeaders(httpArmoury.Header)
	}
	if err != nil {
		return nil, err
	}
	return &httpArmoury, nil
}

func getSelectRequestCtx(handlerCtx *handler.HandlerContext, prov provider.IProvider, node *sqlparser.Select, method *metadata.Method) (httpexec.IHttpContext, error) {
	var path string
	var httpVerb string
	var err error
	currentSvcRsc, _ := sqlparser.TableFromStatement(handlerCtx.Query)
	currentService := currentSvcRsc.Qualifier.GetRawVal()
	currentResource := currentSvcRsc.Name.GetRawVal()
	rsc, err := prov.GetResource(currentService, currentResource, handlerCtx.RuntimeContext)
	path = path + rsc.BaseUrl
	path = path + method.Path
	httpVerb = method.Verb
	return httpexec.CreateTemplatedHttpContext(
			httpVerb,
			path,
			nil,
		),
		err
}

func getDeleteRequestCtx(handlerCtx *handler.HandlerContext, prov provider.IProvider, node *sqlparser.Delete, method *metadata.Method) (httpexec.IHttpContext, error) {
	var path string
	var httpVerb string
	var err error
	currentSvcRsc, err := parserutil.ExtractSingleTableFromTableExprs(node.TableExprs)
	if err != nil {
		return nil, err
	}
	currentService := currentSvcRsc.Qualifier.GetRawVal()
	currentResource := currentSvcRsc.Name.GetRawVal()
	rsc, err := prov.GetResource(currentService, currentResource, handlerCtx.RuntimeContext)
	path = path + rsc.BaseUrl
	path = path + method.Path
	httpVerb = method.Verb
	return httpexec.CreateTemplatedHttpContext(
			httpVerb,
			path,
			nil,
		),
		err
}

func getInsertRequestCtx(handlerCtx *handler.HandlerContext, prov provider.IProvider, node *sqlparser.Insert, method *metadata.Method) (httpexec.IHttpContext, error) {
	var path string
	var httpVerb string
	var err error
	currentSvcRsc := node.Table
	currentService := currentSvcRsc.Qualifier.GetRawVal()
	currentResource := currentSvcRsc.Name.GetRawVal()
	rsc, err := prov.GetResource(currentService, currentResource, handlerCtx.RuntimeContext)
	path = path + rsc.BaseUrl
	path = path + method.Path
	httpVerb = method.Verb
	return httpexec.CreateTemplatedHttpContext(
			httpVerb,
			path,
			nil,
		),
		err
}

func getExecRequestCtx(rsc *metadata.Resource, method *metadata.Method) (httpexec.IHttpContext, error) {
	path := rsc.BaseUrl + method.Path
	httpVerb := method.Verb
	return httpexec.CreateTemplatedHttpContext(
			httpVerb,
			path,
			nil,
		),
		nil
}