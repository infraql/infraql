package asyncmonitor

import (
	"fmt"
	"infraql/internal/iql/drm"
	"infraql/internal/iql/dto"
	"infraql/internal/iql/httpexec"
	"infraql/internal/iql/plan"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/taxonomy"
	"infraql/internal/iql/util"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var MonitorPollIntervalSeconds int = 10

type IAsyncMonitor interface {
	GetMonitorPrimitive(heirarchy *taxonomy.HeirarchyObjects, precursor plan.IPrimitive, initialCtx plan.IPrimitiveCtx) (plan.IPrimitive, error)
}

type AsyncHttpMonitorPrimitive struct {
	heirarchy           *taxonomy.HeirarchyObjects
	initialCtx          plan.IPrimitiveCtx
	precursor           plan.IPrimitive
	transferPayload     map[string]interface{}
	Executor            func(pc plan.IPrimitiveCtx) dto.ExecutorOutput
	monitorExecutor     func(pc plan.IPrimitiveCtx) dto.ExecutorOutput
	elapsedSeconds      int
	pollIntervalSeconds int
	noStatus            bool
}

func (asm *AsyncHttpMonitorPrimitive) Execute(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
	if asm.Executor != nil {
		if pc == nil {
			pc = asm.initialCtx
		}
		pr := asm.precursor.Execute(pc)
		if pr.Err != nil || asm.Executor == nil {
			return pr
		}
		asyP := dto.NewBasicPrimitiveContext(
			pr.OutputBody,
			asm.initialCtx.GetAuthContext(),
			pc.GetWriter(),
			pc.GetErrWriter(),
			pc.GetCommentDirectives(),
		)
		return asm.Executor(asyP)
	}
	return dto.NewExecutorOutput(nil, nil, nil, nil)
}

func (pr *AsyncHttpMonitorPrimitive) GetPreparedStatementContext() *drm.PreparedStatementCtx {
	return nil
}

func NewAsyncMonitor(prov provider.IProvider) (IAsyncMonitor, error) {
	switch prov.GetProviderString() {
	case "google":
		return newGoogleAsyncMonitor(prov, prov.GetVersion())
	}
	return nil, fmt.Errorf("async operation monitor for provider = '%s', api version = '%s' currently not supported", prov.GetProviderString(), prov.GetVersion())
}

func newGoogleAsyncMonitor(prov provider.IProvider, version string) (IAsyncMonitor, error) {
	switch version {
	case "v1":
		return &DefaultGoogleAsyncMonitor{
			provider: prov,
		}, nil
	}
	return nil, fmt.Errorf("async operation monitor for google, api version = '%s' currently not supported", version)
}

type DefaultGoogleAsyncMonitor struct {
	provider  provider.IProvider
	precursor plan.IPrimitive
}

func (gm *DefaultGoogleAsyncMonitor) GetMonitorPrimitive(heirarchy *taxonomy.HeirarchyObjects, precursor plan.IPrimitive, initialCtx plan.IPrimitiveCtx) (plan.IPrimitive, error) {
	switch strings.ToLower(heirarchy.Provider.GetVersion()) {
	case "v1":
		return gm.getV1Monitor(heirarchy, precursor, initialCtx)
	}
	return nil, fmt.Errorf("monitor primitive unavailable for service = '%s', resource = '%s', method = '%s'", heirarchy.HeirarchyIds.ServiceStr, heirarchy.HeirarchyIds.ResourceStr, heirarchy.HeirarchyIds.MethodStr)
}

func getOperationDescriptor(body map[string]interface{}) string {
	operationDescriptor := "operation"
	if body == nil {
		return operationDescriptor
	}
	if descriptor, ok := body["kind"]; ok {
		if descriptorStr, ok := descriptor.(string); ok {
			operationDescriptor = descriptorStr
			if typeElem, ok := body["operationType"]; ok {
				if typeStr, ok := typeElem.(string); ok {
					operationDescriptor = fmt.Sprintf("%s: %s", descriptorStr, typeStr)
				}
			}
		}
	}
	return operationDescriptor
}

func (gm *DefaultGoogleAsyncMonitor) getV1Monitor(heirarchy *taxonomy.HeirarchyObjects, precursor plan.IPrimitive, initialCtx plan.IPrimitiveCtx) (plan.IPrimitive, error) {
	asyncPrim := AsyncHttpMonitorPrimitive{
		heirarchy:           heirarchy,
		initialCtx:          initialCtx,
		precursor:           precursor,
		elapsedSeconds:      0,
		pollIntervalSeconds: MonitorPollIntervalSeconds,
	}
	if cd := initialCtx.GetCommentDirectives(); cd != nil {
		asyncPrim.noStatus = cd.IsSet("NOSTATUS")
	}
	if heirarchy.Method.ResponseType.Type == "Operation" {
		asyncPrim.Executor = func(pc plan.IPrimitiveCtx) dto.ExecutorOutput {
			if pc == nil {
				return dto.NewExecutorOutput(nil, nil, nil, fmt.Errorf("cannot execute monitor: nil plan primitive"))
			}
			body := pc.GetBody()
			if body == nil {
				return dto.NewExecutorOutput(nil, nil, nil, fmt.Errorf("cannot execute monitor: no body present"))
			}
			log.Infoln(fmt.Sprintf("body = %v", body))

			operationDescriptor := getOperationDescriptor(body)
			endTime, endTimeOk := body["endTime"]
			if endTimeOk && endTime != "" {
				return prepareReultSet(&asyncPrim, pc, body, operationDescriptor)
			}
			url, ok := body["selfLink"]
			if !ok {
				return dto.NewExecutorOutput(nil, nil, nil, fmt.Errorf("cannot execute monitor: no 'selfLink' property present"))
			}
			authCtx := pc.GetAuthContext()
			if authCtx == nil {
				return dto.NewExecutorOutput(nil, nil, nil, fmt.Errorf("cannot execute monitor: no auth context"))
			}
			httpClient, httpClientErr := gm.provider.Auth(authCtx, authCtx.Type, false)
			if httpClientErr != nil {
				return dto.NewExecutorOutput(nil, nil, nil, httpClientErr)
			}
			time.Sleep(time.Duration(asyncPrim.pollIntervalSeconds) * time.Second)
			asyncPrim.elapsedSeconds += asyncPrim.pollIntervalSeconds
			if !asyncPrim.noStatus {
				pc.GetWriter().Write([]byte(fmt.Sprintf("%s in progress, %d seconds elapsed", operationDescriptor, asyncPrim.elapsedSeconds) + fmt.Sprintln("")))
			}
			rc, err := getMonitorRequestCtx(url.(string))
			response, apiErr := httpexec.HTTPApiCall(httpClient, rc)
			if apiErr != nil {
				return dto.NewExecutorOutput(nil, nil, nil, apiErr)
			}
			target, err := httpexec.ProcessHttpResponse(response)
			if err != nil {
				return dto.NewExecutorOutput(nil, nil, nil, err)
			}
			return asyncPrim.Executor(dto.NewBasicPrimitiveContext(
				target,
				authCtx,
				pc.GetWriter(),
				pc.GetErrWriter(),
				pc.GetCommentDirectives(),
			))
		}
		return &asyncPrim, nil
	}
	return nil, nil
}

func prepareReultSet(prim *AsyncHttpMonitorPrimitive, pc plan.IPrimitiveCtx, target map[string]interface{}, operationDescriptor string) dto.ExecutorOutput {
	payload := dto.PrepareResultSetDTO{
		OutputBody:  target,
		Msg:         nil,
		RowMap:      nil,
		ColumnOrder: nil,
		RowSort:     nil,
		Err:         nil,
	}
	if !prim.noStatus {
		pc.GetWriter().Write([]byte(fmt.Sprintf("%s complete", operationDescriptor) + fmt.Sprintln("")))
	}
	return util.PrepareResultSet(payload)
}

func getMonitorRequestCtx(url string) (httpexec.IHttpContext, error) {
	return httpexec.CreateNonTemplatedHttpContext(
			"GET",
			url,
			nil,
		),
		nil
}
