package querysubmit_test

import (
	"net/url"
	"os"
	"testing"

	"infraql/internal/iql/entryutil"
	. "infraql/internal/iql/querysubmit"

	"infraql/internal/iql/config"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/provider"

	"infraql/internal/test/infraqltestutil"
	"infraql/internal/test/testhttpapi"
	"infraql/internal/test/testobjects"

	lrucache "vitess.io/vitess/go/cache"
)

func TestSimpleSelectGoogleComputeInstanceQuerySubmit(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	path := "/compute/v1/projects/testing-project/zones/australia-southeast1-b/instances"
	url := &url.URL{
		Path: path,
	}
	ex := testhttpapi.NewHTTPRequestExpectations(nil, nil, "GET", url, testobjects.GoogleComputeHost, testobjects.SimpleSelectGoogleComputeInstanceResponse, nil)
	exp := testhttpapi.NewExpectationStore(1)
	exp.Put(testobjects.GoogleComputeHost+path, *ex)

	testhttpapi.StartServer(t, exp)
	provider.DummyAuth = true

	sqlEng, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)

	handlerCtx, err := handler.GetHandlerCtx(testobjects.SimpleSelectGoogleComputeInstance, *runtimeCtx, lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEng)
	handlerCtx.Outfile = os.Stdout
	handlerCtx.OutErrFile = os.Stderr

	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

	tc, err := entryutil.GetTxnCounterManager(handlerCtx)

	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

	handlerCtx.TxnCounterMgr = tc

	handlerCtx.Query = testobjects.SimpleSelectGoogleComputeInstance
	response := SubmitQuery(&handlerCtx)

	if len(response.Result.Rows) != 2 {
		t.Fatalf("response size not as expected, actual != expected: %d != %d", len(response.Result.Rows), 2)
	}

	t.Logf("simple select driver integration test passed")
}
