package driver_test

import (
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"testing"

	"bufio"

	. "infraql/internal/iql/driver"
	"infraql/internal/iql/util"

	"infraql/internal/iql/config"
	"infraql/internal/iql/entryutil"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/querysubmit"
	"infraql/internal/iql/responsehandler"

	"infraql/internal/test/infraqltestutil"
	"infraql/internal/test/testhttpapi"
	"infraql/internal/test/testobjects"

	lrucache "vitess.io/vitess/go/cache"
)

func TestSimpleSelectGoogleComputeInstanceDriver(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	path := "/compute/v1/projects/testing-project/zones/australia-southeast1-b/instances"
	url := &url.URL{
		Path: path,
	}
	ex := testhttpapi.NewHTTPRequestExpectations(nil, nil, "GET", url, "compute.googleapis.com", testobjects.SimpleSelectGoogleComputeInstanceResponse, nil)
	expectations := map[string]testhttpapi.HTTPRequestExpectations{
		"compute.googleapis.com" + path: *ex,
	}
	exp := testhttpapi.NewExpectationStore()
	for k, v := range expectations {
		exp.Put(k, v)
	}
	testhttpapi.StartServer(t, exp)
	provider.DummyAuth = true

	sqlEng, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

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

	ProcessQuery(handlerCtx)

	t.Logf("simple select driver integration test passed")
}

func TestSimpleSelectGoogleComputeInstanceDriverOutput(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	sqlEngine, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

	testSubject := func(t *testing.T, outFile *bufio.Writer) {

		handlerCtx, err := entryutil.BuildHandlerContext(*runtimeCtx, strings.NewReader(""), lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEngine)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		handlerCtx.TxnCounterMgr = tc

		handlerCtx.Outfile = outFile
		handlerCtx.OutErrFile = os.Stderr

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.Query = testobjects.SimpleSelectGoogleComputeInstance
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeInstance(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSimpleSelectGoogleComputeInstanceTextFile01, testobjects.ExpectedSimpleSelectGoogleComputeInstanceTextFile02})

}

func TestSimpleSelectGoogleComputeInstanceDriverOutputRepeated(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	sqlEngine, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

	testSubject := func(t *testing.T, outFile *bufio.Writer) {

		handlerCtx, err := entryutil.BuildHandlerContext(*runtimeCtx, strings.NewReader(""), lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEngine)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.Outfile = outFile
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
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeInstance(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSimpleSelectGoogleComputeInstanceTextFile01, testobjects.ExpectedSimpleSelectGoogleComputeInstanceTextFile02})

}

func TestSimpleSelectGoogleContainerSubnetworksAllowedDriverOutput(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	sqlEngine, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

	testSubject := func(t *testing.T, outFile *bufio.Writer) {

		handlerCtx, err := entryutil.BuildHandlerContext(*runtimeCtx, strings.NewReader(""), lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEngine)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.Outfile = outFile
		handlerCtx.OutErrFile = os.Stderr

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		handlerCtx.TxnCounterMgr = tc

		handlerCtx.Query = testobjects.SimpleSelectGoogleContainerSubnetworks
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)
	}

	infraqltestutil.SetupSimpleSelectGoogleContainerAggAllowedSubnetworks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSimpleSelectGoogleCotainerSubnetworkTextFile01, testobjects.ExpectedSimpleSelectGoogleCotainerSubnetworkTextFile02})

}

func TestSimpleInsertGoogleComputeNetworkAsync(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	sqlEngine, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

	testSubject := func(t *testing.T, outFile *bufio.Writer) {

		handlerCtx, err := entryutil.BuildHandlerContext(*runtimeCtx, strings.NewReader(""), lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEngine)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.Outfile = outFile
		handlerCtx.OutErrFile = os.Stderr

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		handlerCtx.TxnCounterMgr = tc

		handlerCtx.Query = testobjects.SimpleInsertComputeNetwork
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)
	}

	infraqltestutil.SetupSimpleInsertGoogleComputeNetworks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedComputeNetworkInsertAsyncFile})

}

func TestK8sTheHardWayAsync(t *testing.T) {

	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	sqlEngine, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

	testSubject := func(t *testing.T, outFile *bufio.Writer) {
		k8sthwRenderedFile, err := util.GetFilePathFromRepositoryRoot(testobjects.ExpectedK8STheHardWayRenderedFile)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		megaQueryConcat, err := ioutil.ReadFile(k8sthwRenderedFile)
		if err != nil {
			t.Fatalf("%v", err)
		}
		runtimeCtx.InfilePath = k8sthwRenderedFile
		runtimeCtx.CSVHeadersDisable = true

		handlerCtx, err := entryutil.BuildHandlerContext(*runtimeCtx, strings.NewReader(""), lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEngine)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.Outfile = outFile
		handlerCtx.OutErrFile = os.Stderr

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		handlerCtx.TxnCounterMgr = tc

		handlerCtx.RawQuery = strings.TrimSpace(string(megaQueryConcat))
		ProcessQuery(handlerCtx)
	}

	infraqltestutil.SetupK8sTheHardWayE2eSuccess(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedK8STheHardWayAsyncFile})

}

func TestSimpleShowResourcesFiltered(t *testing.T) {

	testSubject := func(t *testing.T, outFile *bufio.Writer) {

		runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
		if err != nil {
			t.Fatalf("TestSimpleShowResourcesFiltered failed: %v", err)
		}
		sqlEngine, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		showInsertFile, err := util.GetFilePathFromRepositoryRoot(testobjects.SimpleShowResourcesFilteredFile)
		if err != nil {
			t.Fatalf("TestSimpleShowResourcesFiltered failed: %v", err)
		}
		runtimeCtx.InfilePath = showInsertFile
		runtimeCtx.OutputFormat = "text"
		runtimeCtx.CSVHeadersDisable = true

		rdr, err := os.Open(runtimeCtx.InfilePath)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx, err := entryutil.BuildHandlerContext(*runtimeCtx, rdr, lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEngine)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.Outfile = outFile
		handlerCtx.OutErrFile = os.Stderr

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		handlerCtx.TxnCounterMgr = tc

		ProcessQuery(handlerCtx)
	}

	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedShowResourcesFilteredFile})

}

func TestSimpleDryRunK8sTheHardWayDriver(t *testing.T) {

	testSubject := func(t *testing.T, outFile *bufio.Writer) {

		runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
		if err != nil {
			t.Fatalf("TestSimpleDryRunDriver failed: %v", err)
		}
		sqlEngine, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		templateFile, err := util.GetFilePathFromRepositoryRoot(testobjects.K8STheHardWayTemplateFile)
		if err != nil {
			t.Fatalf("TestSimpleDryRunDriver failed: %v", err)
		}
		templateCtxFile, err := util.GetFilePathFromRepositoryRoot(testobjects.K8STheHardWayTemplateContextFile)
		if err != nil {
			t.Fatalf("TestSimpleDryRunDriver failed: %v", err)
		}
		runtimeCtx.InfilePath = templateFile
		runtimeCtx.TemplateCtxFilePath = templateCtxFile
		runtimeCtx.DryRunFlag = true
		runtimeCtx.CSVHeadersDisable = true

		rdr, err := os.Open(runtimeCtx.InfilePath)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx, err := entryutil.BuildHandlerContext(*runtimeCtx, rdr, lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEngine)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.Outfile = outFile
		handlerCtx.OutErrFile = os.Stderr

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		handlerCtx.TxnCounterMgr = tc

		ProcessDryRun(handlerCtx)
	}

	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedK8STheHardWayRenderedFile})

}

func TestSimpleShowInsertComputeAddressesRequired(t *testing.T) {

	testSubject := func(t *testing.T, outFile *bufio.Writer) {

		runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
		if err != nil {
			t.Fatalf("TestSimpleTemplateComputeAddressesRequired failed: %v", err)
		}
		sqlEngine, err := infraqltestutil.BuildSQLEngine(*runtimeCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		showInsertFile, err := util.GetFilePathFromRepositoryRoot(testobjects.ShowInsertAddressesRequiredInputFile)
		if err != nil {
			t.Fatalf("TestSimpleTemplateComputeAddressesRequired failed: %v", err)
		}
		runtimeCtx.InfilePath = showInsertFile
		runtimeCtx.CSVHeadersDisable = true

		rdr, err := os.Open(runtimeCtx.InfilePath)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx, err := entryutil.BuildHandlerContext(*runtimeCtx, rdr, lrucache.NewLRUCache(int64(runtimeCtx.QueryCacheSize)), sqlEngine)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.Outfile = outFile
		handlerCtx.OutErrFile = os.Stderr

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}
		handlerCtx.TxnCounterMgr = tc

		ProcessQuery(handlerCtx)
	}

	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedShowInsertAddressesRequiredFile})

}
