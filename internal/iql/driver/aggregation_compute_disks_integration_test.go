package driver_test

import (
	"bufio"
	"infraql/internal/iql/config"
	. "infraql/internal/iql/driver"
	"infraql/internal/iql/entryutil"
	"infraql/internal/iql/querysubmit"
	"infraql/internal/iql/responsehandler"
	"infraql/internal/test/infraqltestutil"
	"infraql/internal/test/testobjects"
	"strings"
	"testing"

	lrucache "vitess.io/vitess/go/cache"
)

func TestSelectComputeDisksOrderByCrtTmstpAsc(t *testing.T) {

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

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.TxnCounterMgr = tc

		handlerCtx.Query = testobjects.SelectGoogleComputeDisksOrderCreationTmstpAsc
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)

		ProcessQuery(&handlerCtx)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeDisks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSelectComputeDisksOrderCrtTmstpAsc})

}

func TestSelectComputeDisksAggOrderBySizeAsc(t *testing.T) {

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

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.TxnCounterMgr = tc

		handlerCtx.Query = testobjects.SelectGoogleComputeDisksAggOrderSizeAsc
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)

		ProcessQuery(&handlerCtx)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeDisks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSelectComputeDisksAggSizeOrderSizeAsc})

}

func TestSelectComputeDisksAggOrderBySizeDesc(t *testing.T) {

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

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.TxnCounterMgr = tc

		handlerCtx.Query = testobjects.SelectGoogleComputeDisksAggOrderSizeDesc
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)

		ProcessQuery(&handlerCtx)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeDisks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSelectComputeDisksAggSizeOrderSizeDesc})

}

func TestSelectComputeDisksAggTotalSize(t *testing.T) {

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

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.TxnCounterMgr = tc

		handlerCtx.Query = testobjects.SelectGoogleComputeDisksAggSizeTotal
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)

		ProcessQuery(&handlerCtx)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeDisks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSelectComputeDisksAggSizeTotal})

}

func TestSelectComputeDisksAggTotalString(t *testing.T) {

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

		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		tc, err := entryutil.GetTxnCounterManager(handlerCtx)
		if err != nil {
			t.Fatalf("Test failed: %v", err)
		}

		handlerCtx.TxnCounterMgr = tc

		handlerCtx.Query = testobjects.SelectGoogleComputeDisksAggStringTotal
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)

		ProcessQuery(&handlerCtx)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeDisks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSelectComputeDisksAggStringTotal})

}
