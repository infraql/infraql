package driver_test

import (
	"strings"
	"testing"

	"bufio"

	. "infraql/internal/iql/driver"

	"infraql/internal/iql/config"
	"infraql/internal/iql/entryutil"
	"infraql/internal/iql/querysubmit"
	"infraql/internal/iql/responsehandler"

	"infraql/internal/test/infraqltestutil"
	"infraql/internal/test/testobjects"

	lrucache "vitess.io/vitess/go/cache"
)

func TestSelectComputeDisksOrderByCrtTmstpAscPlusJsonExtract(t *testing.T) {

	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "csv")
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

		handlerCtx.Query = testobjects.SelectGoogleComputeDisksOrderCreationTmstpAscPlusJsonExtract
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)

		ProcessQuery(&handlerCtx)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeDisks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSelectComputeDisksOrderCrtTmstpAscPlusJsonExtract})

}

func TestSelectComputeDisksOrderByCrtTmstpAscPlusCoalesceJsonExtract(t *testing.T) {

	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "csv")
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

		handlerCtx.Query = testobjects.SelectGoogleComputeDisksOrderCreationTmstpAscPlusJsonExtractCoalesce
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)

		ProcessQuery(&handlerCtx)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeDisks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSelectComputeDisksOrderCrtTmstpAscPlusJsonExtractCoalesce})

}

func TestSelectComputeDisksOrderByCrtTmstpAscPlusCoalesceJsonInstr(t *testing.T) {

	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "csv")
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

		handlerCtx.Query = testobjects.SelectGoogleComputeDisksOrderCreationTmstpAscPlusJsonExtractInstr
		response := querysubmit.SubmitQuery(&handlerCtx)
		handlerCtx.Outfile = outFile
		responsehandler.HandleResponse(&handlerCtx, response)

		ProcessQuery(&handlerCtx)
	}

	infraqltestutil.SetupSimpleSelectGoogleComputeDisks(t)
	infraqltestutil.RunCaptureTestAgainstFiles(t, testSubject, []string{testobjects.ExpectedSelectComputeDisksOrderCrtTmstpAscPlusJsonExtractInstr})

}
