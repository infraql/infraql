package main

import (
	"fmt"

	"infraql/internal/iql/config"
	"infraql/internal/iql/provider"

	"infraql/internal/test/testobjects"
	"infraql/internal/test/testhttpapi"
	"infraql/internal/test/infraqltestutil"

	"net/url"
	"os"
	"strings"
	"testing"
)

func TestSimpleSelectGoogleComputeInstance(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	path := "/compute/v1/projects/testing-project/zones/australia-southeast1-b/instances"
	url := &url.URL{
		Path: path,
	}
	ex := testhttpapi.NewHTTPRequestExpectations(nil, nil, "GET", url, "compute.googleapis.com", testobjects.SimpleSelectGoogleComputeInstanceResponse, nil)
	exp := testhttpapi.NewExpectationStore()
	exp.Put("compute.googleapis.com" + path, *ex)
	testhttpapi.StartServer(t, exp)
	provider.DummyAuth = true
	args := []string{
		"--loglevel=info",
		fmt.Sprintf("--keyfilepath=%s", runtimeCtx.KeyFilePath),
		fmt.Sprintf("--providerroot=%s", runtimeCtx.ProviderRootPath),
		"-i=stdin",
		"exec",
		testobjects.SimpleSelectGoogleComputeInstance,
	}
	t.Logf("simple select integration: about to invoke main() with args:\n\t%s", strings.Join(args, ",\n\t"))
	os.Args = args
	err = execute()
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	t.Logf("simple select integration test passed")
}

func TestExecSimpleSelectGoogleComputeInstance(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "table")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	path := "/compute/v1/projects/testing-project/zones/australia-southeast1-b/instances"
	url := &url.URL{
		Path: path,
	}
	ex := testhttpapi.NewHTTPRequestExpectations(nil, nil, "GET", url, "compute.googleapis.com", testobjects.SimpleSelectGoogleComputeInstanceResponse, nil)
	exp := testhttpapi.NewExpectationStore()
	exp.Put("compute.googleapis.com" + path, *ex)
	testhttpapi.StartServer(t, exp)
	provider.DummyAuth = true
	args := []string{
		"--loglevel=info",
		fmt.Sprintf("--keyfilepath=%s", runtimeCtx.KeyFilePath),
		fmt.Sprintf("--providerroot=%s", runtimeCtx.ProviderRootPath),
		"-i=stdin",
		"exec",
		testobjects.SimpleSelectGoogleComputeInstance,
	}
	t.Logf("simple select integration: about to invoke main() with args:\n\t%s", strings.Join(args, ",\n\t"))
	os.Args = args
	infraqltestutil.RunStdOutTestAgainstFiles(t, execStuff, []string{testobjects.ExpectedSimpleSelectGoogleComputeInstanceTableFile01, testobjects.ExpectedSimpleSelectGoogleComputeInstanceTableFile02})
}

func TestExecSimpleSelectGoogleContainerSubnetworks(t *testing.T) {
	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "table")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	path := "/v1/projects/testing-project/aggregated/usableSubnetworks"
	url := &url.URL{
		Path: path,
	}
	ex := testhttpapi.NewHTTPRequestExpectations(nil, nil, "GET", url, "container.googleapis.com", testobjects.SimpleSelectGoogleContainerAggregatedSubnetworksResponse, nil)
	exp := testhttpapi.NewExpectationStore()
	exp.Put("container.googleapis.com" + path, *ex)
	testhttpapi.StartServer(t, exp)
	provider.DummyAuth = true
	args := []string{
		"--loglevel=info",
		fmt.Sprintf("--keyfilepath=%s", runtimeCtx.KeyFilePath),
		fmt.Sprintf("--providerroot=%s", runtimeCtx.ProviderRootPath),
		"-i=stdin",
		"exec",
		testobjects.SimpleSelectGoogleContainerSubnetworks,
	}
	t.Logf("simple select integration: about to invoke main() with args:\n\t%s", strings.Join(args, ",\n\t"))
	os.Args = args
	infraqltestutil.RunStdOutTestAgainstFiles(t, execStuff, []string{testobjects.ExpectedSimpleSelectGoogleCotainerSubnetworkTableFile01, testobjects.ExpectedSimpleSelectGoogleCotainerSubnetworkTableFile02})
}

func TestK8STemplatedE2eSuccess(t *testing.T) {

	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}

	k8sthwRenderedFile, err := infraqltestutil.GetFilePathFromRepositoryRoot(testobjects.ExpectedK8STheHardWayRenderedFile)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	
	args := []string{
		"--loglevel=warn",
		fmt.Sprintf("--keyfilepath=%s", runtimeCtx.KeyFilePath),
		fmt.Sprintf("--providerroot=%s", runtimeCtx.ProviderRootPath),
		fmt.Sprintf("-i=%s",k8sthwRenderedFile),
		"exec",
	}
	t.Logf("k8s e2e integration: about to invoke main() with args:\n\t%s", strings.Join(args, ",\n\t"))
	
	infraqltestutil.SetupK8sTheHardWayE2eSuccess(t)
	
	os.Args = args
	
	infraqltestutil.RunStdOutTestAgainstFiles(t, execStuff, []string{testobjects.ExpectedK8STheHardWayAsyncFile})
}

func TestInsertAwaitExecSuccess(t *testing.T) {

	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	
	
	args := []string{
		"--loglevel=warn",
		fmt.Sprintf("--keyfilepath=%s", runtimeCtx.KeyFilePath),
		fmt.Sprintf("--providerroot=%s", runtimeCtx.ProviderRootPath),
		"-i=stdin",
		"exec",
		testobjects.SimpleInsertExecComputeNetwork,
	}
	t.Logf("k8s e2e integration: about to invoke main() with args:\n\t%s", strings.Join(args, ",\n\t"))
	
	infraqltestutil.SetupSimpleInsertGoogleComputeNetworks(t)
	
	os.Args = args
	
	infraqltestutil.RunStdOutTestAgainstFiles(t, execStuff, []string{testobjects.ExpectedComputeNetworkInsertAsyncFile})
}

func TestDeleteAwaitSuccess(t *testing.T) {

	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	
	
	args := []string{
		"--loglevel=warn",
		fmt.Sprintf("--keyfilepath=%s", runtimeCtx.KeyFilePath),
		fmt.Sprintf("--providerroot=%s", runtimeCtx.ProviderRootPath),
		"-i=stdin",
		"exec",
		testobjects.SimpleDeleteComputeNetwork,
	}
	t.Logf("k8s e2e integration: about to invoke main() with args:\n\t%s", strings.Join(args, ",\n\t"))
	
	infraqltestutil.SetupSimpleDeleteGoogleComputeNetworks(t)
	
	os.Args = args
	
	infraqltestutil.RunStdOutTestAgainstFiles(t, execStuff, []string{testobjects.ExpectedComputeNetworkDeleteAsyncFile})
}

func TestDeleteAwaitExecSuccess(t *testing.T) {

	runtimeCtx, err := infraqltestutil.GetRuntimeCtx(config.GetGoogleProviderString(), "text")
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	
	
	args := []string{
		"--loglevel=warn",
		fmt.Sprintf("--keyfilepath=%s", runtimeCtx.KeyFilePath),
		fmt.Sprintf("--providerroot=%s", runtimeCtx.ProviderRootPath),
		"-i=stdin",
		"exec",
		testobjects.SimpleDeleteExecComputeNetwork,
	}
	t.Logf("k8s e2e integration: about to invoke main() with args:\n\t%s", strings.Join(args, ",\n\t"))
	
	infraqltestutil.SetupSimpleDeleteGoogleComputeNetworks(t)
	
	os.Args = args
	
	infraqltestutil.RunStdOutTestAgainstFiles(t, execStuff, []string{testobjects.ExpectedComputeNetworkDeleteAsyncFile})
}


func execStuff(t *testing.T) {
	err := execute()
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
}
