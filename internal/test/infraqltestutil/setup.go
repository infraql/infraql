package infraqltestutil

import (
	"fmt"
	"io/ioutil"

	"net/url"
	"testing"

	"infraql/internal/iql/asyncmonitor"
	"infraql/internal/iql/provider"
	"infraql/internal/iql/util"

	"infraql/internal/test/testhttpapi"
	"infraql/internal/test/testobjects"
	"infraql/internal/test/testutil"
)

func SetupSimpleSelectGoogleComputeInstance(t *testing.T) {
	path := "/compute/v1/projects/testing-project/zones/australia-southeast1-b/instances"
	url := &url.URL{
		Path: path,
	}
	ex := testhttpapi.NewHTTPRequestExpectations(nil, nil, "GET", url, testobjects.GoogleComputeHost, testobjects.SimpleSelectGoogleComputeInstanceResponse, nil)
	expectations := testhttpapi.NewExpectationStore()
	expectations.Put(testobjects.GoogleComputeHost+path, *ex)
	testhttpapi.StartServer(t, expectations)
	provider.DummyAuth = true
}

func SetupSimpleSelectGoogleComputeDisks(t *testing.T) {
	path := "/compute/v1/projects/testing-project/zones/australia-southeast1-b/disks"
	url := &url.URL{
		Path: path,
	}
	responseFile, err := util.GetFilePathFromRepositoryRoot(testobjects.SimpleGoogleComputeDisksListResponseFile)
	if err != nil {
		t.Fatalf("Test failed: %v", err)
	}
	responseBytes, err := ioutil.ReadFile(responseFile)
	if err != nil {
		t.Fatalf("%v", err)
	}
	ex := testhttpapi.NewHTTPRequestExpectations(nil, nil, "GET", url, testobjects.GoogleComputeHost, string(responseBytes), nil)
	expectations := testhttpapi.NewExpectationStore()
	expectations.Put(testobjects.GoogleComputeHost+path, *ex)
	testhttpapi.StartServer(t, expectations)
	provider.DummyAuth = true
}

func SetupSimpleSelectGoogleContainerAggAllowedSubnetworks(t *testing.T) {
	path := "/v1/projects/testing-project/aggregated/usableSubnetworks"
	url := &url.URL{
		Path: path,
	}
	ex := testhttpapi.NewHTTPRequestExpectations(nil, nil, "GET", url, testobjects.GoogleContainerHost, testobjects.SimpleSelectGoogleContainerAggregatedSubnetworksResponse, nil)
	expectations := testhttpapi.NewExpectationStore()
	expectations.Put(testobjects.GoogleContainerHost+path, *ex)
	testhttpapi.StartServer(t, expectations)
	provider.DummyAuth = true
}

func getNetworkInsertSuccessExpectations() map[string]testhttpapi.HTTPRequestExpectations {
	networkInsertURL := &url.URL{
		Path: testobjects.NetworkInsertPath,
	}
	networkInsertExpectation := testhttpapi.NewHTTPRequestExpectations(
		testutil.CreateReadCloserFromString(testobjects.CreateGoogleComputeNetworkRequestPayload),
		nil,
		"POST",
		networkInsertURL,
		testobjects.GoogleComputeHost,
		testobjects.GetSimpleGoogleNetworkInsertResponse(),
		nil,
	)

	networkInsertOpPollURL := &url.URL{
		Path: testobjects.GoogleComputeInsertOperationPath,
	}
	networkInsertOpPollExpectation := testhttpapi.NewHTTPRequestExpectations(
		nil,
		nil,
		"GET",
		networkInsertOpPollURL,
		testobjects.GoogleApisHost,
		testobjects.GetSimplePollOperationGoogleNetworkInsertResponse(),
		nil,
	)

	return map[string]testhttpapi.HTTPRequestExpectations{
		testobjects.GoogleComputeHost + testobjects.NetworkInsertPath:             *networkInsertExpectation,
		testobjects.GoogleApisHost + testobjects.GoogleComputeInsertOperationPath: *networkInsertOpPollExpectation,
	}
}

func getSubnetworkInsertSuccessExpectations() map[string]testhttpapi.HTTPRequestExpectations {
	networkInsertURL := &url.URL{
		Path: testobjects.SubnetworkInsertPath,
	}
	networkInsertExpectation := testhttpapi.NewHTTPRequestExpectations(
		testutil.CreateReadCloserFromString(testobjects.CreateGoogleComputeSubnetworkRequestPayload),
		nil,
		"POST",
		networkInsertURL,
		testobjects.GoogleComputeHost,
		testobjects.GetSimpleGoogleSubnetworkInsertResponse(),
		nil,
	)

	networkInsertOpPollURL := &url.URL{
		Path: testobjects.GoogleComputeInsertOperationPath,
	}
	networkInsertOpPollExpectation := testhttpapi.NewHTTPRequestExpectations(
		nil,
		nil,
		"GET",
		networkInsertOpPollURL,
		testobjects.GoogleApisHost,
		testobjects.GetSimplePollOperationGoogleSubnetworkInsertResponse(),
		nil,
	)

	return map[string]testhttpapi.HTTPRequestExpectations{
		testobjects.GoogleComputeHost + testobjects.SubnetworkInsertPath:          *networkInsertExpectation,
		testobjects.GoogleApisHost + testobjects.GoogleComputeInsertOperationPath: *networkInsertOpPollExpectation,
	}
}

func getIPInsertSuccessExpectations() map[string]testhttpapi.HTTPRequestExpectations {
	networkInsertURL := &url.URL{
		Path: testobjects.IPInsertPath,
	}
	networkInsertExpectation := testhttpapi.NewHTTPRequestExpectations(
		testutil.CreateReadCloserFromString(testobjects.CreateGoogleComputeIPRequestPayload),
		nil,
		"POST",
		networkInsertURL,
		testobjects.GoogleComputeHost,
		testobjects.GetSimpleGoogleIPInsertResponse(),
		nil,
	)

	networkInsertOpPollURL := &url.URL{
		Path: testobjects.GoogleComputeInsertOperationPath,
	}
	networkInsertOpPollExpectation := testhttpapi.NewHTTPRequestExpectations(
		nil,
		nil,
		"GET",
		networkInsertOpPollURL,
		testobjects.GoogleApisHost,
		testobjects.GetSimplePollOperationGoogleIPInsertResponse(),
		nil,
	)

	return map[string]testhttpapi.HTTPRequestExpectations{
		testobjects.GoogleComputeHost + testobjects.IPInsertPath:                  *networkInsertExpectation,
		testobjects.GoogleApisHost + testobjects.GoogleComputeInsertOperationPath: *networkInsertOpPollExpectation,
	}
}

func getInternalFirewallInsertSuccessExpectations() map[string]testhttpapi.HTTPRequestExpectations {
	networkInsertURL := &url.URL{
		Path: testobjects.FirewallInsertPath,
	}
	networkInsertExpectation := testhttpapi.NewHTTPRequestExpectations(
		testutil.CreateReadCloserFromString(testobjects.CreateGoogleComputeInternalFirewallRequestPayload),
		nil,
		"POST",
		networkInsertURL,
		testobjects.GoogleComputeHost,
		testobjects.GetSimpleGoogleFirewallInsertResponse(),
		nil,
	)

	networkInsertOpPollURL := &url.URL{
		Path: testobjects.GoogleComputeInsertOperationPath,
	}
	networkInsertOpPollExpectation := testhttpapi.NewHTTPRequestExpectations(
		nil,
		nil,
		"GET",
		networkInsertOpPollURL,
		testobjects.GoogleApisHost,
		testobjects.GetSimplePollOperationGoogleFirewallInsertResponse(),
		nil,
	)

	return map[string]testhttpapi.HTTPRequestExpectations{
		testobjects.GoogleComputeHost + testobjects.FirewallInsertPath:            *networkInsertExpectation,
		testobjects.GoogleApisHost + testobjects.GoogleComputeInsertOperationPath: *networkInsertOpPollExpectation,
	}
}

func getExternalFirewallInsertSuccessExpectations() map[string]testhttpapi.HTTPRequestExpectations {
	networkInsertURL := &url.URL{
		Path: testobjects.FirewallInsertPath,
	}
	networkInsertExpectation := testhttpapi.NewHTTPRequestExpectations(
		testutil.CreateReadCloserFromString(testobjects.CreateGoogleComputeExternalFirewallRequestPayload),
		nil,
		"POST",
		networkInsertURL,
		testobjects.GoogleComputeHost,
		testobjects.GetSimpleGoogleFirewallInsertResponse(),
		nil,
	)

	networkInsertOpPollURL := &url.URL{
		Path: testobjects.GoogleComputeInsertOperationPath,
	}
	networkInsertOpPollExpectation := testhttpapi.NewHTTPRequestExpectations(
		nil,
		nil,
		"GET",
		networkInsertOpPollURL,
		testobjects.GoogleApisHost,
		testobjects.GetSimplePollOperationGoogleFirewallInsertResponse(),
		nil,
	)

	return map[string]testhttpapi.HTTPRequestExpectations{
		testobjects.GoogleComputeHost + testobjects.FirewallInsertPath:            *networkInsertExpectation,
		testobjects.GoogleApisHost + testobjects.GoogleComputeInsertOperationPath: *networkInsertOpPollExpectation,
	}
}

func getComputeInstanceInsertSuccessExpectations(name string, secondaryTag string, networkIP string) map[string]testhttpapi.HTTPRequestExpectations {
	networkInsertURL := &url.URL{
		Path: testobjects.ComputeInstanceInsertPath,
	}
	networkInsertExpectation := testhttpapi.NewHTTPRequestExpectations(
		testutil.CreateReadCloserFromString(testobjects.GetCreateGoogleComputeInstancePayload(name, secondaryTag, networkIP)),
		nil,
		"POST",
		networkInsertURL,
		testobjects.GoogleComputeHost,
		testobjects.GetSimpleGoogleComputeInstanceInsertResponse(),
		nil,
	)

	networkInsertOpPollURL := &url.URL{
		Path: testobjects.GoogleComputeInsertOperationPath,
	}
	networkInsertOpPollExpectation := testhttpapi.NewHTTPRequestExpectations(
		nil,
		nil,
		"GET",
		networkInsertOpPollURL,
		testobjects.GoogleApisHost,
		testobjects.GetSimplePollOperationGoogleComputeInstanceInsertResponse(),
		nil,
	)

	return map[string]testhttpapi.HTTPRequestExpectations{
		testobjects.GoogleComputeHost + testobjects.ComputeInstanceInsertPath:     *networkInsertExpectation,
		testobjects.GoogleApisHost + testobjects.GoogleComputeInsertOperationPath: *networkInsertOpPollExpectation,
	}
}

func getNetworkDeleteSuccessExpectations() map[string]testhttpapi.HTTPRequestExpectations {
	path := testobjects.GetSimpleNetworkDeletePath(testobjects.GoogleProjectDefault, "kubernetes-the-hard-way-vpc")
	networkDeleteURL := &url.URL{
		Path: path,
	}
	networkDeleteExpectation := testhttpapi.NewHTTPRequestExpectations(
		nil,
		nil,
		"DELETE",
		networkDeleteURL,
		testobjects.GoogleComputeHost,
		testobjects.GetSimpleGoogleNetworkDeleteResponse(),
		nil,
	)

	networkInsertOpPollURL := &url.URL{
		Path: testobjects.GoogleComputeInsertOperationPath,
	}
	networkInsertOpPollExpectation := testhttpapi.NewHTTPRequestExpectations(
		nil,
		nil,
		"GET",
		networkInsertOpPollURL,
		testobjects.GoogleApisHost,
		testobjects.GetSimplePollOperationGoogleNetworkDeleteResponse(),
		nil,
	)

	return map[string]testhttpapi.HTTPRequestExpectations{
		testobjects.GoogleComputeHost + path:                                      *networkDeleteExpectation,
		testobjects.GoogleApisHost + testobjects.GoogleComputeInsertOperationPath: *networkInsertOpPollExpectation,
	}
}

func SetupSimpleInsertGoogleComputeNetworks(t *testing.T) {

	expectations := testhttpapi.NewExpectationStore()
	for k, v := range getNetworkInsertSuccessExpectations() {
		expectations.Put(k, v)
	}
	testhttpapi.StartServer(t, expectations)
	provider.DummyAuth = true
	asyncmonitor.MonitorPollIntervalSeconds = 2
}

func SetupSimpleDeleteGoogleComputeNetworks(t *testing.T) {

	expectations := testhttpapi.NewExpectationStore()
	for k, v := range getNetworkDeleteSuccessExpectations() {
		expectations.Put(k, v)
	}
	testhttpapi.StartServer(t, expectations)
	provider.DummyAuth = true
	asyncmonitor.MonitorPollIntervalSeconds = 2
}

func SetupK8sTheHardWayE2eSuccess(t *testing.T) {

	computeControllerInstanceCount := 3
	computeWorkerInstanceCount := 3

	expectations := testhttpapi.NewExpectationStore()
	for k, v := range getNetworkInsertSuccessExpectations() {
		expectations.Put(k, v)
	}
	for k, v := range getSubnetworkInsertSuccessExpectations() {
		expectations.Put(k, v)
	}
	for k, v := range getIPInsertSuccessExpectations() {
		expectations.Put(k, v)
	}
	for k, v := range getInternalFirewallInsertSuccessExpectations() {
		expectations.Put(k, v)
	}
	for k, v := range getExternalFirewallInsertSuccessExpectations() {
		expectations.Put(k, v)
	}
	for i := 0; i < computeControllerInstanceCount; i++ {
		for k, v := range getComputeInstanceInsertSuccessExpectations(fmt.Sprintf("controller-%d", i), "controller", fmt.Sprintf("10.240.0.%d", 10+i)) {
			expectations.Put(k, v)
		}
	}
	for i := 0; i < computeWorkerInstanceCount; i++ {
		for k, v := range getComputeInstanceInsertSuccessExpectations(fmt.Sprintf("worker-%d", i), "worker", fmt.Sprintf("10.240.0.%d", 20+i)) {
			expectations.Put(k, v)
		}
	}
	testhttpapi.StartServer(t, expectations)
	provider.DummyAuth = true
	asyncmonitor.MonitorPollIntervalSeconds = 2
}
