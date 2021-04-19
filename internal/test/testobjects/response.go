package testobjects

import (
	"fmt"
)

const (
	SimpleSelectGoogleComputeInstanceResponse string = `{
		"id": "projects/testing-project/zones/australia-southeast1-b/instances",
		"items": [
			{
				"id": "0001",
				"creationTimestamp": "2021-02-20T15:55:46.907-08:00",
				"name": "demo-vm-tt1",
				"tags": {
					"fingerprint": "z="
				},
				"machineType": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b/machineTypes/f1-micro",
				"status": "RUNNING",
				"zone": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b",
				"networkInterfaces": [
					{
						"network": "https://www.googleapis.com/compute/v1/projects/testing-project/global/networks/testing-vpc-01",
						"subnetwork": "https://www.googleapis.com/compute/v1/projects/testing-project/regions/australia-southeast1/subnetworks/aus-sn-01",
						"networkIP": "10.0.0.13",
						"name": "nic0",
						"fingerprint": "z=",
						"kind": "compute#networkInterface"
					}
				],
				"disks": [
					{
						"type": "PERSISTENT",
						"mode": "READ_WRITE",
						"source": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b/disks/demo-disk-qq1",
						"deviceName": "persistent-disk-0",
						"index": 0,
						"boot": true,
						"autoDelete": false,
						"interface": "SCSI",
						"diskSizeGb": "10",
						"kind": "compute#attachedDisk"
					}
				],
				"metadata": {
					"fingerprint": "z=",
					"kind": "compute#metadata"
				},
				"selfLink": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b/instances/demo-vm-tt1",
				"scheduling": {
					"onHostMaintenance": "MIGRATE",
					"automaticRestart": true,
					"preemptible": false
				},
				"cpuPlatform": "Intel Broadwell",
				"labelFingerprint": "z=",
				"startRestricted": false,
				"deletionProtection": false,
				"fingerprint": "z=",
				"lastStartTimestamp": "2021-03-10T11:28:58.562-08:00",
				"kind": "compute#instance"
			},
			{
				"id": "8852892103879695477",
				"creationTimestamp": "2021-02-20T16:00:27.118-08:00",
				"name": "demo-vm-tt2",
				"tags": {
					"fingerprint": "z="
				},
				"machineType": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b/machineTypes/f1-micro",
				"status": "RUNNING",
				"zone": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b",
				"networkInterfaces": [
					{
						"network": "https://www.googleapis.com/compute/v1/projects/testing-project/global/networks/testing-vpc-01",
						"subnetwork": "https://www.googleapis.com/compute/v1/projects/testing-project/regions/australia-southeast1/subnetworks/aus-sn-01",
						"networkIP": "10.0.0.14",
						"name": "nic0",
						"fingerprint": "z=",
						"kind": "compute#networkInterface"
					}
				],
				"disks": [
					{
						"type": "PERSISTENT",
						"mode": "READ_WRITE",
						"source": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b/disks/demo-disk-qq2",
						"deviceName": "persistent-disk-0",
						"index": 0,
						"boot": true,
						"autoDelete": false,
						"interface": "SCSI",
						"diskSizeGb": "10",
						"kind": "compute#attachedDisk"
					}
				],
				"metadata": {
					"fingerprint": "z=",
					"kind": "compute#metadata"
				},
				"selfLink": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b/instances/demo-vm-tt2",
				"scheduling": {
					"onHostMaintenance": "MIGRATE",
					"automaticRestart": true,
					"preemptible": false
				},
				"cpuPlatform": "Intel Broadwell",
				"labelFingerprint": "z=",
				"startRestricted": false,
				"deletionProtection": false,
				"fingerprint": "z=",
				"lastStartTimestamp": "2021-03-10T11:02:37.848-08:00",
				"kind": "compute#instance"
			}
		],
		"selfLink": "https://www.googleapis.com/compute/v1/projects/testing-project/zones/australia-southeast1-b/instances",
		"kind": "compute#instanceList"
	}
	`
	SimpleSelectGoogleContainerAggregatedSubnetworksResponse string = `
	{
		"subnetworks": [
			{
				"subnetwork": "projects/testing-project/regions/australia-southeast1/subnetworks/sn-02",
				"network": "projects/testing-project/global/networks/vpc-01",
				"ipCidrRange": "10.0.1.0/24"
			},
			{
				"subnetwork": "projects/testing-project/regions/australia-southeast1/subnetworks/sn-01",
				"network": "projects/testing-project/global/networks/vpc-01",
				"ipCidrRange": "10.0.0.0/24"
			}
		]
	}
	`
	GoogleContainerHost string = "container.googleapis.com"
	GoogleComputeHost string = "compute.googleapis.com"
	GoogleProjectDefault string = "infraql-demo"
	NetworkInsertPath string = "/compute/v1/projects/infraql-demo/global/networks"
	networkDeletePath string = "/compute/v1/projects/%s/global/networks/%s"
	NetworkInsertURL string = "https://" + GoogleComputeHost + NetworkInsertPath
	SubnetworkInsertPath string = "/compute/v1/projects/infraql-demo/regions/australia-southeast1/subnetworks"
	IPInsertPath string = "/compute/v1/projects/infraql-demo/regions/australia-southeast1/addresses"
	FirewallInsertPath string = "/compute/v1/projects/infraql-demo/global/firewalls"
	ComputeInstanceInsertPath string = "/compute/v1/projects/infraql-demo/zones/australia-southeast1-a/instances"
	SubnetworkInsertURL string = "https://" + GoogleComputeHost + NetworkInsertPath
	GoogleApisHost string = "www.googleapis.com"
	GoogleComputeInsertOperationPath string = "/compute/v1/projects/infraql-demo/global/operations/operation-xxxxx-yyyyy-0001"
	GoogleComputeInsertOperationURL string = "https://" + GoogleApisHost + GoogleComputeInsertOperationPath
	simpleGoogleComputeOperationInitialResponse string = `
	{
		"id": "8485551673440766140",
		"name": "operation-xxxxx-yyyyy-0001",
		"operationType": "%s",
		"targetLink": "%s",
		"targetId": "6645238333082165609",
		"status": "%s",
		"user": "test-user@gmail.com",
		"progress": 0,
		"insertTime": "2021-03-21T02:24:38.285-07:00",
		"startTime": "2021-03-21T02:24:38.293-07:00",
		"selfLink": "%s",
		"kind": "compute#operation"
	}
	`
	simpleGoogleComputePollOperationResponse string = `
	{
		"id": "8485551673440766140",
		"name": "operation-xxxxx-yyyyy-0001",
		"operationType": "%s",
		"targetLink": "%s",
		"targetId": "6645238333082165609",
		"status": "%s",
		"user": "test-user@gmail.com",
		"progress": 100,
		"insertTime": "2021-03-21T02:24:38.285-07:00",
		"startTime": "2021-03-21T02:24:38.293-07:00",
		"endTime": "2021-03-21T02:24:45.870-07:00",
		"selfLink": "%s",
		"kind": "compute#operation"
	}
	`
)

func GetSimpleGoogleNetworkInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputeOperationInitialResponse,
		"insert", 
		NetworkInsertURL + "/kubernetes-the-hard-way-vpc", 
		"RUNNING", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimplePollOperationGoogleNetworkInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputePollOperationResponse, 
		"insert",
		NetworkInsertURL + "/kubernetes-the-hard-way-vpc", 
		"DONE", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimpleGoogleNetworkDeleteResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputeOperationInitialResponse,
		"delete", 
		NetworkInsertURL + "/kubernetes-the-hard-way-vpc", 
		"RUNNING", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimplePollOperationGoogleNetworkDeleteResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputePollOperationResponse, 
		"delete",
		NetworkInsertURL + "/kubernetes-the-hard-way-vpc", 
		"DONE", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimpleGoogleSubnetworkInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputeOperationInitialResponse,
		"insert", 
		NetworkInsertURL + "/kubernetes-the-hard-way-subnet", 
		"RUNNING", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimplePollOperationGoogleSubnetworkInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputePollOperationResponse, 
		"insert",
		NetworkInsertURL + "/kubernetes-the-hard-way-subnet", 
		"DONE", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimpleGoogleIPInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputeOperationInitialResponse,
		"insert", 
		NetworkInsertURL + "/kubernetes-the-hard-way-ip", 
		"RUNNING", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimplePollOperationGoogleIPInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputePollOperationResponse,
		"insert", 
		NetworkInsertURL + "/kubernetes-the-hard-way-ip", 
		"DONE", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimpleGoogleFirewallInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputeOperationInitialResponse,
		"insert",
		NetworkInsertURL + "/kubernetes-the-hard-way-allow-internal-fw", 
		"RUNNING", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimplePollOperationGoogleFirewallInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputePollOperationResponse, 
		"insert",
		NetworkInsertURL + "/kubernetes-the-hard-way-allow-internal-fw", 
		"DONE", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimpleGoogleComputeInstanceInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputeOperationInitialResponse,
		"insert", 
		NetworkInsertURL + "/controller-0", 
		"RUNNING", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimplePollOperationGoogleComputeInstanceInsertResponse() string {
	return fmt.Sprintf(
		simpleGoogleComputePollOperationResponse, 
		"insert",
		NetworkInsertURL + "/controller-0", 
		"DONE", 
		GoogleComputeInsertOperationURL,
	)
}

func GetSimpleNetworkDeletePath(proj string, network string) string {
	return fmt.Sprintf(networkDeletePath, proj, network)
}
