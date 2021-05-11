package testobjects

const (
	SimpleSelectGoogleComputeInstance      string = `select name, zone from google.compute.instances where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project';`
	SimpleSelectGoogleContainerSubnetworks string = "select subnetwork, ipCidrRange from  google.container.`projects.aggregated.usableSubnetworks` where parent = 'projects/testing-project' ;"
	K8STheHardWayTemplateFile              string = "test/assets/input/k8s-the-hard-way/k8s-the-hard-way.iql"
	K8STheHardWayTemplateContextFile       string = "test/assets/input/k8s-the-hard-way/vars.jsonnet"
	SimpleShowResourcesFilteredFile        string = "test/assets/input/show/show-resources-filtered.iql"
	SimpleInsertComputeNetwork             string = `
	--
	-- create VPC 
	--
	INSERT /*+ AWAIT  */ INTO compute.networks
	(
	project,
	data__name,
	data__autoCreateSubnetworks,
	data__routingConfig
	) 
	SELECT
	'infraql-demo',
	'kubernetes-the-hard-way-vpc',
	false,
	'{"routingMode":"REGIONAL"}';
	`
	SimpleInsertExecComputeNetwork string = `EXEC /*+ AWAIT */ compute.networks.insert @project='infraql-demo' @@json='{ 
		"name": "kubernetes-the-hard-way-vpc",
	  "autoCreateSubnetworks": false,
	  "routingConfig": {"routingMode":"REGIONAL"}
		}';`
	SimpleDeleteComputeNetwork     string = `delete /*+ AWAIT  */ from compute.networks WHERE project = 'infraql-demo' and network = 'kubernetes-the-hard-way-vpc';`
	SimpleDeleteExecComputeNetwork string = `EXEC /*+ AWAIT */ compute.networks.delete @project = 'infraql-demo', @network = 'kubernetes-the-hard-way-vpc';`
)
