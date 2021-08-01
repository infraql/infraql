package testobjects

const (
	SimpleSelectGoogleComputeInstance      string = `select name, zone from google.compute.instances where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project';`
	SimpleSelectGoogleContainerSubnetworks string = "select subnetwork, ipCidrRange from  google.container.`projects.aggregated.usableSubnetworks` where parent = 'projects/testing-project' ;"
	K8STheHardWayTemplateFile              string = "test/assets/input/k8s-the-hard-way/k8s-the-hard-way.iql"
	K8STheHardWayTemplateContextFile       string = "test/assets/input/k8s-the-hard-way/vars.jsonnet"
	SimpleShowResourcesFilteredFile        string = "test/assets/input/show/show-resources-filtered.iql"
	ShowInsertAddressesRequiredInputFile   string = "test/assets/input/simple-templating/show-insert-compute-addresses-required.iql"
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
	SimpleDeleteComputeNetwork                                           string = `delete /*+ AWAIT  */ from compute.networks WHERE project = 'infraql-demo' and network = 'kubernetes-the-hard-way-vpc';`
	SimpleDeleteExecComputeNetwork                                       string = `EXEC /*+ AWAIT */ compute.networks.delete @project = 'infraql-demo', @network = 'kubernetes-the-hard-way-vpc';`
	SimpleAggCountGroupedGoogleContainerSubnetworkAsc                    string = "select ipCidrRange, sum(5) cc  from  google.container.`projects.aggregated.usableSubnetworks` where parent = 'projects/testing-project' group by \"ipCidrRange\" having sum(5) >= 5 order by ipCidrRange asc;"
	SimpleAggCountGroupedGoogleContainerSubnetworkDesc                   string = "select ipCidrRange, sum(5) cc  from  google.container.`projects.aggregated.usableSubnetworks` where parent = 'projects/testing-project' group by \"ipCidrRange\" having sum(5) >= 5 order by ipCidrRange desc;"
	SelectGoogleComputeDisksOrderCreationTmstpAsc                        string = `select name, sizeGb, creationTimestamp from google.compute.disks where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project' ORDER BY creationTimestamp asc;`
	SelectGoogleComputeDisksOrderCreationTmstpAscPlusJsonExtract         string = `select name, json_extract('{"a":2,"c":[4,5,{"f":7}]}', '$.c') as json_rendition, sizeGb, creationTimestamp from google.compute.disks where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project' ORDER BY creationTimestamp asc;`
	SelectGoogleComputeDisksOrderCreationTmstpAscPlusJsonExtractCoalesce string = `select name, coalesce(json_extract(labels, '$.k1'), 'dummy_value') as json_rendition, sizeGb, creationTimestamp from google.compute.disks where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project' ORDER BY creationTimestamp asc;`
	SelectGoogleComputeDisksOrderCreationTmstpAscPlusJsonExtractInstr    string = `select name, INSTR(name, 'qq') as instr_rendition, sizeGb, creationTimestamp from google.compute.disks where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project' ORDER BY creationTimestamp asc;`
	SelectGoogleComputeDisksAggOrderSizeAsc                              string = `select sizeGb, COUNT(1) as cc from google.compute.disks where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project' GROUP BY sizeGb ORDER BY sizeGb ASC;`
	SelectGoogleComputeDisksAggOrderSizeDesc                             string = `select sizeGb, COUNT(1) as cc from google.compute.disks where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project' GROUP BY sizeGb ORDER BY sizeGb DESC;`
	SelectGoogleComputeDisksAggSizeTotal                                 string = `select sum(cast(sizeGb as unsigned)) - 10 as cc from google.compute.disks where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project';`
	SelectGoogleComputeDisksAggStringTotal                               string = `select group_concat(substr(name, 0, 5)) || ' lalala' as cc from google.compute.disks where zone = 'australia-southeast1-b' AND /* */ project = 'testing-project';`
)
