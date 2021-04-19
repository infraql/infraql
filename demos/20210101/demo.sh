#!/usr/bin/env bash

IQL_EXE="${PWD}/infraql"

PROJECT_ID="lab-kr-network-01"
GCP_REGION="australia-southeast1"
GCP_ZONE="${GCP_REGION}-b"
DISK_NAME_01="demo-disk-001"
DISK_NAME_02="demo-disk-002"
DISK_SIZE_GB="10"
SUBNETWORK_NAME="aus-sn-01"
VM_INSTANCE_NAME_01="demo-vm-001"
VM_INSTANCE_NAME_02="demo-vm-002"
MACHINE_TYPE="f1-micro"


echo
echo "SHOW SERVICES IN google WHERE name = 'compute';"
echo 
echo "SHOW RESOURCES IN compute WHERE name = 'instances';"
echo
echo "DESCRIBE compute.instances;"
echo 
echo "SHOW METHODS IN compute.instances;"
echo
echo "SELECT id, name, status, zone FROM compute.instances WHERE project = '${PROJECT_ID}' AND zone = '${GCP_ZONE}';"
echo
echo "EXEC compute.disks.insert @project='${PROJECT_ID}', @zone= '${GCP_ZONE}' @@json='{ \"name\": \"${DISK_NAME_01}\", \"sizeGb\": ${DISK_SIZE_GB} }';"
echo
echo "EXEC compute.instances.insert @zone='${GCP_ZONE}', @project='${PROJECT_ID}' @@json='{ \"name\": \"${VM_INSTANCE_NAME_01}\", \"machineType\": \"zones/${GCP_ZONE}/machineTypes/${MACHINE_TYPE}\", \"disks\": [ {\"source\": \"projects/${PROJECT_ID}/zones/${GCP_ZONE}/disks/${DISK_NAME_01}\", \"boot\": true } ], \"networkInterfaces\": [ { \"subnetwork\": \"projects/${PROJECT_ID}/regions/${GCP_REGION}/subnetworks/${SUBNETWORK_NAME}\" } ] }';"
echo
echo "INSERT INTO google.compute.disks (project, zone, name, sizeGb) SELECT '${PROJECT_ID}', '${GCP_ZONE}', '${DISK_NAME_02}', ${DISK_SIZE_GB};"
echo
echo "## Complex inserts with nested json not yet supported ## INSERT INTO compute.instances (name, project, zone, machineType, disks, networkInterfaces) SELECT '${VM_INSTANCE_NAME_02}', '${PROJECT_ID}', '${GCP_ZONE}', 'zones/${GCP_ZONE}/machineTypes/${MACHINE_TYPE}', '[ {\"source\": \"projects/${PROJECT_ID}/zones/${GCP_ZONE}/disks/${DISK_NAME_02}\", \"boot\": true } ]', '[ { \"subnetwork\": \"projects/${PROJECT_ID}/regions/${GCP_REGION}/subnetworks/${SUBNETWORK_NAME}\" } ] }' ;"
echo
echo "EXEC compute.instances.insert @zone='${GCP_ZONE}', @project='${PROJECT_ID}' @@json='{ \"name\": \"${VM_INSTANCE_NAME_02}\", \"machineType\": \"zones/${GCP_ZONE}/machineTypes/${MACHINE_TYPE}\", \"disks\": [ {\"source\": \"projects/${PROJECT_ID}/zones/${GCP_ZONE}/disks/${DISK_NAME_02}\", \"boot\": true } ], \"networkInterfaces\": [ { \"subnetwork\": \"projects/${PROJECT_ID}/regions/${GCP_REGION}/subnetworks/${SUBNETWORK_NAME}\" } ] }';"
echo
echo "EXEC compute.instances.stop @instance = '${VM_INSTANCE_NAME_01}', @project = '${PROJECT_ID}', @zone = '${GCP_ZONE}';"
echo
echo "SELECT id, name, status, zone FROM compute.instances WHERE project = '${PROJECT_ID}' AND zone = '${GCP_ZONE}';"
echo
echo "EXEC compute.instances.start @instance = '${VM_INSTANCE_NAME_01}', @project = '${PROJECT_ID}', @zone = '${GCP_ZONE}';"
echo
echo "SELECT id, name, status, zone FROM compute.instances WHERE project = '${PROJECT_ID}' AND zone = '${GCP_ZONE}';"
echo
echo "EXEC compute.instances.delete @instance = '${VM_INSTANCE_NAME_01}', @project = '${PROJECT_ID}', @zone = '${GCP_ZONE}';"
echo
echo "SELECT id, name, status, zone FROM compute.instances WHERE project = '${PROJECT_ID}' AND zone = '${GCP_ZONE}';"
echo 
echo "DELETE FROM compute.instances WHERE instance = '${VM_INSTANCE_NAME_02}' AND project = '${PROJECT_ID}' AND zone = '${GCP_ZONE}';"
echo
echo "SELECT id, name, status, zone FROM compute.instances WHERE project = '${PROJECT_ID}' AND zone = '${GCP_ZONE}';"
echo