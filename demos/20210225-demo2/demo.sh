#!/usr/bin/env bash

PROJECT_ID="${PROJECT_ID:-"lab-kr-network-01"}"
GCP_REGION="${GCP_REGION:-"australia-southeast1"}"
GCP_ZONE="${GCP_ZONE:-"${GCP_REGION}-b"}"
BASE_NAME_01="${BASE_NAME_01:-"dd2-008"}"
DISK_NAME_01="${DISK_NAME_01:-"demo-disk-${BASE_NAME_01}"}"
DISK_SIZE_GB="${DISK_SIZE_GB:-"10"}"
SUBNETWORK_NAME="${SUBNETWORK_NAME:-"aus-sn-01"}"
VM_INSTANCE_NAME_01="${VM_INSTANCE_NAME_01:-"demo-vm-${BASE_NAME_01}"}"
MACHINE_TYPE="${MACHINE_TYPE:-"f1-micro"}"


JSONNET_BLOCK='
local base_name = "'${BASE_NAME_01}'";

local disk_name = "'${DISK_NAME_01}'";

local vm_name = "'${VM_INSTANCE_NAME_01}'";

{
  "values": {
    "project": "'${PROJECT_ID}'",
    "zone": "'${GCP_ZONE}'",
    "disk": {
      "name": disk_name,
      "sizeGb": '${DISK_SIZE_GB}'
    },
    "vm": {
      "name": vm_name,
      "sizeGb": '${DISK_SIZE_GB}',
      "disks": [ { "source": "projects/lab-kr-network-01/zones/australia-southeast1-b/disks/" + disk_name, "boot": true } ]
    }
  }
}
'

echo
echo
echo "<<<jsonnet"
echo
echo "${JSONNET_BLOCK}"
echo
echo ">>>"
echo
echo "insert /*+ AWAIT */ into compute.disks( "
echo "  project, "
echo "  zone, "
echo "  data__name, " 
echo "  data__sizeGb "
echo ") "
echo "SELECT "
echo "  '{{ .values.project }}', "
echo "  '{{ .values.zone }}', " 
echo "  '{{ .values.disk.name }}', "
echo "  {{ .values.disk.sizeGb }} "
echo "; "
echo
echo "INSERT /*+ AWAIT */ INTO compute.instances( "
echo "  zone, "
echo "  project, "
echo "  data__name, "
echo "  data__machineType, "
echo "  data__disks, "
echo "  data__networkInterfaces "
echo ") "
echo "VALUES ( "
echo "   '{{ .values.zone }}',  "
echo "   '{{ .values.project }}', "
echo "   '{{ .values.vm.name }}', "
echo "   'zones/${GCP_ZONE}/machineTypes/${MACHINE_TYPE}', "
echo "   '{{ .values.vm.disks }}', "
echo "   '[ { \"subnetwork\": \"projects/${PROJECT_ID}/regions/${GCP_REGION}/subnetworks/${SUBNETWORK_NAME}\"} ]' "
echo "); "
echo
echo "SELECT id, name, status, zone FROM compute.instances WHERE project = '${PROJECT_ID}' AND zone = '${GCP_ZONE}';"
echo