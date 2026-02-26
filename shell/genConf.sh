#!/bin/bash

set -e

# Load configuration from config.sh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

if [ $# -ne 1 ]; then
    echo "Usage: genConf.sh <baseDir>"
    echo "  baseDir  - Base directory for configuration files"
    exit 1
fi

baseDir=$1
confDir=${baseDir}/conf
tplDir=shell/tpl

if [ ! -d "$confDir" ]; then
    echo "mkdir -p $confDir"
    mkdir -p $confDir
fi

echo "Starting configuration file generation..."
echo "Master nodes: $MASTER_COUNT"
echo "Data nodes: $DATA_COUNT"
echo "Meta nodes: $META_COUNT"

genMaster() {
    local id=${1}
    local ip=${2}
    echo "start gen master$id.conf for IP $ip"
    local masterDir="${baseDir}/master${id}"
    local confFile="${confDir}/master$id.conf"
    if [ ! -f "$confFile" ]; then
        sed "s/_id_/${id}/g" ${tplDir}/master.tpl | sed "s/_ip_/${ip}/g" | sed "s/_peers_/${PEERS}/g" | sed "s|_dir_|${masterDir}|g" > "$confFile"
        echo "gen master$id.conf success"
    else
        echo "master$id.conf already exists, skipping generation"
    fi
} 

# Generate Master node configurations
master_id=1
for ip in $MASTER_IPS; do
    genMaster $master_id "$ip"
    master_id=$((master_id + 1))
done

genData() {
    local id=${1}
    local ip=${2}
    echo "start gen data$id.conf for IP $ip"
    
    # Get IP-specific configuration
    local rack=$(get_ip_config "$ip" "rack")
    local zone=$(get_ip_config "$ip" "zone")
    local disk_size=$(get_ip_config "$ip" "disk_size")
    local media_type=$(get_ip_config "$ip" "mediaType")
    local pool_id=$(get_ip_config "$ip" "poolId")
    
    echo "  IP $ip: rack=$rack, zone=$zone, disk_size=$disk_size, mediaType=$media_type, poolId=$pool_id"
    
    local dataDir=$baseDir/data$id
    if [ ! -d "$dataDir/disk" ]; then
        echo "mkdir -p $dataDir/disk"
        mkdir -p $dataDir/disk
    fi

    local confFile="${confDir}/data$id.conf"
    if [ ! -f "$confFile" ]; then
        sed "s/_ip_/${ip}/g" ${tplDir}/data.tpl | \
        sed "s/_rack_/${rack}/g" | \
        sed "s/_zone_/${zone}/g" | \
        sed "s/_disk_size_/${disk_size}/g" | \
        sed "s/_media_type_/${media_type}/g" | \
        sed "s/_poolId_/${pool_id}/g" | \
        sed "s|_dir_|${dataDir}|g" | \
        sed "s|_master_addr_|${MASTER_ADDR}|g" > "$confFile"
        echo "gen data$id.conf success"
    else
        echo "data$id.conf already exists, skipping generation"
    fi
}

# Generate Data node configurations
data_id=1
for ip in $DATA_IPS; do
    genData $data_id "$ip"
    data_id=$((data_id + 1))
done


genLcNode() {
    local id=${1}
    local ip=${2}
    echo "start gen data$id.conf for IP $ip"
    
    local lcDir=$baseDir/lc$id
    if [ ! -d "$lcDir/logs" ]; then
        echo "mkdir -p $lcDir/logs"
        mkdir -p $lcDir/logs
    fi

    local confFile="${confDir}/lc$id.conf"
    if [ ! -f "$confFile" ]; then
        sed "s/_ip_/${ip}/g" ${tplDir}/lcnode.tpl | \
        sed "s|_dir_|${lcDir}|g" | \
        sed "s|_master_addr_|${MASTER_ADDR}|g" > "$confFile"
        echo "gen lc$id.conf success"
    else
        echo "lc$id.conf already exists, skipping generation"
    fi
}

# Generate Data node configurations
lc_id=1
for ip in $LC_IPS; do
    genLcNode $lc_id "$ip"
    lc_id=$((lc_id + 1))
done

genMeta() {
    local id=${1}
    local ip=${2}
    echo "start gen meta$id.conf for IP $ip"
    
    # Get IP-specific configuration
    local rack=$(get_ip_config "$ip" "rack")
    local zone=$(get_ip_config "$ip" "zone")
    local region=$(get_ip_config "$ip" "region")
    
    echo "  IP $ip: rack=$rack, zone=$zone, region=$region"
    
    local metaDir=$baseDir/meta$id
    local confFile="${confDir}/meta$id.conf"
    if [ ! -f "$confFile" ]; then
        sed "s/_ip_/${ip}/g" ${tplDir}/meta.tpl | \
        sed "s/_rack_/${rack}/g" | \
        sed "s/_zone_/${zone}/g" | \
        sed "s/_region_/${region}/g" | \
        sed "s|_dir_|${metaDir}|g" | \
        sed "s|_master_addr_|${MASTER_ADDR}|g" > "$confFile"
        echo "gen meta$id.conf success"
    else
        echo "meta$id.conf already exists, skipping generation"
    fi
}

# Generate Meta node configurations
meta_id=1
for ip in $META_IPS; do
    genMeta $meta_id "$ip"
    meta_id=$((meta_id + 1))
done

genClient() {
    local confFile="${confDir}/client.conf"
    echo "start gen client.conf"
    if [ ! -f "$confFile" ]; then
        sed "s|_master_host_|${MASTER_HOST}|g" ${tplDir}/client.tpl | sed "s|_dir_|${baseDir}|g" > "$confFile"
        echo "gen client.conf success"
    else
        echo "client.conf already exists, skipping generation"
    fi
}

genClient

genObject() {
    local confFile="${confDir}/object.conf"
    echo "start gen object.conf"
    if [ ! -f "$confFile" ]; then
        sed "s|_master_addr_|${MASTER_ADDR}|g" ${tplDir}/object.tpl | sed "s|_dir_|${baseDir}|g" > "$confFile"
        echo "gen object.conf success"
    else
        echo "object.conf already exists, skipping generation"
    fi
}

genObject

echo "Configuration file generation completed!"