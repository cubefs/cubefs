#!/bin/bash

# Exit on any error
set -e

# Load configuration from config.sh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# Check command line arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <baseDir> <bond0>"
    echo "  baseDir  - Base directory for CubeFS deployment"
    echo "  bond0    - Network interface name (e.g., bond0, eth0)"
    exit 1
fi

baseDir=$1
bond0=$2
confDir=${baseDir}/conf

echo "=========================================="
echo "Starting CubeFS cluster deployment..."
echo "Base directory: $baseDir"
echo "Network interface: $bond0"
echo "Master nodes: $MASTER_COUNT"
echo "Meta nodes: $META_COUNT"
echo "Data nodes: $DATA_COUNT"
echo "TOTAL_IP_COUNT: $TOTAL_IP_COUNT"
echo "NETWORK_BASE: $NETWORK_BASE"
echo "=========================================="

# Generate network sub-IPs
echo "Generating network sub-IPs..."
export bond0=$bond0
bash ./shell/genIp.sh $bond0
echo "Network sub-IPs generated successfully"

# Generate configuration files
echo "Generating configuration files..."
export baseDir=$baseDir
bash ./shell/genConf.sh $baseDir
echo "Configuration files generated successfully"

# Start master services
echo "Starting master services..."
i=1
while [ $i -le $MASTER_COUNT ]; do
    echo "Starting master${i}..."
    ./build/bin/cfs-server -c ${confDir}/master${i}.conf &
    i=$((i + 1))
done
echo "Master services started successfully"

# Wait for master nodes to be ready
echo "Waiting for master nodes to initialize..."
sleep 5

# Start meta services
echo "Starting meta node services..."
i=1
while [ $i -le $META_COUNT ]; do
    echo "Starting meta${i}..."
    ./build/bin/cfs-server -c ${confDir}/meta${i}.conf &
    i=$((i + 1))
done
echo "Meta node services started successfully"

# Start data services
echo "Starting data node services..."
i=1
while [ $i -le $DATA_COUNT ]; do
    echo "Starting data${i}..."
    ./build/bin/cfs-server -c ${confDir}/data${i}.conf &
    i=$((i + 1))
done
echo "Data node services started successfully"


# Start lc services
echo "Starting lc node services..."
i=1
while [ $i -le $LC_COUNT ]; do
    echo "Starting lc${i}..."
    ./build/bin/cfs-server -c ${confDir}/lc${i}.conf &
    i=$((i + 1))
done
echo "Lc node services started successfully"

# Configure cluster
echo "Configuring cluster..."
./build/bin/cfs-cli config set --addr 172.16.1.101:17010

echo "=========================================="
echo "CubeFS cluster deployment completed!"
echo "Master address: 172.16.1.101:17010"
echo "Please wait a moment for the cluster to fully initialize..."
echo "=========================================="