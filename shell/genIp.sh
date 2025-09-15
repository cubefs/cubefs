#!/bin/bash
set -e

# Load configuration from config.sh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# Check command line arguments
if [ $# -ne 1 ]; then
    echo "Usage: genIp.sh <bond0>"
    echo "  bond0  - Network interface name (e.g., bond0, eth0)"
    exit 1
fi

interface=$1

# Check if network interface exists
if ! [ -n "$(ip link show "$interface" 2>/dev/null)" ]; then
    echo "Error: Network interface '$interface' does not exist"
    exit 2
fi

echo "=========================================="
echo "Generating network sub-IPs..."
echo "Interface: $interface"
echo "Total IPs needed: $TOTAL_IP_COUNT"
echo "Network base: $NETWORK_BASE"
echo "Start IP: $NETWORK_START_IP"
echo "=========================================="

# Function to generate IP for a specific ID
genIp() {
    local id=$1
    local ip=$2
    
    # Check if IP already exists
    local cnt=$(ifconfig | grep "$ip" | wc -l)
    if [ $cnt -eq 0 ]; then
        echo "Adding sub-IP for $interface: $ip (ID: $id)"
        ifconfig ${interface}:${id} ${ip} netmask 255.255.255.255 broadcast 172.16.1.255 up
    else
        echo "IP $ip already exists, skipping..."
    fi
}

# Generate IPs based on configuration
echo "Generating IPs based on configuration..."
i=0
while [ $i -lt $TOTAL_IP_COUNT ]; do
    ip_num=$((NETWORK_START_IP + i))
    ip="${NETWORK_BASE}.${ip_num}"
    id=$((i + 1))
    
    genIp $id $ip
    i=$((i + 1))
done

echo "=========================================="
echo "Network sub-IP generation completed!"
echo "Generated $TOTAL_IP_COUNT IPs"
echo "=========================================="