#!/bin/bash

# =============================================================================
# CubeFS Cluster Configuration
# =============================================================================

# Network Configuration
export NETWORK_BASE="172.16.1"
export NETWORK_START_IP=101

# Node Count Configuration
export MASTER_COUNT=3          # Number of master nodes (usually 3)
export DATA_COUNT=14            # Number of data nodes
export META_COUNT=6            # Number of meta nodes
export LC_COUNT=1            # Number of lc nodes

# Port Configuration
export MASTER_PORT=17010
export DATA_PORT=17310
export META_PORT=17210

# Calculate total IP count
export TOTAL_IP_COUNT=$((MASTER_COUNT + DATA_COUNT + META_COUNT))

# Generate IP address list
generate_ips() {
    local ips=""
    i=0
    while [ $i -lt $TOTAL_IP_COUNT ]; do
        local ip_num=$((NETWORK_START_IP + i))
        if [ $i -gt 0 ]; then
            ips="$ips "
        fi
        ips="$ips${NETWORK_BASE}.${ip_num}"
        i=$((i + 1))
    done
    echo "$ips"
}

# Get IP address array
IP_LIST=$(generate_ips)

# Allocate IP addresses - All nodes use the same IP with different ports
# Generate IP arrays manually - all nodes share the same IP addresses
MASTER_IPS=""
DATA_IPS=""
META_IPS=""
LC_IPS=""

# All nodes use the same IP addresses starting from 172.16.1.101
i=0
while [ $i -lt $MASTER_COUNT ]; do
    ip_num=$((NETWORK_START_IP + i))
    ip="${NETWORK_BASE}.${ip_num}"
    
    if [ $i -gt 0 ]; then
        MASTER_IPS="$MASTER_IPS "
    fi
    MASTER_IPS="$MASTER_IPS$ip"
    i=$((i + 1))
done

# Data nodes use the same IP addresses as master nodes
i=0
while [ $i -lt $DATA_COUNT ]; do
    ip_num=$((NETWORK_START_IP + i))
    ip="${NETWORK_BASE}.${ip_num}"
    
    if [ $i -gt 0 ]; then
        DATA_IPS="$DATA_IPS "
    fi
    DATA_IPS="$DATA_IPS$ip"
    i=$((i + 1))
done

# Meta nodes use the same IP addresses as master nodes
i=0
while [ $i -lt $META_COUNT ]; do
    ip_num=$((NETWORK_START_IP + i))
    ip="${NETWORK_BASE}.${ip_num}"
    
    if [ $i -gt 0 ]; then
        META_IPS="$META_IPS "
    fi
    META_IPS="$META_IPS$ip"
    i=$((i + 1))
done

# Lc nodes use the same IP addresses as master nodes
i=0
while [ $i -lt $LC_COUNT ]; do
    ip_num=$((NETWORK_START_IP + i))
    ip="${NETWORK_BASE}.${ip_num}"
    
    if [ $i -gt 0 ]; then
        LC_IPS="$LC_IPS "
    fi
    LC_IPS="$LC_IPS$ip"
    i=$((i + 1))
done

export MASTER_IPS
export DATA_IPS
export META_IPS
export LC_IPS

# IP-specific configuration mapping
# Format: IP_CONFIG["ip_address"]="rack=value,zone=value,mediaType=value,poolId=value"
# mediaType values: 1=SSD, 2=HDD, 3=EC
# poolId: storage pool ID (0 means not specified, use default)
# disk_size uses default value (3930691768) if not specified
# Example configurations for different IPs
# master
IP_CONFIG_172_16_1_101="rack=r1,zone=default,mediaType=1,poolId=1"
IP_CONFIG_172_16_1_102="rack=r1,zone=default,mediaType=1,poolId=1"
IP_CONFIG_172_16_1_103="rack=r2,zone=default,mediaType=1,poolId=1"
IP_CONFIG_172_16_1_104="rack=r2,zone=default,mediaType=1,poolId=1"
IP_CONFIG_172_16_1_105="rack=r3,zone=default,mediaType=1,poolId=1"
IP_CONFIG_172_16_1_106="rack=r3,zone=default,mediaType=1,poolId=1"
IP_CONFIG_172_16_1_107="rack=r1,zone=z2,mediaType=2,poolId=2"
IP_CONFIG_172_16_1_108="rack=r1,zone=z2,mediaType=2,poolId=2"
IP_CONFIG_172_16_1_109="rack=r1,zone=z2,mediaType=2,poolId=2"
IP_CONFIG_172_16_1_110="rack=r1,zone=z2,mediaType=2,poolId=2"
IP_CONFIG_172_16_1_111="rack=r1,zone=z3,mediaType=2,poolId=4"
IP_CONFIG_172_16_1_112="rack=r1,zone=z3,mediaType=2,poolId=4"
IP_CONFIG_172_16_1_113="rack=r1,zone=z3,mediaType=2,poolId=4"
IP_CONFIG_172_16_1_114="rack=r1,zone=z3,mediaType=2,poolId=4"
IP_CONFIG_172_16_1_115="rack=r1,zone=z1,mediaType=1,poolId=1"

# Function to get IP-specific configuration
get_ip_config() {
    local ip=$1
    local key=$2
    
    # Convert IP to variable name format (replace dots with underscores)
    local var_name="IP_CONFIG_$(echo $ip | tr '.' '_')"
    local config_string=$(eval echo \$$var_name)
    
    if [ -z "$config_string" ]; then
        # Return default values if no specific configuration
        case $key in
            "rack") echo "r1" ;;
            "zone") echo "z1" ;;
            "disk_size") echo "3930691768" ;;
            "mediaType") echo "1" ;;
            "poolId") echo "0" ;;
            *) echo "" ;;
        esac
        return
    fi
    
    # Parse the configuration string
    case $key in
        "rack")
            echo "$config_string" | sed 's/.*rack=\([^,]*\).*/\1/'
            ;;
        "zone")
            echo "$config_string" | sed 's/.*zone=\([^,]*\).*/\1/'
            ;;
        "disk_size")
            echo "$config_string" | sed 's/.*disk_size=\([^,]*\).*/\1/'
            ;;
        "mediaType")
            echo "$config_string" | sed 's/.*mediaType=\([^,]*\).*/\1/'
            ;;
        "poolId")
            echo "$config_string" | sed 's/.*poolId=\([^,]*\).*/\1/'
            ;;
        *)
            echo ""
            ;;
    esac
}

# Generate peers string (for master configuration)
generate_peers() {
    local peers=""
    i=1
    for ip in $MASTER_IPS; do
        if [ $i -gt 1 ]; then
            peers="$peers,"
        fi
        peers="$peers$i:$ip:$MASTER_PORT"
        i=$((i + 1))
    done
    echo "$peers"
}

export PEERS=$(generate_peers)

# Generate master address array (for data and meta configuration)
generate_master_addr() {
    local master_addr=""
    first=true
    for ip in $MASTER_IPS; do
        if [ "$first" = "true" ]; then
            first=false
        else
            master_addr="$master_addr,"
        fi
        master_addr="$master_addr\"$ip:$MASTER_PORT\""
    done
    echo "$master_addr"
}

export MASTER_ADDR=$(generate_master_addr)

# Generate master host string (for client configuration)
generate_master_host() {
    local master_host=""
    first=true
    for ip in $MASTER_IPS; do
        if [ "$first" = "true" ]; then
            first=false
        else
            master_host="$master_host,"
        fi
        master_host="$master_host$ip:$MASTER_PORT"
    done
    echo "$master_host"
}

export MASTER_HOST=$(generate_master_host)

# Print configuration information
print_config() {
    echo "=========================================="
    echo "CubeFS Cluster Configuration"
    echo "=========================================="
    echo "Master nodes: $MASTER_COUNT"
    echo "Data nodes: $DATA_COUNT"
    echo "Meta nodes: $META_COUNT"
    echo "Total IPs: $TOTAL_IP_COUNT"
    echo ""
    echo "Master IPs: ${MASTER_IPS[*]}"
    echo "Data IPs: ${DATA_IPS[*]}"
    echo "Meta IPs: ${META_IPS[*]}"
    echo ""
    echo "IP-specific configurations:"
    for ip in "${IP_LIST[@]}"; do
        local config="${IP_CONFIG[$ip]}"
        if [ -n "$config" ]; then
            echo "  $ip: $config"
        fi
    done
    echo ""
    echo "Peers: $PEERS"
    echo "Master Addr: $MASTER_ADDR"
    echo "Master Host: $MASTER_HOST"
    echo "=========================================="
} 
