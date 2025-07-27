# Create NodeSet with Specified Nodes

This document describes how to use the `createNodeSetWithSpecifiedNodes` functionality in CubeFS, which allows you to create a nodeset with specific datanode and metanode addresses while ignoring FaultDomain logic.

## Overview

The `createNodeSetWithSpecifiedNodes` function creates a restricted nodeset that:
- Accepts specified datanode and metanode addresses
- Ignores FaultDomain and domainManager logic
- Only accepts HTTP interface calls to create specified volume datapartitions
- Does not allow automatic creation or migration of other datapartitions into this nodeset
- Permits data partition migration out of this nodeset (迁出)
- **NEW**: Restricts the nodeset to be used only by specified volumes

## API Usage

### HTTP API

**Endpoint:** `POST /nodeSet/createWithSpecifiedNodes`

**Parameters:**
- `zoneName` (optional): Zone name where the nodeset will be created. Defaults to "default" if not specified.
- `dataNodeAddrs` (optional): Comma-separated list of datanode addresses (e.g., "192.168.1.10:17310,192.168.1.11:17310")
- `metaNodeAddrs` (optional): Comma-separated list of metanode addresses (e.g., "192.168.1.20:17210,192.168.1.21:17210")
- `allowedVolumes` (required): Comma-separated list of volume names that are allowed to use this nodeset (e.g., "vol1,vol2,vol3")

**Note:** At least one of `dataNodeAddrs` or `metaNodeAddrs` must be specified. `allowedVolumes` is required for restricted nodesets.

**Example Request:**
```bash
curl -X POST "http://master:8080/nodeSet/createWithSpecifiedNodes" \
  -d "zoneName=zone1" \
  -d "dataNodeAddrs=192.168.1.10:17310,192.168.1.11:17310" \
  -d "metaNodeAddrs=192.168.1.20:17210,192.168.1.21:17210" \
  -d "allowedVolumes=vol1,vol2,vol3"
```

**Example Response:**
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "nodeSetId": 12345,
    "zoneName": "zone1",
    "dataNodeAddrs": ["192.168.1.10:17310", "192.168.1.11:17310"],
    "metaNodeAddrs": ["192.168.1.20:17210", "192.168.1.21:17210"],
    "allowedVolumes": ["vol1", "vol2", "vol3"],
    "isRestricted": true
  }
}
```

### SDK Usage

**Go SDK:**
```go
package main

import (
    "fmt"
    "github.com/cubefs/cubefs/sdk/master"
)

func main() {
    // Create master client
    mc := master.NewMasterClient([]string{"master:8080"}, false)
    
    // Create NodeAPI
    nodeAPI := master.NewNodeAPI(mc, nil)
    
    // Define node addresses
    dataNodeAddrs := []string{"192.168.1.10:17310", "192.168.1.11:17310"}
    metaNodeAddrs := []string{"192.168.1.20:17210", "192.168.1.21:17210"}
    allowedVolumes := []string{"vol1", "vol2", "vol3"}
    
    // Create nodeset with specified nodes
    nodeSetId, err := nodeAPI.CreateNodeSetWithSpecifiedNodes("zone1", dataNodeAddrs, metaNodeAddrs, allowedVolumes)
    if err != nil {
        fmt.Printf("Failed to create nodeset: %v\n", err)
        return
    }
    
    fmt.Printf("Successfully created nodeset with ID: %d\n", nodeSetId)
}
```

## Behavior Details

### Node Handling

1. **Existing Nodes**: If a datanode or metanode with the specified address already exists in the cluster:
   - The node will be moved to the new nodeset
   - The node's ID will be preserved
   - The node's NodeSetID will be updated to the new nodeset ID

2. **New Nodes**: If a datanode or metanode with the specified address doesn't exist:
   - A new node will be created with a new ID
   - The node will be assigned to the new nodeset
   - The node will be added to the cluster's node maps

### Zone Handling

1. **Existing Zone**: If the specified zone exists, the nodeset will be created in that zone
2. **New Zone**: If the specified zone doesn't exist:
   - A new zone will be created
   - The media type will be determined from the first datanode (if available) or set to unspecified
   - The zone will be persisted to the cluster

### FaultDomain Logic

This function completely ignores FaultDomain logic:
- No domain manager initialization checks
- No domain-based nodeset group creation
- No fault domain constraints

### Data Partition Behavior

The created nodeset has specific behavior regarding data partitions:

1. **Manual Creation**: Only accepts HTTP interface calls to create specified volume datapartitions
2. **No Automatic Creation**: Does not allow automatic creation of datapartitions
3. **No Automatic Migration**: Does not allow automatic migration of other datapartitions into this nodeset
4. **Migration Out**: Permits data partition migration out of this nodeset (迁出)

### Volume Restrictions

The created nodeset is restricted to specific volumes:

1. **Restricted Access**: The nodeset can only be used by volumes specified in the `allowedVolumes` parameter
2. **Automatic Exclusion**: The nodeset will be automatically excluded from normal allocation logic for non-allowed volumes
3. **Volume-Specific Allocation**: When creating data partitions for allowed volumes, the nodeset can be selected normally
4. **Dynamic Updates**: The allowed volumes list can be modified after nodeset creation (future enhancement)

**Example:**
- If `allowedVolumes=["vol1", "vol2"]` is specified:
  - Only `vol1` and `vol2` can use this nodeset for data partition allocation
  - Other volumes (`vol3`, `vol4`, etc.) will not be able to allocate data partitions to this nodeset
  - The nodeset will be excluded from the normal nodeset selection process for non-allowed volumes

## Error Handling

The function may return the following errors:

- **Invalid Address**: If any datanode or metanode address is invalid
- **Node Already in Nodeset**: If a specified node already belongs to another nodeset
- **Persistence Errors**: If there are issues persisting the nodeset or nodes to the cluster
- **Zone Creation Errors**: If there are issues creating a new zone

## Use Cases

This functionality is particularly useful for:

1. **Isolated Storage**: Creating dedicated nodesets for specific applications or workloads
2. **Manual Control**: When you need precise control over which nodes are used for specific volumes
3. **Bypassing FaultDomain**: When you need to create nodesets without fault domain constraints
4. **Testing**: Creating test environments with specific node configurations

## Limitations

1. **Manual Management**: The nodeset requires manual management of data partitions
2. **No Automatic Balancing**: The cluster's automatic balancing features will not affect this nodeset
3. **Zone Dependencies**: The nodeset is tied to a specific zone and cannot span multiple zones

## Related APIs

- `POST /dataNode/add` - Add a datanode to the cluster
- `POST /metaNode/add` - Add a metanode to the cluster
- `GET /topology` - Get cluster topology information
- `GET /nodeSet/{id}` - Get specific nodeset information 