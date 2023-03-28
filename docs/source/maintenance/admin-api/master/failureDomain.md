# Fault Domain Configuration and Management

## Upgrade and Configuration Items

### Cluster-level Configuration

Enabling fault domains requires adding cluster-level configuration, otherwise it cannot be distinguished. Adding a new zone is a fault domain zone or belongs to the original cross_zone.

```bash
FaultDomain               bool  // Default is false
```

### Volume-level Configuration

Reserved:

```bash
crossZone        bool  # Cross-zone
```

New:

```bash
default_priority  bool  # True means prefer the original zone rather than allocating from the fault domain
```

### Configuration Summary

1. For existing clusters, whether self-built or community-built, whether single zone or cross-zone, if fault domains need to be enabled, the cluster needs to support it, the master needs to be restarted, the configuration needs to be updated, and the policy for updating existing volumes needs to be managed. Otherwise, continue to use the original policy.
2. If the cluster supports it but the volume does not choose to use it, continue to use the original volume policy and allocate resources according to the original policy in the original zone. Use new zone resources when existing resources are exhausted.
3. If the cluster does not support it, the volume cannot enable its own fault domain policy.

| Cluster:faultDomain | Vol:crossZone | Vol:normalZonesFirst | Rules for volume to use domain                                                |
|---------------------|---------------|----------------------|-------------------------------------------------------------------------------|
| N                   | N/A           | N/A                  | Do not support domain                                                         |
| Y                   | N             | N/A                  | Write origin resources first before fault domain until origin reach threshold |
| Y                   | Y             | N                    | Write fault domain only                                                       |
| Y                   | Y             | Y                    | Write origin resources first before fault domain until origin reach threshold |

## Notes

Fault domains solve the problem of copyset distribution without planning in multi-zone scenarios, which affects the durability of data, but existing data cannot be automatically migrated.

1. After enabling fault domains, all devices in the new region will be added to the fault domain.
2. The volume created will prefer the resources of the original zone.
3. When creating a new volume, use the domain resources according to the configuration items in the table above. By default, if available, the original zone resources are used first.

## Management Commands

Create a volume using fault domains

```bash
curl "http://192.168.0.11:17010/admin/createVol?name=volDomain&capacity=1000&owner=cfs&crossZone=true&normalZonesFirst=false"
```

Parameter List

| Parameter        | Type   | Description               |
|------------------|--------|---------------------------|
| crossZone        | string | Whether to cross zones    |
| normalZonesFirst | bool   | Non-fault domain priority |

### Check if Fault Domain is Enabled

```bash
curl "http://192.168.0.11:17010/admin/getIsDomainOn"
```

### Check Fault Domain Usage

```bash
curl -v  "http://192.168.0.11:17010/admin/getDomainInfo"
```

Check the usage of fault domain copyset groups

```bash
curl "http://192.168.0.11:17010/admin/getDomainNodeSetGrpInfo?id=37"
```

Update the upper limit of non-fault domain data usage

```bash
curl "http://192.168.0.11:17010/admin/updateZoneExcludeRatio?ratio=0.7"
```