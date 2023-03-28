# Special Status Code Explanation

## Erasure Coding Subsystem

Each module of the erasure coding subsystem supports audit logs. If audit logs and monitoring metrics are enabled (you can [refer to the documentation](../maintenance/configs/blobstore/base.md)), you can view the service request status code information in the audit log file or the monitoring metric `service_response_code`. The following describes the special status codes of each module.

### Access

::: tip Note
The error status code range of the Access service is [550,599].
:::

| Status Code | Error Message                              | Description                                                           |
|-------------|--------------------------------------------|-----------------------------------------------------------------------|
| 551         | access client service discovery disconnect | The access client cannot discover available access nodes from Consul. |
| 552         | access limited                             | The service interface has reached the connection limit.               |
| 553         | access exceed object size                  | The uploaded file exceeds the maximum size limit.                     |

### Proxy

::: tip Note
The error status code range of the Proxy service is [800,899].
:::

| Status Code | Error Message                         | Description                                                                                |
|-------------|---------------------------------------|--------------------------------------------------------------------------------------------|
| 801         | this codemode has no avaliable volume | There is no available volume for the corresponding encoding mode.                          |
| 802         | alloc bid from clustermgr error       | Failed to obtain a bid from the CM.                                                        |
| 803         | clusterId not match                   | The requested cluster ID does not match the cluster ID where the proxy service is located. |

### Clustermgr

::: tip Note
The error status code range of the Clustermgr service is [900,999].
:::

| Status Code | Error Message                                                   | Description                                                                                                                        |
|-------------|-----------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| 900         | cm: unexpected error                                            | Internal error occurred.                                                                                                           |
| 902         | lock volume not allow                                           | Failed to lock, for example, because it is in the active state.                                                                    |
| 903         | unlock volume not allow                                         | Failed to unlock.                                                                                                                  |
| 904         | volume not exist                                                | The volume does not exist.                                                                                                         |
| 906         | raft propose error                                              | Raft proposal information error.                                                                                                   |
| 907         | no leader                                                       | No leader is available.                                                                                                            |
| 908         | raft read index error                                           | Linear consistency read timeout.                                                                                                   |
| 910         | duplicated member info                                          | Duplicate member information.                                                                                                      |
| 911         | disk not found                                                  | The disk cannot be found.                                                                                                          |
| 912         | invalid status                                                  | The disk status is invalid.                                                                                                        |
| 913         | not allow to change status back                                 | The disk status cannot be rolled back, for example, rolling back a bad disk to the normal state.                                   |
| 914         | alloc volume unit concurrently                                  | Concurrently applying for volume units. Retry is available.                                                                        |
| 916         | alloc volume request params is invalid                          | Invalid request parameters for volume allocation.                                                                                  |
| 917         | no available volume                                             | No available volumes are available for allocation.                                                                                 |
| 918         | update volume unit, old vuid not match                          | The new and old VUIDs do not match when updating the volume unit.                                                                  |
| 919         | update volume unit, new vuid not match                          | The new VUID does not match when updating the volume unit.                                                                         |
| 920         | update volume unit, new diskID not match                        | The new disk ID does not match when updating the volume unit.                                                                      |
| 921         | config argument marshal error                                   | Configuration serialization error.                                                                                                 |
| 922         | request params error, invalid clusterID                         | Invalid request parameters, the cluster ID is invalid.                                                                             |
| 923         | request params error,invalid idc                                | Invalid request parameters, the IDC is invalid.                                                                                    |
| 924         | volume unit not exist                                           | The volume unit does not exist.                                                                                                    |
| 925         | register service params is invalid                              | Invalid registration service parameters.                                                                                           |
| 926         | disk is abnormal or not readonly, can't add into dropping list  | The disk is abnormal or not in the read-only state, and cannot be set to offline.                                                  |
| 927         | stat blob node chunk failed                                     | Failed to obtain chunk information for the blob node.                                                                              |
| 928         | request alloc volume codeMode not invalid                       | The requested volume mode does not exist.                                                                                          |
| 929         | retain volume is not alloc                                      | The retained volume is not allocated.                                                                                              |
| 930         | dropped disk still has volume unit remain, migrate them firstly | The disk cannot be marked as offline in advance because there are remaining chunks on the disk. Migration must be completed first. |
| 931         | list volume v2 not support idle status                          | The v2 version does not support the idle status for listing volumes.                                                               |
| 932         | dropping disk not allow change state or set readonly            | The disk in the offline state cannot have its status changed or be set to read-only.                                               |
| 933         | reject delete system config                                     | The system configuration cannot be deleted.                                                                                        |

### BlobNode

::: tip Note
The error status code range of the BlobNode service is [600,699].
:::

| Status Code | Error Message                      | Description                                                                                                                                                  |
|-------------|------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 600         | blobnode: invalid params           | Invalid parameters.                                                                                                                                          |
| 601         | blobnode: entry already exist      | The VUID already exists.                                                                                                                                     |
| 602         | blobnode: out of limit             | The request exceeds the concurrency limit.                                                                                                                   |
| 603         | blobnode: internal error           | Internal error.                                                                                                                                              |
| 604         | blobnode: service is overload      | The request is overloaded.                                                                                                                                   |
| 605         | blobnode: path is not exist        | The registered disk directory does not exist.                                                                                                                |
| 606         | blobnode: path is not empty        | The registered disk directory is not empty.                                                                                                                  |
| 607         | blobnode: path find online disk    | The registered path is still in the active state and needs to be offline before re-registration.                                                             |
| 611         | disk not found                     | The disk does not exist.                                                                                                                                     |
| 613         | disk is broken                     | The disk is bad.                                                                                                                                             |
| 614         | disk id is invalid                 | The disk ID is invalid.                                                                                                                                      |
| 615         | disk no space                      | The disk has no available space.                                                                                                                             |
| 621         | vuid not found                     | The VUID does not exist.                                                                                                                                     |
| 622         | vuid readonly                      | The VUID is in read-only state.                                                                                                                              |
| 623         | vuid released                      | The VUID is in the released state.                                                                                                                           |
| 624         | vuid not match                     | The VUID does not match.                                                                                                                                     |
| 625         | chunk must readonly                | The chunk must be in read-only state.                                                                                                                        |
| 626         | chunk must normal                  | The chunk must be in normal state.                                                                                                                           |
| 627         | chunk no space                     | The chunk has no available write space.                                                                                                                      |
| 628         | chunk is compacting                | The chunk is being compressed.                                                                                                                               |
| 630         | chunk id is invalid                | The chunk ID is invalid.                                                                                                                                     |
| 632         | too many chunks                    | The number of chunks exceeds the threshold.                                                                                                                  |
| 633         | chunk in use                       | The chunk has a request being processed.                                                                                                                     |
| 651         | bid not found                      | The BID does not exist.                                                                                                                                      |
| 652         | shard size too large               | The shard size exceeds the threshold (1<<32 - 1).                                                                                                            |
| 653         | shard must mark delete             | The shard must be marked for deletion first.                                                                                                                 |
| 654         | shard already mark delete          | The shard has already been marked for deletion.                                                                                                              |
| 655         | shard offset is invalid            | The shard offset is invalid.                                                                                                                                 |
| 656         | shard list exceed the limit        | The number of shards in the shard list exceeds the threshold (65536).                                                                                        |
| 657         | shard key bid is invalid           | The BID is invalid.                                                                                                                                          |
| 670         | dest replica is bad can not repair | The target node to be repaired is in an abnormal state, possibly due to a request timeout or the service not being started.                                  |
| 671         | shard is an orphan                 | The shard is an orphan and cannot be repaired. If this error occurs when the Scheduler initiates a repair, the relevant orphan information will be recorded. |
| 672         | illegal task                       | The background task is illegal.                                                                                                                              |
| 673         | request limited                    | The repair interface request is overloaded.                                                                                                                  |

### Scheduler

::: tip Note
The error status code range of the Scheduler service is [700,799].
:::

| Status Code | Error Message | Description                                                                                                                                                                             |
|-------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 700         | nothing to do | When the BlobNode service requests the Scheduler to pull background tasks, if there are no relevant tasks to be done, the Scheduler will return this status code, which can be ignored. |