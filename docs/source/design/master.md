# Resource Management Subsystem

The Master is responsible for asynchronously processing different types of tasks, such as creating/deleting/updating/comparing whether replicas are consistent, managing the survival status of data nodes and metadata nodes, and creating and maintaining volume information.

The Master has multiple nodes, which ensure metadata consistency through the Raft algorithm and persist metadata to RocksDB.

## Utilization-Based Distribution Strategy

The utilization-based distribution strategy for placing file metadata and content is the main feature of the Master, which can more efficiently utilize cluster resources. The workflow of the data partition and metadata partition distribution strategy is as follows:

1. When creating a volume, the Master selects the data node with the highest weight based on the remaining disk/memory space weighted calculation to create a data/metadata partition. When writing a file, the client randomly selects a data partition and a metadata partition.
2. If most of the data partitions of a volume are read-only and only a few data partitions are read-write, the Master will automatically create new data partitions to distribute write requests.

The utilization-based distribution strategy brings two additional benefits:

1. When a new node joins, there is no need to rebalance the data, avoiding the overhead caused by data migration.
2. Because a unified distribution strategy is used, the possibility of generating hot data is significantly reduced.

## Replica Placement

The Master ensures that multiple replicas of a partition are on different machines.

## Splitting Metadata Partitions

If any of the following conditions are met, the metadata partition will be split:

1. The memory usage of the metadata node reaches the set threshold, such as if the total memory is 64GB and the threshold is 0.75, if the metadata node uses 48GB of memory, all metadata partitions on that node will be split.
2. The memory occupied by the metadata partition reaches 16GB.

::: warning Note
Only when the metadata partition ID is the largest among all metadata partitions of the volume, will it be truly split.
:::

If the metadata partition A meets the split condition and its inode range is `[0, +∞)`, after the split, the range of A is `[0, A.MaxInodeID+step)`, and the range of the newly generated B partition is `[A.MaxInodeID+step+1, +∞)`, where step is the step size, which is 2 to the power of 24 by default, and MaxInodeID is reported by the metadata node.

## Exception Handling

If a replica of a data/metadata partition is unavailable (due to disk failure, hardware error, etc.), the data on that replica will eventually be migrated to a new replica.