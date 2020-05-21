FAQ
===

1. How does ChubaoFS compare with its alternatives ?
------------------------------------------------------

Ceph
^^^^^
Ceph (https://ceph.com/) is a widely-used free-software storage platform. It can be configured for object storage, block storage, as well as file system. But many nice features provided by
Ceph (e.g., various storage backends) also make it very complicated and difficult to learn and deploy. In addition, in certain scenarios such as metadata operations and small file operations, its performance in a multi-client environment can be hard to optimize.

GFS and HDFS
^^^^^^^^^^^^^
GFS and its open source implementation HDFS (https://github.com/apache/hadoop) are designed for storing large files with sequential access.
Both of them adopt the master-slave architecture, where the single master stores all the file metadata. Unlike GFS and HDFS, ChubaoFS employs  a separate  metadata subsystem  to provide a scalable solution for   the  metadata storage so that the resource manager has less chance to become the bottleneck.

Hadoop Ozone
^^^^^^^^^^^^
Hadoop Ozone is a scalable distributed object storage system designed for Hadoop. It was originally proposed to solve the problem of HDFS namenode expansion. It reconstructs the namenode metadata management part of hdfs and reuses the datanode of hdfs.
ChubaoFS has many of the same design concepts like ozone such as: supporting for volume isolation, compatible with both raft/master-slave synchronous replication mechanisms, implenting for s3-compatible interfaces. In addition, ChubaoFS's POSIX fuse-client interface supports random file reading and writing, and optimizes reading and writing of small files.

Haystack
^^^^^^^^
Haystack from Facebook takes after log-structured filesystems to serve long tail of requests seen by sharing photos in a large social network. The key insight is to avoid disk operations when accessing metadata.
ChubaoFS adopts similar ideas by putting the file metadata into the main memory.

However, different from Haystack,  the actually physical offsets instead of logical indices of the file contents are stored in the memory,
and deleting a  file  is achieved by the punch hole interface provided by the underlying file system instead of relying on the garbage collector to perform merging and compacting regularly for more efficient disk utilization. In addition, Haystack does not guarantee the strong consistency among the replicas when deleting the files, and it needs to perform merging and compacting regularly for more efficient disk utilization, which could be a performance killer.
ChubaoFS takes a different design principle to separate the storage of file metadata and contents. In this way, we can have more flexible and cost-effective deployments of meta and data nodes.

Public Cloud
^^^^^^^^^^^^^
Windows Azure Storage (https://azure.microsoft.com/en-us/) is a cloud storage system that provides strong consistency and multi-tenancy to the clients.
Different from ChubaoFS, it builds an extra partition layer to handle random writes before streaming data into the lower level.
AWS EFS (https://aws.amazon.com/efs/) is a cloud storage service  that provides scalable and elastic file storage.
Depending on the use cases, there could be a considerable amount of cost associated with using these cloud storage services.

Others
^^^^^^^
GlusterFS (https://www.gluster.org/) is a scalable distributed file system that aggregates disk storage resources from multiple servers into a single global namespace.  MooseFS (https://moosefs.com/) is a fault- tolerant, highly available, POSIX-compliant, and scalable distributed file system. However, similar to HDFS, it employs a single master to manage the file metadata.


2. A problem similar to undefined reference to 'ZSTD_versionNumber'.
---------------------------------------------------------------------

There are two solutions.

- It can be compiled by adding the specified library to CGO_LDFLAGS.

For example: ``CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lzstd"``. This requires that the ``zstd`` library is also installed on other deployment machines.

- Remove the script that automatically detects whether the 'zstd' library is installed.

Example of file location: rockdb-5.9.2/build_tools/build_detect_platform

The deleted content is as follows.

.. code-block:: bash

    # Test whether zstd library is installed
        $CXX $CFLAGS $COMMON_FLAGS -x c++ - -o /dev/null 2>/dev/null  <<EOF
        #include <zstd.h>
        int main() {}
    EOF
        if [ "$?" = 0 ]; then
            COMMON_FLAGS="$COMMON_FLAGS -DZSTD"
            PLATFORM_LDFLAGS="$PLATFORM_LDFLAGS -lzstd"
            JAVA_LDFLAGS="$JAVA_LDFLAGS -lzstd"
        fi


3. Compile ChubaoFS on one machine, but it cannot be started when deployed to other machines.
-------------------------------------------------------------------------------------------------

First please make sure to use the ``PORTABLE=1 make static_lib`` command to compile RocksDB, then use the ``ldd`` command to check whether the dependent libraries are installed on the machine. After installing the missing libraries, execute the ``ldconfig`` command.


4. Why is the status of MetaNode "ReadOnly"?
----------------------------------------------

- Reason 1: The MetaNode heartbeat has not been received for a long time.

- Reason 2: The used ratio of MetaNode has reached the set threshold.

- Reason 3: The remaining available memory of MetaNode (``TotalWeight`` minus ``UsedWeight``) is not greater than the value of Master's configuration ``metaNodeReservedMem``. You can appropriately increase the MetaNode's ``TotalWeight`` configuration parameter.

- Reason 4: The number of MetaNode meta partitions reaches the default maximum number of partitions (10000).


5. How to change the parameters specified during volume creation?
---------------------------------------------------------------------

Use Mater API ``updateVol`` to change the parameters ``capacity``, ``replicaNum``, ``zoneName``, ``enableToken`` and ``followerRead``.


6. How to launch a new disk?
------------------------------

Add the path of the new disk to the configuration parameter ``disks`` of ``DataNode``, and restart the node.
