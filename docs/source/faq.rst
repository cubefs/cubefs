FAQ
===

How does ChubaoFS compare with its alternatives ?
---------------------------------------------------

Ceph
^^^^^
Ceph (https://ceph.com/) is a widely-used free-software storage platform. It can be configured for object storage, block storage, as well as file system. But many nice features provided by
Ceph (e.g., various storage backends) also make it very complicated and difficult to learn and deploy. In addition, in certain scenarios such as metadata operations and small file operations, its performance in a multi-client environment can be hard to optimize.

GFS and HDFS
^^^^^^^^^^^^^
GFS and its open source implementation HDFS (https://github.com/apache/hadoop) are designed for storing large files with sequential access.
Both of them adopt the master-slave architecture, where the single master stores all the file metadata. Unlike GFS and HDFS, ChubaoFS employs  a separate  metadata subsystem  to provide a scalable solution for   the  metadata storage so that the resource manager has less chance to become the bottleneck.

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