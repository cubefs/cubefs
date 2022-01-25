Client
=========

The client runs on each container executing application code and exposes a file system interface to applications and can access a mounted volume via a user-space file system interface such as FUSE.

Client Caching
-----------------------
The client process runs entirely in the user space with its own cache, which has been used in the following cases.

To reduce the communication with the resource manager,  the client caches the addresses of the available meta and data partitions assigned to the mounted volume, which can be  obtained at the startup, and periodically synchronizes this available partitions with the resource manager.

To reduce the communication with the meta nodes, the client also  caches the returned inodes and dentries  when creating new files, as well as the data partition id, the extent id and the offset, after the file has been written to the data node successfully.  When a file is opened for read/write, the client will force  the cache metadata to be synchronous with the meta node.

To reduce the communication with the data nodes,  the client caches the most recently identified leader. Our observation is that, when reading a file, the client may not know which data node is the current leader because the leader could change after a failure recovery. As a result, the client may try to send the read request to each replica one by one until a leader is identified.  However, since the leader does not change  frequently, by caching the last identified leader, the client can have minimized  number of retries in most cases.

Integration with FUSE
-----------------------

The ChubaoFS client has been integrated with FUSE to provide a file system interface in the user space. In the past, low performance is considered the main disadvantage of such user-space file systems. But over the years, FUSE has made several improvement on its performance such as  multithreading and write-back cache. In the future, we plan to develop our own POSIX-compliant file system interface in the kernel space  to completely eliminate the overhead from FUSE.

Currently the write-back cache feature does not work well in ChubaoFS due to the following reason. The default write behavior of FUSE is called directIO, which bypasses the kernel's  page cache. This results in performance problems on writing small files as each write pushes the file data to the user daemon. The solution FUSE implemented was to make the  page cache support a write-back policy that aggregates small data first, and then make writes asynchronous. With that change, file data can be pushed to the user daemon in larger blobs at a time. However, in real production, we found that  the write-back cache is not very useful,  because a write operation usually invoke another process that tries to balance the dirty pages (pages in the main memory that have been modified during writing data to disk are marked as "dirty" and have to be flushed to disk before they can be freed), which incurs extra overhead. This overhead becomes more obvious when small files are continuously written through FUSE.

Live Upgrade
-----------------------

.. image:: ../pic/cfs-client-live-upgrade.png
   :align: center
   :scale: 100 %
   :alt: LiveUpgrade
