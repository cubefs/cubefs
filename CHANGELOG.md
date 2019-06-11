## Release v1.1.1 - 2019/06/11

### Feature

* client: enable support to fifo and socket file types. https://github.com/chubaofs/chubaofs/pull/80/commits/a1b118423334c075b0fbdc0b059b7225e8c2173f
* clientv2: use jacobsa fuse package instead of bazil. https://github.com/chubaofs/chubaofs/pull/68
* docker: introduce docker compose to create a local testing ChubaoFS cluster. https://github.com/chubaofs/chubaofs/pull/79


### Bugfix

* meta: update atime in inode get operation. [metanode] https://github.com/chubaofs/chubaofs/pull/80/commits/cf76479d251ee7214d0d27625fab95498ee1ae0c
* fix: potential panic if send returns nil resp with no error. [client] https://github.com/chubaofs/chubaofs/pull/80/commits/22f3623d5e24a84c7d1ec49fcb72be375d0d4b92
* fix: raft election takes a long time and timeout; issue a panic if raft server is down. [raft] https://github.com/chubaofs/chubaofs/pull/80/commits/26a1f2f826d5ddd3bb6803ec462f928d12597bdd
* fix: potential deadlock if applyHandler. [master] https://github.com/chubaofs/chubaofs/pull/80/commits/cb1eb6ebfcfedc4d1f8bd97e6b7d776bc8ecf4f4
* fix: put vol to cache after it is persistent. [master] https://github.com/chubaofs/chubaofs/pull/80/commits/f31c5d8e260a878b5bfe9d09d8ce196c9aa2abc8
* fix: partition is nil when apply remove raft node. [datanode] https://github.com/chubaofs/chubaofs/pull/80/commits/971cc4b9105af77b4ada52159125225e713754c0
* fix: metanode painc. [metanode] https://github.com/chubaofs/chubaofs/pull/80/commits/a3d8b1f19b2c3f52af561e46c0c8b3eea15472fa
* fix: panic when pprof does not start. https://github.com/chubaofs/chubaofs/pull/80/commits/77a0efe9aa35d68c039a05dc2780d6902ec08d53

### Enhancement

* sdk: retry if mount failed in case master is unavailable temporarily. https://github.com/chubaofs/chubaofs/pull/80/commits/d20732dbbc343dffe1893f3766305322ae8d05de
* build: add verbose build info. https://github.com/chubaofs/chubaofs/pull/80/commits/e5316f98429ed0b680cda4e4af994774c59ac8bd
* master: introduce data partition over-provision. https://github.com/chubaofs/chubaofs/pull/80/commits/642d1f15696b42d8470392c4dab40e4e3b6d3d8a
* master: reserve writable data partition amount according to capacity instead of a const. https://github.com/chubaofs/chubaofs/pull/80/commits/06186f0b62df534fae3d2f817ccfb61dba921c01
* monitor: Use UMP performance monitor if exporter is not enabled. https://github.com/chubaofs/chubaofs/pull/80/commits/ddf608f7e1705e79ba1c07285f4e61dbdf86189d



## Release v1.1.0 - 2019/05/07

### Change / Refactoring

* Rename the repository from cfs to chubaofs.
* Use own errors module instead of juju errors due to license incompatibility.
* Metanode: Change not raft leader error to tryOtherAddr error.

### Bugfix

* Master: Partition recovered but the status not changed.
* Datanode: Report to client with proto.OptryAgain resultcode when datapartition does not exsit.
* Raft: A member must apply playback from old apply id to commit id after elected as leader.
* Metanode: generate identical inode number under extreme conditions. https://github.com/chubaofs/chubaofs/commit/45b6daa88911eaaebabe299b05fad565761f97ed

### Enhancement

* Master: Add ump warn packet.
* Master: Remove redundant calling of loadMetaData method.
* Master: Reload meta data after leader changed.
* Master: make dataPartition disk Path persistent. https://github.com/chubaofs/chubaofs/commit/6dce99a755d0e32828296a002c6aa50ebfe07c63
* Master: Volume creation supports specifying the amount of meta partitions. https://github.com/chubaofs/chubaofs/commit/2e7bbf2dde555496f9476eb3a9e0ab8200d44d8f
* Metanode: Add totalMem in configFile.
* Datanode: Change default disk reserved space.
* Datanode: Add volname in heartbeat report.
* Raft: Use raft.ErrNotLeader instead of ErrNotLeader.
* Client: Create a dummy node instance if inode does not exist.
* Client: Add UMP monitor alarms for read/write/fsync errors.
* Client: Suppress some error messages. https://github.com/chubaofs/chubaofs/commit/1f6062000d2049a875c3b16a5cc65d61cad1b367
* Log: Automatically create subdirectory under the log directory.

## Release v1.0.0 - 2019/04/02

The initial release version.