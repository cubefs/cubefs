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