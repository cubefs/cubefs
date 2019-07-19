## Release v1.2.1 - 2019/07/19

### Enhancement
* when datanode or metanode start,must compare partition with master and gofmt project. https://github.com/chubaofs/chubaofs/commit/02e1989065cd89691af0991b376527ef3a029a34
* datanode add backend check disk status func. https://github.com/chubaofs/chubaofs/commit/1b5ac0e674e37602410b2762254d093b62eb4cdf


### Bugfix
* update rocksdb build depends. https://github.com/chubaofs/chubaofs/commit/f665f2f33a63b55835a759c207fc0d7b00ca05a1
* metanode panic when deleting meta partition. https://github.com/chubaofs/chubaofs/commit/c228ced18d810ef527c8d7a36e278a826ed045a3
* when raft member become leader,must apply from appliyID to commitID. https://github.com/chubaofs/chubaofs/commit/f5c2d3211717bb14664b32a4f6bbcaba7b1e7d18


## Release v1.2.0 - 2019/07/16

### Feature

* clientv2: add dentry cache. https://github.com/chubaofs/chubaofs/commit/add53510fd96b406572c99f781660aae29736981
* vendor: introduce jacobsa daemonize package. https://github.com/chubaofs/chubaofs/commit/97a1360de29fbe3007a32b80d601ea966d5140a2
* client: start client as a daemon. https://github.com/chubaofs/chubaofs/commit/f1efb1c0a7f41bb4673cf350fb636781c6321ca4
* clientv2: daemonize client.https://github.com/chubaofs/chubaofs/commit/2055e73092c34f86dc0982612063c3a33ceb0ff7

### Enhancement:

* Update docker helper script. https://github.com/chubaofs/chubaofs/commit/5e00b4e9021a197f581c3344059aafbf549da080
* data sdk: use ip address instead of partition id to rule out unavailable data partitions. https://github.com/chubaofs/chubaofs/commit/568b40cd7d9169fe6d72882ef725971dce2a46fd
* add monitor add prometheus, grafana in docker helper script.https://github.com/chubaofs/chubaofs/commit/354a66cf660f4eaef1049045d7daa3b6b18565d7
* add GOPATH check in build.sh. https://github.com/chubaofs/chubaofs/commit/7b314c5f031aec16d8f5fcda5adf9ec089e2c4fb
* Integrated rocksdb compilation. https://github.com/chubaofs/chubaofs/commit/5dbc0b2d8c906166bb2b592c4dd52206d096742c
* update makefile. https://github.com/chubaofs/chubaofs/commit/2ec8603e7300e8eb14778c5add90f5db795757ab
* docs: start client as a daemon. https://github.com/chubaofs/chubaofs/commit/714209a76dfed3c020df6485185f199ccb45d3a1
* update docs: add make build in readme and docs. https://github.com/chubaofs/chubaofs/commit/3c6ccae977ab4c2054fb49bb5fa65d4d22fbb783
* use one thread to send and recive from follower on primary backup replication protocol .https://github.com/chubaofs/chubaofs/commit/1009af799dc91a089adf70fca97ea3b473f2eaf6
* change random write raft serialize not use json. https://github.com/chubaofs/chubaofs/commit/6144a94c280cee34eae42bd9dc04d009969bea33


### Bugfix:

* fix: when datanode is killed, maxBaseExtentId is corrupted.https://github.com/chubaofs/chubaofs/commit/e8552fe1c194acc7d58db969dc8673c1a150bbc5
* Once vol is created successfully, the state of the data partition it has should be writable. https://github.com/chubaofs/chubaofs/commit/2d954a376fb500cc6dc6d32f3191cfe2541cd3a6
* metanode: fix create dentry exist error. https://github.com/chubaofs/chubaofs/commit/f55846af3c2522b4773e5e2dd2c2871b88f80267
* add privileged for docker-compose server node. https://github.com/chubaofs/chubaofs/commit/718e26308c2ee19ca98c52abdef6c92718ebbbd6
* the retain space field in config file is not allowed to be larger than the default value. https://github.com/chubaofs/chubaofs/commit/70a42d35369f6fb3fb5050539eee063e128cdb52
* fix: when data node gets a disk error, the disk status does not changed. https://github.com/chubaofs/chubaofs/commit/2b056234193351fad1dbedde2ea11626c1d6e97a
* fix: build error. https://github.com/chubaofs/chubaofs/commit/f3eead28cbb3640fb8a60abea16422f8dd4b54e9
* fix build with lua bug. https://github.com/chubaofs/chubaofs/commit/6207f7e7a8ddc46691b9058cad0662cb4c27f84d
* Check whether vol is valid. If there is no meta partition with start being. https://github.com/chubaofs/chubaofs/commit/dd001daf37c6517685eb7515bc46c4ed85a0d7a0
* build: update build.sh for user permission. https://github.com/chubaofs/chubaofs/commit/7532a51ed150e95c69cae8b2b890f545608264f9
* clientv2: fix to get the latest file size. https://github.com/chubaofs/chubaofs/commit/fba924fb1ef0fb40a8cc25956812c222b7309fc2
* when create dp failed,then Check all dp host if the host is alive. https://github.com/chubaofs/chubaofs/commit/7e0eed1b700e8b6c9f8a2a5b43a9432b52198219
* fix bug: when read tinyDeleteFile return eof error. https://github.com/chubaofs/chubaofs/commit/b7ee83f908024e042a296faf40090e218e462e4d
* build: fix multi users build bug. https://github.com/chubaofs/chubaofs/commit/a885777b9da97957cc1eddfee102be3707763b64https://github.com/chubaofs/chubaofs/commit/a885777b9da97957cc1eddfee102be3707763b64
* build: fix no root user permission bug. https://github.com/chubaofs/chubaofs/commit/abe6e1fb5a4193183729a2d4d06e395d51a567e6
* init replicas info from create data partition request. https://github.com/chubaofs/chubaofs/commit/63b6d11d721f8d03b2df07e5cbba10ffa83fc4a2


#### Change/Refactoring

* add cfs-base dockerfile. https://github.com/chubaofs/chubaofs/commit/7b240f21ba6a0037484dd5591bfce0b8f88f2844
* Specification code and metanode config file. https://github.com/chubaofs/chubaofs/commit/03f79e737099cd3f6e782f6abbb781b8ab871aa7
* add go mod. https://github.com/chubaofs/chubaofs/commit/5735625b3c60bcba72403cd80ccf0238552f0db8
* docs: update according to recent changes. https://github.com/chubaofs/chubaofs/commit/65e0aa355568424b5fa6137f138be6e28ac9c647
* if warnLogDir is not specified in the config files, server or client will return an error. https://github.com/chubaofs/chubaofs/commit/236e454c376e8edaa8216f1b384a8afd6268037c
* rename RestSize to ReservedSize. https://github.com/chubaofs/chubaofs/commit/f018e4b20f0f4828a39fe86996940ccfd1779932
* print more detailed error information when server starts failed. https://github.com/chubaofs/chubaofs/commit/0c1b9532f7b50bf7b0d6a5943996bf0f1a11badf
* remove docker client security opt. https://github.com/chubaofs/chubaofs/commit/d346a7d83548aec83590f94e1a788eeed6627455
* add disk config detailed description. https://github.com/chubaofs/chubaofs/commit/3142c54aa229451de02ae8059c248a9f89e6c835
* by default create 10 data partitions and 3 meta partitions when creating volume. https://github.com/chubaofs/chubaofs/commit/06045cd2c8960fc4b23126e8c42b52557d3525c6
* doc: add clarification that resource manager is AKA master. https://github.com/chubaofs/chubaofs/commit/02781071b2dc7ceac751d746113b3cfb205d75ce
* normalize exporter cluster name. https://github.com/chubaofs/chubaofs/commit/8543c15380a8628962e54e1a3eff15bc28f425bf
* set the value of rack for data node to default. https://github.com/chubaofs/chubaofs/commit/39000f6d3240bed35de23f459e7c57ecef9a7c76
* refine decommission disk. https://github.com/chubaofs/chubaofs/commit/6b6ce51f62389fcfc6d078768fa6bdf621c2220f
* change document about metanode config. https://github.com/chubaofs/chubaofs/commit/f142311c8d9a5d7c7c66fcdbec2323d03f40e72d
* doc: update docker helper section. https://github.com/chubaofs/chubaofs/commit/851f90c85386af3b603480d009cdb708794120ec
* Update quick-start-guide.rst. https://github.com/chubaofs/chubaofs/commit/ac3f176ea3e8a2157d66637abdd8d552bcafcb10
* change dataPartition IntervalToUpdateReplica to 600 seconds. https://github.com/chubaofs/chubaofs/commit/4c0521d7466950dcfdc33dad3e31c50e229c3d26
* add third-party directory. https://github.com/chubaofs/chubaofs/commit/8cb94afd07afdcfedb58227275f69dad13278283
* write data partition decommission url message to log. https://github.com/chubaofs/chubaofs/commit/2520f73b2b794b1206f3401f8316a736121d487e
* go fmt project. https://github.com/chubaofs/chubaofs/commit/e841bb8e4b01813ba3ba9f29ca9e6cdf4f501bd6
* data sdk: check amount of writable data partitions when mount. https://github.com/chubaofs/chubaofs/commit/9f55ccd37fafe2f989e75341117711ecb3a7dc88
* delete third-party directory. https://github.com/chubaofs/chubaofs/commit/6d7838a3ba8924c5d41661ff56737ec0d2ab4636
* update docs: remove server and client. https://github.com/chubaofs/chubaofs/commit/ff5e4c9951422bd5d4176efd2f004b10ef0f138b
* pass the hosts to data node when creating the data partition. https://github.com/chubaofs/chubaofs/commit/8cf3447de95224744eece033aa96d81dc2755727
* warn log message unicode encoding is converted to utf8. https://github.com/chubaofs/chubaofs/commit/1bbdc2bef584051cb71c09423f0e81aa8fb94f4c
* Ignore generated build files. https://github.com/chubaofs/chubaofs/commit/e243fa79a8306e616b49b5cc87ebcaf120e0d703
* build: update build.sh and Makefile. https://github.com/chubaofs/chubaofs/commit/b6004b225f32df326b3dae7a664f8ff56c91bf81
* change packet attr func and ReadTinyDelete impl log. https://github.com/chubaofs/chubaofs/commit/e37c7d7ee0575d67f7758802a9f4e08c4bcfdecd
* add log when read tinyDeleteFile failed. https://github.com/chubaofs/chubaofs/commit/fc1e8f3b9fc55774267aea615c8c2904356cb0dc
* add log when streamRead and ExtentRepairRead. https://github.com/chubaofs/chubaofs/commit/dfa1ae5eb8a13df6b8b1dd9b144fa1018f08b71a
* remove unused source code. https://github.com/chubaofs/chubaofs/commit/9a91850c4cfa04cb321372a1008c0dfff2c0520a
* build: update build.sh. https://github.com/chubaofs/chubaofs/commit/1f65750588ada80c1908d9b85fb7de2bee901133
* Update README.md. https://github.com/chubaofs/chubaofs/commit/bf8cd97f8979083b4832cabdb44db025fe39c22b
* build: remove client unused depends. https://github.com/chubaofs/chubaofs/commit/f613e804cc2b664b1f154fed66829a0fea9b933d


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
