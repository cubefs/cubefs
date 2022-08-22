Use Cases
=============

CubeFS is a distributed file system that is compatible with most POSIX file system semantics. When ChubaoFS is mounted, it can be as simple as using a local file system. Basically, it can be used in any case where a file system is needed, replacing the local file system, and realizing infinitely expandable storage without physical boundaries. It has been applied in various scenarios, and the following are some of the extracted scenes.

Machine Learning
--------------------

Disadvantages of using a local disk to store training data sets:

- The local disk space is small, and there are multiple models. The training data set of each model reaches the TB level. If you use a local disk to store the training data set, you need to reduce the size of the training data set.
- Training data sets need to be updated frequently, requiring more disk space.
- Risk of loss of training data set if machine crashes.

The advantages of using CubeFS to store training data sets:

- Unlimited disk space, easy to expand capacity. It can automatically expand disk capacity according to the percentage of disk usage, enabling storage system capacity expansion on demand, greatly saving storage costs.
- Multiple replicas of data to ensure high data reliability without worrying about losing data.
- Compatible with POSIX file system interface, no changes required by the application.


ElasticSearch
------------------

Using local disks to store data often encounters the following problems:

- Disk usage is uneven and disk IO cannot be fully utilized.
- Local disk space is limited.

The advantages of using CubeFS as a backend storage:

- Unlimited disk space, easy to expand capacity. It can automatically expand disk capacity according to the percentage of disk usage, enabling storage system capacity expansion on demand, greatly saving storage costs.
- Disk IO usage is uniform and disk IO is fully utilized.
- Ensure data is highly reliable without worrying about losing data.


Nginx Log Storage
---------------------

Do you often worry about running out of local disk space? With CubeFS, you can store datas in a distributed file system without worrying about running out of disk space.
If you use a local disk to store logs, you may often worry about the following issues:

- Docker local disk space is small.
- If the docker container crashes, the log is lost and unrecoverable.
- The mixed deployment of physical machine and docker machine is difficult to manage and has high operation and maintenance cost.

The advantages of using CubeFS to store nginx logs are:

- The disk space is unlimited and easy to expand. The disk capacity is automatically expanded according to the percentage of disk usage. The storage system can be expanded on demand, which greatly saves storage costs.
- Ensure data is highly reliable and do not worry about losing data.
- Multiple replicas to solve the problem of unable to write to the log caused by disk error and datanode crashes.
- Compatible with the POSIX file system interface, no changes required by the application.
- The operation and maintenance of CubeFS are simple, and one person can easily manage the cluster of tens of thousands of machines.


Spark
----------

In the big data set scenario, are you worried about the amount of data stored in Spark intermediate calculation results in order to carefully calculate each task? You can store the shuffle results to CubeFS, and no longer worry about the disk has no free space which causes the task to fail. This enables the separation of storage and computation.
The pain points of using local disk to store shuffle intermediate results:

- Insufficient disk space.
- Too many temporary directory files to create new files.

The advantages of using CubeFS:

- Unlimited   disk space, easy to expand capacity. It can automatically expand disk capacity according to the percentage of disk usage, enabling storage system capacity expansion on demand, greatly saving storage costs.
- Meta node manages file metadata, which can be expanded horizontally and the number of files is unlimited.


MySQL Database Backup
---------------------------

Disadvantages of using OSS(Object Storage Service) to back up MySQL database:

- Need to use the OSS SDK or RESTful API to develop backup programs which increases the difficulty of operation and maintenance.
- If backup file fails, troubleshooting is more difficult.
- After backing up files to OSS, it is not convenient to check whether the files are successfully uploaded.
- Backup files are processed by multiple layers of services, which affects performance.

Advantages of using CubeFS to back up MySQL databases:

- Easy to use, compatible with POSIX file interface, and can be used as a local file system.
- Complete and detailed operation logs  are stored in the local file system, making it easy to troubleshoot problems.
- Simply execute the ls command to verify that the file was successfully uploaded.
- Supports PageCache and WriteCache, and file read and write performance is significantly improved compared to OSS.



