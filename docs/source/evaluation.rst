Performance
----------------

Environment
^^^^^^^^^^^

**Cluster Information**

.. csv-table::
   :file: csv/performance-environment.csv

**Volume Setup**

.. csv-table::
   :file: csv/performance-volume.csv

Set volume parameters by following:

.. code-block:: bash

    $ cfs-cli volume create test-vol {owner} --capacity=300000000 --mp-count=10
    Create a new volume:
      Name                : test-vol
      Owner               : ltptest
      Dara partition size : 120 GB
      Meta partition count: 10
      Capacity            : 300000000 GB
      Replicas            : 3
      Allow follower read : Enabled

    Confirm (yes/no)[yes]: yes
    Create volume success.

    $ cfs-cli volume add-dp test-vol 1490

**client configuration**

.. csv-table::
   :file: csv/performance-client.csv


.. code-block:: bash

   #get current iops, default:-1(no limits on iops)：
   $ http://[ClientIP]:[ProfPort]/rate/get
   #set iops
   $ http://[ClientIP]:[ProfPort]/rate/set?write=800&read=800


Small File Performance and Scalability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Small file operation performance and scalability benchmark test by mdtest_.

.. _mdtest: https://github.com/LLNL/mdtest

**Setup**

.. code-block:: bash

    #!/bin/bash
    set -e
    TARGET_PATH="/mnt/test/mdtest" # mount point of CubeFS volume
    for FILE_SIZE in 1024 2048 4096 8192 16384 32768 65536 131072 # file size
    do
    mpirun --allow-run-as-root -np 512 --hostfile hfile64 mdtest -n 1000 -w $i -e $FILE_SIZE -y -u -i 3 -N 1 -F -R -d $TARGET_PATH;
    done

**Benchmark**

.. image:: pic/cfs-small-file-benchmark.png
   :align: left
   :scale: 50 %
   :alt: Small File Benchmark

.. csv-table::
   :file: csv/cfs-small-file-benchmark.csv

IO Performance and Scalability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

IO Performance and benchmark scalability test by fio_.

*Note: Multiple clients mount the same volume. And the process refers to the fio process.*

.. _fio: https://github.com/axboe/fio

1. Sequential Read
===================

**Setup**

.. code-block:: bash

    #!/bin/bash
    fio -directory={} \
        -ioengine=psync \
        -rw=read \  # sequential read
        -bs=128k \  # block size
        -direct=1 \ # enable direct IO
        -group_reporting=1 \
        -fallocate=none \
        -time_based=1 \
        -runtime=120 \
        -name=test_file_c{} \
        -numjobs={} \
        -nrfiles=1 \
        -size=10G

**Bandwidth(MB/s)**

.. image:: pic/cfs-fio-sequential-read-bandwidth.png
   :align: left
   :scale: 50 %
   :alt: Sequential Read Bandwidth (MB/s)

.. csv-table::
   :file: csv/cfs-fio-sequential-read-bandwidth.csv

**IOPS**

.. image:: pic/cfs-fio-sequential-read-iops.png
   :align: left
   :scale: 50 %
   :alt: Sequential Read IOPS

.. csv-table::
   :file: csv/cfs-fio-sequential-read-iops.csv

**Latency(Microsecond)**

.. image:: pic/cfs-fio-sequential-read-latency.png
   :align: left
   :scale: 50 %
   :alt: Sequential Read Latency (Microsecond)

.. csv-table::
   :file: csv/cfs-fio-sequential-read-latency.csv

2. Sequential Write
===================

**Setup**

.. code-block:: bash

    #!/bin/bash
    fio -directory={} \
        -ioengine=psync \
        -rw=write \ # sequential write
        -bs=128k \  # block size
        -direct=1 \ # enable direct IO
        -group_reporting=1 \
        -fallocate=none \
        -name=test_file_c{} \
        -numjobs={} \
        -nrfiles=1 \
        -size=10G

**Bandwidth(MB/s)**

.. image:: pic/cfs-fio-sequential-write-bandwidth.png
   :align: left
   :scale: 50 %
   :alt: Sequential Write Bandwidth (MB/s)

.. csv-table::
   :file: csv/cfs-fio-sequential-write-bandwidth.csv

**IOPS**

.. image:: pic/cfs-fio-sequential-write-iops.png
   :align: left
   :scale: 50 %
   :alt: Sequential Write IOPS

.. csv-table::
   :file: csv/cfs-fio-sequential-write-iops.csv

**Latency(Microsecond)**

.. image:: pic/cfs-fio-sequential-write-latency.png
   :align: left
   :scale: 50 %
   :alt: Sequential Write Latency (Microsecond)

.. csv-table::
   :file: csv/cfs-fio-sequential-write-latency.csv

3. Random Read
===================

**Setup**

.. code-block:: bash

    #!/bin/bash
    fio -directory={} \
        -ioengine=psync \
        -rw=randread \ # random read
        -bs=4k \       # block size
        -direct=1 \    # enable direct IO
        -group_reporting=1 \
        -fallocate=none \
        -time_based=1 \
        -runtime=120 \
        -name=test_file_c{} \
        -numjobs={} \
        -nrfiles=1 \
        -size=10G

**Bandwidth(MB/s)**

.. image:: pic/cfs-fio-random-read-bandwidth.png
   :align: left
   :scale: 50 %
   :alt:  Random Read Bandwidth (MB/s)

.. csv-table::
   :file: csv/cfs-fio-random-read-bandwidth.csv

**IOPS**

.. image:: pic/cfs-fio-random-read-iops.png
   :align: left
   :scale: 50 %
   :alt:  Random Read IOPS

.. csv-table::
   :file: csv/cfs-fio-random-read-iops.csv

**Latency(Microsecond)**

.. image:: pic/cfs-fio-random-read-latency.png
   :align: left
   :scale: 50 %
   :alt:  Random Read Latency (Microsecond)

.. csv-table::
   :file: csv/cfs-fio-random-read-latency.csv

4. Random Write
===================

**Setup**

.. code-block:: bash

    #!/bin/bash
    fio -directory={} \
        -ioengine=psync \
        -rw=randwrite \ # random write
        -bs=4k \        # block size
        -direct=1 \     # enable direct IO
        -group_reporting=1 \
        -fallocate=none \
        -time_based=1 \
        -runtime=120 \
        -name=test_file_c{} \
        -numjobs={} \
        -nrfiles=1 \
        -size=10G

**Bandwidth(MB/s)**

.. image:: pic/cfs-fio-random-write-bandwidth.png
   :align: left
   :scale: 50 %
   :alt:  Random Write Bandwidth (MB/s)

.. csv-table::
   :file: csv/cfs-fio-random-write-bandwidth.csv

**IOPS**

.. image:: pic/cfs-fio-random-write-iops.png
   :align: left
   :scale: 50 %
   :alt:  Random Write IOPS

.. csv-table::
   :file: csv/cfs-fio-random-write-iops.csv

**Latency**

.. image:: pic/cfs-fio-random-write-latency.png
   :align: left
   :scale: 50 %
   :alt:  Random Write Latency

.. csv-table::
   :file: csv/cfs-fio-random-write-latency.csv

Metadata Performance and Scalability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Metadata performance and scalability benchmark test by mdtest_.

.. _mdtest: https://github.com/LLNL/mdtest

**Setup**

.. code-block:: bash

    #!/bin/bash
    TEST_PATH=/mnt/cfs/mdtest # mount point of CubeFS volume
    for CLIENTS in 1 2 4 8 # number of clients
    do
    mpirun --allow-run-as-root -np $CLIENTS --hostfile hfile01 mdtest -n 5000 -u -z 2 -i 3 -d $TEST_PATH;
    done

**Dir Creation**

.. image:: pic/cfs-mdtest-dir-creation.png
   :align: left
   :scale: 50 %
   :alt: Dir Creation

.. csv-table::
   :file: csv/cfs-mdtest-dir-creation.csv

**Dir Removal**

.. image:: pic/cfs-mdtest-dir-removal.png
   :align: left
   :scale: 50 %
   :alt: Dir Removal

.. csv-table::
   :file: csv/cfs-mdtest-dir-removal.csv

**Dir Stat**

.. image:: pic/cfs-mdtest-dir-stat.png
   :align: left
   :scale: 50 %
   :alt: Dir Stat

.. csv-table::
   :file: csv/cfs-mdtest-dir-stat.csv

**File Creation**

.. image:: pic/cfs-mdtest-file-creation.png
   :align: left
   :scale: 50 %
   :alt: File Creation

.. csv-table::
   :file: csv/cfs-mdtest-file-creation.csv

**File Removal**

.. image:: pic/cfs-mdtest-file-removal.png
   :align: left
   :scale: 50 %
   :alt: File Removal

.. csv-table::
   :file: csv/cfs-mdtest-file-removal.csv

**Tree Creation**

.. image:: pic/cfs-mdtest-tree-creation.png
   :align: left
   :scale: 50 %
   :alt: Tree Creation

.. csv-table::
   :file: csv/cfs-mdtest-tree-creation.csv

**Tree Removal**

.. image:: pic/cfs-mdtest-tree-removal.png
   :align: left
   :scale: 50 %
   :alt: Tree Removal

.. csv-table::
   :file: csv/cfs-mdtest-tree-removal.csv

Integrity
-----------------

- Linux Test Project / fs

Workload
--------------

- Database backup

- Java application logs

- Code git repo

- Database systems
  
  MyRocks,
  MySQL Innodb,
  HBase,

Scalability
----------------

- Volume Scalability: tens to millions of cfs volumes

- Metadata Scalability: a big volume with billions of files/directories



