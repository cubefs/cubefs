Performance
----------------

Environment
^^^^^^^^^^^

.. csv-table:: Environment
   :file: csv/performance-environment.csv

IO Performance and Scalability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

IO Performance and benchmark scalability test by fio_.

.. _fio: https://github.com/axboe/fio

1. Sequential Read
===================

**Bandwidth**

.. image:: pic/cfs-fio-sequential-read-bandwidth.png
   :align: center
   :scale: 50 %
   :alt: Sequential Read Bandwidth (Mb/s)

.. csv-table:: Sequential Read Bandwidth (Mb/s)
   :file: csv/cfs-fio-sequential-read-bandwidth.csv

**IOPS**

.. image:: pic/cfs-fio-sequential-read-iops.png
   :align: center
   :scale: 50 %
   :alt: Sequential Read IOPS

.. csv-table:: Sequential Read IOPS
   :file: csv/cfs-fio-sequential-read-iops.csv

**Latency**

.. image:: pic/cfs-fio-sequential-read-latency.png
   :align: center
   :scale: 50 %
   :alt: Sequential Read Latency (Microsecond)

.. csv-table:: Sequential Read Latency (Microsecond)
   :file: csv/cfs-fio-sequential-read-latency.csv

2. Sequential Write
===================

**Bandwidth**

.. image:: pic/cfs-fio-sequential-write-bandwidth.png
   :align: center
   :scale: 50 %
   :alt: Sequential Write Bandwidth (Mb/s)

.. csv-table:: Sequential Write Bandwidth (Mb/s)
   :file: csv/cfs-fio-sequential-write-bandwidth.csv

**IOPS**

.. image:: pic/cfs-fio-sequential-write-iops.png
   :align: center
   :scale: 50 %
   :alt: Sequential Write IOPS

.. csv-table:: Sequential Write IOPS
   :file: csv/cfs-fio-sequential-write-iops.csv

**Latency**

.. image:: pic/cfs-fio-sequential-write-latency.png
   :align: center
   :scale: 50 %
   :alt: Sequential Write Latency

.. csv-table:: Sequential Write Latency
   :file: csv/cfs-fio-sequential-write-latency.csv

3. Random Read
===================

**Bandwidth**

.. image:: pic/cfs-fio-random-read-bandwidth.png
   :align: center
   :scale: 50 %
   :alt:  Random Read Bandwidth

.. csv-table:: Random Read Bandwidth
   :file: csv/cfs-fio-random-read-bandwidth.csv

**IOPS**

.. image:: pic/cfs-fio-random-read-iops.png
   :align: center
   :scale: 50 %
   :alt:  Random Read IOPS

.. csv-table:: Random Read IOPS
   :file: csv/cfs-fio-random-read-iops.csv

**Latency**

.. image:: pic/cfs-fio-random-read-latency.png
   :align: center
   :scale: 50 %
   :alt:  Random Read Latency

.. csv-table:: Random Read Latency
   :file: csv/cfs-fio-random-read-latency.csv

4. Random Write
===================

**Bandwidth**

.. image:: pic/cfs-fio-random-write-bandwidth.png
   :align: center
   :scale: 50 %
   :alt:  Random Write Bandwidth

.. csv-table:: Random Write Bandwidth
   :file: csv/cfs-fio-random-write-bandwidth.csv

**IOPS**

.. image:: pic/cfs-fio-random-write-iops.png
   :align: center
   :scale: 50 %
   :alt:  Random Write IOPS

.. csv-table:: Random Write IOPS
   :file: csv/cfs-fio-random-write-iops.csv

**Latency**

.. image:: pic/cfs-fio-random-write-latency.png
   :align: center
   :scale: 50 %
   :alt:  Random Write Latency

.. csv-table:: Random Write Latency
   :file: csv/cfs-fio-random-write-latency.csv

Metadata Performance and Scalability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Metadata performance and scalability benchmark test by mdtest_.

.. _mdtest: https://github.com/LLNL/mdtest

**Dir Creation**

.. image:: pic/cfs-mdtest-dir-creation.png
   :align: center
   :scale: 50 %
   :alt: Dir Creation

.. csv-table:: Dir Creation Benchmark
   :file: csv/cfs-mdtest-dir-creation.csv

**Dir Removal**

.. image:: pic/cfs-mdtest-dir-removal.png
   :align: center
   :scale: 50 %
   :alt: Dir Removal

.. csv-table:: Dir Stat Benchmark
   :file: csv/cfs-mdtest-dir-removal.csv

**Dir Stat**

.. image:: pic/cfs-mdtest-dir-stat.png
   :align: center
   :scale: 50 %
   :alt: Dir Stat

.. csv-table:: Dir Removal Benchmark
   :file: csv/cfs-mdtest-dir-stat.csv

**File Creation**

.. image:: pic/cfs-mdtest-file-creation.png
   :align: center
   :scale: 50 %
   :alt: File Creation

.. csv-table:: File Creation Benchmark
   :file: csv/cfs-mdtest-file-creation.csv

**File Removal**

.. image:: pic/cfs-mdtest-file-removal.png
   :align: center
   :scale: 50 %
   :alt: File Removal

.. csv-table:: File Removal Benchmark
   :file: csv/cfs-mdtest-file-removal.csv

**Tree Creation**

.. image:: pic/cfs-mdtest-tree-creation.png
   :align: center
   :scale: 50 %
   :alt: Tree Creation

.. csv-table:: Tree Creation Benchmark
   :file: csv/cfs-mdtest-tree-creation.csv

**Tree Removal**

.. image:: pic/cfs-mdtest-tree-removal.png
   :align: center
   :scale: 50 %
   :alt: Tree Removal

.. csv-table:: Tree Removal Benchmark
   :file: csv/cfs-mdtest-tree-removal.csv

Integrity
-----------------

- Linux Test Project / fs

Workload
--------------

- database backup

- Java application logs

- code git repo

- database systems
  
  MyRocks,
  MySQL Innodb,
  HBase,

Scalability
----------------

- volume scalability: tens to millions of cfs volumes

- metadata scalability: a big volume with billions of files/directories



