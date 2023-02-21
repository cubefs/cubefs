环境准备
----------------

**集群信息**

.. csv-table::
   :file: ../csv/performance-environment.csv

**卷设置**

.. csv-table::
   :file: ../csv/performance-volume.csv

设置方法:

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

**client配置**

.. csv-table::
   :file: csv/performance-client.csv


.. code-block:: bash

   #查看当前iops：
   $ http://[ClientIP]:[ProfPort]/rate/get
   #设置iops，默认值-1代表不限制iops
   $ http://[ClientIP]:[ProfPort]/rate/set?write=800&read=800
