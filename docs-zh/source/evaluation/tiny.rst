小文件性能评估
----------------

通过 mdtest_ 进行小文件性能测试的结果如下：

.. _mdtest: https://github.com/LLNL/mdtest

**配置**

.. code-block:: bash

    #!/bin/bash
    set -e
    TARGET_PATH="/mnt/test/mdtest" # mount point of CubeFS volume
    for FILE_SIZE in 1024 2048 4096 8192 16384 32768 65536 131072 # file size
    do
    mpirun --allow-run-as-root -np 512 --hostfile hfile64 mdtest -n 1000 -w $i -e $FILE_SIZE -y -u -i 3 -N 1 -F -R -d $TARGET_PATH;
    done

**测试结果**

.. image:: ../pic/cfs-small-file-benchmark.png
   :alt: Small File Benchmark

.. csv-table::
   :file: ../csv/cfs-small-file-benchmark.csv
