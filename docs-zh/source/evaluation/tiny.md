# 小文件性能评估

通过 [mdtest](https://github.com/LLNL/mdtest)
进行小文件性能测试的结果如下：

## 配置

``` bash
#!/bin/bash
set -e
TARGET_PATH="/home/service/chubaofs/adls/mnt-perform-test" # mount point of CubeFS volume
for FILE_SIZE in 1024 2048 4096 8192 16384 32768 65536 131072 # file size
do
    CMD="/usr/lib64/openmpi/bin/mpirun --allow-run-as-root -mca plm_rsh_args '-p 18822' -np 512 --hostfile hfile64 mdtest -n 1000 -w $FILE_SIZE -e $FILE_SIZE -y -u -i 3 -N 1 -F -R -d $TARGET_PATH"
   #CMD="/usr/lib64/openmpi/bin/mpirun --allow-run-as-root -mca plm_rsh_args '-p 18822' -np 512 --hostfile hfile64 mdtest -n 1000 -w $FILE_SIZE -e $FILE_SIZE -y -u -i 3 -N 1 -F -R -d $TARGET_PATH"
	echo
	echo $CMD
	eval $CMD | tee -a ${LOGPREFIX}.txt
	echo "start to sleep 5s"
	sleep 5
done
```

## 测试结果

![Small File Benchmark](../pic/cfs-small-file-benchmark.png)

| 文件大小（KB）   | 1      | 2      | 4      | 8      | 16     | 32     | 64     | 128    |
|------------|--------|--------|--------|--------|--------|--------|--------|--------|
| 创建操作 (TPS) | 49808  | 37726  | 42296  | 44826  | 41481  | 35699  | 31609  | 35622  |
| 读取操作 (TPS) | 76743  | 81085  | 84831  | 75397  | 73165  | 69665  | 62135  | 53658  |
| 删除操作 (TPS) | 72522  | 67749  | 70919  | 68689  | 69819  | 71671  | 71568  | 71647  |
| 信息查看 (TPS) | 188609 | 185945 | 188542 | 180602 | 188274 | 174771 | 171100 | 183334 |