# ChubaoFS Perftest scripts

## 1. 环境准备

### 准备测试服务集群

准备一个已经部署好的 chubaofs 服务集群, 集群部署好后, 创建一个供测试的卷.

### 准备测试客户机集群

为便于管理, 可在每台测试客户机上安装 saltstack 进行统一部署, 步骤如下:

1. 配置 salt-master

找一台机器作为 salt-master

```shell
$ yum install -y salt-master
$ echo "nodegroups:" >> /etc/salt/master.d/chubaofs.conf
$ echo "  cfs-perftest-client: '*perftest-client*'" >> /etc/salt/master.d/chubaofs.conf
$ salt-master start -d
```

2. 配置 salt-minion

将 install/init-minion.sh 拷贝到各节点上,执行如下命令:

```shell
# 安装killall命令
$ yum -y install psmisc

$ export SALT_MASTER=xxx.xx.xx.xx
$ install/init-minion.sh
```

确认各节点 salt-minion 加入到 master, master 上执行:

```shell
$ salt -N 'cfs-perftest-client' test.ping

# 如果测试结果为空，查看是否有Unaccepted Keys
$ salt-key
 
# 如果有Unaccepted Keys，则接受连接
$ salt-key -A
 
# 如果仍然没有，分别在master和minion查看日志/var/log/salt/master或/var/log/salt/minion
```

- SALT_MASTER 为 salt master 节点 ip;
- minion的id取值顺序: /etc/salt/minion -> /etc/salt/minion_id -> 本机hostname;

3. 安装测试工具;

将 install 目录 cp 到 salt-master 的/srv/salt/perftest/

最终目录结构如下

```
srv
├─script
|   └perftest.sh
├─salt
|  ├─perftest
|  |    ├─script
|  |    |   ├─fiotest.sh
|  |    |   └mdtest.sh
|  |    ├─pkg
|  |    |  ├─mdtest-bin.tgz
|  |    |  └openmpi-1.10.7.tgz
|  |    ├─install
|  |    |    ├─install-mdtest.sh
|  |    |    ├─install-mpi.sh
|  |    |    ├─install-perftest.sh
|  |    |    └install-rsh.sh
|  |    ├─hosts
|  |    |   ├─hosts1.txt
|  |    |   ├─hosts16.txt
|  |    |   ├─hosts4.txt
|  |    |   └hosts64.txt
|  |    ├─conf
|  |    |  └client.json
|  |    ├─bin
|  |    |  └cfs-client
```

- install/install-rsh.sh 文件用 rsh 来提供 mpi 通信的, 如果需要,需更改里面 ip 为各节点间的 ip

```shell
$ mkdir -p /srv /srv/salt/perftest/script/
$ cp -rf perftest.sh /srv/script/
$ cp -rf fiotest.sh /srv/salt/perftest/script/
$ cp -rf mdtest.sh /srv/salt/perftest/script/
$ cp -rf install/ /srv/salt/perftest/install
$ cp -rf pkg/ /srv/salt/perftest/pkg
$ bash /srv/salt/perftest/install/install-perftest.sh
```

### 准备测试卷

- 将 cfs-client 置于/srv/salt/perftest/bin/cfs-client
- 将 client.json 置于/srv/salt/perftest/conf/client.json

```shell
$ cd /srv
$ script/perftest.sh install
$ script/perftest.sh mount_cfs
```

## 2. 测试

### perftest with saltstack

```shell
# 执行fio测试用例，需先修改mpi_host
$ cd /srv ; script/perftest.sh fio_test

# 执行mdtest op测试用例
$ script/perftest.sh mdtest_op

# 执行mdtest small测试用例
$ script/perftest.sh mdtest_small
```

## 3. 获取测试报告

```shell
# 获取fio测试报告
$ ./perftest.sh print_fio_report


# 获取mdtest测试报告
$ ./perftest.sh print_mdtest_report
```
