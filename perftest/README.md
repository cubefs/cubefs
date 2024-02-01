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
$ export SALT_MASTER=xxx.xx.xx.xx
$ install/init-minion.sh

```

确认各节点 salt-minion 加入到 master, master 上执行:

```shell
$ salt -N 'cfs-perftest-client' test.ping
```

- SALT_MASTER 为 salt master 节点 ip;

3. 安装测试工具;

将 install 目录 cp 到 salt-master 的/srv/salt/perftest//

```shell
$ mkdir -p /srv /srv/salt/perftest/script/
$ cp -rf perftest.sh /srv/script/
$ cp -rf *.sh /srv/salt/perftest/script/
$ cp -rf install/ /srv/salt/perftest/install
$ bash /srv/salt/perftest/install/install-perftest.sh

```

- install/install-rsh.sh 文件用 rsh 来提供 mpi 通信的, 如果需要,需更改里面 ip 为各节点间的 ip

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
# 执行fio测试用例
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
