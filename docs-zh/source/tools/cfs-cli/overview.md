# 概述

使用命令行界面工具（CLI）可以实现方便快捷的集群管理。利用此工具，可以查看集群及各节点的状态，并进行各节点、卷及用户的管理。

::: tip 提示
随着CLI的不断完善，最终将会实现对于集群各节点接口功能的100%覆盖。
:::

## 编译及配置

下载CubeFS源码后，在 `cubefs/cli` 目录下，运行 `build.sh` 文件
，即可生成 `cfs-cli` 可执行程序。

同时，在 `root` 目录下会生成名为 `.cfs-cli.json`
的配置文件，修改master地址为当前集群的master地址即可。也可使用命令
`./cfs-cli config info` 和 `./cfs-cli config set` 来查看和设置配置文件。

## 使用方法

在 `cubefs/cli` 目录下，执行命令 `./cfs-cli --help` 或 `./cfs-cli -h`
，可获取CLI的帮助文档。

CLI主要分为以下几类管理命令：

| 命令                    | 描述         |
|-----------------------|------------|
| cfs-cli cluster       | 集群管理       |
| cfs-cli metanode      | 元数据节点管理    |
| cfs-cli datanode      | 数据节点管理     |
| cfs-cli datapartition | 数据分片管理     |
| cfs-cli metapartition | 元数据分片管理    |
| cfs-cli config        | 配置管理       |
| cfs-cli volume, vol   | 卷管理        |
| cfs-cli user          | 用户管理       |
| cfs-cli nodeset       | nodeset管理  |
| cfs-cli quota         | 目录配额管理     |