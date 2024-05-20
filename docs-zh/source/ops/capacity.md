# 用户容量管理

## 扩容 Volume 空间

```bash
$ cfs-cli volume expand {volume name} {capacity / GB}
```
该接口用于增加 volume 容量空间。

::: tip 提示
DpReadOnlyWhenVolFull 可以严格限制卷容量。

卷写满时不再继续写的配置方式：
(1)master
在创建卷时，将 DpReadOnlyWhenVolFull 参数设置为 true;
若已创建的卷此值为 false，可以 update 成 true。
当此值设置为 true 后，在卷写满时，master 会把卷所有 DP 状态改为 readonly。
(2)client
将client升级，并将client配置里 “minWriteAbleDataPartitionCnt” 参数设置为0。
:::

## Volume 读写性能优化

可读写的 dp 数量越多，数据就会越分散，volume 的读写性能会有响应提升。

CubeFS 采取动态空间分配机制，创建 volume 之后，会为 volume 预分配一定的数据分区 dp，当可读写的 dp 数量少于 10 个，会自动扩充 dp 数量。而如果希望手动提升可读写 dp 数量可以用以下命令：
```bash
$ cfs-cli volume create-dp {volume name} {number}
```

::: tip 提示
一个 dp 的默认大小为 120GB，请根据 volume 实际使用量来创建 dp，避免透支所有 dp。
:::

## 回收 Volume 多余空间

```bash
$ cfs-cli volume shrink {volume name} {capacity in GB}
```

该接口用于减少 volume 容量空间, 会根据实际使用量计算，当设定值 < 已使用量的 120% 时操作会失败。

## 集群空间扩容

准备好新的 dn 和 mn，启动配置文件配置现有 master 地址即可自动将新的节点添加到集群中。