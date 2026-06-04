# 内核 client 两个上线阻断 bug 修复（writeback 死锁 + O_DIRECT 写）

> 分支：feat/client-kernel-5.15　目标内核：Ubuntu 5.15.0-72-generic
> 验证环境：cubefs-deploy test-hb，4 节点 syncnode（10.54.120.40-43）经 deb+systemd 挂 bench vol（pvc-18635）到 /mnt/cubefs-kernel，pod 内 /cfs/posix-bench-kernel
> 对照：fuse CSI /cfs/posix-bench（同 vol）

## 背景与目标

把 CubeFS 内核 client 推上线（Ubuntu 5.15 + CubeFS v3.5）。2026-06-03 在 syncnode 做 fuse CSI vs 内核 client 大文件带宽对比时，暴露两个上线阻断级缺陷：

- **Bug A — writeback 死锁拖垮节点（上线红线）**：buffered 写总 dirty 量超过 pod cgroup 内存上限（12Gi）约 2 倍（~25GB）时，内核 client 回写停摆死锁，节点内存压力拖垮 kubelet，10.54.120.40 实测变 NotReady（压力解除后自愈，无需重启）。任何失控的 buffered 写进程都能把 syncnode 节点干挂。
- **Bug B — O_DIRECT 写失败**：fio 多 IO/高并发 O_DIRECT 写报 `Bad address, buflen=18446744073708503040`（psync）/ `Unknown error -2097152`、`Input/output error`（libaio）。O_DIRECT 读正常（3614 MiB/s，err=0）。数据库 / AI 的 direct IO 会踩。

目标：两个 bug 修复 → 编译 deb → 4 节点更新部署 → 验证不再拖垮节点 + O_DIRECT 写正常 → syncnode 重跑 fuse vs 内核 client 完整对比。

## 范围边界 / 不做什么

- **只改 client_kernel**，不动 cubefs server 端（master/metanode/datanode）。
- **不改 cubefs-deploy IaC**（syncnode 部署形态已稳定）；只在那边重跑 bench 对比。
- **不做 BDI cgroup writeback 深度集成（wb_domain/strictlimit）**：Bug A 用最小根因修复（GFP_NOFS）已能消除死锁；cgroup writeback 协调属未来 backlog，非本期上线红线。
- **不引入 mempool 预分配回写内存**：GFP_NOFS + VFS balance_dirty_pages 限速组合已能保证"最坏变慢、不拖垮节点"；mempool 是过度设计，YAGNI。
- 不动其他已修 bug（symlink/rename/nlink/truncate/buffer 等，已上线）。

## 根因结论

### Bug A：writeback 路径用 GFP_KERNEL → 内存压力下递归 reclaim 死锁

代码级证据（已确认）：

- 回写热路径调用链：`cfs_writepages`(cfs_fs.c:346) → `cfs_extent_write_pages`(cfs_extent_stream.c:513) → `extent_write_pages_normal`(464) → `do_extent_request`(28) → `cfs_socket_create`/send/recv（同步阻塞网络 IO）。
- **`cfs_extent_write_pages` 入口 cfs_extent_stream.c:532 `cpages = kvmalloc(..., GFP_KERNEL)`**。
- 全仓库其余 IO 路径分配**全部用 GFP_NOFS**（cfs_extent_reader/writer/cache、cfs_socket（含 `sk->sk_allocation = GFP_NOFS`）、cfs_page、cfs_packet、`cfs_extent_stream_new`:1119）。`memalloc_nofs_save` 全仓库未使用。
- 唯三漏用 GFP_KERNEL 的地方都在 cfs_extent_stream.c：**532（写热路径，死锁根因）**、892（读热路径入口）、1048（O_DIRECT 路径 `extent_dio_pages_alloc`）。

死锁机理：cgroup memory 撑满（全是待回写 dirty page）→ 内核 reclaim/flusher 调 writepages 回写脏页释放内存 → 回写自己在入口 `kvmalloc(GFP_KERNEL)` 要内存 → GFP_KERNEL 允许 fs reclaim → 递归触发回写 → cgroup 无可回收内存（dirty 还没刷出）→ 分配阻塞 → page 到不了 `set_page_writeback`（故实测 `file_writeback=0`、`file_dirty` 不降）→ dirty 永不下降 → balance_dirty_pages 把写者全卡死 → 死锁 → 节点内存压力 → kubelet 失联 NotReady。

实测数据（numjobs=32 size=2g buffered，pod cgroup 12Gi）：
```
written  dirty   writeback  mem
17640MB  4163MB  57MB       12286MB
24965    1552    0          12286   ← 写到~25GB
25131    6488    0          12286   ← 停滞,writeback=0,dirty累积
25131    9742    0          12286   ← 彻底hang,du也阻塞
→ .40 NotReady（~数分钟后压力解除自愈）
```

修复：532 / 892 / 1048 的 `GFP_KERNEL` → `GFP_NOFS`，与全仓库一致。GFP_NOFS 阻断回写时递归进 fs reclaim，回写能推进 → dirty 能下降 → balance_dirty_pages 正常限速写者（最坏写变慢，不再死锁、不拖垮节点）。这是根因修复（GFP 标志用错），非临时缓解。

对照基线（buffered 写正常，dirty < cgroup 时不触发死锁）：内核 client 1425 MiB/s vs fuse 307 MiB/s（4.6×）。

### Bug B：O_DIRECT 写未 advance iter + 只取首个 iovec + 忽略回写返回值

代码级证据（cfs_extent_stream.c）：

- `extent_dio_pages_alloc`(1033) 1045-1046：`start = iter->iov->iov_base + iter->iov_offset; nbytes = iter->iov->iov_len - iter->iov_offset;`——**只取 `iter->iov`（第一个 iovec）**，multi-segment iov_iter 只处理首段。
- `cfs_extent_dio_read_write`(1068) 1110：`return ret < 0 ? ret : iov_iter_count(iter)`——**全程未 `iov_iter_advance`**。5.15 VFS `generic_file_direct_write` 调 `a_ops->direct_IO` 后按返回值推进 pos；direct_IO 实现需自行消费 iter 并返回真实传输字节。当前返回未消费的原始全量 count，在 multi-iovec / 部分写 / VFS 二次处理时字节计数错乱 → fio 见 buflen 巨大（`(size_t)(-2097152)` 类）/ EFAULT。
- 1096-1098：`cfs_extent_write_pages(...)` **返回值被丢弃**，仅靠后续 page error 兜底。

注意：O_DIRECT **读**(extent_read_pages，type==READ)走同函数但表现正常（读 3614 MiB/s）——读路径 VFS 对返回值/iter 处理更宽容，且 get_user_pages 方向不同。本 bug 集中在写分支语义。

修复方向（待运行时复现精确触发点后定稿）：
1. 先 fio 隔离复现：numjobs=1/4/8、单/多 iovec、O_DIRECT 写，确认触发条件（是 multi-iovec 还是单 iovec 也错），把现象钉到具体输入。
2. `cfs_extent_dio_read_write` 改为按实际传输字节 `iov_iter_advance(iter, transferred)` 并返回 transferred；接收并处理 `cfs_extent_write_pages` 返回值。
3. `extent_dio_pages_alloc` 支持 multi-segment（循环遍历所有 iovec）或按当前内核 iov_iter API（`iov_iter_get_pages`）取页。

Bug B 比 Bug A 复杂（涉及 VFS direct_IO 字节语义），且可被 buffered 绕过（buffered 修好后是主路径），优先级次于 Bug A。但 direct IO 是数据库/AI 场景刚需，上线前需修。

## 分阶段计划

- **S0（done）**：根因定位（读 client_kernel 代码 + GFP 分布 + 实测 dirty/writeback/节点状态）。
- **S1（red line，先做）**：修 Bug A——cfs_extent_stream.c:532/892/1048 `GFP_KERNEL`→`GFP_NOFS`。编译 deb。
- **S2**：S1 部署到 1 个节点（如 .40），重放 numjobs=32 size=2g buffered，验证：dirty 不再无限累积、writeback>0 持续推进、节点保持 Ready、fio 跑完（或被 balance_dirty_pages 限速变慢但不 hang）。
- **S3**：Bug B 隔离复现（fio numjobs/iovec 矩阵）→ 定稿修复 → 改 cfs_extent_dio_read_write + extent_dio_pages_alloc → 编译。
- **S4**：S3 部署单节点，fio O_DIRECT 写多 job 验证（numjobs=1/8/32，md5 校验数据正确）。
- **S5**：4 节点全部更新部署（make client-kernel + systemd 滚动）。
- **S6**：syncnode 重跑 fuse CSI vs 内核 client 完整对比（buffered 读写 + O_DIRECT 读写 + 高并发大文件），更新对比报告。

## 验收标准

- [ ] Bug A：numjobs=32 size=2g（64g，远超 cgroup 12Gi）buffered 写，节点全程 Ready，writeback 持续推进，fio 正常完成（哪怕慢），无 NotReady。
- [ ] Bug B：fio O_DIRECT 写 numjobs=1/8/32 全部 err=0，写后 md5 与源一致。
- [ ] 4 节点 deb 更新部署，systemd 挂载正常，mount 读写正常。
- [ ] syncnode 重跑对比，拿到内核 client（含高并发/大文件/O_DIRECT 写读）vs fuse 完整数据。
- [ ] 回归：之前已修的 buffered 中低并发带宽不退化（内核写 ~1425、读 ~6171/裸读 3614 量级）。

## 当前进度

- S0 done（根因确认）。
- S1 done：cfs_extent_stream.c 三处 `GFP_KERNEL`→`GFP_NOFS`，commit 431dd8e27 已 push。编译部署用 `root`+`~/.ssh/lml_rsa` 上 .40（.42 key 未加），clone+`./configure`(生成 config.h，make-deb.sh 未自动跑—构建缺陷待修)+`make client-kernel`，deb 装到 .40（srcversion F5A60E31，service active）。
- **S2 验证：GFP_NOFS 部分有效，但不够（第二层根因未解）**。.40 实测 numjobs=32 size=2g：
  - **改善**：节点保持 Ready（kubelet 存活）。对比修复前 NotReady——GFP_NOFS 确实防住了"回写递归进 fs reclaim 拖垮整节点"。
  - **未解**：writeback 仍=0、dirty 撑满 cgroup、fio 陷 D 态死锁；且因 reclaim 不再递归崩溃，反而**不自愈**，节点 load 飙到 98、ssh 卡死，需重启 .40 恢复。
- **第二层根因（WQ 已排除）**：`extent_work_queue` 已是 `WQ_MEM_RECLAIM`（cfs_extent_client.c:236，有 rescuer，非此因）。死锁是 **cgroup memory + 网络 fs writeback 的固有死锁**：高并发 buffered 写（numjobs=32）产 dirty 速度 >> 异步 writeback（queue_work 到 extent_wq）速度，dirty 撑满 cgroup 12Gi；回写这批 dirty 需分配内存（cfs_page_vec_new / cfs_extent_write_pages 的 cpages，GFP_NOFS），但 cgroup 已满、唯一可释放的就是待回写的 dirty → 分配阻塞（NOFS 防递归不防"等内存"）→ 死锁。numjobs=8（16g）不触发（writeback 跟得上），numjobs≥32 触发。
- **第二层精确根因（已定位，无需抓栈）**：`cfs_fs_fill_super` 漏设 `SB_I_CGROUPWB`。内核 `CONFIG_CGROUP_WRITEBACK=y`、cubefs BDI 已注册，但 `inode_cgwb_enabled()` 要求 sb 带 `SB_I_CGROUPWB`；未设 → inode dirty 归 root wb 而非进程 memcg wb → `balance_dirty_pages` 按全局节点内存限速（节点内存大、不限）→ 受限 cgroup 内 dirty 撑满 memory limit → 回写要内存而 cgroup 满 → 死锁。cubefs 全无 `s_iflags` 设置（grep 确认）。
- **治本修复（已实施 commit 待补，待验证）**：`cfs_fs_fill_super` 加 `sb->s_iflags |= SB_I_CGROUPWB`（一行）。inode 自动 attach 进程 memcg wb，memcg dirty throttling 生效、按 pod memcg 的 dirty 限制提前限速写者，dirty 不撑满（最坏写变慢不死锁）。memcg throttling 是纯 memory 机制、不依赖 blkcg block device，网络 fs 适用（NFS/ext4/btrfs 均设此标志）。**不需要 mempool**：throttling 从源头防 dirty 撑满，回写不会再卡在满 cgroup 的内存分配。与第一层 GFP_NOFS 互补（前者防撑满、后者防回写递归 reclaim）。
- .40 已 sysrq（`echo b > /proc/sysrq-trigger`）重启恢复，ko/service/mount 开机自启正常。`sshpass -p ... ssh root@.40/.42` 密码可访问（比 lml_rsa key 更可靠，.42 也通）。
- 下一步：commit+push → .40 换 ko → 验证 numjobs=32 size=2g **不再死锁**（dirty 被 memcg 限速、不撑满、节点 Ready、fio 完成）→ 4 节点部署 → Bug B → 重跑 fuse vs 内核 client 对比。

## 验证方式教训

- 反复触发死锁验证成本极高：每次 numjobs=32 大写都把 .40 拖到 load 98 + ssh 卡死 + pod 删不掉，恢复要重启。**后续验证改为：先靠代码+一次 D 态栈精确定位，改对再验一次，不反复试**。
- 换 ko 运维坑：HostToContainer 下 node_exporter（监控 DaemonSet，bind host rootfs）会继承 host 的 cubefs mount，持有 stale 引用让 cubefs use_count 卡住、rmmod 失败——换 ko 前需先删持有 stale 引用的进程/pod。pod 重建若在 host umount 期间启动，会 bind 到 ext4 空目录而非 cubefs，需 host 挂好后重建 pod。

## 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| GFP_NOFS 改完 cgroup 真满时分配仍慢 | 写变慢 | 这是预期且可接受（最坏变慢≠死锁）；balance_dirty_pages 会提前限速避免撑满 |
| Bug B 根因未运行时钉死，改了不对 | direct 写仍错 | S3 先 fio 复现矩阵把触发条件钉到具体输入再改，不盲改 |
| 修改触发内核 panic（历史有 symlink panic 整机重启） | 节点重启 | 单节点先验（S2/S4），4 节点滚动放最后；重启必通知用户 |
| O_DIRECT 读也用同函数(892/1048) | 改 dio 影响读 | 读路径现正常，改写分支时保持读路径行为不变，回归验证读带宽 |
