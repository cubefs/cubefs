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
- **S2 验证（SB_I_CGROUPWB 已部署 .40，srcversion 3907C2EF）——三层结构浮现**：
  - 第一层 GFP_NOFS：✓ 防回写递归 reclaim 拖垮节点。
  - 第二层 SB_I_CGROUPWB：✓ memcg dirty throttling 生效。实测 numjobs=32 size=2g：dirty 被限在 2532MB（不再无限涨）、mem 稳定 10430MB（**不撑满 12286**，对比修复前撑满）、**节点运行时 Ready + ssh 18s 响应**（对比修复前 load 130 ssh 完全死）。致命的"拖垮节点失联"基本解决。
  - **第三层（未解）：高并发下 cgwb writeback 停摆**。numjobs=32 时 written 卡 9026MB（其中 6494MB 已回写落盘、dirty 残 2532MB）、wb=0、fio D 态停滞。但：节点级 dd 512MB → dirty 3s 内回落到 0（root wb 回写正常）；pod cgroup 内 dd 512MB → dirty 立即 0（cgwb 小规模回写正常）。**仅 pod cgroup + 高并发 numjobs=32 触发停摆**。最可能：cgwb flusher 的 `cfs_page_vec_new`/`cpages`（GFP_NOFS）在 mem 逼近 memcg limit（10430/12286）时分配卡住——memcg 内可回收的只有 dirty，回收 dirty 又靠 flusher 自己 → 卡。删 pod 时 cgroup 销毁要回写残留 2532MB dirty，回写卡 → .40 又 NotReady（节点 hung-task 保护自重启）。
- **第三层精确根因（D 态栈铁证，已抓到）**：**不是 mempool/内存死锁，是 workqueue 饱和死锁**。cgwb flusher（`wb_workfn`）回写栈：`cfs_writepages → cfs_extent_write_pages → extent_write_pages_normal → extent_stream_get_writer → cfs_extent_writer_flush → wait_event(tx_inflight==0 && rx_inflight==0)`（卡死）。机制：
  - `extent_stream_get_writer`（cfs_extent_stream.c:209-218）在 writer offset 不连续 / 写满 EXTENT_SIZE 时调 `cfs_extent_writer_flush` 换 writer。
  - `cfs_extent_writer_flush`（cfs_extent_writer.c:71-72）`wait_event` 同步等该 writer 的 tx/rx packet 飞行清零。
  - tx/rx packet 收发**共用同一个 `extent_work_queue`**：`tx_work_cb`（:136 同步 send）发完 `queue_work(extent_work_queue, rx_work)`（:144）；`rx_work_cb`（:180 `cfs_socket_recv_packet` **同步阻塞等 datanode reply**）。
  - 高并发（numjobs=32）下大量 rx_work 同步 recv 阻塞占满 extent_work_queue worker（mem 逼近 memcg limit 时 WQ_MEM_RECLAIM 退化单 rescuer 更甚），flusher 等的 writer 的 rx_work 排队跑不了 → rx_inflight 不减 → flusher 永久 D 态卡 → dirty 不回写。第二个 flusher 卡 `inode_sleep_on_writeback`（等第一个 flusher 持有的 inode I_SYNC）。numjobs≤8 worker 够、不饱和。
- **第三层修复方向（架构级，非一行）**：根治需让 recv 不阻塞 wq worker（独立 recv 线程/socket 异步/epoll），或 writepages 路径不同步等网络往返；最小缓解 tx/rx 拆独立 wq + 调大 max_active（治标，仍可能被阻塞型 recv 占满）。属内核 client 回写架构改造。

### 第三层根治方案（recv 异步化 — per-socket recv kthread，用户选定治本）

核心：rx_work（同步阻塞 recv、共用 extent_work_queue）→ 每个 writer/reader 一个专用 recv kthread。recv 在专用线程阻塞，不再占 wq worker，互不饿死（网络 client 标准模式）。tx_work 仍走 wq（发送不阻塞）。

改动点（writer + reader 对称）：
- **cfs_extent.h**：struct 的 `struct work_struct rx_work` → `struct task_struct *rx_thread` + `wait_queue_head_t rx_pending_wq`（唤醒 recv 线程）。
- **cfs_extent_writer.c / cfs_extent_reader.c**：
  1. `#include <linux/kthread.h>`。
  2. `rx_work_cb` 函数体原样包进 `rx_thread_fn`：`while(!kthread_should_stop()){ wait_event(rx_pending_wq, !list_empty_careful(rx_packets)||kthread_should_stop()); <原 rx_work_cb 体> }`。逻辑（recv + recover 重发 + handle_reply + atomic_sub rx_inflight + wake rx_wq）完全不变。
  3. `*_new`：`init_waitqueue_head(&rx_pending_wq)` + `rx_thread = kthread_run(rx_thread_fn, w, "cfs-rx-%llu", ext_id)`；kthread 创建失败要回滚（release sock + kfree）。
  4. `tx_work_cb`：发完 packet 入 rx_packets + inc rx_inflight 后，`wake_up(&rx_pending_wq)` 替代 `queue_work(extent_work_queue,&rx_work)`。
  5. `*_release`：`cancel_work_sync(&tx_work)` 后 `kthread_stop(rx_thread)` 替代 `cancel_work_sync(&rx_work)`。注意顺序：先停 tx（不再产生新 rx_packets）再停 rx_thread。
- **风险**：① kthread 生命周期（new 失败回滚 / release 先 tx 后 rx）；② wait_event 条件 race（用 list_empty_careful，醒来后 spin_lock 取）；③ recover 路径在 rx_thread 上下文新建 recover writer/reader（recover 自带 rx_thread）+ 重发，逻辑不变；④ kthread 数量 = 活跃 writer/reader 数（顺序写每文件通常 1 活跃 writer，numjobs=32≈32 kthread，可接受）；⑤ 改错 panic（内核模块）——单节点先验。
- **验证**：numjobs=32 size=2g：writeback 持续推进（wb>0、dirty 回落）、fio 完成、节点 Ready、删 pod 不卡。回归 numjobs≤8 带宽不退化。

#### 实施进展（recv kthread 重构）

- 代码完成：cfs_extent.h（struct rx_work→rx_thread+rx_pending_wq）、cfs_extent_writer.c、cfs_extent_reader.c 对称改造。编译成功 srcversion **F2F9738B**（≠ 第二层 3907C2EF），无 error。
- 待验证：换 ko 到 .40 → numjobs=32 不死锁。

#### 第三层根治设计:writer flush 异步化(2026-06-04,#315)

**精确定位**:page writeback 本身已异步(reply_cb 里 end_page_writeback)。flusher 卡的只是 `extent_stream_get_writer`(cfs_extent_stream.c:218-219)在 cgwb flusher 上下文**同步** `cfs_extent_writer_flush`(等 tx/rx_inflight 清零 + meta commit RPC)——writer 切换(offset 不连续/写满 EXTENT_SIZE/RECOVER)时换 writer 要先 flush 旧 writer。根治 = 把这个同步 flush 移出 flusher 上下文。

**数据结构**(cfs_extent.h):
- `struct cfs_extent_stream` 加:`struct list_head pending_flush`(待异步 flush 的 writer,复用 writer->list)、`spinlock_t lock_pending`、`atomic_t nr_pending_flush`、`wait_queue_head_t flush_wq`。
- `struct cfs_extent_writer` 加:`struct work_struct flush_work`。
- 全局 `cfs_flush_workqueue`(alloc_workqueue WQ_UNBOUND|WQ_MEM_RECLAIM,与 extent_work_queue 分开,避免和 tx 抢)。

**改动点**:
1. **新增 `extent_writer_submit_async_flush(writer)`**:摘出后的 writer 加入 es->pending_flush(lock_pending)+ atomic_inc(nr_pending_flush)+ queue_work(cfs_flush_workqueue, &writer->flush_work)。
2. **新增 `extent_writer_flush_work_cb`**:`cfs_extent_writer_flush(writer)`(原同步逻辑:wait tx/rx + cache_append(sync=true)+ meta_append_extent,一致性约束全保留)→ `cfs_extent_writer_release(writer)` → 从 pending_flush 摘除 → atomic_dec(nr_pending_flush)→ wake_up(flush_wq)。meta commit 失败 → mapping_set_error(已在 reply_cb 处理 page error,这里补 inode 级)。
3. **改 `extent_stream_get_writer`(cfs_extent_stream.c:218-219)**:摘出旧 writer 后,`cfs_extent_writer_flush+release` → 改 `extent_writer_submit_async_flush(writer)`,while 继续找/建新 writer。flusher 不再同步等。
4. **改 `cfs_extent_stream_flush`(:1176,fsync/close/stream_release 调)**:先 `wait_event(flush_wq, nr_pending_flush==0)` 等所有异步 flush 完成,再同步 flush es->writers 剩余 active writer(原逻辑,fsync 用户上下文同步等 OK、不死锁)。保证 fsync 语义。
5. **顺带修 :1201 `if(!writer)` → `if(!reader)`** bug(readers flush 循环第一个就退出)。

**一致性/竞态保证**:
- ext_size 在 flush_work_cb 的 wait rx_inflight==0 后读(约束1,不变)。cache_append(sync=true)+meta_commit 仍在 flush_work_cb 内连续(约束2/3)。release 在 flush 完成后(flush_work_cb 内,约束4)。
- **use-after-free(最高风险)**:stream_release→stream_flush→wait nr_pending_flush==0 保证后台 flush_work 全完成才 cache_clear+kfree(es);flush_work_cb 访问 writer->es 安全。
- recover writer:异步 flush 的 writer 已摘出 es->writers,其 rx_thread recover 时新建的 recover writer 仍加 es->writers,由后续 get_writer/stream_flush 处理(不变)。
- 顺序:顺序写同时只 1 活跃 writer,pending flush 串行性由 EXTENT_SIZE(128MB)切换频率保证(不频繁);CFS_OP_EXTENT_ADD_WITH_CHECK meta 端有 check。

**验证**:numjobs=32 size=2g:wb>0 持续、written 推进到完成、节点 Ready、删 pod 不卡;回归 numjobs≤8 带宽 + O_DIRECT + fsync 后数据一致(crc32c verify)。

**回滚**:get_writer 改回同步 flush(去掉 submit_async)即回到当前行为。

#### 异步化 v1 实测(srcversion 18DEF88)+ 背压 v2 需求(2026-06-04)

**v1 结果——方向对但缺背压**:numjobs=32 size=2g 实测,**writeback 持续推进**(written 4s→3904 / 8s→13501 / 16s→31923 / 24s→51626MB,持续涨不停滞;dirty 受控低位 752~1764MB,wb 有活动)——对比修复前卡 25GB 死锁,**flusher 同步等待死锁已解**。但 fio 完成后/期间节点最终仍 **NotReady**:根因是**异步 flush 缺背压**——异步 flush 的 writer 在 flush 完成前不 release,numjobs=32 顺序写 2GB/文件每 128MB 切一次 writer(512 次切换),写速远超 extent commit(meta RPC)速度时,**pending writer 无限堆积**,每个持 1 socket + 1 recv kthread → socket/kthread/内存耗尽拖垮节点。

**v2 背压设计**:`extent_writer_submit_async_flush` 入口加 `wait_event(es->flush_wq, nr_pending_flush < CFS_MAX_PENDING_FLUSH)`(如 16)。pending 满时在 flusher 上下文短暂等一个 flush 腾位——但 flush work 在独立 cfs_flush_workqueue 完成、不依赖本上下文,故**不会回到死锁**(对比原"每次 writer 切换都同步等完整 tx/rx 往返+meta commit");MAX 限制 pending writer 数 = socket/kthread 上限,资源可控。SB_I_CGROUPWB 的 balance_dirty_pages 限速是写入端节流,背压是 flush 端节流,二者互补。

**v2 实测——#313 闭环成功(srcversion 04862A0,2026-06-04)**:
- numjobs=32 size=2g:**64GB 全写完、2287 MiB/s、28.6s**(对比修复前卡 25GB 死锁、v1 无背压 pending 堆积拖垮节点 NotReady)。
- 节点全程 **Ready**、load 正常;fio 完成后 cfs-wrx kthread **归 0**(writer 全 release、无泄漏——背压+异步 flush 正常清理)。
- 回归:numjobs=8 buffered+fsync **3920 MiB/s** crc32c verify 通过;O_DIRECT 写 **1202 MiB/s** crc32c verify 通过(Bug B 不回归、数据一致)。
- CFS_MAX_PENDING_FLUSH=16 实测够用(高带宽 + 资源不爆);后续可按 commit 速度微调。

#### 工程插曲（运维注意）

1. **make-deb.sh 不强制重编**：`if [ ! -f cubefs.ko ]` 残留旧 ko 就跳过编译、打包旧 ko。换版本必须先 `make clean && rm -f client_kernel/cubefs.ko` 再 `make client-kernel`，并核对 srcversion 变化确认。
2. **BDI 泄漏致 mount EEXIST**：`super_setup_bdi_name(sb,"cubefs")` 用固定名；某次 mount 失败/umount 不干净时 sb 没释放 → BDI "cubefs" 泄漏（rmmod 模块都清不掉），后续 mount 报 `kobject_add -EEXIST`、`mount(2) File exists`。清理需重启节点。这是 cubefs BDI 生命周期的潜在 bug（可列后续：mount 失败路径要释放 BDI / 用唯一 BDI 名）。
3. **换 ko use count 难清**：node_exporter + syncnode pod 经 hostPath/HostToContainer 持有 stale cubefs 引用；换 ko 必须删这些 pod 等 use count→0，且 host 要先 umount（否则重建 pod 继承 cubefs 又持有）。
- **务实现状**：numjobs≤8 buffered 写正常完成（前两层后更稳，无残留 dirty、删 pod 不卡）；numjobs=32 极限并发仍卡（第三层）。对比测试用 numjobs≤8 可继续。
- 下一步（待用户定）：A. 接受前两层（常规并发可用）→ numjobs≤8 重跑对比 + 修 Bug B，第三层列 backlog；B. 继续深挖第三层（mempool，成本高、反复拖垮节点）。

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

---

## 第 4 个上线阻断 bug：O_DIRECT 高并发 page lock 死锁（2026-06-04，方案 A 根治）

### 现象
4TB 独立卷 syncnode bench `numjobs=32 size=2g libaio direct=1`，4 节点跑同参，.43 撞死锁：32 个 fio 全 **D 态**、load 32、bench task 卡 3/4 永不 succeeded（平台只在全 succeeded 才聚合 shard 数据 → dashboard 看似"只有部分节点数据"）。numjobs=8 不复现（之前 O_DIRECT 写只验证到 8）。

### D 态栈铁证
```
wait_on_page_bit_common → __lock_page → cfs_extent_dio_read_write+0x1fd
  → cfs_direct_io → generic_file_direct_write → __generic_file_write_iter → aio_write
```
卡在 `cfs_extent_stream.c:1110 lock_page(pages[i])`。

### 根因（设计级）
`extent_dio_pages_alloc`(cfs_extent_stream.c:1067) 用 `get_user_pages_fast` 拿 **fio 用户态 IO buffer 的 page**；`cfs_extent_dio_read_write`(1110) 对这些**用户页** `lock_page`+`set_page_writeback`，把 page 的 PG_locked 借用成"异步 IO 完成"的同步原语（1123 `wait_on_page_locked` 等回调 unlock）。

numjobs=32 iodepth=32 高并发时多个并发 dio 复用/共享同一用户 buffer 页（fio buffer pool 复用、相邻 IO 跨页边界），或单次 get_user_pages 返回重复页 → 对同一页第二次 `lock_page` → D 态死锁。PG_locked/PG_writeback 是 page cache 的同步协议，对 get_user_pages 的用户匿名页滥用，高并发必撞；numjobs=8 并发低、碰撞概率小没暴露。读路径(892/1118 direct_io=true)同样踩用户页，只是这次写先死。

### 方案 A（独立临时内核页 + 逐页 copy，最小闭环根治）
不再让 IO 直接踩用户页：
- `extent_dio_pages_alloc`：`get_user_pages_fast` → `alloc_pages(GFP_NOFS)` 每个 dio 独立分配 npages 个临时页；写路径 `copy_page_from_iter` 把用户数据拷进临时页（按 first_page_offset/end_page_size 布局）。
- `cfs_extent_dio_read_write`：读完成后 `copy_page_to_iter` 临时页→用户；iter 消费由 copy_page_*_iter 完成（替代 Bug B 的手动 iov_iter_advance）。
- `extent_dio_pages_release`：去掉 `set_page_dirty_lock`（临时匿名页无需），put_page 即 free（refcount=1）。

临时页每个 dio 独立、绝不跨 dio 共享 → `lock_page` 永不撞车、自死锁消除。**完成回调（315/418/460/646/740）、cfs_page 结构、write_pages/read_pages 全不改**：临时匿名页 mapping=NULL，对 set/end_page_writeback+lock/unlock 完全安全（回调 `mapping && !PageAnon` 处短路）。

为什么不选"零拷贝+独立 completion"重构：memcpy ~20GB/s 远超实测 O_DIRECT 1.1GB/s（copy 开销 <5%），而 completion 方案要改 5 处回调+cfs_page 结构+read/write 双路径、漏一处即死锁/UAF，风险高一个量级。正确性与稳定优先于零拷贝。

### 阶段
- **C1 改码**：extent_dio_pages_alloc（临时页+copy_page_from_iter）+ dio_read_write（copy_page_to_iter+去手动 advance）+ release（去 set_page_dirty_lock）
- **C2 编译**：`make clean && rm -f client_kernel/cubefs.ko && make client-kernel`，核对 srcversion 变化
- **C3 4 节点换 ko**（.40/.41/.42/.43）：删持有 stale 引用的 pod、host umount、装新 deb、重挂卷 B
- **C4 numjobs=32 重测**：fuse vs 内核 4TB 卷同参对比，验证无死锁、出聚合数据进 dashboard

### 验收（2026-06-04 全部通过，ko srcversion 052A9879）
- [x] numjobs=32 O_DIRECT 写/读 4 节点全完成、无 D 态、节点 Ready（4 shard errors=0）
- [x] O_DIRECT 数据一致（.41 单节点 numjobs=32 crc32c verify 通过，写 1275/读 3724 MiB/s，不回归 Bug B）
- [x] fuse vs 内核 numjobs=32 对比数据聚合进 dashboard

### 实测结果（4TB 独立卷，4 节点聚合，numjobs=32 size=2g libaio direct=1）
| 场景 | fuse CSI | 内核 client | 内核倍数 |
|---|---|---|---|
| seq-write | 822 MB/s | **2098 MB/s** | 2.6× |
| seq-read | 1094 MB/s | **12570 MB/s** | 11.5× |

per-node 内核 写 424~671 / 读 2628~3824 MB/s。之前 .43 正是卡死在 numjobs=32 O_DIRECT 写，方案 A 后 4 节点全 succeeded、errors=0、无死锁。换 ko 全程逐个重启（BDI 泄漏致 umount 后 use_count 仍=1、rmmod 卸不掉，重启是唯一干净路径）。
