# cfs-sync — CubeFS 数据同步工具规划

## 一、目标

不走 FUSE，直接用 CubeFS SDK（meta.MetaWrapper + stream.ExtentClient）读写 CubeFS，
实现与对象存储、本地目录之间的双向同步，功能对标 rclone sync / JuiceFS sync。

---

## 二、支持的同步方向

| 源 | 目标 | 场景 |
|----|------|------|
| 对象存储 (S3/OSS/MinIO) | CubeFS | 数据入湖 |
| CubeFS | 对象存储 | 数据导出 / 备份 |
| 本地目录 | CubeFS | 批量上传 |
| CubeFS | 本地目录 | 批量下载 |
| CubeFS | CubeFS | 卷间迁移 |
| 本地目录 | 本地目录 | 顺带支持，方便测试 |

---

## 三、URI 格式

```
cfs://vol/path/to/dir         CubeFS（需 --master）
s3://bucket/prefix/           S3 兼容（AWS / MinIO / OSS）
/absolute/path/               本地绝对路径
./relative/path/              本地相对路径
```

S3 连接参数通过环境变量或 flag 传入：
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`
- `--endpoint`（MinIO / OSS / 自建 S3 需要）
- `--access-key` / `--secret-key` / `--region`（优先级高于环境变量）

---

## 四、命令行接口

```bash
cfs-sync sync <src> <dst> [flags]
cfs-sync copy <src> <dst> [flags]    # 同 sync 但不删除目标侧多余文件
cfs-sync check <src> <dst> [flags]   # 只校验，不传输

Flags:
  # === 连接 ===
  --master string          CubeFS master 地址（逗号分隔），可读 ~/.cfs-cli.json
  --endpoint string        S3 endpoint URL（MinIO/OSS/自建）
  --access-key string      S3 access key
  --secret-key string      S3 secret key
  --region string          S3 region（默认 us-east-1）
  --no-ssl                 禁用 TLS（HTTP endpoint）

  # === 并发 ===
  --transfers int          并发传输 worker 数（默认 10）
  --checkers int           并发 check worker 数（默认 20，独立于传输）
  --list-workers int       List 阶段并发目录展开数（默认 20）

  # === 比较策略 ===
  --size-only              只比较文件大小（不比 mtime）
  --checksum               用 MD5 校验代替 size+mtime（慢但精确）
  --ignore-existing        跳过目标侧已存在的文件（不管内容）

  # === 传输控制 ===
  --part-size int          大文件分片大小（默认 64 MiB）
  --multi-thread-cutoff    单文件内部开多线程的阈值（默认 256 MiB）
  --max-transfer string    达到总传输量上限后停止（如 100G）
  --max-duration duration  达到时长上限后停止（如 2h）
  --bwlimit string         带宽限制，支持时间表（如 "10M" 或 "9-18 5M,18-9 0"）

  # === 删除 / 备份 ===
  --delete                 删除目标侧多余文件（sync 模式默认开，copy 模式默认关）
  --backup-dir string      被覆盖/删除的文件移到此目录而非直接删除
  --suffix string          配合 --backup-dir，给备份文件加后缀（如 .bak）

  # === 过滤 ===
  --include string         只同步匹配的文件（glob，可多次指定）
  --exclude string         跳过匹配的文件（glob，可多次指定）
  --filter-from string     从文件读取 include/exclude 规则（每行一条）
  --files-from string      只同步此文件中列出的路径（一行一个，不做遍历）
  --min-size string        跳过小于此大小的文件（如 1M）
  --max-size string        跳过大于此大小的文件（如 10G）
  --min-age duration       跳过比此更新的文件（如 1h，只同步旧文件）
  --max-age duration       跳过比此更旧的文件（如 7d，只同步近期文件）

  # === 容错 ===
  --retries int            高层重试次数（默认 3）
  --retries-sleep duration 重试间隔（默认 1s，指数退避）
  --ignore-errors          忽略单文件错误，继续同步其他文件

  # === 小文件合并（差异化功能）===
  --pack                   启用小文件合并传输（适合 >10 万小文件）
  --pack-size string       合并包大小（默认 64 MiB）

  # === 输出 ===
  --dry-run                不执行，只打印要做的操作
  --progress               显示实时进度条
  --stats duration         定期打印统计信息（如 10s，默认 1m）
  --log-dir string         日志目录（默认 /tmp/cfs-sync-logs）
  --log-level string       日志级别（默认 WARN）
```

**示例：**
```bash
# S3 → CubeFS
cfs-sync sync s3://my-bucket/data/ cfs://my-vol/data/ --master 10.0.0.1:17010 --transfers 20

# CubeFS → 本地，只同步 .pt 文件，保留 30 天内的
cfs-sync sync cfs://my-vol/ckpt/ /data/backup/ --include "*.pt" --max-age 720h

# MinIO → CubeFS，被覆盖的文件备份到 /tmp/bak
cfs-sync sync s3://minio/models/ cfs://vol/models/ \
    --endpoint http://10.0.0.5:9000 --backup-dir cfs://vol/.backup --suffix .bak

# 大量小文件上传，启用合并
cfs-sync sync /dataset/ cfs://vol/dataset/ --pack --pack-size 64MiB --transfers 16

# 指定文件列表同步（不遍历目录）
cfs-sync copy cfs://vol/data/ s3://bucket/data/ --files-from changed-files.txt

# dry-run 预览 + 周期统计
cfs-sync sync /src/ cfs://vol/dst/ --dry-run --stats 5s
```

---

## 五、核心数据流

```
                    ┌─ dst List ─────────────────────┐
src List ──►        │                                 │
(并发 BFS)          ▼                                 │
           merge (streaming diff, 字典序归并) ──► check queue
                                                      │
                                               checker pool (N)
                                                      │
                                              transfer queue
                                                      │
                                              worker pool (M)
                                            (copy / delete / pack)
                                                      │
                                               stats collector
```

**三段流水线**，List 未完成即开始 check，check 未完成即开始 transfer，端到端延迟最小。

---

## 六、文件结构

```
tool/cfs-sync/
├── PLAN.md               本规划文档
├── main.go               入口，subcommand dispatch
├── cmd_sync.go           sync/copy/check 子命令解析与调度
├── config.go             CLI 配置读取（复用 cfs-io 的 loadCLIConfig）
├── engine.go             同步引擎（diff、任务分发、流水线）
├── checker.go            check worker pool（比较文件是否需要传输）
├── worker.go             transfer worker pool
├── packer.go             小文件合并打包逻辑
├── filter.go             include/exclude/size/age 过滤器
├── stats.go              进度统计、打印、--stats 定时输出
└── storage/
    ├── storage.go        Storage interface 定义
    ├── cfs.go            CubeFS 后端（SDK 直连）
    ├── s3.go             S3 兼容后端（aws-sdk-go v1）
    └── local.go          本地文件系统后端
```

---

## 七、Storage 接口

```go
// storage/storage.go

type Object struct {
    Key     string    // 相对路径，统一用 / 分隔，目录以 / 结尾
    Size    int64
    Mtime   time.Time
    IsDir   bool
    ETag    string    // 可选，用于 --checksum
}

type Storage interface {
    // 返回 prefix 下所有 Object（递归），通过 channel 流式输出，有序（字典序）
    List(ctx context.Context, prefix string) (<-chan *Object, <-chan error)

    // 读取 key 对应文件的指定范围，返回 ReadCloser
    Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error)

    // 写入 key，从 r 读取 size 字节
    Put(ctx context.Context, key string, r io.Reader, size int64) error

    // 删除 key
    Delete(ctx context.Context, key string) error

    // 创建目录（S3 为 noop，CubeFS/Local 创建真实目录）
    MkdirAll(ctx context.Context, key string) error

    // 返回可读名称，用于日志和进度显示
    String() string
}
```

---

## 八、同步逻辑（engine.go）

```
func Sync(ctx, src Storage, dst Storage, opts SyncOptions):

1. 启动 src.List 和 dst.List 各自 goroutine（有序 channel 流式输出）
2. merge goroutine 双指针归并（字典序）→ 实时产出 check 任务：
   - src 有 dst 无 → copy 任务
   - 两侧都有 → 投入 checker pool 判断是否需要 copy
   - src 无 dst 有 → --delete 时投入 delete 任务，否则 skip
3. checker pool（--checkers 个 goroutine）:
   - --size-only: 比较 size
   - --checksum: 对比 ETag（或现算 MD5）
   - 默认: 比较 size + mtime
   → 需要传输则投入 transfer queue，否则 skip
4. transfer worker pool（--transfers 个 goroutine）:
   - copy: src.Get → dst.Put（流式，不落临时文件）
   - 大文件（>= multi-thread-cutoff）: 分片并发读写
   - delete: dst.Delete（--backup-dir 时先 move）
   - --pack: 积攒小文件到 packer，达到 pack-size 再一次写入
5. 限速：bwlimit 令牌桶（全局共享）
6. 重试：单文件失败后指数退避重试（--retries 次）
7. 统计：
   - 实时: --progress 进度条
   - 定期: --stats 间隔打印吞吐 / 剩余 / 错误数
8. 退出码：
   - 0: 成功
   - 1: 有文件传输失败（--ignore-errors 时仍然退出 1）
   - 2: 参数错误
   - 8: --max-transfer 达到上限
   - 10: --max-duration 达到上限
```

---

## 九、性能设计

### 9.1 CubeFS List 批量 InodeGet

`ReadDir_ll` 返回 `[]Dentry`（name + inode），再逐个 `InodeGet_ll` 获取 size/mtime 是 N 次 RPC。
改为 `BatchInodeGet([]ino)` 一次获取所有属性，降低 MetaNode 往返次数。

```
ReadDir_ll(dir) → [ino1..ino1000]
BatchInodeGet([ino1..ino1000]) → 1 次 RPC 返回所有 size/mtime
```

### 9.2 并发 BFS 目录展开（--list-workers）

串行递归深层目录树 = N 个目录 × 1 RTT/目录（串行）。
改为带信号量的并发 BFS：

```
队列: [root]
list worker × 20:
  取出一个目录 → ReadDir_ll → 子目录重新入队，文件输出到 channel
```

### 9.3 流水线三段（List → Check → Transfer）

List 未完成即开始 Check，Check 未完成即开始 Transfer，三段并行。
减少大规模同步的首文件时延。

### 9.4 Buffer Pool（sync.Pool）

每个 transfer worker 从 pool 取 buffer，写完归还，减少 GC 压力。

### 9.5 大文件分片并发（--multi-thread-cutoff）

单文件内部开 M 个 goroutine，每个负责一个 offset 范围的 Get/Put，合并写入目标。
CubeFS 侧 `ec.Write` 支持任意 offset，天然支持并发写。

### 9.6 S3 List 并行分片

对顶层子目录按前缀并行发起 `ListObjectsV2`，加速大 bucket 遍历。
当目录结构均匀时，--list-workers 个 goroutine 分别列举不同前缀。

### 9.7 小文件合并传输（--pack，差异化功能）

适合 AI 训练数据集（百万级小文件）场景：
- 积攒若干小文件成一个大 tar 流，一次写入 CubeFS
- 同时写 `.cfs-pack-index`（JSON，记录每个文件的 offset + size）
- 读取时通过 index 直接 seek，无需解包整体

**适用场景**：只写一次、批量读。不适合频繁修改。

---

## 十、各后端实现要点

### CubeFS 后端（storage/cfs.go）
- 复用 cfs-io 的 `newCFSClient`（meta.MetaWrapper + stream.ExtentClient）
- `List`: 并发 BFS + BatchInodeGet，channel 有序流出
- `Get`: `ec.Read`，支持 offset + size（用于分片）
- `Put`: `mw.Create_ll` + `ec.OpenStream` + `ec.Write` + `ec.Flush` + `ec.CloseStream`
- `MkdirAll`: 复用 cfs-io 的 `mkdirs`
- `Delete`: `mw.Delete_ll`

### S3 后端（storage/s3.go）
- 使用已有的 `github.com/aws/aws-sdk-go v1`
- `List`: `s3.ListObjectsV2Pages`，顶层子目录并行
- `Get`: `s3.GetObject`，支持 Range header（用于分片）
- `Put`: size < part-size 用 `s3.PutObject`，大文件用 Multipart Upload
- `Delete`: `s3.DeleteObject`
- ETag 直接用 S3 返回的（用于 --checksum 模式）

### 本地后端（storage/local.go）
- `List`: `filepath.WalkDir`，channel 有序流出
- `Get`: `os.Open` + seek
- `Put`: `os.Create` + `io.Copy`，写完后 `os.Chtimes` 保留 mtime

---

## 十一、与 rclone / JuiceFS sync 的对比

| 特性 | rclone | JuiceFS sync | cfs-sync |
|------|--------|-------------|---------|
| CubeFS 访问 | FUSE 路径 | FUSE 路径 | **直连 SDK** |
| 顺序大文件 | ≤FUSE 上限 | ≤FUSE 上限 | **SDK 多流，无上限** |
| List 并发 | --fast-list | - | **BFS 并发 + BatchInodeGet** |
| 小文件合并 | ✗ | ✗ | **✓ --pack** |
| Checker/Worker 分离 | ✓ | ✗ | ✓ |
| 流水线 List→Check→Copy | ✓ | 部分 | ✓ |
| --backup-dir | ✓ | ✗ | ✓ |
| --files-from | ✓ | ✗ | ✓ |
| size/age 过滤 | ✓ | 部分 | ✓ |
| bisync（双向同步） | ✓ | ✗ | 暂不支持 |
| 加密传输 | ✓ | ✗ | 暂不支持 |

---

## 十二、暂不支持（后续可扩展）

- bisync（双向同步，状态复杂）
- 符号链接（symlink）同步
- 扩展属性（xattr）同步
- 加密 / 压缩传输
- 多卷并行（一次 sync 多个 vol）
- 断点续传（crash recovery，--files-from 可手动模拟）
