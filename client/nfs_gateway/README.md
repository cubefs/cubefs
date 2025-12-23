# CubeFS NFS Gateway

## 简介

这是使用 `github.com/willscott/go-nfs` 库实现的CubeFS NFS网关，可以将CubeFS文件系统通过NFS协议暴露的NFS网关功能。


**特点**：
- ✅ 使用成熟的go-nfs库，符合RFC标准
- ✅ 支持CubeFS后端（通过billy.Filesystem接口）
- ✅ 支持内存文件系统（用于测试）
- ✅ 代码简洁，易于维护
- ✅ 可以将CubeFS卷通过NFS协议暴露给客户端
- ✅ 使用标准CubeFS client配置（与fuse client兼容）
- ✅ 完整的文件/目录操作支持

## 目录结构

```
client/nfs_gateway/
├── main.go                    # 主程序入口
├── cubefs_fs.go               # CubeFS文件系统后端（实现billy.Filesystem接口）
├── go.mod                     # Go模块定义
├── go.sum                     # 依赖校验
├── README.md                  # 本文档
├── README_DEBUG.md            # 调试说明
├── quick_test.sh              # 快速测试脚本
├── setup_nfs_source.sh        # NFS源码设置脚本（可选，用于本地调试）
```

## 快速开始

### 1. 编译

```bash
# 使用build脚本编译
cd src/github.com/cubefs/cubefs
bash build/build.sh nfs_gateway

# 或直接编译
cd client/nfs_gateway
go build -o nfs_gateway .
```

### 2. 使用内存文件系统（测试）

```bash
# 运行（使用内存文件系统）
./nfs_gateway -port 20490 -memfs

# 挂载测试
sudo mount -t nfs -o vers=3,port=20490,mountport=20490,proto=tcp localhost:/ /tmp/nfs_test_mount

# 测试文件操作
echo "test" | sudo tee /tmp/nfs_test_mount/test.txt
sudo cat /tmp/nfs_test_mount/test.txt
```

### 3. 使用CubeFS后端

```bash
# 运行（使用CubeFS配置文件，与fuse client配置格式相同）
./nfs_gateway -port 20490 -config /path/to/client.config

# 挂载测试
sudo mount -t nfs -o vers=3,port=20490,mountport=20490,proto=tcp localhost:/ /tmp/nfs_test_mount

# 测试文件操作
echo "test" | sudo tee /tmp/nfs_test_mount/test.txt
sudo cat /tmp/nfs_test_mount/test.txt
```

### 4. 使用快速测试脚本

```bash
# 运行完整测试（编译、启动、测试、检查日志）
bash quick_test.sh
```

## 配置说明

NFS Gateway 使用与 CubeFS FUSE client 相同的配置文件格式，例如：

```json
{
  "masterAddr": "192.168.0.1:17010,192.168.0.2:17010",
  "volName": "your_volume",
  "owner": "your_owner",
  "logDir": "/path/to/logs",
  "logLevel": "info",
  ...
}
```

详细配置选项请参考 CubeFS client 配置文档。

## 功能特性

### 已实现的功能

- ✅ **文件操作**: Create, Open, Read, Write, Seek, Truncate, Stat, Sync
- ✅ **目录操作**: ReadDir, MkdirAll, Rename, Remove
- ✅ **路径管理**: 路径到inode转换，路径缓存
- ✅ **配置管理**: 标准CubeFS client配置支持
- ✅ **日志系统**: 标准日志输出（output.log + SDK logs）

### 部分实现的功能

- ⚠️ **文件属性**: Chmod, Chown, Chtimes（返回nil，CubeFS可能不支持）
- ⚠️ **认证机制**: 当前使用AUTH_NULL（无认证），支持实现AUTH_SYS或Token机制

详细功能评估请参考 [GATEWAY_EVALUATION.md](GATEWAY_EVALUATION.md)

## 技术栈

- **NFS库**: `github.com/willscott/go-nfs v0.0.3` - 成熟的NFS协议实现
- **文件系统接口**: `github.com/go-git/go-billy/v5` - 文件系统抽象接口
- **内存文件系统**: `github.com/go-git/go-billy/v5/memfs` - 用于测试
- **CubeFS SDK**: 使用标准 `client/fs` 和 `sdk` 包

## 相关文档

- [GATEWAY_EVALUATION.md](GATEWAY_EVALUATION.md) - 网关功能评估报告
- [AUTH_IMPLEMENTATION.md](AUTH_IMPLEMENTATION.md) - 认证机制实现方案
- [README_DEBUG.md](README_DEBUG.md) - 调试和故障排查指南

## 参考

- [go-nfs库](https://github.com/willscott/go-nfs)
- [go-billy库](https://github.com/go-git/go-billy)
- [CubeFS文档](https://cubefs.io/)
