# 对象网关

对象网关提供兼容 S3 的对象存储接口。它使得 CubeFS 成为一个可以将两种通用类型接口进行融合的存储（POSIX 和 S3兼容接口）。可以使用户使用原生的 Amazon S3 SDK 操作 CubeFS 中的文件。

## 框架

![image](./pic/cfs-object-subsystem-structure.png)

ObjectNode 是一个功能性的子系统节点。它根据需要从资源管理器（Master）获取卷视图（卷拓扑）。 每个 ObjectNode 直接与元数据子系统（MetaNode）和副本子系统（DataNode）通信。

ObjectNode 是一种无状态设计，具有很高的可扩展性，能够直接操作 CubeFS 集群中存储的所有文件，而无需任何卷装入操作。

::: warning 注意
`v3.2.1`之前的版本暂不支持纠删码卷
:::

## 特性

- 支持原生的 Amazon S3 SDKs 的对象存储接口
- 支持两种通用接口的融合存储（POSIX 和 S3 兼容接口）
- 无状态和高可靠性

## 语义转换

基于原有 POSIX 兼容性的设计。每个来自对象存储接口的文件操作请求都需要对 POSIX 进行语义转换。

| POSIX    | Object Storage |
|----------|----------------|
| `Volume` | `Bucket`       |
| `Path`   | `Key`          |

**示例:**

![image](./pic/cfs-object-subsystem-semantic.png)

> Put object \'*example/a/b.txt*\' will create and write data to file
> \'*/a/b.txt*\' in volume \'*example*\'.

## 用户

在使用对象存储功能前，需要先通过资源管理器创建用户。创建用户的同时，会为每个用户生成 *AccessKey* 和 *SecretKey* ，其中 *AccessKey* 是整个 CubeFS 集群中唯一的 16 个字符的字符串。

CubeFS 以卷的 **Owner** 字段作为用户 ID。创建用户的方式有两种：

1. 通过资源管理器的 API 创建卷时，如果集群中没有与该卷的 Owner 同名的用户时，会自动创建一个用户 ID 为 Owner 的用户
2. 调用资源管理器的用户管理 API 创建用户，[详情请参考](../dev-guide/admin-api/master/user.md)

## 授权与鉴权

对象存储接口中的签名验证算法与 Amazon S3 服务完全兼容。用户可以通过管理API获取用户信息，请参见[Get User Information](../dev-guide/admin-api/master/user.md) 。

从中获取 *AccessKey* 和 *SecretKey* 后，即可利用算法生成签名来访问对象存储功能。

用户对于自己名下的卷，拥有所有的访问权限。用户可以授予其他用户指定权限来访问自己名下的卷。权限分为以下三类：

- 只读或读写权限；
- 单个操作的权限，比如 GetObject、PutObject 等；
- 自定义权限。

当用户使用对象存储功能进行某种操作时，CubeFS 会鉴别该用户是否拥有当前操作的权限。

## 临时隐藏数据

以原子方式在对象存储接口中进行写操作。每个写操作都将创建数据并将其写入一个不可见的临时对象。ObjectNode 中的 volume 运算符将文件数据放入临时文件，临时文件的元数据中只有'
**inode**'而没有'**dentry**'。当所有文件数据都成功存储时，volume 操作符在元数据中创建或更新'**dentry**'使其对用户可见。

## 对象名称冲突（重要）

POSIX 和对象存储是两种不同类型的存储产品，对象存储是一种键-值对存储服务。所以在对象存储中，名称为 `a/b/c` 和名称为 `a/b` 的对象是两个完全没有冲突的对象。

不过 CubeFS 是基于 POSIX 设计的。根据语义转换规则，对象名 `a/b/c` 中的 `b` 部分转换为文件夹 `a` 下的文件夹 `b`，对象名 `a/b` 中的 `b` 部分转换为文件夹 `a` 下的文件 `b`。

类似于上面这样的对象名称在 CubeFS 中是冲突的。
