# 用户管理

## 创建用户

```bash
cfs-cli user create [USER ID] [flags]
```

```bash
Flags:
    --access-key string                     # 指定用户用于对象存储功能的access key
    --secret-key string                     # 指定用户用于对象存储功能的secret key
    --password string                       # 指定用户密码
    --user-type string                      # 指定用户类型，可选项为normal或admin（默认为normal）
    -y, --yes                               # 跳过所有问题并设置回答为"yes"
```

## 删除用户

```bash
cfs-cli user delete [USER ID] [flags]
```

```bash
Flags:
    -y, --yes                               # 跳过所有问题并设置回答为"yes"
```

## 获取用户信息

```bash
cfs-cli user info [USER ID]
```

## 列出所有用户

```bash
cfs-cli user list
```

## 更新用户对某个卷的权限

[PERM]可选项为"只读"（READONLY/RO）、"读写"（READWRITE/RW）、"删除授权"（NONE）

```bash
cfs-cli user perm [USER ID] [VOLUME] [PERM]
```

## 更新用户的信息

```bash
cfs-cli user update [USER ID] [flags]
```

```bash
Flags:
    --access-key string                     # 更新后的access key取值
    --secret-key string                     # 更新后的secret key取值
    --user-type string                      # 更新后的用户类型，可选项为normal或admin
    -y, --yes                               # 跳过所有问题并设置回答为"yes"
```

