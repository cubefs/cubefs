# 缓存节点管理

## 列出所有缓存节点

获取所有缓存节点的信息，包括 id、地址、缓存数据情况

```bash
./cfs-cli flashnode list
```

## 查询flashnode 缓存状态信息

```bash
// 查询flashnode状态的key值不带过期时间
./cfs-cli flashnode httpStat 127.0.0.1:17510
// 查询flashnode状态的key值包含过期时间
./cfs-cli flashnode httpStatAll 127.0.0.1:17510
```

## 清除指定flashnode中某个卷的缓存

```bash
./cfs-cli flashnode httpEvict 127.0.0.1:17510 flash_cache
```


## 启用/禁用flashnode

```bash
./cfs-cli flashnode set 127.0.0.1:17510 true
./cfs-cli flashnode set 127.0.0.1:17510 false 
```

## 删除flashnode

```bash
./cfs-cli flashnode remove 127.0.0.1:17510
```

## 创建fg

```bash
./cfs-cli flashgroup create
```

## 设置flashgroup  active

```bash
./cfs-cli flashgroup set 25 true
```

## flashgroup添加flashnode

```bash
./cfs-cli flashgroup nodeAdd 13 --zone-name=flashcache --addr="127.0.0.1:17510"
```

## 查看flashgroup列表

```bash
./cfs-cli flashgroup list
```

## 查看flashgroup关系表

```bash
./cfs-cli flashgroup graph
```