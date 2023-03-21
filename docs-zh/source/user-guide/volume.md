# 创建卷

**卷**是逻辑上的概念，由多个元数据和数据分片组成
- 从客户端的角度看，卷可以被看作是可被容器访问的文件系统实例，一个卷可以在多个容器中挂载，使得文件可以被不同客户端同时访问。
- 从对象存储的角度来看，一个卷对应着一个bucket。

下面将介绍如何创建不同模式的卷，更多卷操作可以参考[卷管理命令](../maintenance/admin-api/master/volume.md)。

## 创建副本卷

请求master服务接口，创建卷

- name，卷名
- capacity，卷配额，如果配额用完之后需要扩容
- owner，卷的所属用户，如果集群中没有与该卷的Owner同名的用户时，会自动创建一个用户ID为Owner的用户

``` bash
curl -v "http://127.0.0.1:17010/admin/createVol?name=test&capacity=100&owner=cfs"
```

::: tip 提示
扩容示例 
`curl -v "http://127.0.0.1:17010/vol/expand?name=test&authKey=57f0162b2303be3449c7008484b0d306&capacity=200"`，
其中authKey为volume owner字符串的MD5值。
:::

## 创建纠删码卷

如果有部署Blobstore纠删码子系统，可以创建纠删码卷以存放冷数据

```bash
curl -v 'http://127.0.0.1:17010/admin/createVol?name=test-cold&capacity=100&owner=cfs&volType=1'
```

- name，卷名
- capacity，卷配额，如果配额用完之后需要扩容
- owner，卷的所属用户
- volType，卷类型，0为副本卷，1为纠删码卷，默认为0

## 开启多级缓存

可以创建纠删码卷，并设置多副本为读写缓存

**缓存读数据**
```bash
curl -v 'http://127.0.0.1:17010/admin/createVol?name=test-cold&capacity=100&owner=cfs&volType=1&cacheCap=10&cacheAction=1'
```

- name，卷名
- capacity，卷配额，如果配额用完之后需要扩容
- owner，卷的所属用户
- volType，卷类型，0为副本卷，1为纠删码卷，默认为0
- cacheCap，缓存大小，单位GB
- cacheAction，缓存类型，0表示不缓存，1表示缓存读，2表示缓存读写，默认0

**缓存读、写数据**

```bash
curl -v 'http://127.0.0.1:17010/admin/createVol?name=test-cold&capacity=100&owner=cfs&volType=1&cacheCap=10&cacheAction=2'
```

- name，卷名
- capacity，卷配额，如果配额用完之后需要扩容
- owner，卷的所属用户
- volType，卷类型，0为副本卷，1为纠删码卷，默认为0
- cacheCap，缓存大小，单位GB
- cacheAction，缓存类型，0表示不缓存，1表示缓存读，2表示缓存读写，默认0