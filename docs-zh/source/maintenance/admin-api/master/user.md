# 用户管理命令

## 创建用户

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"id":"testuser","pwd":"12345","type":3}' "http://10.196.59.198:17010/user/create"
```

在集群中创建用户，用于访问对象存储功能。在集群启动时，会自动创建root用户(type值为0x1)。

CubeFS将卷的 **Owner** 字段看作一个用户ID。例如，创建卷时Owner取值为
*testuser* 的话，则该卷自动归为用户 *testuser* 的名下。

如果创建卷时不存在与Owner取值相同的用户ID，则创建卷时会自动创建用户ID取值为Owner的用户。

body参数列表

| 参数   | 类型     | 描述                  | 取值范围                  | 是否必需 | 默认值          |
|------|--------|---------------------|-----------------------|------|--------------|
| id   | string | 用户ID                | 由字母、数字及下划线组成，不超过21个字符 | 是    | 无            |
| pwd  | string | 用户密码                | 无限制                   | 否    | `CubeFSUser` |
| ak   | string | 用于对象存储功能的Access Key | 由16位字母及数字组成           | 否    | 系统随机生成       |
| sk   | string | 用于对象存储功能的Secret Key | 由32位字母及数字组成           | 否    | 系统随机生成       |
| type | int    | 用户类型                | 2（管理员）/3（普通用户）        | 是    | 无            |

## 删除用户

``` bash
curl -v "http://10.196.59.198:17010/user/delete?user=testuser"
```

在集群中删除指定的用户。

参数列表

| 参数   | 类型     | 描述   |
|------|--------|------|
| user | string | 用户ID |

## 查询用户信息

展示的用户基本信息,包括用户ID、Access Key、Secret
Key、名下的卷列表、其他用户授予的权限列表、用户类型、创建时间等。

用户信息中 **policy** 字段表示该用户拥有权限的卷，其中 **own_vols**
表示所有者是该用户的卷， **authorized_vols**
表示其他用户授权给该用户的卷，及所拥有的权限限制。

有以下两种方式获取：

### 通过用户ID查询

``` bash
curl -v "http://10.196.59.198:17010/user/info?user=testuser" | python -m json.tool
```

参数列表

| 参数   | 类型     | 描述   |
|------|--------|------|
| user | string | 用户ID |


### 通过Access Key查询

``` bash
curl -v "http://10.196.59.198:17010/user/akInfo?ak=0123456789123456" | python -m json.tool
```

参数列表

| 参数  | 类型     | 描述                |
|-----|--------|-------------------|
| ak  | string | 该用户的16位Access Key |

响应示例

``` json
{
     "user_id": "testuser",
     "access_key": "gDcKaBvqky4g8StT",
     "secret_key": "ZVY5RHlrnOrCjImW9S3MajtYZyxSegcf",
     "policy": {
         "own_vols": ["vol1"],
         "authorized_vols": {
             "ltptest": [
                 "perm:builtin:ReadOnly",
                 "perm:custom:PutObjectAction"
             ]
         }
     },
     "user_type": 3,
     "create_time": "2020-05-11 09:25:04"
}
```

## 查询用户列表

``` bash
curl -v "http://10.196.59.198:17010/user/list?keywords=test" | python -m json.tool
```

查询集群中包含某关键字的所有用户的信息。

参数列表

| 参数       | 类型     | 描述                |
|----------|--------|-------------------|
| keywords | string | 查询用户ID包含此关键字的用户信息 |

## 更新用户信息

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","access_key":"KzuIVYCFqvu0b3Rd","secret_key":"iaawlCchJeeuGSnmFW72J2oDqLlSqvA5","type":3}' "http://10.196.59.198:17010/user/update"
```

更新指定UserID的用户信息，可修改的内容包括Access Key、Secret
Key和用户类型。

参数列表

| 参数         | 类型     | 描述               | 是否必需 |
|------------|--------|------------------|------|
| user_id    | string | 待更新信息的用户ID       | 是    |
| access_key | string | 更新后的Access Key取值 | 否    |
| secret_key | string | 更新后的Secret Key取值 | 否    |
| type       | int    | 更新后的用户类型         | 否    |

## 用户授权

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","volume":"vol","policy":["perm:builtin:ReadOnly","perm:custom:PutObjectAction"]}' "http://10.196.59.198:17010/user/updatePolicy"
```

更新指定用户对于某个卷的访问权限。 **policy** 的取值有三类：

-   授予只读或读写权限，取值为 `perm:builtin:ReadOnly` 或
    `perm:builtin:Writable` ；
-   授予指定操作的权限，格式为 `action:oss:XXX` ，以 *GetObject*
    操作为例，policy取值为 **action:oss:GetObject** ；
-   授予自定义权限，格式为 `perm:custom:XXX` ，其中 *XXX* 由用户自定义。

指定权限后，用户在使用对象存储功能时，仅能在指定权限范围内对卷进行访问。如果该用户已有对此卷的权限设置，则本操作会覆盖原有权限。

参数列表

| 参数      | 类型           | 描述         | 是否必需 |
|---------|--------------|------------|------|
| user_id | string       | 待设置权限的用户ID | 是    |
| volume  | string       | 待设置权限的卷名   | 是    |
| policy  | string slice | 待设置的权限     | 是    |

## 移除用户权限

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"user_id":"testuser","volume":"vol"}' "http://10.196.59.198:17010/user/removePolicy"
```

移除指定用户对于某个卷的所有权限。

参数列表

| 参数      | 类型     | 描述         | 是否必需 |
|---------|--------|------------|------|
| user_id | string | 待删除权限的用户ID | 是    |
| volume  | string | 待删除权限的卷名   | 是    |

## 转交卷

``` bash
curl -H "Content-Type:application/json" -X POST --data '{"volume":"vol","user_src":"user1","user_dst":"user2","force":true}' "http://10.196.59.198:17010/user/transferVol"
```

转交指定卷的所有权。此操作将指定卷从源用户名下移除，并添加至目标用户名下；同时，卷结构中的Owner字段的取值也将更新为目标用户的用户ID。

参数列表

| 参数       | 类型     | 描述                                                         | 是否必需 |
|----------|--------|------------------------------------------------------------|------|
| volume   | string | 待转交权限的卷名                                                   | 是    |
| user_src | string | 该卷原来的所有者，必须与卷的Owner字段原取值相同                                 | 是    |
| user_dst | string | 转交权限后的目标用户ID                                               | 是    |
| force    | bool   | 是否强制转交卷。如果该值设为true，即使user_src的取值与卷的Owner取值不等，也会将卷变更至目标用户名下 | 否    |
