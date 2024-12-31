# GUI使用
# 一、 安装
## 1.1 编译
下载 CubeFS 源码后，用 make 命令编译代码。编译成功后，在 bin 目录里面可生成可执行文件 cfs-gui，配置文件模板 config.yml，前端静态文件目录 dist。
```
## clone code
git clone https://github.com/cubefs/cubefs-dashboard.git

## build
make

## product
ll bin/
total 26256
-rwxr-xr-x 1 root root 26876984 Jul 27 17:26 cfs-gui
-rw-r--r-- 1 root root     1009 Jul 27 17:26 config.yml
drwxr-xr-x 6 root root     4096 Jul 27 17:27 dist
```

## 1.2 配置
配置文件里面数据库的密码不能是明文，需要加密。加密方式：
```
./cfs-gui  -e  password
```
#示例
```
./cfs-gui  -e  111111
v6NEbIgCdJAYDqLiDE9UMA==
```

配置文件 config.yml 说明：
```
server:
  port: 6007  //服务监听端口
  mode: dev   //运行模式 test dev prod
  static_resource:
    enable: true  //是否开启静态资源
    relative_path: /portal  //静态资源前缀
    root_path: ./dist  //静态资源目录  默认在运行的目录下
prefix:
  api: /api/cubefs //后端接口前缀
mysql:
  host: xxxxxx   //mysql 读写域名
  port: xxxxxx   //mysql 读写域名的端口
  slaveHost: xxxxxx   //mysql 只读域名，没有的话可以填读写域名
  slavePort: xxxxxx   //mysql 只读域名的端口，没有的话可以填读写域名的端口
  user: xxxxxx   //mysql  用户名
  password: xxxxxx   //mysql  密码（此处不能是明文密码，需要先加密）
  database: xxxxxx   //mysql  数据库名
  maxIdleConn: 20
  MaxOpenConn: 300
```

## 1.3 运行
在 bin 目录下，执行 ./cfs-gui -c config.yml
![image](./pic/gui/1.3.png)

# 二、 使用方法
## 2.1 注册、修改密码、登录
## 2.1.1 url 拼接
```
http://{{ ip }}:{{ server.port }}/{{ server.static_resource.relative_path }}
```
示例：
如果部署的机器为 192.168.10.10 且 server.port 和 server.static_resource.relative_path 用默认配置
```
http://192.168.10.10:6007/portal
```
### 2.1.2 注册账号
账号：只能包含数字、大写字母、小写字母、下划线
密码：密码长度大于等于8小于等于16
密码只能包含数字、大写字母、小写字母、特殊字符（~!@#$%^&*_.?），且至少两种类型以上
此处仅是注册账号，账号的权限需要有 root 角色的账号授予
![image](./pic/gui/2.1.1_1.png)
![image](./pic/gui/2.1.1_2.png)
![image](./pic/gui/2.1.1_3.png)

### 2.1.3 修改密码
![image](./pic/gui/2.1.2_1.png)
![image](./pic/gui/2.1.2_2.png)

### 2.1.4 登录
![image](./pic/gui/2.1.3.png)

## 2.2 上架集群、修改集群
### 2.2.1 上架集群
![image](./pic/gui/2.2.1_1.png)
只有三副本卷、无纠删码（EC）冷卷
![image](./pic/gui/2.2.1_2.png)
有EC冷卷
![image](./pic/gui/2.2.1_3.png)

### 2.2.2  修改集群
![image](./pic/gui/2.2.2_1.png)
![image](./pic/gui/2.2.2_2.png)

## 2.3 进入集群
点击集群名进入集群
![image](./pic/gui/2.3.png)

## 2.4 集群详情

### 2.4.1 集群详情主页
![image](./pic/gui/2.4.1.png)

### 2.4.2 租户管理
进入“租户管理”
给租户授权卷的权限在卷的页面操作，不在此处
![image](./pic/gui/2.4.2_1.png)
创建租户：必须以字母开头,可有下划线,数字,字母
![image](./pic/gui/2.4.2_2.png)

# 2.5 卷管理

### 2.5.1 首页
![image](./pic/gui/2.5.1.png)

### 2.5.2 创建卷
创建卷的时候，会给卷选择一个owner租户，此owner租户有卷的读写权限
![image](./pic/gui/2.5.2_1.png)
![image](./pic/gui/2.5.2_2.png)

### 2.5.3 查看卷详情
![image](./pic/gui/2.5.3.png)

### 2.5.4 多副本数据分区（DP）详情
![image](./pic/gui/2.5.4_1.png)
![image](./pic/gui/2.5.4_2.png)

### 2.5.5 元数据分区（MP）详情
![image](./pic/gui/2.5.5_1.png)
![image](./pic/gui/2.5.5_2.png)

### 2.5.6 卷扩容
![image](./pic/gui/2.5.6_1.png)
![image](./pic/gui/2.5.6_2.png)

### 2.5.7 卷缩容
![image](./pic/gui/2.5.7_1.png)
![image](./pic/gui/2.5.7_2.png)

### 2.5.8 卷授权
卷授权能把卷授权给 owner 租户外的其他用户
![image](./pic/gui/2.5.8_1.png)
![image](./pic/gui/2.5.8_2.png)

## 2.6 数据管理

### 2.6.1 多副本数据管理
![image](./pic/gui/2.6.1.png)

#### 2.6.1.1 DP详情
![image](./pic/gui/2.6.1.1_1.png)
![image](./pic/gui/2.6.1.1_2.png)

### 2.6.2 纠删码数据管理

#### 2.6.2.1 没冷卷时页面置灰
当创建集群时没有关联冷卷的话，“纠删码”页面是置灰的，不能点击
![image](./pic/gui/2.6.2.1.png)

#### 2.6.2.2 条带组状态
![image](./pic/gui/2.6.2.2.png)

#### 2.6.2.3 正在写入的条带组
![image](./pic/gui/2.6.2.3_1.png)
![image](./pic/gui/2.6.2.3_2.png)

#### 2.6.2.4 条带组详情
点击条带组的“详情”进入条带组详情
条带组详情展示组成条带的数据块信息和相关的磁盘和机器
![image](./pic/gui/2.6.2.4_1.png)
组成条带的数据块信息
![image](./pic/gui/2.6.2.4_2.png)
数据块所在的磁盘信息
![image](./pic/gui/2.6.2.4_3.png)

## 2.7 元数据管理

### 2.7.1 首页
![image](./pic/gui/2.7.1.png)

### 2.7.2 元数据分区（MP）详情
![image](./pic/gui/2.7.2_1.png)
![image](./pic/gui/2.7.2_2.png)

## 2.8 节点管理
### 2.8.1 三副本节点
#### 2.8.1.1 首页
![image](./pic/gui/2.8.1.1.png)

#### 2.8.1.2 节点下线（自动迁移）
节点下线即将节点里面的所有 DP 自动迁移（目的节点由 CubeFS 自动选择）到其他节点
![image](./pic/gui/2.8.1.2.png)

#### 2.8.1.3 节点迁移
节点迁移即将节点里面的所有 DP 迁移到其他节点（由用户指定）
![image](./pic/gui/2.8.1.3.png)

#### 2.8.1.4 磁盘下线
点击“节点地址”进入节点详情
磁盘下线即将磁盘里面的所有 DP 自动迁移到其他的磁盘
![image](./pic/gui/2.8.1.4.png)

#### 2.8.1.5 分区下线
点击“分区数”进入节点详情
分区下线即将数据分区（DP）从当前磁盘自动迁移到其他磁盘
![image](./pic/gui/2.8.1.5.png)

### 2.8.2 纠删码节点

#### 2.8.2.1 首页
![image](./pic/gui/2.8.2.1.png)

#### 2.8.2.2 机器切读写/只读
操作按钮在“更多操作”的下面
![image](./pic/gui/2.8.2.2.png)

#### 2.8.2.3 机器详情
![image](./pic/gui/2.8.2.3_1.png)
![image](./pic/gui/2.8.2.3_2.png)

### 2.8.3 元数据节点

#### 2.8.3.1 首页
![image](./pic/gui/2.8.3.1.png)

#### 2.8.3.2 节点下线
节点下线即将节点里面的所有 MP 自动迁移（目的节点由 CubeFS 自动选择）到其他节点
![image](./pic/gui/2.8.3.2.png)

#### 2.8.3.3 节点迁移
节点迁移即将节点里面的所有 MP 迁移到其他节点（由用户指定）
![image](./pic/gui/2.8.3.3.png)

#### 2.8.3.4 分区下线
点击“分区数”进入分区详情
分区下线即将元数据分区（MP）从当前机器自动迁移到其他机器
![image](./pic/gui/2.8.3.4.png)

## 2.9 文件管理
文件管理是为了方便在管控台查看、上传、下载文件

### 2.9.1 文件管理使用前提
文件管理是通过S3模块实现的，所以需要配一些S3相关的配置

#### 2.9.1.1 集群配置S3模块的域名或者IP
上架或者修改集群里面，要配置“s3 endpoint”
![image](./pic/gui/2.9.1.1_1.png)
当没有配置“s3 endpoint”时，点击卷名会报错，不能进去卷的文件管理
![image](./pic/gui/2.9.1.1_2.png)

#### 2.9.1.2 给卷配置跨域
配置“s3 endpoint”后，点击卷名进入卷的文件管理
点击“跨域设置”里面的“新增规则”
如果只是为了页面上传下载，可以参照“提示”里面配置
当没有正确配置 GUI 用的跨域时，“文件管理”置灰，不能操作
![image](./pic/gui/2.9.1.2_1.png)
![image](./pic/gui/2.9.1.2_2.png)

### 2.9.2 上传文件、下载文件、新建文件夹
正确配置“s3 endpoint”和跨域后，点击“文件管理”就能执行上传文件、下载文件、新建文件夹操作
![image](./pic/gui/2.9.2.png)

## 2.10 权限管理

### 2.10.1 权限管理说明

#### 2.10.1.1 GUI账号与CubeFS集群租户区分
此处的账号仅仅是登录 GUI 平台使用，跟 CubeFS 集群里通过 ak、sk 访问卷里面文件的租户无关

#### 2.10.1.2 初始账户
GUI 后端部署好之后，会生成一个初始有最高权限的账号
admin/Admin@1234
第一次登录时，需要修改密码

#### 2.10.1.2 角色
每个角色对应一组权限，初始有三个默认角色，用户可以新增自定义角色
三个默认角：
admin_role：拥有所有权限
operator_role：定位为运维角色，相比 admin_role 削减了权限管理相关权限
viewer_role：定位为查看角色，相比 operator_role 削减了运维操作相关权限

#### 2.10.1.3 用户
一个用户可以关联多个角色，用户拥有对应角色的权限

#### 2.10.1.4 用户申请方式
a. 在登录界面申请
b. 有“创建用户”权限的账户在“用户管理”->“添加用户”里面创建

### 2.10.2 用户管理

#### 2.10.2.1 添加用户
点击“用户管理”->“添加用户”
也可以顺带给用户关联角色，需要权限：用户授权
![image](./pic/gui/2.10.2.1_1.png)
![image](./pic/gui/2.10.2.1_2.png)

#### 2.10.2.2 编辑用户、修改密码、删除用户
需要权限：更新用户、修改用户密码、删除用户
![image](./pic/gui/2.10.2.2.png)

### 2.10.3 角色管理
#### 2.10.3.1 添加角色
点击“角色管理”-->“添加角色”，输入“角色code”、“角色名称”并在“角色权限”里面选中需要的权限
需要权限：添加角色
![image](./pic/gui/2.10.3.1_1.png)
![image](./pic/gui/2.10.3.1_2.png)

#### 2.10.3.2 修改角色
需要权限：修改角色权限
![image](./pic/gui/2.10.3.2.png)

## 2.11 集群事件
### 2.11.1 纠删码后台任务
![image](./pic/gui/2.11.1.png)


