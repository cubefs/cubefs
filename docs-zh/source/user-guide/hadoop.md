# 对接Hadoop

Cubefs兼容Hadoop FileSystem接口协议，用户可以使用CubeFS来替换Hadoop 文件系统（ HDFS ）。本章描述了 Cubefs在 Hadoop 存储生态的安装和配置过程。

## 依赖项

- 搭建一个可访问的CubeFS集群，并需要提前[创建卷（文件系统）](volume.md)。
- CubeFS 提供的可供Java调用的sdk动态库[libcfs.so](https://github.com/cubefs/cubefs/tree/master/libsdk)
- Hadoop 的 CubeFS插件[cfs-hadoop.jar](https://github.com/cubefs/cubefs-hadoop.git)
- cfs-hadoop.jar插件的第三方依赖包jna-5.4.0.jar(最小支持版本4.0，建议5.4以上)

> 约束
>
> 1. 当前CubeFS Hadoop不支持HDFS的文件权限管理

## 编译资源包

### 编译libcfs.so

```shell
git clone https://github.com/cubefs/cubefs.git
cd libsdk
sh build.sh
```

> 只能在linux环境下编译，由于编译的包依赖 glibc，因此需要编译环境和运行环境的glibc版本一致。

### 编译cfs-hadoop.jar

```shell
git clone https://github.com/cubefs/cubefs-hadoop.git
mvn package -Dmaven.test.skip=true
```

## 安装

以上依赖包必须安装到 Hadoop 集群的各节点、且必须能从 `CLASSPATH` 里找到。

Hadoop 集群内的各参与节点都必须安装原生 CubeFS Hadoop客户端。

| 资源包名称     | 安装路径                             |
| -------------- | ------------------------------------ |
| cfs-hadoop.jar | $HADOOP_HOME/share/hadoop/common/lib |
| jna-5.4.0.jar  | $HADOOP_HOME/share/hadoop/common/lib |
| libcfs.so      | $HADOOP_HOME/lib/native              |

## 修改配置

正确放置上述资源包之后，需要对core-site.xml配置文件进行简单修改，其路径为：$HADOOP_HOME/etc/hadoop/core-site.xml。在core-site.xml中添加以下配置内容：

```xml
<property>
	<name>fs.cfs.impl</name>
	<value>io.cubefs.CubefsFileSystem</value>
</property>

<property>
	<name>cfs.master.address</name>
	<value>your.master.address[ip:port,ip:port,ip:port]</value>
</property>

<property>
	<name>cfs.log.dir</name>   
	<value>your.log.dir[/tmp/cfs-access-log]</value>
</property>

<property>
	<name>cfs.log.level</name> 
	<value>INFO</value>
</property>

<property>
    <name>cfs.access.key</name>
    <value>your.access.key</value>
</property>

<property>
    <name>cfs.secret.key</name>
    <value>your.secret.key</value>
</property>

<property>
    <name>cfs.min.buffersize</name>
    <value>67108864</value>
</property>
<property>
    <name>cfs.min.read.buffersize</name>
    <value>4194304</value>
</property>
<property>
```

配置参数说明：

| Property                | Value                      | Notes                                                        |
| :---------------------- | :------------------------- | :----------------------------------------------------------- |
| fs.cfs.impl             | io.cubefs.CubefsFileSystem | 指定scheme为cfs://的存储实现类                               |
| cfs.master.address      |                            | CubeFS master地址，可以是ip+port格式，ip:port,ip:port,ip:port，也可以是域名 |
| cfs.log.dir             | /tmp/cfs-access-log        | 日志路径                                                     |
| cfs.log.level           | INFO                       | 日志级别                                                     |
| cfs.access.key          |                            | CubeFS 文件系统的所属用户的 accessKey                        |
| cfs.secret.key          |                            | CubeFS 文件系统的所属用户的 secretKey                        |
| cfs.min.buffersize      | 8MB                        | 写缓存区大小,对于副本卷按默认值就行，EC卷建议64MB            |
| cfs.min.read.buffersize | 128KB                      | 读缓冲区大小,对于副本卷按默认值就行，EC卷建议4MB             |

## 环境验证

配置完成后，可以通过ls命令简单验证配置是否成功：

```shell
hadoop fs -ls cfs://volumename/
```

没有错误信息既表示成功

## 其他大数据组件配置

> **Hive的场景**：在yarn集群的所有nodemanager,hive server, metastore进行拷贝jar包和修改配置的动作
>
> **Spark的场景**：在Spark计算集群的所有执行节点（Yarn nodemanager）以及Spark客户端进行拷贝jar包和修改配置的动作；
>
> **Presto的场景**：在Presto的所有worker节点和Coordinator节点进行拷贝jar包和修改配置的动作；
>
> **Flink的场景**:  在Flink的所有JobManager节点进行拷贝jar包和修改配置的动作；

### HDFS Shell、YARN、Hive

```shell
cp cfs-hadoop.jar $HADOOP_HOME/share/hadoop/common/lib
cp jna-5.4.0 $HADOOP_HOME/share/hadoop/common/lib 
cp libcfs.so $HADOOP_HOME/lib/native
```

> hive server, hive metastore, presto worker和Coordinator配置变更后需要服务端进行服务进程重启后才能生效

### Spark

```shell
cp cfs-hadoop.jar $SPARK_HOME/jars/ 
cp libcfs.so $SPARK_HOME/jars/ 
cp jna-5.4.0 $SPARK_HOME/jars/
```

### Presto/Trino

```shell
cp cfs-hadoop.jar $PRESTO_HOME/plugin/hive-hadoop2 
cp libcfs.so $PRESTO_HOME/plugin/hive-hadoop2 
cp jna-5.4.0.jar $PRESTO_HOME/plugin/hive-hadoop2 

ln -s $PRESTO_HOME/plugin/hive-hadoop2/libcfs.so /usr/lib
sudo ldconfig
```

### Flink

```shell
cp cfs-hadoop.jar $FLINK_HOME/lib 
cp jna-5.4.0.jar $FLINK_HOME/lib 
cp libcfs.so $FLINK_HOME/lib
ln -s $FLINK_HOME/lib/libcfs.so /usr/lib
sudo ldconfig
```



### Iceberg

```shell
cp cfs-hadoop.jar $TRINO_HOME/plugin/iceberg 
cp jna-5.4.0.jar $TRINO_HOME/plugin/iceberg
```

## 常见问题

部署之后最常见的问题在于缺包，缺包问题对照安装步骤检查资源包是否拷贝到对应位置，常见报错信息如下：

### 缺少cfs-hadoop.jar报错信息

```java
java.lang.RuntimeException: java.lang.ClassNotFoundException: Class io.chubaofs.CubeFSFileSystem not found 
 at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2349)   
 at org.apache.hadoop.fs.FileSystem.getFileSystemClass(FileSystem.java:2790)   
 at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:2810)    
 at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:98)   
 at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:2853)  
 at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:2835)   
 at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:387)
```

### 缺少libcfs.so报错信息

```java
Suppressed: java.lang.UnsatisfiedLinkError: libcfs.so: cannot open shared object file: 
No such file or directory    
at com.sun.jna.Native.open(Native Method)    
at com.sun.jna.NativeLibrary.loadLibrary(NativeLibrary.java:191)    
... 21 more Suppressed: java.lang.UnsatisfiedLinkError: libcfs.so: 
cannot open shared object file: No such file or directory    
at com.sun.jna.Native.open(Native Method)    
at com.sun.jna.NativeLibrary.loadLibrary(NativeLibrary.java:204)  
  ... 21 more
```


### 缺少jna.jar报错信息

```java
Exception in thread "main" java.lang.NoClassDefFoundError: com/sun/jna/Library   
 at java.lang.ClassLoader.defineClass1(Native Method)   
 at java.lang.ClassLoader.defineClass(ClassLoader.java:763)   
 at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)   
 at java.net.URLClassLoader.defineClass(URLClassLoader.java:468)   
 at java.net.URLClassLoader.access$100(URLClassLoader.java:74)`
```



### 报错信息：volume name is required.

  volume名称不能包含“_”划线