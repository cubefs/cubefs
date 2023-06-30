# Integration with Hadoop

CubeFS is compatible with the Hadoop FileSystem interface protocol, and users can use CubeFS to replace the Hadoop file system (HDFS).

This chapter describes the installation and configuration process of CubeFS in the Hadoop storage ecosystem.

## Dependencies

- Set up an accessible CubeFS cluster and need to [create a volume (file system)](./volume.md) in advance.
- The SDK dynamic library [libcfs.so](https://github.com/cubefs/cubefs/tree/master/libsdk) provided by CubeFS for Java calling.
- The CubeFS plugin [cfs-hadoop.jar](https://github.com/cubefs/cubefs-hadoop.git) for Hadoop.
- The third-party dependency package jna-5.4.0.jar (minimum supported version 4.0, recommended 5.4 or above) for the cfs-hadoop.jar plugin.

::: warning Note
The current CubeFS Hadoop does not support file permission management of HDFS.
:::

## Compile Resource Package

### Compile libcfs.so

```shell
git clone https://github.com/cubefs/cubefs.git
cd cubefs
make libsdk
```

::: warning Note
Since the compiled package depends on glibc, the glibc version of the compilation environment and the runtime environment must be consistent.
:::

### Compile cfs-hadoop.jar

```shell
git clone https://github.com/cubefs/cubefs-hadoop.git
mvn package -Dmaven.test.skip=true
```

## Installation

The above dependency packages must be installed on each node of the Hadoop cluster and must be found in the `CLASSPATH`.

Each participating node in the Hadoop cluster must install the native CubeFS Hadoop client.

| Resource Package Name | Installation Path                    |
|-----------------------|--------------------------------------|
| cfs-hadoop.jar        | $HADOOP_HOME/share/hadoop/common/lib |
| jna-5.4.0.jar         | $HADOOP_HOME/share/hadoop/common/lib |
| libcfs.so             | $HADOOP_HOME/lib/native              |

## Modifying Configuration

After correctly placing the above resource packages, you need to make simple modifications to the core-site.xml configuration file, whose path is: `$HADOOP_HOME/etc/hadoop/core-site.xml`.

Add the following configuration content to `core-site.xml`:

```yuml
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

Parameter Description:

| Property                | Value                      | Notes                                                                                                            |
|:------------------------|:---------------------------|:-----------------------------------------------------------------------------------------------------------------|
| fs.cfs.impl             | io.cubefs.CubefsFileSystem | Specify the storage implementation class with scheme `cfs://`                                                    |
| cfs.master.address      |                            | CubeFS master address, can be `ip+port` format, `ip:port`, `ip:port`, `ip:port`, or domain name                  |
| cfs.log.dir             | /tmp/cfs-access-log        | Log path                                                                                                         |
| cfs.log.level           | INFO                       | Log level                                                                                                        |
| cfs.access.key          |                            | AccessKey of the user to which the CubeFS file system belongs                                                    |
| cfs.secret.key          |                            | SecretKey of the user to which the CubeFS file system belongs                                                    |
| cfs.min.buffersize      | 8MB                        | Write buffer size. The default value is recommended for replica volumes, and 64MB is recommended for EC volumes. |
| cfs.min.read.buffersize | 128KB                      | Read buffer size. The default value is recommended for replica volumes, and 4MB is recommended for EC volumes.   |

## Environment Verification

After the configuration is completed, you can use the `ls` command to verify whether the configuration is successful:

```shell
hadoop fs -ls cfs://volumename/
```

If there is no error message, the configuration is successful.

## Configuration for Other Big Data Components

- **Hive scenario**: Copy the jar package and modify the configuration on all nodemanagers, hive servers, and metastores in the Yarn cluster.
- **Spark scenario**: Copy the jar package and modify the configuration on all execution nodes (Yarn nodemanagers) in the Spark computing cluster and the Spark client.
- **Presto scenario**: Copy the jar package and modify the configuration on all worker nodes and coordinator nodes in Presto.
- **Flink scenario**: Copy the jar package and modify the configuration on all JobManager nodes in Flink.

### HDFS Shell, YARN, Hive

```shell
cp cfs-hadoop.jar $HADOOP_HOME/share/hadoop/common/lib
cp jna-5.4.0 $HADOOP_HOME/share/hadoop/common/lib 
cp libcfs.so $HADOOP_HOME/lib/native
```

::: tip Note
After the configuration is changed for hive server, hive metastore, presto worker, and coordinator, the service process needs to be restarted on the server to take effect.
:::

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

## Common Issues

The most common problem after deployment is the lack of packages. Check whether the resource package is copied to the corresponding location according to the installation steps. The common error messages are as follows:

### Missing cfs-hadoop.jar

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

### Missing libcfs.so

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


### Missing jna.jar

```java
Exception in thread "main" java.lang.NoClassDefFoundError: com/sun/jna/Library   
 at java.lang.ClassLoader.defineClass1(Native Method)   
 at java.lang.ClassLoader.defineClass(ClassLoader.java:763)   
 at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)   
 at java.net.URLClassLoader.defineClass(URLClassLoader.java:468)   
 at java.net.URLClassLoader.access$100(URLClassLoader.java:74)`
```



### volume name is required

he volume name cannot contain underscores.