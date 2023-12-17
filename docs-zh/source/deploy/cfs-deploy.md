## 环境
centos 

未支持Ubuntu
## 依赖
fuse
## 编译

- 整体编译

方法一：make

方法二：./build/build.sh

- 单独编译deploy

./build/build.sh deploy

**note：**在已经整体编译结束之后，单独编译可用于cfs-deploy的测试环节

## 使用流程

1. 系统环境：centos

2. 安装依赖

`yum install fuse`

3. 导入环境变量

`export CUBEFS=/path/to/cubefs`

4. 进入build/bin，执行./cfs-deploy

![image.png](./pic/deploy1.png)

5. 加载配置文件

`./cfs-deploy cluster config -f ../../config.yaml `

deploy/conf目录下生成配置文件

![image.png](./pic/deploy2.png)

6. 初始化配置文件中的集群

`./cfs-deploy cluster init`
![image.png](./pic/deploy3.png)

7. 启动服务

`./cfs-deploy start -a`

![image.png](./pic/deploy4.png)

配置master节点信息

./cfs-cli config set --addr 192.168.128.128:17010

8. 集群信息查看

`./cfs-deploy cluster info`
![image.png](./pic/deploy5.png)

9. 创建卷

`./cfs-cli volume create ltptest ltptest`

10. 查看用户信息

`./cfs-cli user list`

将ACCESS KEY 和 SECRET KEY 填入对应的client.json配置文件中，该配置文件位于docker/conf下面

11. 挂载卷

 `./cfs-client -f -c /work/cubefs-master/docker/conf/client.json &`

`df `查看

note：如果没有安装fuse，会失败

![image.png](./pic/deploy6.png)

12. 先kill挂载卷的后台进程，再删除卷，然后再停止服务

## 配置文件
### 示例
```yaml
global:
  ssh_port: 22
  container_image: docker.io/cubefs/cbfs-base:1.0-golang-1.17.13
  data_dir: /data
  variable:
    target: 0.0.1


master: 
  config:
    listen: 17010
    prof: 17020
    data_dir: /data

metanode:
  config:
    listen: 17210
    prof: 17220
    data_dir: /data

datanode:
  config:
    listen: 17310
    prof: 17320
    data_dir: /data


deplopy_hosts_list:
  master:
    hosts:
      - 10.1.0.44
      - 10.1.0.45
      - 10.1.0.46
  metanode:
    hosts:
      - 10.1.0.44
      - 10.1.0.45
      - 10.1.0.46
  datanode:
    - hosts: 10.1.0.44
      disk:
        - path: /data/disk0
          size: 10737418240
    - hosts: 10.1.0.45
      disk:
        - path: /data/disk0
          size: 10737418240
    - hosts: 10.1.0.46
      disk:
        - path: /data/disk0
          size: 10737418240


```
### 说明
data_dir 优先读取局部自定义的值。


## 命令
![image.png](./pic/deploy7.png)
### start
![image.png](./pic/deploy8.png)
`./cfs-deploy start master`
![image.png](./pic/deploy9.png)
`./cfs-deploy start metanode`
![image.png](./pic/deploy10.png)
`./cfs-deploy  start metanode  --ip 10.1.0.44`
![image.png](./pic/deploy11.png)
`./cfs-deploy start datanode`
![image.png](./pic/deploy12.png)
`./cfs-deploy  start datanode  --ip 10.1.0.44`
![image.png](./pic/deploy13.png)
`./cfs-deploy  start -a`
![image.png](./pic/deploy14.png)

`./cfs-deploy start test --disk /data`

开启单机测试，会启动以docker-compose方式编排的cubefs服务。

### stop
![image.png](./pic/deploy15.png)
`./cfs-deploy  stop master`
![image.png](./pic/deploy16.png)
`./cfs-deploy  stop metanode`
![image.png](./pic/deploy17.png)
` ./cfs-deploy  stop metanode  --ip 10.1.0.44`
![image.png](./pic/deploy18.png)
`./cfs-deploy  stop datanode`
![image.png](./pic/deploy19.png)
`./cfs-deploy  stop datanode  --ip 10.1.0.44`
![image.png](./pic/deploy20.png)
`./cfs-deploy  stop  -a`
![image.png](./pic/deploy21.png)

`./cfs-deploy stop test`

停止测试集群
### restart
![image.png](./pic/deploy22.png)
`./cfs-deploy restart -a`
![image.png](./pic/deploy23.png)
`./cfs-deploy restart master`
![image.png](./pic/deploy24.png)
`./cfs-deploy restart metanode`
![image.png](./pic/deploy25.png)

`./cfs-deploy restart metanode --ip 10.1.0.44`
![image.png](./pic/deploy26.png)
`./cfs-deploy restart datanode`
![image.png](./pic/deploy27.png)
.`/cfs-deploy  restart datanode --ip 10.1.0.44`
![image.png](./pic/deploy28.png)
### cluster
![image.png](./pic/deploy29.png)
`./cfs-deploy cluster config -f [filename]`
![image.png](./pic/deploy30.png)

`./cfs-deploy cluster init`
![image.png](./pic/deploy31.png)

`./cfs-deploy cluster info`
![image.png](./pic/deploy32.png)
` ./cfs-deploy cluster clear`

容器还在运行的情况下，无法清除集群中的镜像

![image.png](./pic/deploy33.png)
![image.png](./pic/deploy34.png)



