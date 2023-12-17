## Environment

centos 

Unsupported for Ubuntu

## Dependence

fuse


## Compile

- Overall compilation

method1：make

method2：./build/build.sh

- Compile separately deploy

./build/build.sh deploy

**note：**After the overall compilation is completed, separate compilation can be used for the testing phase of cfs deploy

## Use Flow

1. System environment：centos

2. Installation dependencies

`yum install fuse`

3. Import environment variables

`export CUBEFS=/path/to/cubefs`

4. Enter build/bin，execute ./cfs-deploy

![image.png](./pic/deploy1.png)

5. load profile

`./cfs-deploy cluster config -f ../../config.yaml `

Generate configuration files in the 'deploy/conf' directory

![image.png](./pic/deploy2.png)

6. Initialize the cluster in the configuration file

`./cfs-deploy cluster init`

![image.png](./pic/deploy3.png)

7. 启动服务

`./cfs-deploy start -a`

![image.png](./pic/deploy4.png)

Configure master node information

./cfs-cli config set --addr 192.168.128.128:17010

8. Cluster information viewing

`./cfs-deploy cluster info`
![image.png](./pic/deploy5.png)

9. Create volume

`./cfs-cli volume create ltptest ltptest`

10. Viewing User Information

`./cfs-cli user list`

Fill the ACCESS KEY and SECRET KEY into the corresponding client.json configuration file, which is located under 'docker/conf'

11. Mount volume

 `./cfs-client -f -c /work/cubefs-master/docker/conf/client.json &`

`df `view 

note：If fuse is not installed, it will fail

![image.png](./pic/deploy6.png)

12. Kill the backend process for mounting the volume first, then delete the volume, and then stop the service

## profile
### example
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
### illustrate

data_dir:Prioritize reading locally customized values.


## Command
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

Starting a standalone test will start the cubefs service orchestrated in Docker Compose mode.

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

Stop testing cluster

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

Unable to clear mirrors in the cluster while the container is still running

![image.png](./pic/deploy33.png)
![image.png](./pic/deploy34.png)



