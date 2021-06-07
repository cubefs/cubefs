# Ansible Tools to Run a ChubaoFS Cluster for latest compiled binary
  Use Ansible to deploy many hosts based on yum install method from jd.com.
  To deploy multiple hosts, yum install method using Ansible from jd.com is very valuable.
  We need to modify it to use latest compiled result files.
To deploy role monitor, we need to download a 80MB file cfs-monitor.tar.gz (which including consul,grafana,prometheus binary program) from  github to build/install/src.
Also change default port to avoid confict:
  console 80 to 17080，consul 8500 to 17085 (using 17081~17086), grafana dashboard 80 to 17088，prometheus 9090 to 17090.
  exporterPort 9500 to 17091~17095.




## Build and package

```bash
git clone https://github.com/chubaofs/chubaofs.git
cd chubaofs
export version=2.4.0
./build.sh
```
After build successful,download cfs-monitor.tar.gz to build/install/src
```bash
cd build
mkdir install/src/
\cp -r bin/* install/src/
\cp -r ../docker/monitor/* install/template/
tar -czvf ChubaoFS-$version.tar.gz install
```
## install cluster to multiple nodes

Install chrony or ntp to sync time on every node，and make sure timezone the same.
Modify iplist . 
Mount multiple disks(avaliable capacity must > 50GB) under directory /data/cfs/.
install ansible  using yum or apt on one linux node:
```bash
    yum install -y ansible
    apt install -y ansible
```


```bash
# 
## make sure sshd config:

 vi /etc/ssh/sshd_config 
 PermitRootLogin yes
 StrictModes no
 UseDNS no


tar -xzvf ChubaoFS-$version.tar.gz .
cd install

ansible -i iplist all -m command  -a "mkdir -p /data/cfs/disk0"
# mount multiple disks(avaliable capacity must > 50GB) under directory /data/cfs/ 
ansible -i iplist all -m command  -a "ls -al /data/cfs"
ansible -i iplist all -m command  -a "df -h"

# Will install deps such as: fuse and libzstd.
./install.sh -r all
```
## Start ChubaoFS cluster 
```bash
ansible -i iplist master -a "/cfs/master/start.sh" 
ansible -i iplist metanode -a "/cfs/metanode/start.sh" 
ansible -i iplist datanode -a "/cfs/datanode/start.sh"
ansible -i iplist objectnode -a "/cfs/objectnode/start.sh" 
 
ansible -i iplist monitor -a "/cfs/monitor/start.sh"
ansible -i iplist monitor -a "/cfs/monitor/prometheus/start.sh" 

ansible -i iplist console -a "/cfs/console/start.sh"
ansible -i iplist client -a "/cfs/client/start.sh" 

# verify cfs process
ansible -i iplist all -m command  -a "ls -al /cfs"
ansible -i iplist all -m shell -a " ps -ef | grep cfs"

```
##

## Stop ChubaoFS cluster
```bash
ansible -i iplist datanode -a "/cfs/datanode/stop.sh"
ansible -i iplist metanode -a "/cfs/metanode/stop.sh" 
ansible -i iplist master -a "/cfs/master/stop.sh" 
ansible -i iplist objectnode -a "/cfs/objectnode/stop.sh" 
 
ansible -i iplist monitor -a "/cfs/monitor/stop.sh" 
ansible -i iplist console -a "/cfs/console/stop.sh"
ansible -i iplist client -a "/cfs/client/stop.sh" 

ansible -i iplist client -a "fusermount -u /cfs/mountpoint " 
ansible -i iplist all -m shell -a " ps -ef | grep cfs"

```
## Archive cfs-monitor.tar.gz
```bash
 cd build/install/src/
 # rpm -ivh --force cfs-monitor-latest-el7.x86_64.rpm 
 tar czvf cfs-monitor.tar.gz monitor/
 
```

##  Dump cluster data then reinstall,all data under /cfs will be removed!
```bash
ansible -i iplist all -m command  -a "\rm -rf /cfs"
ansible -i iplist 10.2.3.96 -m command  -a "\rm -rf /cfs"
```
## Use web console and grafana
The web console's url is http://[console's ip address]:17080 ，login with root/ChubaoFSRoot.  
The grafana  port 17088，login with admin/123456, then click following Dashboards --> Manage --> chubaofs
If grafana has blank chart,you can restart it on the  monitor node to let prometheus run!
```bash
 cd /cfs/monitor 
 ./stop.sh
 ./start.sh
 ps -ef|grep monitor
```
## Change default port to avoid confict:
    console 80 to 17080，consul 8500 to 17085, grafana dashboard 80 to  17088，prometheus 9090 to  17090
    exporterPort 9500 to 17091~17095

### consul
```bash
vi /cfs/monitor/consul/conf/basic.json 
# default port: 8500 8600 8400 8301 8302 8300

{
 
    "ports": { 
        "http": 17085 , 
        "dns": 17081, 
        "grpc": 17082, 
        "serf_lan": 17083,    
        "serf_wan": 17084, 
        "server": 17086
 
    }
}
```

###     grafana
```bash
vi /cfs/monitor/grafana/conf/defaults.ini

    # The http port to use
    http_port = 17088


vi /cfs/monitor/grafana/provisioning/datasources/datasource.yml

    url: http://192.168.0.100:17090

# must use absolute path, then start.sh add one line: cd ${RootPath}
vi /cfs/monitor/grafana/conf/defaults.ini

    [dashboards.json]
    enabled = true
    path = provisioning/dashboards


vi /cfs/monitor/grafana/provisioning/dashboards/dashboard.yml 

    options:
        path: provisioning/dashboards

```
### prometheus
```bash
vi /cfs/monitor/prometheus/bin/start.sh

#systemctl start prometheus.service
nohup $BASEDIR/prometheus --config.file=$BASEDIR/prometheus.yml --web.enable-admin-api --web.listen-address=:17090 &> nohup.out &


vi /cfs/monitor/prometheus/prometheus.yml
 consul_sd_configs:
     - server: 192.168.0.101:17085
 
```