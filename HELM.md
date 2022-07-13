## Helm chart to Run a CubeFS Cluster in Kubernetes 

The [cubefs-helm](https://github.com/cubefs/cubefs-helm) repository can help you deploy CubeFS cluster quickly in containers orchestrated by kubernetes.
Kubernetes 1.12+ and Helm 3 are required. cubefs-helm has already integrated CubeFS CSI plugin

### Download cubefs-helm

```
$ git clone https://github.com/cubefs/cubefs-helm
$ cd cubefs-helm
```

### Copy kubeconfig file
CubeFS CSI driver will use client-go to connect the Kubernetes API Server. First you need to copy the kubeconfig file to `cubefs-helm/cubefs/config/` directory, and rename to kubeconfig

```
$ cp ~/.kube/config cubefs/config/kubeconfig
```

### Create configuration yaml file

Create a `cubefs.yaml` file, and put it in a user-defined path. Suppose this is where we put it.

```
$ cat ~/cubefs.yaml 
```

``` yaml
path:
  data: /cubefs/data
  log: /cubefs/log

datanode:
  disks:
    - /data0:21474836480
    - /data1:21474836480 

metanode:
  total_mem: "26843545600"

provisioner:
  kubelet_path: /var/lib/kubelet
```

> Note that `cubefs-helm/cubefs/values.yaml` shows all the config parameters of CubeFS.
> The parameters `path.data` and `path.log` are used to store server data and logs, respectively.

### Add labels to Kubernetes node

You should tag each Kubernetes node with the appropriate labels accorindly for server node and CSI node of CubeFS.

```
kubectl label node <nodename> cubefs-master=enabled
kubectl label node <nodename> cubefs-metanode=enabled
kubectl label node <nodename> cubefs-datanode=enabled
kubectl label node <nodename> cubefs-csi-node=enabled
```

### Deploy CubeFS cluster
```
$ helm install cubefs ./cubefs -f ~/cubefs.yaml
```

