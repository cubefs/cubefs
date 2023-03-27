# 对接 Kubernetes

CubeFS 基于 [Container Storage Interface (CSI)](https://kubernetes-csi.github.io/docs/) 接口规范开发了 CSI 插件，以支持在 Kubernetes
集群中使用云存储。

目前 CSI 插件有两个不同的分支：
- master 分支：兼容 CSI协议 1.0.0 之后的版本
- csi-spec-v0.3.0 分支：兼容 CSI 协议 0.3.0 之前的版本

这里有两个分支的原因是 k8s 官方在演进 CSI 协议时 1.0.0 版本与之前的 0.\* 版本不兼容。

根据 CSI 协议的兼容情况，用户的 k8s 版本如果是 v1.13 之前的版本，请采用 csi-spec-v0.3.0 分支代码，而 k8s v1.13 及以后的版本直接采用 master 分支。

::: warning 注意
csi-spec-v0.3.0 分支的代码基本上处于冻结不更新状态，往后 CSI 新功能特性都会在 master 分支上进行演进。
:::

| csi 协议 | kubernetes 兼容版本      |
|--------|----------------------|
| v0.3.0 | v1.13之前的版本，例如 v1.12  |
| v1.0.0 | v1.13及之后的版本，例如 v1.15 |

CSI 部署之前，请搭建好 CubeFS 集群，文档请参考[快速入门章节](../deploy/requirement.md)。

::: tip 提示
本文部署步骤基于 master 分支进行。
:::

## 添加标签

部署 CSI 之前需要给集群节点打标签。CSI 容器会根据 k8s 节点标签进行驻守，比如 k8s 集群节点打了以下标签，则 CSI 容器会自动部署在这台机器上，如果没有这个标签，则 CSI 容器不会驻守。

用户根据自身情况给对应的 k8s节点执行如下命令。

``` bash
$ kubectl label node <nodename> component.cubefs.io/csi=enabled
```

## CSI 部署

CSI 部署有两种方式，一种是用 helm 自动化部署，另一种是人工根据步骤按顺序部署。使用 helm 方式能减少一部分人工操作，用户可以根据自身情况选择部署方式。

### CSI 部署（非 helm）

#### 获取插件源码

需要用到的部署文件基本都在 csi 仓库的 deploy 目录下。

``` bash
$ git clone https://github.com/cubefs/cubefs-csi.git
$ cd cubefs-csi
```

#### 创建 CSI RBAC

cfs-rbac.yaml 内容基本不用动，里面主要声明 CSI 组件的角色和对应的权限，所以直接执行如下命令：

``` bash
$ kubectl apply -f deploy/cfs-rbac.yaml
```

#### 创建 StorageClass

storageclass.yaml 内容如下（需要修改一下 masterAddr）：

``` yaml
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: cfs-sc
provisioner: csi.cubefs.com
allowVolumeExpansion: true
reclaimPolicy: Delete
parameters:
  # Resource manager IP address or URL
  masterAddr: "master-service.cubefs.svc.cluster.local:17010"
  # Owner name as authentication
  owner: "csiuser"
```

`masterAddr`：cubefs 集群中 master 组件的地址。这里需要修改为实际部署的 master ip 地址，格式为`ip:port`，如果有多个地址，则用英文逗号隔开，格式为 `ip1:port1,ip2:
port2`（使用域名也是可以的）。

修改好后执行如下命令：

``` bash
$ kubectl apply -f deploy/storageclass.yaml
```

#### 部署CSI controller

csi controller 组件整个集群只会部署一个 pod，部署完成后使用命令 `kubectl get pod -n cubefs` 可以发现只有一个 controller 处于 RUNING。

``` bash
$ kubectl apply -f deploy/csi-controller-deployment.yaml
```

#### 部署CSI node

csi node 会部署多个 pod，数量个数与 k8s node 打标签的数量一致。

``` bash
$ kubectl apply -f deploy/csi-node-daemonset.yaml
```

### CSI 部署（helm）

#### 安装 helm

使用 helm 部署需要先安装 helm，[参考官方文档](https://helm.sh/docs/intro/install/)。

#### 下载 cubefs-helm

``` bash
$ git clone https://github.com/cubefs/cubefs-helm.git
$ cd cubefs-helm
```

#### 编写 helm 部署文件

在 cubefs-helm 编写一个 csi 相关的 helm 部署 yaml 文件： cubefs-csi-helm.yaml

``` bash
$ touch cubefs-csi-helm.yaml
```

cubefs-csi-helm.yaml 内容如下：

``` yaml
component:
  master: false
  datanode: false
  metanode: false
  objectnode: false
  client: false
  csi: true
  monitor: false
  ingress: false

image:
  # CSI related images
  csi_driver: ghcr.io/cubefs/cfs-csi-driver:3.2.0.150.0
  csi_provisioner: registry.k8s.io/sig-storage/csi-provisioner:v2.2.2
  csi_attacher: registry.k8s.io/sig-storage/csi-attacher:v3.4.0
  csi_resizer: registry.k8s.io/sig-storage/csi-resizer:v1.3.0
  driver_registrar: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.5.0

csi:
  driverName: csi.cubefs.com
  logLevel: error
  # If you changed the default kubelet home path, this
  # value needs to be modified accordingly
  kubeletPath: /var/lib/kubelet
  controller:
    tolerations: [ ]
    nodeSelector:
      "component.cubefs.io/csi": "enabled"
  node:
    tolerations: [ ]
    nodeSelector:
      "component.cubefs.io/csi": "enabled"
    resources:
      enabled: false
      requests:
        memory: "4048Mi"
        cpu: "2000m"
      limits:
        memory: "4048Mi"
        cpu: "2000m"
  storageClass:
    # Whether automatically set this StorageClass to default volume provisioner
    setToDefault: true
    # StorageClass reclaim policy, 'Delete' or 'Retain' is supported
    reclaimPolicy: "Delete"
    # Override the master address parameter to connect to external cluster
    # 如果是要连接外部 CubeFS 集群的设置该参数，否则可以忽略
    masterAddr: ""
    otherParameters:
```

- masterAddr 为对应的 CubeFS 集群的地址，多个地址则采用英文逗号方式隔开，格式为`ip1:port1,ip2:port2`。
- `csi.kubeletPath` 参数为默认的 kubelet 路径，可以根据需要进行修改。
- `csi.resources.requests/limits` 里面的资源限制默认为 2 cpu 4G 内存，这里也可以根据需要进行修改。
- `image.csi_driver` 为 csi 组件镜像，如果是用户自己编译的镜像，可以按需修改。
- 其他几个 csi 镜像尽量基本保持不动，除非用户对 CSI 运行原理比较了解。

#### 部署

接下来在 cubefs-helm 根目录下执行 helm 安装 CSI 命令：

``` bash
$ helm upgrade --install cubefs ./cubefs -f ./cubefs-csi-helm.yaml -n cubefs --create-namespace
```

## 检查 CSI 组件是否部署完成

当以上 CSI 部署操作完成后，需要对 CSI 的部署情况进行检查，命令如下：

``` bash
$ kubectl get pod -n cubefs
```

正常情况下，可以看到`controller`组件容器只有一个，`node`组件容器的数量与 k8s 节点打标签的数量一致，并且状态都为`RUNNING`。

如果发现有 pod 的状态不为`RUNNING`，则先查看这个有问题的`pod`报错情况，命令如下：

``` bash
$ kubectl describe pod -o wide -n cubefs pod名称
```

根据上面的报错信息进行排查，如果报错信息有限，可以进行下一步排查：去到`pod`所在的宿主机上，执行如下命令。

``` bash
$ docker ps -a | grep pod名称
```

上面这条命令是用来过滤与这个`pod`相关的容器情况，接着找到有问题的容器查看它的日志输出：

``` bash
$ docker logs 有问题的docker容器id
```

根据容器的日志输出来排查问题。如果日志出现没有权限之类的字眼，则很可能是`rbac`的权限没有生成好，直接再次部署一次`CSI RBAC`即可。

## 创建PVC

CSI 组件部署好之后可以创建`PVC`进行验证，pvc.yaml 内容如下：

``` yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: cubefs-pvc
  namespace: default
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
  storageClassName: cubefs-sc
```

`storageClassName` 需要和刚刚创建的StorageClass的 `metadata` 中的name属性保持一致。这样就会根据 `cubefs-sc` 中定义的参数来创建存储卷。

用户编写 pvc yaml 主要注意一下参数：

- `metadata.name`：pvc 的名称，可以按需修改，同一个 namespace 下 pvc 名称是唯一的，不允许有两个相同的名称。
- `metadata.namespace`：pvc 所在的命名空间，按需修改
- `spec.resources.request.storage`：pvc 容量大小。
- `storageClassName`：这是 storage class 的名称。如果想知道当前集群有哪些`storageclass`，可以通过命令 `kubectl get sc` 来查看。

有了 pvc yaml 则可以通过如下命令将 PVC 创建出来：

``` bash
$ kubectl create -f pvc.yaml
```

执行命令完成后，可以通过命令 `kubectl get pvc -n 命名空间` 来查看对应 pvc 的状态，Pending 代表正在等待，Bound 代表创建成功。

如果 PVC 的状态一直处于 Pending，可以通过命令查看原因：

``` bash
$ kubectl describe pvc -n 命名空间 PVC名称
```

如果报错消息不明显或者看不出错误，则可以使用 `kubectl logs` 相关命令先查看csi controller pod里面的 csi-provisioner 容器的报错信息，`csi-provisioner`是 k8s 与 csi
driver 的中间桥梁，很多信息都可以在这里的日志查看。

如果 csi-provisioner 的日志还看不出具体问题，则使用 `kubectl exec` 相关命令查看 csi controller pod 里面的 cfs-driver 容器的日志，它的日志放在容器里面的`/cfs/logs`下。

这里不能使用 `Kubectl logs` 相关命令是因为 cfs-driver 的日志并不是打印到标准输出，而其它几个类似 csi-provisioner 的 sidecar
容器的日志是打印到标准输出的，所以可以使用 `kubectl logs` 相关命令查看。

## 挂载PVC

有了 PVC 则接下来就可以在应用中挂载到指定目录了，示例 yaml 如下：

``` yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cfs-csi-demo
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cfs-csi-demo-pod
  template:
    metadata:
      labels:
        app: cfs-csi-demo-pod
    spec:
      nodeSelector:
        cubefs-csi-node: enabled
      containers:
        - name: cfs-csi-demo
          image: nginx:1.17.9
          imagePullPolicy: "IfNotPresent"
          ports:
            - containerPort: 80
              name: "http-server"
          volumeMounts:
            - mountPath: "/usr/share/nginx/html"
              mountPropagation: HostToContainer
              name: mypvc
      volumes:
        - name: mypvc
          persistentVolumeClaim:
            claimName: cubefs-pvc
```

上面的 yaml 表示将一个名称为 cubefs-pvc 的 PVC 挂载到 cfs-csi-demo 容器里面的 /usr/share/nginx/html 下。

以上的 pvc 和 cfs-csi-demo 使用范例， yaml[示例源文件请参考](https://github.com/cubefs/cubefs-csi/tree/master/examples)

## 常见问题

### 编译最新 CSI 镜像

下载 cubefs-csi 源码，执行如下命令：

``` bash
$ make image
```

### 查看 PVC 里面的文件

如果 PVC 已经挂载给容器使用，则可以进入容器对应的挂载位置进行查看；或者将 PVC 对应的卷使用 cubefs client 挂载到宿主机某个目录上进行查看， client
使用方式请[参考文档](./file.md)。

### k8s v1.13 之前版本配置

kube-apiserver启动参数:

``` bash
--feature-gates=CSIPersistentVolume=true,MountPropagation=true
--runtime-config=api/all
```

kube-controller-manager启动参数:

``` bash
--feature-gates=CSIPersistentVolume=true
```

kubelet启动参数:

``` bash
--feature-gates=CSIPersistentVolume=true,MountPropagation=true,KubeletPluginsWatcher=true
--enable-controller-attach-detach=true
```

### 查看 PVC 对应的 CubeFS 卷

PVC 会与一个 PV 进行绑定，而这个绑定的 PV 名称就是底下 cubefs 卷的名称，通常是以 pvc- 开头。PVC 对应的 PV 直接使用命令 `kubectl get pvc -n 命名空间` 可以看到。

### CubeFS卷管理

可以参考 [cubefs cli 工具使用指南](../maintenance/tool.md)
