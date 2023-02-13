CSI插件支持
==============================
Cubefs基于Container Storage Interface (CSI) (https://kubernetes-csi.github.io/docs/) 接口规范开发了CSI插件，以支持在Kubernetes集群中使用云存储。

.. csv-table::
  :header: "cfscsi", "kubernetes"

  "v0.3.0", "v1.12"
  "v1.0.0", "v1.15"

Kubernetes v1.12
-------------------

在Kubernetes v1.12集群中使用CubeFS CSI。

Kubernetes配置要求
^^^^^^^^^^^^^^^^^^^^^^^^

为了在kubernetes集群中部署cfscsi插件，kubernetes集群需要满足以下配置。

kube-apiserver启动参数:

.. code-block:: bash

    --feature-gates=CSIPersistentVolume=true,MountPropagation=true
    --runtime-config=api/all

kube-controller-manager启动参数:

.. code-block:: bash

    --feature-gates=CSIPersistentVolume=true

kubelet启动参数:

.. code-block:: bash

    --feature-gates=CSIPersistentVolume=true,MountPropagation=true,KubeletPluginsWatcher=true
    --enable-controller-attach-detach=true

获取插件源码及脚本
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    $ git clone -b csi-spec-v0.3.0 https://github.com/cubeFS/cubefs-csi.git
    $ cd cubefs-csi

拉取官方CSI镜像
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    docker pull quay.io/k8scsi/csi-attacher:v0.3.0
    docker pull quay.io/k8scsi/driver-registrar:v0.3.0
    docker pull quay.io/k8scsi/csi-provisioner:v0.3.0

获取cfscsi镜像
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

有两种方式可以实现。

* 从docker.io拉取镜像

.. code-block:: bash

    docker pull docker.io/cubefs/cfscsi:v0.3.0

* 根据源码编译镜像

.. code-block:: bash

    make cfs-image

创建kubeconfig
^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    kubectl create configmap kubecfg --from-file=pkg/cfs/deploy/kubernetes/kubecfg

创建RBAC和StorageClass
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    kubectl apply -f pkg/cfs/deploy/dynamic_provision/cfs-rbac.yaml
    kubectl apply -f pkg/cfs/deploy/dynamic_provision/cfs-sc.yaml

部署cfscsi插件
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* 方式一：将cfscsi ControllerServer和NodeServer绑定在同一个sidecar容器

修改 ``pkg/cfs/deploy/dynamic_provision/sidecar/cfs-sidecar.yaml`` 文件，将环境变量 ``MASTER_ADDRESS`` 设置为Cubefs的实际Master地址，将 ``<NodeServer IP>`` 设置为kubernetes集群任意IP（如果被调度到该IP的pod需要动态挂载Cubefs网盘，则必须为该IP部署cfscsi sidecar容器）。

.. code-block:: bash

    kubectl apply -f pkg/cfs/deploy/dynamic_provision/sidecar/cfs-sidecar.yaml

* 方式二：将cfscsi插件ControllerServer和NodeServer分别部署为statefulset和daemonset（推荐此种）

修改 ``pkg/cfs/deploy/dynamic_provision/independent`` 文件夹下 ``csi-controller-statefulset.yaml`` 和 ``csi-node-daemonset.yaml`` 文件，将环境变量 ``MASTER_ADDRESS`` 设置为Cubefs的实际Master地址 ，将 ``<ControllerServer IP>`` 设置为kubernetes集群中任意节点IP。

为Kubernetes集群中的节点添加标签，拥有 ``csi-role=controller`` 标签的节点为ControllerServer。拥有 ``csi-role=node`` 标签的节点为NodeServer，也可以删除 ``csi-node-daemonset.yaml`` 文件中的 ``nodeSelector`` ，这样kubernetes集群所有节点均为NodeServer。

.. code-block:: bash

    kubectl label nodes <ControllerServer IP> csi-role=controller
    kubectl label nodes <NodeServer IP1> csi-role=node
    kubectl label nodes <NodeServer IP2> csi-role=node
    ...

部署：

.. code-block:: bash

    kubectl apply -f pkg/cfs/deploy/dynamic_provision/independent/csi-controller-statefulset.yaml
    kubectl apply -f pkg/cfs/deploy/dynamic_provision/independent/csi-node-daemonset.yaml

创建PVC
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    kubectl apply -f pkg/cfs/deploy/dynamic_provision/cfs-pvc.yaml

nginx动态挂载Cubefs示例
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    docker pull nginx
    kubectl apply -f pkg/cfs/deploy/dynamic_provision/pv-pod.yaml



Kubernetes v1.15+
--------------------

在Kubernetes v1.15+ 集群中使用CubeFS CSI。

Kubernetes配置要求
^^^^^^^^^^^^^^^^^^^^^^^^

为了在kubernetes集群中部署cfscsi插件，kubernetes api-server需要设置 ``--allow-privileged=true``。

从Kubernetes 1.13.0开始， ``allow-privileged=true`` 成为kubelet启动的默认值。参考CSI官方github: https://kubernetes-csi.github.io/docs/deploying.html

准备一个CubeFS集群
^^^^^^^^^^^^^^^^^

CubeFS集群部署可参考 https://github.com/cubeFS/cubefs.

获取插件源码及脚本
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    $ git clone https://github.com/cubeFS/cubefs-csi.git
    $ cd cubefs-csi

CubeFS CSI插件部署
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    $ kubectl apply -f deploy/csi-controller-deployment.yaml
    $ kubectl apply -f deploy/csi-node-daemonset.yaml

创建StorageClass
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

    kind: StorageClass
    apiVersion: storage.k8s.io/v1
    metadata:
      name: cubefs-sc
    provisioner: csi.cubefs.com
    reclaimPolicy: Delete
    parameters:
      masterAddr: "master-service.cubefs.svc.cluster.local:8080"
      owner: "csi-user"
      consulAddr: "consul-service.cubefs.svc.cluster.local:8500"
      logLevel: "debug"

参数 ``provisioner`` 指定插件名称。这里设置为 ``csi.cubefs.com`` , kubernetes会将PVC的创建、挂载等任务调度给 ``deploy/csi-controller-deployment.yaml`` 和 ``deploy/csi-node-daemonset.yaml`` 中定义的CubeFS CSI插件去处理。


.. csv-table::
   :header: "参数名", "描述"

   "MasterAddr", "CubeFS Master地址"
   "consulAddr", "监控地址"

.. code-block:: bash

    $ kubectl create -f deploy/storageclass-cubefs.yaml

创建PVC
^^^^^^^^^^^^^

.. code-block:: yaml

    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: cubefs-pvc
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 5Gi
      storageClassName: cubefs-sc

``storageClassName`` 需要和刚刚创建的StorageClass的 ``metadata`` 中的name属性保持一致。这样就会根据 ``cubefs-sc`` 中定义的参数来创建存储卷。

.. code-block:: bash

    $ kubectl create -f examples/pvc.yaml

在应用中挂载PVC
^^^^^^^^^^^^^

接下来就可以在你自己的应用中挂载刚刚创建的PVC到指定目录了。

.. code-block:: yaml

    ...
    spec:
      containers:
        - name: csi-demo
          image: alpine:3.10.3
          volumeMounts:
            - name: mypvc
              mountPath: /data
      volumes:
        - name: mypvc
          persistentVolumeClaim:
            claimName: cubefs-pvc
    ···

.. code-block:: bash

    $ kubectl create -f examples/deployment.yaml


