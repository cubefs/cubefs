# Integration with Kubernetes

CubeFS has developed a CSI plugin based on the [Container Storage Interface (CSI)](https://kubernetes-csi.github.io/docs/) interface specification to support cloud storage in Kubernetes clusters.

Currently, there are two different branches of the CSI plugin:
- Master branch: compatible with CSI protocol versions after 1.0.0
- Csi-spec-v0.3.0 branch: compatible with CSI protocol versions before 0.3.0

The reason for these two branches is that the official k8s is not compatible with version 1.0.0 and previous versions of 0.\* when evolving the CSI protocol.

According to the compatibility of the CSI protocol, if the user's k8s version is before v1.13, please use the csi-spec-v0.3.0 branch code, and k8s v1.13 and later versions can directly use the master branch.

::: warning Note
The code of the csi-spec-v0.3.0 branch is basically frozen and not updated. New CSI features will be developed on the master branch in the future.
:::

| CSI Protocol | Kubernetes Compatible Version           |
|--------------|-----------------------------------------|
| v0.3.0       | Versions before v1.13, such as v1.12    |
| v1.0.0       | Versions v1.13 and later, such as v1.15 |

Before deploying CSI, please set up the CubeFS cluster. Please refer to the [Quick Start](../deploy/requirement.md) section for documentation.

::: tip Note
The deployment steps in this article are based on the master branch.
:::

## Adding Labels

Before deploying CSI, you need to label the cluster nodes. The CSI container will be stationed based on the k8s node label. For example, if the k8s cluster node has the following labels, the CSI container will be automatically deployed on this machine. If there is no such label, the CSI container will not be stationed.

Users execute the following command on the corresponding k8s node according to their own situation.

``` bash
$ kubectl label node <nodename> component.cubefs.io/csi=enabled
```

## CSI Deployment

There are two ways to deploy CSI, one is to use helm for automated deployment, and the other is to manually deploy according to the steps in order. Using helm can reduce some manual operations, and users can choose the deployment method according to their own situation.

### CSI Deployment (Non-Helm)

#### Get the Plugin Source Code

The required deployment files are basically in the deploy directory of the csi repository.

``` bash
$ git clone https://github.com/cubefs/cubefs-csi.git
$ cd cubefs-csi
```

#### Create CSI RBAC

The content of cfs-rbac.yaml does not need to be modified. It mainly declares the role of the CSI component and the corresponding permissions, so execute the following command directly:

``` bash
$ kubectl apply -f deploy/cfs-rbac.yaml
```

#### Create StorageClass

The content of storageclass.yaml is as follows (you need to modify the masterAddr):

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

`masterAddr`: The address of the master component in the CubeFS cluster. Here, you need to modify it to the actual deployed master IP address, in the format of `ip:port`. If there are multiple addresses, separate them with commas in the format of `ip1:port1,ip2:port2` (domain names can also be used).

After modification, execute the following command:

``` bash
$ kubectl apply -f deploy/storageclass.yaml
```

#### Deploy CSI Controller

The CSI controller component will only deploy one pod in the entire cluster. After the deployment is completed, you can use the command `kubectl get pod -n cubefs` to find that only one controller is in the RUNING state.

``` bash
$ kubectl apply -f deploy/csi-controller-deployment.yaml
```

#### Deploy CSI Node

The CSI node will deploy multiple pods, and the number of pods is the same as the number of k8s nodes labeled.

``` bash
$ kubectl apply -f deploy/csi-node-daemonset.yaml
```

### CSI Deployment (Helm)

#### Install Helm

To use helm for deployment, you need to install helm first. [Refer to the official documentation](https://helm.sh/docs/intro/install/).

#### Download cubefs-helm

``` bash
$ git clone https://github.com/cubefs/cubefs-helm.git
$ cd cubefs-helm
```

#### Write Helm Deployment Files

Write a helm deployment yaml file related to csi in cubefs-helm: cubefs-csi-helm.yaml

``` bash
$ touch cubefs-csi-helm.yaml
```

The content of cubefs-csi-helm.yaml is as follows:

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
    # If you want to connect to an external CubeFS cluster, set this parameter. Otherwise, it can be ignored.
    masterAddr: ""
    otherParameters:
```

- `masterAddr` is the address of the corresponding CubeFS cluster. If there are multiple addresses, separate them with commas in English in the format of `ip1:port1,ip2:port2`.
- The `csi.kubeletPath` parameter is the default kubelet path, which can be modified as needed.
- The resource limit in `csi.resources.requests/limits` is set to 2 CPUs and 4G memory by default, which can also be modified as needed.
- `image.csi_driver` is the csi component image. If it is a user's own compiled image, it can be modified as needed.
- The other csi images should be kept as close to the default as possible, unless the user understands the running principle of CSI well.

#### Deployment

Next, execute the helm installation command to install CSI in the cubefs-helm directory:

``` bash
$ helm upgrade --install cubefs ./cubefs -f ./cubefs-csi-helm.yaml -n cubefs --create-namespace
```

## Check if the CSI component is deployed

After the above CSI deployment operations are completed, you need to check the deployment status of CSI. The command is as follows:

``` bash
$ kubectl get pod -n cubefs
```

Under normal circumstances, you can see that there is only one `controller` component container, and the number of `node` component containers is the same as the number of k8s nodes labeled, and their status is `RUNNING`.

If you find that the status of a pod is not `RUNNING`, first check the error situation of this problematic `pod`, the command is as follows:

``` bash
$ kubectl describe pod -o wide -n cubefs pod_name
```

Troubleshoot based on the above error information. If the error information is limited, you can proceed to the next step: go to the host where the `pod` is located and execute the following command.

``` bash
$ docker ps -a | grep pod_name
```

The above command is used to filter the container information related to this `pod`. Then find the problematic container and view its log output:

``` bash
$ docker logs problematic_docker_container_id
```

Troubleshoot based on the container's log output. If the log shows words like "no permission", it is likely that the `rbac` permission has not been generated properly. Just deploy the `CSI RBAC` again.

## Create PVC

After the CSI component is deployed, you can create a `PVC` for verification. The content of pvc.yaml is as follows:

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

`storageClassName` needs to be consistent with the `name` attribute in the `metadata` of the StorageClass created just now. This will create a storage volume based on the parameters defined in `cubefs-sc`.

When users write pvc yaml, pay attention to the following parameters:

- `metadata.name`: The name of the pvc, which can be modified as needed. The pvc name is unique in the same namespace, and two names that are the same are not allowed.
- `metadata.namespace`: The namespace where the pvc is located, which can be modified as needed.
- `spec.resources.request.storage`: The capacity of the pvc.
- `storageClassName`: This is the name of the storage class. If you want to know which `storageclass` the current cluster has, you can use the command `kubectl get sc` to view it.

With the pvc yaml, you can create the PVC with the following command:

``` bash
$ kubectl create -f pvc.yaml
```

After the command is executed, you can use the command `kubectl get pvc -n namespace` to check the status of the corresponding pvc. Pending means it is waiting, and Bound means it has been created successfully.

If the status of the PVC has been Pending, you can use the command to check the reason:

``` bash
$ kubectl describe pvc -n namespace pvc_name
```

If the error message is not obvious or there is no error, you can use the `kubectl logs` command to first check the error message of the `csi-provisioner` container in the csi controller pod. `csi-provisioner` is the intermediate bridge between k8s and the csi driver, and many information can be viewed in the logs here.

If the log of `csi-provisioner` still cannot show the specific problem, you can use the `kubectl exec` command to view the log of the `cfs-driver` container in the csi controller pod. Its log is located under `/cfs/logs` in the container.

The reason why `kubectl logs` command cannot be used here is that the log of `cfs-driver` is not printed to standard output, while the logs of other sidecar containers similar to `csi-provisioner` are printed to standard output, so you can use the `kubectl logs` command to view them.

## Mount PVC

With the PVC, you can mount it to the specified directory in the application. The sample yaml is as follows:

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

The above yaml indicates that a PVC named cubefs-pvc is mounted to /usr/share/nginx/html in the cfs-csi-demo container.

The above PVC and cfs-csi-demo usage examples, please refer to the [example source file](https://github.com/cubefs/cubefs-csi/tree/master/examples).

## Common Issues

### Compile the Latest CSI Image

Download the cubefs-csi source code and execute the following command:

``` bash
$ make image
```

### View Files in PVC

If the PVC has been mounted for container use, you can enter the corresponding mount location of the container to view it; or mount the volume corresponding to the PVC to a directory on the host using the cubefs client for viewing. Please refer to the [documentation](./file.md) for how to use the client.

### Configuration for k8s v1.13 and Earlier Versions

kube-apiserver startup parameters:

``` bash
--feature-gates=CSIPersistentVolume=true,MountPropagation=true
--runtime-config=api/all
```

kube-controller-manager startup parameters:

``` bash
--feature-gates=CSIPersistentVolume=true
```

kubelet startup parameters:

``` bash
--feature-gates=CSIPersistentVolume=true,MountPropagation=true,KubeletPluginsWatcher=true
--enable-controller-attach-detach=true
```

### View the CubeFS Volume Corresponding to the PVC

The PVC is bound to a PV, and the name of the bound PV is the name of the underlying cubefs volume, usually starting with pvc-. The PV corresponding to the PVC can be viewed directly using the command `kubectl get pvc -n namespace`.

### CubeFS Volume Management

Please refer to the [cubefs cli tool usage guide](../maintenance/tool.md).