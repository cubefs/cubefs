<div align="center"><font size="100">cube_torch 使用文档</font></div>

#一、cube_torch 是什么？

##1.1 cube_torch 模块简介：
在大量图片训练场景中,由于本地存储空间限制,不得不采用分布式文件系统存储训练数据,但这将使训练数据读写变慢。业界的分布式文件系统对小文件读写效率很低。所以开发此软件,针对大量小文件训练场景,提高pytorch框架下实现很高的训练速度。

由于pytorch 迭代速度较快，并且cube_torch 和cubefs进行深度绑定。 因此该软件并不侵入pytorch的代码。 用户使用较为简单，并不需要改变用户的训练代码

##1.2 cube_torch 技术亮点:

*  训练速度高: 针对大量的小文件训练场景，让AI的训练速度和本地NVME速度一样快。 
*  简单易用: 用户不需要改变自己的训练代码，仅需要加入`import cube_torch`的字样，并设置几个环境变量即可进行加速。
*  适用范围广: 适用于所有的pytorch的训练模型。 


#二、cube_torch加速原理

  当pytorch训练框架需要从分布式文件存储读取文件时,通过修改pytorch的文件读取流程,会提前30秒将要读取的文件列表下载到内存中。当pytorch真正需要读取文件时,直接从内存中读取文件,而不是从磁盘中读取,从而减少磁盘延迟,实现对训练模型更快的迭代速度。


## 2.1 现有pytorch的训练文件读取流程:
![](https://github.com/cubefs/cubefs/assets/47099843/2c27d688-ec26-4996-b6f7-963643e60ed4)

  文件读取的延迟会影响深度学习框架训练模型的速度。磁盘读取小文件存在较高的延迟,这会降低训练速度。相比之下,将文件提前读取到内存中可以有效减少读取延迟,从而提升训练速度。

* 分布式存储对小文件读取的延迟一般在300微秒左右,而本地NVME介质的小文件读取延迟约为5微秒。分布式存储与本地NVME读写效率的差距会使得在包含大量小图片文件的图像数据集训练场景中训练速度较低。
* 每个worker节点每次需要从存储介质读取一个batch的文件,常见batch大小为1024个文件。如果其中一个文件的读取延迟较高,整个batch训练所需时间会相应增加。这就会影响pytorch框架下模型训练的效率。
* 由于GPU为深度学习模型提供很高的计算能力,但与此相比,分布式存储小文件读取效率较低且不稳定。这会增加训练图计算的等待时间,从而直接影响pytorch框架下模型训练的效率。

业界部分分布式存储软件，对于AI训练框架，采用如下几种优化:

* 利用操作系统文件缓存对已经训练过的数据集第一个epoch读过的文件进行加速,但此方法也存在缺陷:
    1. 第一个epoch的速度比本地读慢近2/3;    
    2. 当训练集图像数超过1亿张时,第二个epoch训练无法利用缓存,因为第一个epoch缓存的文件在linux pagecache中已失效。所以此方法不适用于大规模图像数据集。
* 利用RDMA网络技术进行优化,仅对文件读取进行网络传输层的优化, 文件读取延迟约为100微秒,但仍高于本地NVME的5微秒。此外,要求分布式存储软硬件完全支持RDMA,其成本比普通网络传输要高。
* 通过RDMA和GDS技术简化文件读取流程,减少了内核中数据的复制开销。但存储端小文件读取延迟(如100us)仍远高于内核拷贝时间(1us),无法从根本上解决训练速度受限于分布式存储小文件读取延迟 dieser问题。

基于以上的Pytorch的IO训练流程，  cube_torch 将采用全新

## 2.2引入cube_torch 加速包改造后的pytorch的训练文件读取流程

![](https://github.com/cubefs/cubefs/assets/47099843/e86bf22a-2038-44ef-a7fe-946b7002b341)

如上图所示: 
*  cubeDataLoader会启动1个异步worker，该worker只是将预读的数据发送到cubefs client
*  cubefs client接收到指令会，会将该文件读取到linux pagecache
*  dataLoader worker将会自动从linux pagecache读取本次要读取的数据。




#三、如何使用Cube_torch 进行加速

##3.1 编译、安装cube_torch:
```python
  python3 setup.py bdist_wheel
```

编译完成后，会在dist/cube_torch-0.2-py3-none-any.whl 生成安装包
```python
pip3 uninstall -y cube_torch
pip3 install dist/cube_torch-0.2-py3-none-any.whl
```
验证是否安装完成：
```python
python3
import cube_torch
```


##3.2 cubefs client挂载文件修改：
```python
{
    "masterAddr": "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010",
    "mountPoint": "/cfs/mnt",
    "volName": "ltptest",
    "owner": "ltptest",
    "logDir": "/cfs/log",
    "logLevel": "debug",
    "consulAddr": "http://192.168.0.101:8500",
    "exporterPort": 9500,
    "profPort": "17410",
    "keepcache": true,
    "fsyncOnClose": false,
    "localIP": "192.168.0.17",
    "prefetchThread": 4000,
    "icacheTimeout": 300,
    "lookupValid": 300,
    "attrValid": 300,
    "maxBackground": 24,
    "congestionThresh": 18
}
```
注意这里的localIP 必须是cubefs client 的IP地址，请根据实际情况来修改


##3.3 训练代码修改
```python
import cube_torch
os.environ['localIP'] = '192.168.0.17'
os.environ["CubeFS_ROOT_DIR"] = "/cfs/mnt"
```
注意这里的localIP 必须要和cubefs client 的配置文件保持一致;CubeFS_ROOT_DIR 必须是 cubefs client 的挂载点路径。

### cube_torch目前支持的pytorch dataset:
```python
torchvision.datasets.ImageFolder、torchvision.datasets.DatasetFolder、torchvision.datasets.VOCDetection、torchvision.datasets.CocoDetection
以上4种pytorch官方的dataset类型，cube_torch已支持自动加速
```

### 如果您是自定义dataset集，需要如以下方式新增函数train_data_list：
```python
class ClipDataset(Dataset):
    def __init__(self, image_file_names: List[str], title_file_names: List[str], max_length=50, image_transform=std_transform):
        self.image_file_names= image_file_names
        with ProcessPool() as p:
            img_file_list = p.map(read_img_meta, self.image_file_names)
        self.img_list = reduce(concat_list, img_file_list)
        with ProcessPool() as p:
            title_file_list = p.map(read_img_meta, title_file_names)
        self.title_file_names= reduce(concat_list, title_file_list)
        self.max_length = max_length
        self.transform = image_transform
        self.masking_func = masking_func

    def train_data_list(self):
        return [self.img_list, self.title_list]
    ##新增train_data_list函数，表示需要告诉chubaofs 所需要关注的训练集文件名列表。
    #注意，该函数可以返回多个，如上：返回训练图片文件列表、该图片对应的标题文件列表
    #self.img_list可能是：[/mnt/cfs/jpg/1.jpg,/mnt/cfs/jpg/2.jpg,/mnt/cfs/jpg/3.jpg,/mnt/cfs/jpg/4.jpg]
    #self.title_list可能是[/mnt/cfs/title/1.title,/mnt/cfs/title/2.title,/mnt/cfs/title/3.title,/mnt/cfs/title/4.title]
    #注意img_list 里面的文件名和title_list里面的文件名，必须一一对应。
```


##3.4 如何监控cube_torch 运行过程中的缓存命中率：

```shell

cd /export/Logs/cfs/chubaofs_tech_data;

tail -f tech-data_info.log | grep hit

```
通过以上命令，即可观测缓存命中率。 在100%的情况下，即可加速pytorch 的训练速度