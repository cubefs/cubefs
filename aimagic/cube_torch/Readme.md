<div align="center"><font size="100">cube_torch 使用文档</font></div>

# 一、cube_torch 是什么？

## 1.1 cube_torch 模块简介：
在大量图片训练场景中,由于本地存储空间限制,不得不采用分布式文件系统存储训练数据,但这将使训练数据读写变慢。业界的分布式文件系统对小文件读写效率很低。所以开发此软件,针对大量小文件训练场景,提高pytorch框架下实现很高的训练速度。

由于pytorch 迭代速度较快，并且cube_torch 和cubefs进行深度绑定。 因此该软件并不侵入pytorch的代码。 用户使用较为简单，并不需要改变用户的训练代码

## 1.2 cube_torch 技术亮点:

*  训练速度高: 针对大量的小文件训练场景，让AI的训练速度和本地NVME速度一样快。 
*  简单易用: 用户不需要改变自己的训练代码，仅需要加入`import cube_torch`的字样，并设置几个环境变量即可进行加速。
*  适用范围广: 适用于所有的pytorch的训练模型。已支持DEEPSPPED等框架



# 二、业内AI训练加速现状

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

基于以上的Pytorch的IO训练流程，  cube_torch 将采用全新架构来加速小文件的训练速度。






# 三、如何使用Cube_torch 进行加速

## 3.1 编译、安装cube_torch:
```python
python3 setup.py bdist_wheel
pip3 install dist/cube_torch-0.3-py3-none-any.whl
```

验证是否安装完成：
```python
python3
import cube_torch
```


## 3.2 cubefs client挂载文件修改：
```python
{
    "masterAddr": "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010",
    "mountPoint": "/cfs/mnt",
    "volName": "ltptest",
    "owner": "ltptest",
    "logDir": "/cfs/log",
    "logLevel": "info",
    "consulAddr": "http://192.168.0.101:8500",
    "exporterPort": 9500,
    "profPort": "17410",
    "profile":"ai_prefetch"
}
```
注意如果要开启AI 预读功能，请增加配置文件，"profile":"ai_prefetch"，如果成功挂载，会自动生成：/tmp/cube_torch.config.ltptest 文件

## 3.3 训练代码修改
```python
import cube_torch
os.environ['VOL_NAME'] = 'ltptest'
```
注意这里的VOL_NAME 必须要和cubefs client 的配置文件保持一致;

### cube_torch目前支持的pytorch dataset:
```python
torchvision.datasets.ImageFolder、torchvision.datasets.DatasetFolder、torchvision.datasets.VOCDetection、torchvision.datasets.CocoDetection
以上4种pytorch官方的dataset类型，cube_torch已支持自动加速
```

### 如果您是自定义dataset集，需要如以下方式新增函数train_data_list：

```python
import numpy as np
import cube_torch

os.environ['VOL_NAME'] = 'ltptest'



class VLPDataset(Dataset):
    def __init__(self, image_metas: List[str], title_metas: List[str], max_length=50, image_transform=std_transform,
                 masking_func=None, attrs: List[str] = [], tokenizer=None):
        self.image_metas = image_metas

        with ProcessPool() as p:
            img_file_list = p.map(read_img_meta, self.image_metas)
        self.img_list = reduce(concat_list, img_file_list)

        with ProcessPool() as p:
            title_file_list = p.map(read_img_meta, title_metas)
        self.title_list = reduce(concat_list, title_file_list)
        
        self.img_list=np.array(self.img_list)
        self.title_list=np.array(self.title_list)
        super().__init__()

    def train_data_list(self):
        return np.array([self.img_list, self.title_list])
      ##新增train_data_list函数，表示需要告诉chubaofs 所需要关注的训练集文件名列表。
      # 注意，该函数可以返回多个，如上：返回训练图片文件列表、该图片对应的标题文件列表
      # self.img_list 必须是一个numpy的一维数组，可能是：[/mnt/cfs/jpg/1.jpg,/mnt/cfs/jpg/2.jpg,/mnt/cfs/jpg/3.jpg,/mnt/cfs/jpg/4.jpg]
      # self.title_list 必须是一个numpy的一维数组，可能是[/mnt/cfs/title/1.title,/mnt/cfs/title/2.title,/mnt/cfs/title/3.title,/mnt/cfs/title/4.title]
      # 注意img_list 里面的文件名和title_list里面的文件名，必须一一对应。

    def __len__(self):
        return len(self.img_list)

    def __getitem__(self, index):
        try:
            x = utils.default_loader(self.img_list[index])
        except Exception as e:
               return self.__getitem__((index + 1) % self.__len__())

        try:
            title_tokens = torch.load(self.title_list[index])
        except Exception as e:
                return self.__getitem__((index + 1) % self.__len__())



```


## 3.4 如何监控cube_torch 运行过程中的缓存命中率：

```shell
在训练的过程中，cube_torch会自动打印当前的缓存命中率

user memory last_cycle_metrics:([request_count:4801 hit_count:4801 miss_count:0 hit_rate:100.00% miss_rate:0.00% ])  sum_metrics:([request_count:295154 hit_count:295001 miss_count:153 hit_rate:99.95% miss_rate:0.05%])

```
通过以上命令，即可观测缓存命中率。 在100%的情况下，即可加速pytorch 的训练速度


# 四、cube_torch 加速效果测试



|                    | 18000w vlp(samples/s) | 7400w vlp(samples/s) | 7400w 224x224 imagenet(samples/s) |7400w imagenet  224x224 (每个epoch 耗费秒数) |128w imagenet 1280x857 (samples/s) |128w imagenet  1280x857 (每个epoch 耗费秒数)|
| :-----             | ----:                | :----:               | :----:                            |:----:                                      | :----:                            | :----:                        |
| 本地NVME            |                      | 3100                 | 2200                              |  32444s                                    | 1036                              |   1030                        | 
| cubeFS             | 2200                 | 2400                 | 1800                              |                                            | 1356                              |   1350                        | 
| cube_torch + cubeFS| 3120                 | 3096                 | 2185                              |  33357s                                    | 1958                              |   640                         | 
