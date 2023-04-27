# 使用对象存储
## 启动对象网关
通过执行如下的命令来启动对象网关ObjectNode

```bash
nohup cfs-server -c objectnode.json &
```

配置文件的示例如下：

```json
{
     "role": "objectnode", 
     "listen": "127.0.0.1:17410",
     "domains": [
         "object.cfs.local"
     ],
     "logDir": "/cfs/Logs/objectnode",
     "logLevel": "info",
     "masterAddr": [
         "10.196.59.198:17010",
         "10.196.59.199:17010",
         "10.196.59.200:17010"
     ],
     "exporterPort": 9503,
     "prof": "7013"
}
```

配置文件中各参数的含义如下表所示：


| 参数           | 类型           | 含义                                   | 必需  |
|--------------|--------------|--------------------------------------|-----|
| role         | string       | 进程角色，必须设置为 `objectnode`              | 是   |
| listen       | string       | 对象存储子系统监听的端口号.<br>格式: `PORT`    | 是   |
| domains      | string slice | 为S3兼容接口配置域名以支持DNS风格访问资源              | 否   |
| logDir       | string       | 日志存放路径                               | 是   |
| logLevel     | string       | 日志级别. 默认: `error`                    | 否   |
| masterAddr   | string slice | 资源管理Master的IP和端口号.<br>格式: `IP:PORT`  | 是   |
| exporterPort | string       | prometheus获取监控数据端口                   | 否   |
| prof         | string       | 调试和管理员API接口                          | 是   |

## 支持的S3兼容接口

ObjectNode提供兼容S3的对象存储接口来操作CubeFS中的文件，因此可以使用[S3Browser](https://s3browser.com)、[S3Cmd](https://s3tools.org/s3cmd)等开源工具或者原生的Amazon S3 SDK操作CubeFS中的文件。具体支持的主要接口如下：

### 桶接口

| API                 | Reference                                                                    |
|---------------------|------------------------------------------------------------------------------|
| `HeadBucket`        | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html>        |
| `GetBucketLocation` | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html> |

### 对象接口

| API             | Reference                                                                |
|-----------------|--------------------------------------------------------------------------|
| `PutObject`     | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>     |
| `GetObject`     | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html>     |
| `HeadObject`    | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html>    |
| `CopyObject`    | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>    |
| `ListObjects`   | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html>   |
| `ListObjectsV2` | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html> |
| `DeleteObject`  | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>  |
| `DeleteObjects` | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html> |

### 并发上传接口

| API                       | Reference                                                                          |
|---------------------------|------------------------------------------------------------------------------------|
| `CreateMultipartUpload`   | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html>   |
| `UploadPart`              | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html>              |
| `CompleteMultipartUpload` | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html> |
| `AbortMultipartUpload`    | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html>    |
| `ListParts`               | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html>               |
| `ListMultipartUploads`    | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html>    |

## 支持的SDK

| Name                              | Language     | Link                                      |
|-----------------------------------|--------------|-------------------------------------------|
| AWS SDK for Java                  | `Java`       | <https://aws.amazon.com/sdk-for-java/>    |
| AWS SDK for JavaScript            | `JavaScript` | <https://aws.amazon.com/sdk-for-browser/> |
| AWS SDK for JavaScript in Node.js | `JavaScript` | <https://aws.amazon.com/sdk-for-node-js/> |
| AWS SDK for Go                    | `Go`         | <https://docs.aws.amazon.com/sdk-for-go/> |
| AWS SDK for PHP                   | `PHP`        | <https://aws.amazon.com/sdk-for-php/>     |
| AWS SDK for Ruby                  | `Ruby`       | <https://aws.amazon.com/sdk-for-ruby/>    |
| AWS SDK for .NET                  | `.NET`       | <https://aws.amazon.com/sdk-for-net/>     |
| AWS SDK for C++                   | `C++`        | <https://aws.amazon.com/sdk-for-cpp/>     |
| Boto3                             | `Python`     | <http://boto.cloudhackers.com>            |

## 创建用户
创建用户可以参见链接：[用户管理命令](../maintenance/admin-api/master/user.md)

如已创建用户，用户可以通过相关API获取用户密钥 Access Key 和 Secret Key 。

下面以AWS GO SDK为例，演示如何使用对象存储。

## 创建Bucket

下面演示如何创建Bucket

```go
const (
	Endpoint    = "127.0.0.1:17410"     //对象存储ObjectNode的IP和监听端口 
	Region      = "cfs_dev"             //资源管理Master的集群名称clusterName 
	AccessKeyId = "Qkr2zxKm8D6ZOh"      //用户Access Key
	SecretKeyId = "wygX0NzgshoezVNo"    //用户Secret Key
	BucketName  = "BucketName"          //桶名
	key         = "key"                 //文件名
)
func CreateBucket() {
    conf := &aws.Config{
		Region:           aws.String(Region),
		Endpoint:         aws.String(Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(AccessKeyId, SecretKeyId, ""),
		LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
    service := s3.New(sess)
	bucketConfig := s3.CreateBucketConfiguration{
		LocationConstraint: aws.String(Region),
	}
	req, out := service.CreateBucketRequest(&s3.CreateBucketInput{
		Bucket:                    aws.String(BucketName),
		CreateBucketConfiguration: &bucketConfig,
	})
	err := req.Send()
	if err != nil {
		fmt.Println("Failed to CreateBucket ", err)
	} else {
		fmt.Println("CreateBucket succeed ", out)
	}
}
```

响应：

```http
HTTP/1.1 200 OK
Connection: close
Content-Length: 0
Date: Wed, 01 Mar 2023 07:55:35 GMT
Location: /BucketName
Server: CubeFS
X-Amz-Request-Id: cb9bafab3e8a4e56a296b47604f6a415

CreateBucket succeed  {
  Location: "/BucketName"
}
```

## 上传对象

对象存储子系统支持普通上传和分片上传两种上传方式

### 普通上传

下面演示如何使用普通上传接口上传对象

```go
func PutObject() {
	conf := &aws.Config{
		Region:           aws.String(Region),
		Endpoint:         aws.String(Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(AccessKeyId, SecretKeyId, ""),
		LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	svc := s3.New(sess)
	input := &s3.PutObjectInput{
		Body:     aws.ReadSeekCloser(bytes.NewBuffer([]byte("test"))),
		Bucket:   aws.String(BucketName),
		Key:      aws.String(key),
	}
	result, err := svc.PutObject(input)
	if err != nil {
        fmt.Println("Failed to Put object ", err)
	} else {
		fmt.Println("Put object succeed ", result)
	}
}
```

响应：

```http
HTTP/1.1 200 OK
Content-Length: 0
Connection: keep-alive
Date: Wed, 01 Mar 2023 08:03:44 GMT
Etag: "098f6bcd4621d373cade4e832627b4f6"
Server: CubeFS
X-Amz-Request-Id: 7a2f7cd926f14284abf18716c17c01f9

Put object succeed  {
  ETag: "\"098f6bcd4621d373cade4e832627b4f6\""
}
```

### 分片上传

下面演示如何使用分片上传接口上传大对象

```go
func UploadWithManager() {
	conf := &aws.Config{
		Region:           aws.String(Region),
		Endpoint:         aws.String(Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(AccessKeyId, SecretKeyId, ""),
		LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	file, err := ioutil.ReadFile("D:/Users/80303220/Desktop/largeFile")
	if err != nil {
		fmt.Println("Unable to read file ", err)
		return
	}
	uploader := s3manager.NewUploader(sess)
	upParams := &s3manager.UploadInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(strings.NewReader(string(file))),
	}
	uploaderResult, err := uploader.Upload(upParams, func(u *s3manager.Uploader) {
		//设置分段大小：16M
		u.PartSize = 64 * 1024 * 1024
		//如果上传失败不删除分段
		u.LeavePartsOnError = true
		//设置并发数，并发数并非越多越快，请结合自身网络状况和设备负载综合考虑
		u.Concurrency = 5
	})
	if err != nil {
        fmt.Println("Failed to upload ", err)
	} else {
		fmt.Println("upload succeed ", uploaderResult.Location, *uploaderResult.ETag)
	}
}
```

响应：

```http
HTTP/1.1 200 OK
Content-Length: 227
Connection: keep-alive
Content-Type: application/xml
Date: Wed, 01 Mar 2023 08:15:43 GMT
Server: CubeFS
X-Amz-Request-Id: b20ba3fe9ab34321a3428bf69c1e98a4
```

## 拷贝对象

下面演示如何拷贝对象

```go
func CopyObject() {
	conf := &aws.Config{
		Region:           aws.String(Region),
		Endpoint:         aws.String(Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(AccessKeyId, SecretKeyId, ""),
		LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	svc := s3.New(sess)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(BucketName),
		Key:        aws.String("test-dst"),
		CopySource: aws.String(BucketName + "/" + "test"),
	}
	result, err := svc.CopyObject(input)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}
```

响应：

```http
HTTP/1.1 200 OK
Content-Length: 184
Connection: keep-alive
Content-Type: application/xml
Date: Wed, 01 Mar 2023 08:21:25 GMT
Server: CubeFS
X-Amz-Request-Id: 8889dd739a1a4c2492238e48724e491e

{
  CopyObjectResult: {
    ETag: "\"098f6bcd4621d373cade4e832627b4f6\"",
    LastModified: 2023-03-01 08:21:23 +0000 UTC
  }
}
```

## 下载对象

下面演示如何下载对象

```go
func GetObject(key string) error {
	conf := &aws.Config{
		Region:           aws.String(Region),
		Endpoint:         aws.String(Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(AccessKeyId, SecretKeyId, ""),
		LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	svc := s3.New(sess)
	input := &s3.GetObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
		//Range:  aws.String("bytes=0-1"), 
	}
	req, srcObject := svc.GetObjectRequest(input)
	err := req.Send()
	if srcObject.Body != nil {
		defer srcObject.Body.Close()
	}
	if err != nil {
		fmt.Println("Failed to Get object ", err)
		return err
	} else {
		file, err := os.Create("./test-dst")
		if err != nil {
			return err
		}
		i, err := io.Copy(file, srcObject.Body)
		if err == nil {
			fmt.Println("Get object succeed ", i)
			return nil
		}
		fmt.Println("Failed to Get object ", err)
		return err
	}
}
```

响应：

```http
HTTP/1.1 200 OK
Content-Length: 4
Accept-Ranges: bytes
Connection: keep-alive
Content-Type: application/octet-stream
Date: Wed, 01 Mar 2023 08:28:48 GMT
Etag: "098f6bcd4621d373cade4e832627b4f6"
Last-Modified: Wed, 01 Mar 2023 08:03:43 GMT
Server: CubeFS
X-Amz-Request-Id: 71fecfb8e9bd4d3db8d4a71cb50c4c47
```


## 删除对象

下面演示如何删除对象

```go
func DeleteObject() {
	conf := &aws.Config{
		Region:           aws.String(Region),
		Endpoint:         aws.String(Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(AccessKeyId, SecretKeyId, ""),
		LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	service := s3.New(sess)
	req, out := service.DeleteObjectRequest(&s3.DeleteObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
	})
	err := req.Send()
	if err != nil {
		fmt.Println("Failed to Delete ", err)
	} else {
		fmt.Println("Delete succeed ", out)
	}
}
```

响应：

```http
HTTP/1.1 204 No Content
Connection: keep-alive
Date: Wed, 01 Mar 2023 08:31:11 GMT
Server: CubeFS
X-Amz-Request-Id: a4a5d27d3cb64466837ba6324eb8b1c2
```