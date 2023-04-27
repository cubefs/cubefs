# Using Object Storage
## Start Object Gateway
Start the ObjectNode object gateway by executing the following command:

```bash
nohup cfs-server -c objectnode.json &
```

The configuration file is as follows:

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

The meaning of each parameter in the configuration file is shown in the following table:


| Parameter    | Type         | Meaning                                                                                       | Required |
|--------------|--------------|-----------------------------------------------------------------------------------------------|----------|
| role         | string       | Process role, must be set to `objectnode`                                                     | Yes      |
| listen       | string       | Port number that the object storage subsystem listens to.<br>Format: `PORT`                   | Yes      |
| domains      | string slice | Configure domain names for S3-compatible interfaces to support DNS-style access to resources  | No       |
| logDir       | string       | Log storage path                                                                              | Yes      |
| logLevel     | string       | Log level. Default: `error`                                                                   | No       |
| masterAddr   | string slice | IP and port number of the resource management master.<br>Format: `IP:PORT`                    | Yes      |
| exporterPort | string       | Port for Prometheus to obtain monitoring data                                                 | No       |
| prof         | string       | Debug and administrator API interface                                                         | Yes      |

## Supported S3-Compatible Interfaces

ObjectNode provides S3-compatible object storage interfaces to operate on files in CubeFS, so you can use open source tools such as [S3Browser](https://s3browser.com) and [S3Cmd](https://s3tools.org/s3cmd) or the native Amazon S3 SDK to operate on files in CubeFS. The main supported interfaces are as follows:

### Bucket Interface

| API                 | Reference                                                                    |
|---------------------|------------------------------------------------------------------------------|
| `HeadBucket`        | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html>        |
| `GetBucketLocation` | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html> |

### Object Interface

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

### Concurrent Upload Interface

| API                       | Reference                                                                          |
|---------------------------|------------------------------------------------------------------------------------|
| `CreateMultipartUpload`   | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html>   |
| `UploadPart`              | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html>              |
| `CompleteMultipartUpload` | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html> |
| `AbortMultipartUpload`    | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html>    |
| `ListParts`               | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html>               |
| `ListMultipartUploads`    | <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html>    |

## Supported SDKs

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

## Create User

You can refer to the link: [User Management Commands](../maintenance/admin-api/master/user.md) to create a user.

If the user has been created, the user can obtain the Access Key and Secret Key through the relevant API.

Below is an example of using the object storage with the AWS GO SDK.

## Create Bucket

The following shows how to create a bucket.

```go
const (
    Endpoint    = "127.0.0.1:17410"     // IP and listening port of the ObjectNode object storage
    Region      = "cfs_dev"             // Cluster name of the resource management master
    AccessKeyId = "Qkr2zxKm8D6ZOh"      // User Access Key
    SecretKeyId = "wygX0NzgshoezVNo"    // User Secret Key
    BucketName  = "BucketName"          // Bucket name
    key         = "key"                 // File name
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

Response:

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

## Upload Object

The object storage subsystem supports two upload methods: normal upload and multipart upload.

### Normal Upload

The following shows how to use the normal upload interface to upload an object.

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

Response:

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

### Multipart Upload

The following shows how to use the multipart upload interface to upload a large object.

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
		//Set the part size to 16MB
		u.PartSize = 64 * 1024 * 1024
		// Do not delete parts if the upload fails
		u.LeavePartsOnError = true
		//Set the concurrency. The concurrency is not the more the better. Please consider the network condition and device load comprehensively.
		u.Concurrency = 5
	})
	if err != nil {
        fmt.Println("Failed to upload ", err)
	} else {
		fmt.Println("upload succeed ", uploaderResult.Location, *uploaderResult.ETag)
	}
}
```

Response:

```http
HTTP/1.1 200 OK
Content-Length: 227
Connection: keep-alive
Content-Type: application/xml
Date: Wed, 01 Mar 2023 08:15:43 GMT
Server: CubeFS
X-Amz-Request-Id: b20ba3fe9ab34321a3428bf69c1e98a4
```

## Copy Object

The following shows how to copy an object.

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

Response:

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

## Download Object

The following shows how to download an object.

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

Response:

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


## Delete Object

The following shows how to delete an object.

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

Response:

```http
HTTP/1.1 204 No Content
Connection: keep-alive
Date: Wed, 01 Mar 2023 08:31:11 GMT
Server: CubeFS
X-Amz-Request-Id: a4a5d27d3cb64466837ba6324eb8b1c2
```