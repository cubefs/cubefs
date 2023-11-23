// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package objectnode

//service api refer to: https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/archive-RESTServiceOps.html
//bucket api refer to:   https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/archive-RESTBucketOps.html
//object api refer to: https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/archive-RESTObjectOps
//account api:  https://docs.aws.amazon.com/en_pv/AmazonS3/latest/API/archive-RESTAccountOps.html ,  batch jobs

const (
	UNSUPPORT_API              = "UnSupportAPI"
	GET_FEDERATION_TOKEN       = "GetFederationToken"         //api:  POST /,  host=s3-cn-east-1.cs.com, create sts token
	List_BUCKETS               = "ListBuckets"                //api:  GET / , host=s3-cn-east-1.cs.com, list all buckets
	DELETE_BUCKET              = "DeleteBucket"               //api:  Delete /  , host=<bucket>.domain
	DELETE_BUCKET_CORS         = "DeleteBucketCors"           //api:  Delete /?cors  , host=<bucket>.domain
	DELETE_BUCKET_ENCRYPTION   = "DeleteBucketEncryption"     //api:  Delete /?encryption  , host=<bucket>.domain
	DELETE_BUCKET_LIFECYCLE    = "DeleteBucketLifeCycle"      //api:  Delete /?lifycycle  , host=<bucket>.domain
	DELETE_BUCKET_METRICS      = "DeleteBucketMetrics"        //api:  Delete /?metrics&id=<ID>  , host=<bucket>.domain
	DELETE_BUCKET_POLICY       = "DeleteBucketPolicy"         //api:  Delete /?policy  , host=<bucket>.domain
	DELETE_BUCKET_REPLICATION  = "DeleteBucketReplication"    //api:  Delete /?replication  , host=<bucket>.domain
	DELETE_BUCKET_TAGGING      = "DeleteBucketTagging"        //api:  Delete /?tagging  , host=<bucket>.domain
	DELETE_BUCKET_WEBSITE      = "DeleteBucketWebsite"        //api:  Delete /?website  , host=<bucket>.domain
	LIST_OBJECTS               = "ListObjects"                //api:  Get /  ,  host=<bucket>.domain ,  GetBucket version1
	LIST_OBJECTS_V2            = "ListObjectsV2"              //api:  Get /?list-type=2, host=<bucket>.domain, GetBucket Version2
	GET_BUCKET_ACCELERATE      = "GetBucketAccelerate"        //api:  GET /<bucketname>?accelerate
	GET_BUCKET_ACL             = "GetBucketAcl"               //api:  GET /<bucketname>?acl
	GET_BUCKET_CORS            = "GetBucketCors"              //api:  Get /?cors  , host=<bucket>.domain
	GET_BUCKET_ENCRYPTION      = "GetBucketEncryption"        //api:  Get /?encryption  , host=<bucket>.domain
	GET_BUCKET_LIFECYCLE       = "GetBucketLifeCycle"         //api:  Get /?lifycycle  , host=<bucket>.domain
	GET_BUCKET_LOCATION        = "GetBucketLocation"          //api:  GET /?location , host=<bucket>.domain
	GET_PUBLIC_ACCESS_BLOCK    = "GetPublicAccessBlock"       //api:  Get /?publicAccessBlock  , host=<bucket>.domain
	GET_BUCKET_LOGGING         = "GetBucketLogging"           //api:  Get /?logging  , host=<bucket>.domain
	GET_BUCKET_METRICS         = "GetBucketMetrics"           //api:  Get /?metrics&id=<id>  , host=<bucket>.domain
	GET_BUCKET_NOTIFICATION    = "GetBucketNotification"      //api:  Get /?notification  , host=<bucket>.domain
	GET_BUCKET_POLICY_STATUS   = "GetBucketPolicyStatus"      //api:  Get /?policyStatus  , host=<bucket>.domain
	GET_BUCKET_OBJECT_VERSIONS = "GetBucketObjectVersions"    //api:  Get /?versions  , host=<bucket>.domain
	GET_BUCKET_POLICY          = "GetBucketPolicy"            //api:  Get /?policy  , host=<bucket>.domain
	GET_BUCKET_REPLICATION     = "GetBucketReplication"       //api:  Get /?replication  , host=<bucket>.domain
	GET_BUCKET_TAGGING         = "GetBucketTagging"           //api:  Get /?tagging  , host=<bucket>.domain
	GET_BUCKET_VERSIONING      = "GetBucketVersioning"        //api:  Get /?versioning  , host=<bucket>.domain
	GET_BUCKET_WEBSITE         = "GetBucketWebsite"           //api:  Get /?website  , host=<bucket>.domain
	GET_OBJECT_LOCK_CFG        = "GetObjectLockConfiguration" //api:  Get /?object-lock, host=<bucket>.domain
	HEAD_BUCKET                = "HeadBucket"                 //api:  Head / , host=<bucket>.domain
	LIST_MULTIPART_UPLOADS     = "ListMultipartUploads"       //api:  GET /?uploads , host=<bucket>.domain
	PUT_BUCKET                 = "CreateBucket"               //api:  Put  / , host=<bucket>.domain, CreateBucket
	PUT_BUCKET_ACCELERATE      = "PutBucketAccelerate"        //api:  Put  /?accelerate , host=<bucket>.domain,
	PUT_BUCKET_ACL             = "PutBucketAcl"               //api:  Put  /?acl , host=<bucket>.domain
	PUT_BUCKET_CORS            = "PutBucketCors"              //api:  PUT /?cors , host=<bucket>.domain
	PUT_BUCKET_ENCRYPTION      = "PutBucketEncryption"        //api:  PUT /?encryption , host=<bucket>.domain
	PUT_BUCKET_LIFECYCLE       = "PutBucketLifecycle"         //api:  PUT /?lifecycle , host=<bucket>.domain
	PUT_PUBLIC_ACCESS_BLOCK    = "PutPublicAccessBlock"       //api:  PUT /<bucketname>?publicAccessBlock , host=<bucket>.domain,
	PUT_BUCKET_LOGGING         = "PutBucketLogging"           //api:  PUT /?logging , host=<bucket>.domain
	PUT_BUCKET_METRICS         = "PutBucketMetrics"           //api:  PUT /?metrics&id=<id> , host=<bucket>.domain
	PUT_BUCKET_NOTIFICATION    = "PutBucketNotification"      //api:  PUT /?notification , host=<bucket>.domain
	PUT_BUCKET_POLICY          = "PutBucketPolicy"            //api:  PUT /?policy , host=<bucket>.domain
	PUT_BUCKET_REPLICATION     = "PutBucketReplication"       //api:  PUT /?replication , host=<bucket>.domain
	PUT_BUCKET_REQUEST_PAYMENT = "PutBucketRequestPayment"    //api:  PUT /?requestPayment , host=<bucket>.domain
	PUT_BUCKET_TAGGING         = "PutBucketTagging"           //api:  PUT /?tagging , host=<bucket>.domain
	PUT_BUCKET_VERSIONING      = "PutBucketVersioning"        //api:  PUT /?versioning , host=<bucket>.domain
	PUT_BUCKET_WEBSITE         = "PutBucketWebsite"           //api:  PUT /?website , host=<bucket>.domain
	PUT_OBJECT_LOCK_CFG        = "PutObjectLockConfiguration" //api:  Put /?object-lock, host=<bucket>.domain
	BATCH_DELETE               = "DeleteObjects"              //api:  POST /?delete , host=<bucket>.domain,  "DeleteObjects"
	DELETE_OBJECT              = "DeleteObject"               //api:  Delete /<objname>, host=<bucket>.domain
	DELETE_OBJECT_TAGGING      = "DeleteObjectTagging"        //api:  Delete /<objname>?tagging, host=<bucket>.domain
	GET_OBJECT                 = "GetObject"                  //api:  Get /<objname> , host=<bucket>.domain
	GET_OBJECT_ACL             = "GetObjectAcl"               //api:  Get /<bucketname>/<objname>?acl   , host=<bucket>.domain
	GET_OBJECT_TAGGING         = "GetObjectTagging"           //api:  Get /<bucketname>/<objname>?tagging   , host=<bucket>.domain
	GET_OBJECT_RETENTION       = "GetObjectRetention"         //api:  Get /<bucketname>/<objname>?retention, host=<bucket>.domain
	HEAD_OBJECT                = "HeadObject"                 //api:  HEAD /<ObjectName> , host=<bucket>.domain
	OPTIONS_OBJECT             = "OptionsObject"              //api:  OPTIONS /<ObjectName>, host=<bucket>.domain
	POST_OBJECT                = "PostObject"                 //api:  Post /  , host=<bucket>.domain
	PUT_OBJECT                 = "PutObject"                  //api:  Put  /<objname>,  host=<bucket>.domain
	COPY_OBJECT                = "CopyObject"                 //api:  Put /<destObjname>  ,host=<destbucket>.domain,  header["x-amz-copy-source"]
	PUT_OBJECT_ACL             = "PutObjectAcl"               //api:  Put /<ObjectName>?acl  , host=<bucket>.domain
	PUT_OBJECT_TAGGING         = "PutObjectTagging"           //api:  Put /<ObjectName>?tagging  , host=<bucket>.domain
	INITIALE_MULTIPART_UPLOAD  = "CreateMultipartUpload"      //api:  POST /<ObjectName>?uploads , host=<bucket>.domain
	UPLOAD_PART                = "UploadPart"                 //api:  PUT /<ObjectName>?partNumber=<PartNumber>&uploadId=<Id>, host=<bucket>.domain
	UPLOAD_PART_COPY           = "UploadPartCopy"             //api:  PUT /<ObjectName>?partNumber=<PartNumber>&uploadId=<Id>, host=<bucket>.domain , header["x-amz-copy-source"]
	LIST_PARTS                 = "ListParts"                  //api:  GET /<ObjectName>?uploadId=<Id> , host=<bucket>.domain
	COMPLETE_MULTIPART_UPLOAD  = "CompleteMultipartUpload"    //api:  POST /<ObjectName>?uploadId=<Id> , host=<bucket>.domain
	ABORT_MULTIPART_UPLOAD     = "AbortMultipartUpload"       //api:  DELETE /<ObjectName>?uploadId=<Id> , host=<bucket>.domain
)
