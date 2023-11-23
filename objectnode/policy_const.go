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

type PolicyCheckResult int

const (
	POLICY_UNKNOW PolicyCheckResult = iota + 1 //no policy or not match
	POLICY_ALLOW
	POLICY_DENY
)

type ConditionEnum int

const (
	KEYNAME  = "KeyName"
	SOURCEIP = "SourceIp"
	REFERER  = "Referer"
	HOST     = "Host"
)
const (
	Principal_Any       PrincipalElementType = "*"
	S3_ACTION_PREFIX                         = "s3:"
	S3_RESOURCE_PREFIX                       = "arn:aws:s3:::"
	S3_PRINCIPAL_PREFIX                      = "AWS"
)

//if more s3 api is supported by policy, need extend bucketApiList, objectApiList
var bucketApiList = SliceString{LIST_OBJECTS, LIST_OBJECTS_V2, HEAD_BUCKET, DELETE_BUCKET, LIST_MULTIPART_UPLOADS, GET_BUCKET_LOCATION, GET_OBJECT_LOCK_CFG, PUT_OBJECT_LOCK_CFG}
var objectApiList = SliceString{GET_OBJECT, HEAD_OBJECT, DELETE_OBJECT, PUT_OBJECT, POST_OBJECT, INITIALE_MULTIPART_UPLOAD, UPLOAD_PART, UPLOAD_PART_COPY, COMPLETE_MULTIPART_UPLOAD, COPY_OBJECT, ABORT_MULTIPART_UPLOAD, LIST_PARTS, BATCH_DELETE, GET_OBJECT_RETENTION}

type SliceString []string

//https://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html
const (
	//object level
	ACTION_GET_OBJECT                  = "getobject"
	ACTION_DELETE_OBJECT               = "deleteobject"
	ACTION_PUT_OBJECT                  = "putobject"
	ACTION_ABORT_MULTIPART_UPLOAD      = "abortmultipartupload"
	ACTION_LIST_MULTIPART_UPLOAD_PARTS = "listmultipartuploadparts"
	ACTION_GET_OBJECT_RETENTION        = "getobjectretention"

	//bucket level
	ACTION_LIST_BUCKET                   = "listbucket"
	ACTION_DELETE_BUCKET                 = "deletebucket"
	ACTION_LIST_BUCKET_MULTIPART_UPLOADS = "listbucketmultipartuploads"
	ACTION_GET_BUCKET_LOCATION           = "getbucketlocation"
	ACTION_GET_OBJECT_LOCK_CFG           = "getobjectlockconfiguration"
	ACTION_PUT_OBJECT_LOCK_CFG           = "putobjectlockconfiguration"

	//bucket + object level
	ACTION_ANY = "*"
)

// action => api list, this should be consistent with bucketApiList&&objectApiList
var S3ActionToApis = map[string]SliceString{
	ACTION_PUT_OBJECT:                    {PUT_OBJECT, POST_OBJECT, COPY_OBJECT, INITIALE_MULTIPART_UPLOAD, UPLOAD_PART, UPLOAD_PART_COPY, COMPLETE_MULTIPART_UPLOAD},
	ACTION_GET_OBJECT:                    {GET_OBJECT, HEAD_OBJECT},
	ACTION_DELETE_OBJECT:                 {DELETE_OBJECT, BATCH_DELETE},
	ACTION_ABORT_MULTIPART_UPLOAD:        {ABORT_MULTIPART_UPLOAD},
	ACTION_LIST_BUCKET:                   {LIST_OBJECTS, LIST_OBJECTS_V2, HEAD_BUCKET},
	ACTION_LIST_MULTIPART_UPLOAD_PARTS:   {LIST_PARTS},
	ACTION_LIST_BUCKET_MULTIPART_UPLOADS: {LIST_MULTIPART_UPLOADS},
	ACTION_DELETE_BUCKET:                 {DELETE_BUCKET},
	ACTION_GET_BUCKET_LOCATION:           {GET_BUCKET_LOCATION},
	ACTION_GET_OBJECT_LOCK_CFG:           {GET_OBJECT_LOCK_CFG},
	ACTION_PUT_OBJECT_LOCK_CFG:           {PUT_OBJECT_LOCK_CFG},
	ACTION_GET_OBJECT_RETENTION:          {GET_OBJECT_RETENTION},
}

var allowAnonymousActions = SliceString{ACTION_GET_OBJECT}

//if more bucket actions support policy, need extend validBucketActions
var validBucketActions = SliceString{ACTION_LIST_BUCKET, ACTION_DELETE_BUCKET, ACTION_LIST_BUCKET_MULTIPART_UPLOADS, ACTION_GET_BUCKET_LOCATION, ACTION_PUT_OBJECT_LOCK_CFG, ACTION_GET_OBJECT_LOCK_CFG}

func isPolicyApi(apiName string) bool {
	return apiName == PUT_BUCKET_POLICY || apiName == GET_BUCKET_POLICY || apiName == DELETE_BUCKET_POLICY
}

func (list SliceString) Contain(v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func IsBucketApi(apiName string) bool {
	return bucketApiList.Contain(apiName)
}

func (p PolicyCheckResult) String() string {
	switch p {
	case POLICY_ALLOW:
		return "allow"
	case POLICY_DENY:
		return "deny"
	default:
		return "unknow"
	}
}

func supportByPolicy(apiName string) bool {
	return bucketApiList.Contain(apiName) || objectApiList.Contain(apiName)
}

func IsAccountLevelApi(apiName string) bool {
	return apiName == PUT_BUCKET || apiName == List_BUCKETS
}

func isAnonymous(accessKey string) bool {
	return accessKey == ""
}

func apiAllowAnonymous(apiName string) bool {
	for _, action := range allowAnonymousActions {
		if S3ActionToApis[action].Contain(apiName) {
			return true
		}
	}
	return false
}
