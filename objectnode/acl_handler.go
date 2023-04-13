// Copyright 2019 The CubeFS Authors.
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

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/acl-using-rest-api.html

import (
	"net/http"
	"syscall"

	"github.com/cubefs/cubefs/util/log"
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
func (o *ObjectNode) getBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()

	param := ParseRequestParam(r)
	log.LogDebugf("Get bucket acl with request param: %+v, requestID(%v)", param, GetRequestID(r))
	if param.Bucket() == "" {
		ec = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("getBucketACLHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.bucket, err)
		return
	}
	var acl *AccessControlPolicy
	if acl, err = vol.metaLoader.loadACL(); err != nil {
		log.LogErrorf("getBucketACLHandler: load acl fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.bucket, err)
		return
	}
	if acl == nil || acl.IsEmpty() {
		acl = CreateDefaultACL(vol.owner)
	}
	var data []byte
	if data, err = acl.XmlMarshal(); err != nil {
		log.LogErrorf("getBucketACLHandler: acl xml marshal fail: requestID(%v) volume(%v) acl(%+v) err(%v)",
			GetRequestID(r), param.bucket, acl, err)
		return
	}
	if _, err = w.Write(data); err != nil {
		log.LogErrorf("getBucketACLHandler: write response body fail: requestID(%v) volume(%v) body(%v) err(%v)",
			GetRequestID(r), param.bucket, string(data), err)
	}

	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
func (o *ObjectNode) putBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()

	param := ParseRequestParam(r)
	log.LogDebugf("Put bucket acl with request param: %+v, requestID(%v)", param, GetRequestID(r))
	if param.Bucket() == "" {
		ec = InvalidBucketName
		return
	}
	if !HasAclInRequest(r) {
		ec = ErrMissingSecurityHeader
		return
	}

	var vol *Volume
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("putBucketACLHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.bucket, err)
		return
	}
	var acl *AccessControlPolicy
	if acl, err = ParseACL(r, vol.owner, r.ContentLength > 0); err != nil {
		log.LogErrorf("putBucketACLHandler: parse acl fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.bucket, err)
		return
	}
	if err = putBucketACL(vol, acl); err != nil {
		log.LogErrorf("putBucketACLHandler: put acl fail: requestID(%v) volume(%v) acl(%+v) err(%v)",
			GetRequestID(r), param.bucket, acl, err)
		return
	}
	vol.metaLoader.storeACL(acl)

	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
func (o *ObjectNode) getObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()

	param := ParseRequestParam(r)
	log.LogDebugf("Get object acl with request param: %+v, requestID(%v)", param, GetRequestID(r))
	if param.Bucket() == "" {
		ec = InvalidBucketName
		return
	}
	if param.Object() == "" {
		ec = InvalidKey
		return
	}

	var vol *Volume
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("getObjectACLHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.bucket, err)
		return
	}
	var acl *AccessControlPolicy
	if acl, err = getObjectACL(vol, param.object, true); err != nil {
		log.LogErrorf("getObjectACLHandler: get acl fail: requestID(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), param.bucket, param.object, err)
		if err == syscall.ENOENT {
			ec = NoSuchKey
		}
		return
	}
	var data []byte
	if data, err = acl.XmlMarshal(); err != nil {
		log.LogErrorf("getObjectACLHandler: xml marshal fail: requestID(%v) volume(%v) path(%v) acl(%+v) err(%v)",
			GetRequestID(r), param.bucket, param.object, acl, err)
		return
	}
	if _, err = w.Write(data); err != nil {
		log.LogErrorf("getObjectACLHandler: write response body fail: requestID(%v) volume(%v) path(%v) body(%v) err(%v)",
			GetRequestID(r), param.bucket, param.object, string(data), err)
	}
	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
func (o *ObjectNode) putObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()

	param := ParseRequestParam(r)
	log.LogDebugf("Put object acl with request param: %+v, requestID(%v)", param, GetRequestID(r))
	if param.Bucket() == "" {
		ec = InvalidBucketName
		return
	}
	if param.Object() == "" {
		ec = InvalidKey
		return
	}
	if !HasAclInRequest(r) {
		ec = ErrMissingSecurityHeader
		return
	}

	var vol *Volume
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("putObjectACLHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.bucket, err)
		return
	}
	var acl, oldAcl *AccessControlPolicy
	if oldAcl, err = getObjectACL(vol, param.object, false); err != nil {
		log.LogErrorf("putObjectACLHandler: get acl fail: requestID(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), param.bucket, param.object, err)
		if err == syscall.ENOENT {
			ec = NoSuchKey
		}
		return
	}
	owner := vol.owner
	if oldAcl != nil {
		owner = oldAcl.GetOwner()
	}
	if acl, err = ParseACL(r, owner, r.ContentLength > 0); err != nil {
		log.LogErrorf("putObjectACLHandler: parse acl fail: requestID(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), param.bucket, param.object, err)
		return
	}
	if oldAcl != nil {
		originalOwner := oldAcl.GetOwner()
		if oldAcl.IsEmpty() {
			originalOwner = vol.owner
		}
		if originalOwner != acl.GetOwner() {
			log.LogErrorf("putObjectACLHandler: owner cannot be modified: requestID(%v) volume(%v) path(%v) acl(%+v) err(%v)",
				GetRequestID(r), param.bucket, param.object, oldAcl, err)
			ec = AccessDenied
			return
		}
	}
	if err = putObjectACL(vol, param.object, acl); err != nil {
		log.LogErrorf("putObjectACLHandler: store acl fail: requestID(%v) volume(%v) path(%v) acl(%+v) err(%v)",
			GetRequestID(r), param.bucket, param.object, acl, err)
		if err == syscall.ENOENT {
			ec = NoSuchKey
		}
	}

	return
}
