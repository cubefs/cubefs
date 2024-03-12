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
	"time"
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
func (o *ObjectNode) getBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetBucketAcl")
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		erc = InvalidBucketName
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	var acl *AccessControlPolicy
	if acl, err = vol.metaLoader.loadACL(ctx); err != nil {
		span.Errorf("load ACL fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}
	if acl == nil || acl.IsEmpty() {
		acl = CreateDefaultACL(vol.owner)
	}

	var data []byte
	if data, err = acl.XmlMarshal(); err != nil {
		span.Errorf("marshal xml acl fail: acl(%+v) err(%v)", acl, err)
		return
	}

	writeSuccessResponseXML(w, data)
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
func (o *ObjectNode) putBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutBucketAcl")
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		erc = InvalidBucketName
		return
	}
	if !HasAclInRequest(r) {
		erc = ErrMissingSecurityHeader
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	var acl *AccessControlPolicy
	if acl, err = ParseACL(r, vol.owner, r.ContentLength > 0, true); err != nil {
		return
	}

	if err = putBucketACL(ctx, vol, acl); err != nil {
		span.Errorf("put acl fail: volume(%v) acl(%+v) err(%v)", vol.Name(), acl, err)
		return
	}

	vol.metaLoader.storeACL(acl)
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
func (o *ObjectNode) getObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetObjectAcl")
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		erc = InvalidBucketName
		return
	}
	if param.Object() == "" {
		erc = InvalidKey
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	start := time.Now()
	acl, err := getObjectACL(ctx, vol, param.Object(), true)
	span.AppendTrackLog("xattr.r", start, err)
	if err != nil {
		span.Errorf("get acl fail: volume(%v) path(%v) err(%v)", vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			erc = NoSuchKey
		}
		return
	}

	var data []byte
	if data, err = acl.XmlMarshal(); err != nil {
		span.Errorf("marshal xml acl fail: acl(%+v) err(%v)", acl, err)
		return
	}

	writeSuccessResponseXML(w, data)
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html
func (o *ObjectNode) putObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutObjectAcl")
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		erc = InvalidBucketName
		return
	}
	if param.Object() == "" {
		erc = InvalidKey
		return
	}
	if !HasAclInRequest(r) {
		erc = ErrMissingSecurityHeader
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	var acl, oldAcl *AccessControlPolicy
	start := time.Now()
	oldAcl, err = getObjectACL(ctx, vol, param.Object(), false)
	span.AppendTrackLog("xattr.r", start, err)
	if err != nil {
		span.Errorf("get acl fail: volume(%v) path(%v) err(%v)", vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			erc = NoSuchKey
		}
		return
	}
	owner := vol.owner
	if oldAcl != nil {
		owner = oldAcl.GetOwner()
	}
	if acl, err = ParseACL(r, owner, r.ContentLength > 0, true); err != nil {
		return
	}
	if oldAcl != nil {
		originalOwner := oldAcl.GetOwner()
		if oldAcl.IsEmpty() {
			originalOwner = vol.owner
		}
		if originalOwner != acl.GetOwner() {
			span.Errorf("owner cannot be modified: volume(%v) path(%v) oldACL(%+v) newACL(%+v)",
				vol.Name(), param.Object(), oldAcl, acl)
			erc = AccessDenied
			return
		}
	}

	start = time.Now()
	err = putObjectACL(ctx, vol, param.Object(), acl)
	span.AppendTrackLog("xattr.w", start, err)
	if err != nil {
		span.Errorf("put acl fail: volume(%v) path(%v) acl(%+v) err(%v)",
			vol.Name(), param.Object(), acl, err)
		if err == syscall.ENOENT {
			erc = NoSuchKey
		}
		return
	}
}
