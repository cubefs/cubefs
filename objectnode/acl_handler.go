// Copyright 2019 The ChubaoFS Authors.
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
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/cubefs/cubefs/util/log"
)

const (
	XMLNS    = "http://www.w3.org/2001/XMLSchema-instance"
	XSI_TYPE = "CanonicalUser" //

)

var (
	defaultGrant = Grant{
		Grantee: Grantee{
			Xmlns: XMLNS,
			Type:  XSI_TYPE,
		},
		Permission: FullControlPermission,
	}
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
func (o *ObjectNode) getBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("Get bucket acl")
	var (
		err error
		ec  *ErrorCode
	)
	defer o.errorResponse(w, r, err, ec)

	var param *RequestParam
	param = ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = NoSuchBucket
		return
	}

	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		ec = NoSuchBucket
		return
	}
	var acl *AccessControlPolicy
	if acl, err = vol.metaLoader.loadACL(); err != nil {
		ec = InternalErrorCode(err)
		return
	}
	var aclData []byte
	if acl == nil {
		acl = &AccessControlPolicy{
			Owner: Owner{Id: param.AccessKey(), DisplayName: param.AccessKey()},
		}
		acl.Acl.Grants = append(acl.Acl.Grants, defaultGrant)
	}

	acl.Acl.Grants[0].Grantee.Xmlxsi = acl.Acl.Grants[0].Grantee.Xmlns
	acl.Acl.Grants[0].Grantee.XsiType = acl.Acl.Grants[0].Grantee.Type

	aclData, err = xml.Marshal(acl)
	if err != nil {
		ec = InternalErrorCode(err)
		return
	}

	w.Write(aclData)

	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html#API_PutBucketAcl_RequestSyntax
//
func (o *ObjectNode) putBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)
	defer o.errorResponse(w, r, err, ec)

	log.LogInfof("Put bucket acl")

	var param *RequestParam
	param = ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = NoSuchBucket
		return
	}
	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		ec = NoSuchBucket
		return
	}

	var bytes []byte
	if bytes, err = ioutil.ReadAll(r.Body); err != nil && err != io.EOF {
		return
	}

	var acp *AccessControlPolicy
	if acp, err = ParseACL(bytes, param.Bucket()); err != nil {
		return
	}
	if acp == nil {
		return
	}

	//add standard acl request header
	// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/acl-overview.html
	if standardAcls, found := r.Header["x-amz-acl"]; found {
		acp.SetBucketStandardACL(param, standardAcls[0])
	} else {
		for grant, permission := range aclGrantKeyPermissionMap {
			if _, found2 := r.Header[grant]; found2 {
				acp.SetBucketGrantACL(param, permission)
			}
		}
	}

	var newBytes []byte
	if newBytes, err = acp.Marshal(); err != nil {
		return
	}

	// store bucket acl
	if _, err = storeBucketACL(newBytes, vol); err != nil {
		return
	}
	return
}

func (o *ObjectNode) getObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	//TODO: implement get object acl handler

	return
}

func (o *ObjectNode) putObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	//TODO: implement get object acl handler

	return
}
