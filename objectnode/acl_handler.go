// Copyright 2018 The ChubaoFS Authors.
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
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	OSS_ACL_KEY      = "oss:acl"
	XMLNS            = "http://www.w3.org/2001/XMLSchema-instance"
	XMLXSI           = "CanonicalUser"
	DEF_GRANTEE_TYPE = "CanonicalUser" //

)

var (
	defaultGrant = Grant{
		Grantee: Grantee{
			Xmlns: XMLNS,
			Xmlsi: XMLXSI,
			Type:  DEF_GRANTEE_TYPE,
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

	_, bucket, _, vol, err := o.parseRequestParams(r)
	if bucket == "" {
		ec = &NoSuchBucket
		return
	}

	//om := vol.OSSMeta()
	acl := vol.loadACL()
	var aclData []byte
	if acl != nil {
		aclData, err = xml.Marshal(acl)
		if err != nil {
			ec = &InternalError
			return
		}

	} else {
		accessKey, _ := vol.OSSSecure()
		acl = &AccessControlPolicy{
			Owner: Owner{Id: accessKey, DisplayName: accessKey},
		}
		acl.Acl.Grants = append(acl.Acl.Grants, defaultGrant)
		aclData, err = xml.Marshal(acl)
		if err != nil {
			ec = &InternalError
			return
		}
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
	_, bucket, _, vol, err1 := o.parseRequestParams(r)
	if err1 != nil {
		err = err1
		return
	}
	if bucket == "" {
		err = errors.New("")
		ec = &NoSuchBucket
		return
	}

	bytes, err2 := ioutil.ReadAll(r.Body)
	if err2 != nil && err2 != io.EOF {
		err = err2
		return
	}

	acl, err3 := ParseACL(bytes, vol.name)
	if err3 != nil {
		err = err3
		return
	}
	if acl == nil {
		err = errors.New("")
		return
	}

	//add standard acl request header
	// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/acl-overview.html
	p, err5 := o.parseRequestParam(r)
	if err5 != nil {
		err = err5
		return
	}
	if standardAcls, found := r.Header["x-amz-acl"]; found {
		acl.SetBucketStandardACL(p, standardAcls[0])
	} else {
		for grant, permission := range aclGrantKeyPermissionMap {
			if _, found2 := r.Header[grant]; found2 {
				acl.SetBucketGrantACL(p, permission)
			}
		}
	}

	newBytes, err6 := acl.Marshal()
	if err6 != nil {
		err = err6
		return
	}

	// store bucket acl
	_, err4 := storeBucketACL(newBytes, vol)
	if err4 != nil {
		err = err4
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
