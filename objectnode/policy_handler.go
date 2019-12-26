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

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
)

func (o *ObjectNode) getBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
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

	ossMeta := vol.OSSMeta()
	if ossMeta == nil {
		ec = &InternalError
		return
	}

	policyData, err2 := json.Marshal(ossMeta.policy)
	if err2 != nil {
		err = err2
		ec = &InternalError
		return
	}

	w.Write(policyData)

	return
}

func (o *ObjectNode) putBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
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

	if r.ContentLength > BucketPolicyLimitSize {
		ec = &MaxContentLength
		return
	}

	bytes, err2 := ioutil.ReadAll(r.Body)
	if err2 != nil && err2 != io.EOF {
		err = err2
		log.LogInfof("read body err, %v", err)
		return
	}

	policy, err3 := storeBucketPolicy(bytes, vol)
	if err3 != nil {
		err = err3
		log.LogErrorf("store policy err, %v", err)
		return
	}

	log.LogInfof("put bucket policy %v %v", bucket, policy)

	return
}

func (o *ObjectNode) deleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("delete bucket policy...")
	var (
		err error
		ec  *ErrorCode
	)
	defer o.errorResponse(w, r, err, ec)

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		err = errors.New("")
		ec = &NoSuchBucket
		return
	}

	return
}
