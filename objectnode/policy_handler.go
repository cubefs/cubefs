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

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/cubefs/cubefs/util/log"
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
func (o *ObjectNode) getBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()

	var param = ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getBucketPolicyHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}
	var policy *Policy
	if policy, err = vol.metaLoader.loadPolicy(); err != nil {
		log.LogErrorf("getBucketPolicyHandler: load policy fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	if policy == nil {
		ec = NoSuchBucketPolicy
		log.LogErrorf("getBucketPolicyHandler: NoSuchBucketPolicy, requestID(%v), err(%v), ec(%v)",
			GetRequestID(r), err, ec)
		return
	}

	var policyData []byte
	policyData, err = json.Marshal(policy)
	if err != nil {
		return
	}

	w.Write(policyData)

	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
func (o *ObjectNode) putBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()

	var param = ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("putBucketPolicyHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	policyRaw, err := ioutil.ReadAll(io.LimitReader(r.Body, BucketPolicyLimitSize+1))
	if err != nil {
		log.LogErrorf("putBucketPolicyHandler: read request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
		return
	}
	if len(policyRaw) > BucketPolicyLimitSize {
		ec = EntityTooLarge
		return
	}

	policy, err := ParsePolicy(policyRaw)
	if err != nil {
		log.LogErrorf("putBucketPolicyHandler: parse policy fail: requestID(%v) policy(%v) err(%v)",
			GetRequestID(r), string(policyRaw), err)
		ec = &ErrorCode{
			ErrorCode:    "InvalidPolicySyntax",
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}
	if _, err = policy.Validate(vol.name); err != nil {
		log.LogErrorf("putBucketPolicyHandler: policy validate fail: requestID(%v) policy(%v) bucket(%v) err(%v)",
			GetRequestID(r), policy, vol.name, err)
		return
	}
	if err = storeBucketPolicy(vol, policyRaw); err != nil {
		log.LogErrorf("putBucketPolicyHandler: store policy fail: requestID(%v) err(%v)", GetRequestID(r), err)
		return
	}

	vol.metaLoader.storePolicy(policy)
	w.WriteHeader(http.StatusNoContent)

	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
func (o *ObjectNode) deleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("deleteBucketPolicyHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	if err = deleteBucketPolicy(vol); err != nil {
		log.LogErrorf("deleteBucketPolicyHandler: delete policy fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}
	vol.metaLoader.storePolicy(nil)

	w.WriteHeader(http.StatusNoContent)
	return
}
