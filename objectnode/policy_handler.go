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
	"net/http"
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
func (o *ObjectNode) getBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetBucketPolicy")
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = InvalidBucketName
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	var policy *Policy
	if policy, err = vol.metaLoader.loadPolicy(ctx); err != nil {
		span.Errorf("load policy fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}
	if policy == nil {
		ec = NoSuchBucketPolicy
		return
	}

	response, err := json.Marshal(policy)
	if err != nil {
		span.Errorf("marshal json response fail: response(%+v) err(%v)", policy, err)
		return
	}

	writeSuccessResponseJSON(w, response)
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
func (o *ObjectNode) putBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		ec  *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutBucketPolicy")
	defer func() {
		o.errorResponse(w, r, err, ec)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = InvalidBucketName
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	policyRaw, err := io.ReadAll(io.LimitReader(r.Body, BucketPolicyLimitSize+1))
	if err != nil {
		span.Errorf("read request body fail: err(%v)", err)
		return
	}
	if len(policyRaw) > BucketPolicyLimitSize {
		ec = EntityTooLarge
		return
	}

	policy, err := ParsePolicy(policyRaw)
	if err != nil {
		span.Errorf("parse policy fail: policy(%v) err(%v)", string(policyRaw), err)
		ec = &ErrorCode{
			ErrorCode:    "InvalidPolicySyntax",
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}
	if _, err = policy.Validate(vol.name); err != nil {
		span.Errorf("policy validate fail: policy(%v) volume(%v) err(%v)", policy, vol.name, err)
		return
	}

	if err = storeBucketPolicy(ctx, vol, policyRaw); err != nil {
		span.Errorf("store policy fail: volume(%v) policy(%v) err(%v)", vol.Name(), string(policyRaw), err)
		return
	}
	vol.metaLoader.storePolicy(policy)

	w.WriteHeader(http.StatusNoContent)
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
func (o *ObjectNode) deleteBucketPolicyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "DeleteBucketPolicy")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	if err = deleteBucketPolicy(ctx, vol); err != nil {
		span.Errorf("delete policy fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}
	vol.metaLoader.storePolicy(nil)

	w.WriteHeader(http.StatusNoContent)
}
