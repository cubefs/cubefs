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

import (
	"encoding/xml"
	"io"
	"net/http"

	"github.com/cubefs/cubefs/proto"
)

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
func (o *ObjectNode) getBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetBucketLifecycleConfiguration")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}

	if _, err = o.vm.Volume(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		errorCode = NoSuchBucket
		return
	}

	var lcConf *proto.LcConfiguration
	if lcConf, err = o.mc.AdminAPI().GetBucketLifecycle(ctx, param.Bucket()); err != nil {
		span.Errorf("get lifeCycle from master fail: volume(%v) err(%v)", param.Bucket(), err)
		errorCode = NoSuchLifecycleConfiguration
		return
	}

	lifeCycle := NewLifeCycle()
	lifeCycle.Rules = make([]*Rule, 0)
	for _, lc := range lcConf.Rules {
		rule := &Rule{
			ID:     lc.ID,
			Status: lc.Status,
		}
		if lc.Expire != nil {
			rule.Expire = &Expiration{}
			if lc.Expire.Date != nil {
				rule.Expire.Date = lc.Expire.Date
			}
			if lc.Expire.Days != 0 {
				rule.Expire.Days = &lc.Expire.Days
			}
		}
		if lc.Filter != nil {
			rule.Filter = &Filter{
				Prefix: lc.Filter.Prefix,
			}
		}
		lifeCycle.Rules = append(lifeCycle.Rules, rule)
	}

	var data []byte
	data, err = xml.Marshal(lifeCycle)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", lifeCycle, err)
		errorCode = NoSuchLifecycleConfiguration
		return
	}

	writeSuccessResponseXML(w, data)
}

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
func (o *ObjectNode) putBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutBucketLifecycleConfiguration")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}

	if _, err = o.vm.Volume(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		errorCode = NoSuchBucket
		return
	}

	_, errorCode = VerifyContentLength(r, BodyLimit)
	if errorCode != nil {
		return
	}
	var requestBody []byte
	if requestBody, err = io.ReadAll(r.Body); err != nil && err != io.EOF {
		span.Errorf("read request body fail: %v", err)
		errorCode = &ErrorCode{
			ErrorCode:    http.StatusText(http.StatusBadRequest),
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}

	lifeCycle := NewLifeCycle()
	if err = UnmarshalXMLEntity(requestBody, lifeCycle); err != nil {
		span.Errorf("marshal xml body fail: body(%v) err(%v)", string(requestBody), err)
		errorCode = LifeCycleErrMalformedXML
		return
	}
	ok, errorCode := lifeCycle.Validate()
	if !ok {
		return
	}

	req := proto.LcConfiguration{
		VolName: param.Bucket(),
		Rules:   make([]*proto.Rule, 0),
	}
	for _, lr := range lifeCycle.Rules {
		rule := &proto.Rule{
			ID:     lr.ID,
			Status: lr.Status,
		}
		if lr.Expire != nil {
			rule.Expire = &proto.ExpirationConfig{}
			if lr.Expire.Date != nil {
				rule.Expire.Date = lr.Expire.Date
			}
			if lr.Expire.Days != nil {
				rule.Expire.Days = *lr.Expire.Days
			}
		}
		if lr.Filter != nil {
			rule.Filter = &proto.FilterConfig{
				Prefix: lr.Filter.Prefix,
			}
		}
		req.Rules = append(req.Rules, rule)
	}

	if err = o.mc.AdminAPI().SetBucketLifecycle(ctx, &req); err != nil {
		span.Errorf("set lifeCycle by master fail: volume(%v) lifeCycle(%+v) err(%v)",
			param.Bucket(), req, err)
		return
	}
}

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
func (o *ObjectNode) deleteBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	ctx := r.Context()
	span := spanWithOperation(ctx, "DeleteBucketLifecycle")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}

	if _, err = o.vm.Volume(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		errorCode = NoSuchBucket
		return
	}

	if err = o.mc.AdminAPI().DelBucketLifecycle(ctx, param.Bucket()); err != nil {
		span.Errorf("delete lifeCycle by master fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
