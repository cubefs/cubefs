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
	"io/ioutil"
	"net/http"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
func (o *ObjectNode) getBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if _, err = o.vm.Volume(param.Bucket()); err != nil {
		errorCode = NoSuchBucket
		return
	}

	var lcConf *proto.LcConfiguration
	if lcConf, err = o.mc.AdminAPI().GetBucketLifecycle(param.Bucket()); err != nil {
		log.LogErrorf("getBucketLifecycle failed: bucket[%v] err(%v)", param.Bucket(), err)
		errorCode = NoSuchLifecycleConfiguration
		return
	}

	var lifeCycle = NewLifeCycle()
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
		log.LogErrorf("getBucketLifecycle failed: bucket[%v] err(%v)", param.Bucket(), err)
		errorCode = NoSuchLifecycleConfiguration
		return
	}

	_, _ = w.Write(data)
	return

}

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
func (o *ObjectNode) putBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if _, err = o.vm.Volume(param.Bucket()); err != nil {
		errorCode = NoSuchBucket
		return
	}

	var requestBody []byte
	if requestBody, err = ioutil.ReadAll(r.Body); err != nil && err != io.EOF {
		log.LogErrorf("putBucketLifecycle failed: read request body data err: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = &ErrorCode{
			ErrorCode:    http.StatusText(http.StatusBadRequest),
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}

	var lifeCycle = NewLifeCycle()
	if err = UnmarshalXMLEntity(requestBody, lifeCycle); err != nil {
		log.LogWarnf("putBucketLifecycle failed: decode request body err: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = LifeCycleErrMalformedXML
		return
	}

	ok, errorCode := lifeCycle.Validate()
	if !ok {
		log.LogErrorf("putBucketLifecycle failed: validate err: requestID(%v) lifeCycle(%v) err(%v)", GetRequestID(r), lifeCycle, errorCode)
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

	if err = o.mc.AdminAPI().SetBucketLifecycle(&req); err != nil {
		log.LogErrorf("putBucketLifecycle failed: SetBucketLifecycle err: bucket[%v] err(%v)", param.Bucket(), err)
		errorCode = InternalErrorCode(err)
		return
	}

	log.LogInfof("putBucketLifecycle success: requestID(%v) volume(%v) lifeCycle(%v)",
		GetRequestID(r), param.Bucket(), lifeCycle)
}

// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
func (o *ObjectNode) deleteBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if _, err = o.vm.Volume(param.Bucket()); err != nil {
		errorCode = NoSuchBucket
		return
	}

	if err = o.mc.AdminAPI().DelBucketLifecycle(param.Bucket()); err != nil {
		log.LogErrorf("deleteBucketLifecycle failed: bucket[%v] err(%v)", param.Bucket(), err)
		errorCode = InternalErrorCode(err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
