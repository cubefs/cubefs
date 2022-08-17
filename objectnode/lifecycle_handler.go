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

import (
	"encoding/xml"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
)

//Get bucket lifecycle configuration
// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
func (o *ObjectNode) getBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
		}
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
		log.LogErrorf("getBucketLifecycleConfigurationHandler failed: bucket[%v] err(%v)", param.Bucket(), err)
		errorCode = NoSuchLifecycleConfiguration
		return
	}

	var lifeCycle = NewLifeCycle()
	lifeCycle.Rules = make([]*Rule, 0)
	for _, lc := range lcConf.Rules {
		rule := &Rule{
			ID:     lc.ID,
			Prefix: lc.Prefix,
			Status: ExpirationStatus(lc.Status),
		}
		if lc.AbortMultipartUpload != nil {
			rule.AbortMultipartUpload = &AbortIncompleteMultipartUpload{
				DaysAfterInitiation: lc.AbortMultipartUpload.DaysAfterInitiation,
			}
		}
		if lc.Expire != nil {
			rule.Expire = &Expiration{
				Date: lc.Expire.Date,
				Days: lc.Expire.Days,
			}
		}
		if lc.Filter != nil {
			rule.Filter = &FilterConfig{
				ObjectSizeGreaterThan: lc.Filter.ObjectSizeGreaterThan,
				ObjectSizeLessThan:    lc.Filter.ObjectSizeLessThan,
				Prefix:                lc.Filter.Prefix,
			}
			if lc.Filter.And != nil {
				rule.Filter.And = &AndOpr{
					ObjectSizeGreaterThan: lc.Filter.And.ObjectSizeGreaterThan,
					ObjectSizeLessThan:    lc.Filter.And.ObjectSizeLessThan,
					Prefix:                lc.Filter.And.Prefix,
					Tag:                   make([]*TagConfig, 0),
				}
				for _, at := range lc.Filter.And.Tags {
					tag := &TagConfig{
						Key:   at.Key,
						Value: at.Value,
					}
					rule.Filter.And.Tag = append(rule.Filter.And.Tag, tag)
				}
			}
			if lc.Filter.Tag != nil {
				rule.Filter.Tag = &TagConfig{
					Key:   lc.Filter.Tag.Key,
					Value: lc.Filter.Tag.Value,
				}
			}
		}

		lifeCycle.Rules = append(lifeCycle.Rules, rule)
	}

	var data []byte
	data, err = xml.Marshal(lifeCycle)
	if err != nil {
		log.LogErrorf("getBucketLifecycleConfigurationHandler failed: bucket[%v] err(%v)", param.Bucket(), err)
		errorCode = NoSuchLifecycleConfiguration
		return
	}

	_, _ = w.Write(data)
	return

}

//Put bucket lifecycle configuration
// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
func (o *ObjectNode) putBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
		}
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
	if requestBody, err = ioutil.ReadAll(r.Body); err != nil {
		log.LogErrorf("putBucketLifecycleConfigurationHandler: read request body data fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	var lifeCycle = NewLifeCycle()
	if err = UnmarshalXMLEntity(requestBody, lifeCycle); err != nil {
		log.LogWarnf("putBucketLifecycleConfigurationHandler: decode request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	validateRes, errorCode := lifeCycle.Validate()
	if !validateRes {
		log.LogErrorf("putBucketLifecycleConfigurationHandler: lifeCycle validate fail: requestID(%v) lifeCycle(%v) err(%v)", GetRequestID(r), lifeCycle, err)
		_ = errorCode.ServeResponse(w, r)
		return
	}

	req := proto.SetBucketLifecycleRequest{
		VolName: param.Bucket(),
		Rules:   make([]*proto.Rule, 0),
	}

	for _, lr := range lifeCycle.Rules {

		rule := &proto.Rule{
			ID:     lr.ID,
			Prefix: lr.Prefix,
			Status: string(lr.Status),
		}
		if lr.AbortMultipartUpload != nil {
			rule.AbortMultipartUpload = &proto.AbortIncompleteMultipartUpload{
				DaysAfterInitiation: lr.AbortMultipartUpload.DaysAfterInitiation,
			}
		}

		if lr.Expire != nil {
			rule.Expire = &proto.Expiration{
				Date: lr.Expire.Date,
				Days: lr.Expire.Days,
			}
		}

		if lr.Filter != nil {
			rule.Filter = &proto.FilterConfig{
				ObjectSizeGreaterThan: lr.Filter.ObjectSizeGreaterThan,
				ObjectSizeLessThan:    lr.Filter.ObjectSizeLessThan,
				Prefix:                lr.Filter.Prefix,
			}
			if lr.Filter.And != nil {
				rule.Filter.And = &proto.AndOpr{
					ObjectSizeGreaterThan: lr.Filter.And.ObjectSizeGreaterThan,
					ObjectSizeLessThan:    lr.Filter.And.ObjectSizeLessThan,
					Prefix:                lr.Filter.And.Prefix,
					Tags:                  make([]*proto.TagConfig, 0),
				}

				for _, at := range lr.Filter.And.Tag {
					tag := &proto.TagConfig{
						Key:   at.Key,
						Value: at.Value,
					}
					rule.Filter.And.Tags = append(rule.Filter.And.Tags, tag)
				}
			}

			if lr.Filter.Tag != nil {
				rule.Filter.Tag = &proto.TagConfig{
					Key:   lr.Filter.Tag.Key,
					Value: lr.Filter.Tag.Value,
				}
			}
		}

		if valid, err := rule.Validate(); !valid {
			log.LogErrorf("putBucketLifecycleConfigurationHandler: rule validate fail: requestID(%v) rule(%v) err(%v)", GetRequestID(r), rule, err)
			errorCode = LifeCycleRulesInvalid
			_ = errorCode.ServeResponse(w, r)
			return
		}
		req.Rules = append(req.Rules, rule)
	}

	if err = o.mc.AdminAPI().SetBucketLifecycle(&req); err != nil {
		log.LogErrorf("putBucketLifecycleConfigurationHandler failed: bucket[%v] err(%v)", param.Bucket(), err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}

}

//Delete Bucket Lifecycle
// API reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
func (o *ObjectNode) deleteBucketLifecycleConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
		}
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
		log.LogErrorf("deleteBucketLifecycleConfigurationHandler failed: bucket[%v] err(%v)", param.Bucket(), err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
