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
	"fmt"
	"io"
	"net/http"

	"github.com/cubefs/cubefs/util/log"
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html
func (o *ObjectNode) putBucketNotificationConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	param := ParseRequestParam(r)
	bucket := param.Bucket()
	if bucket == "" {
		erc = InvalidBucketName
		return
	}

	volume, err := o.getVol(bucket)
	if err != nil {
		log.LogErrorf("putBucketNotificationHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), bucket, err)
		return
	}

	if r.ContentLength <= 0 {
		erc = MissingContentLength
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		log.LogErrorf("putBucketNotificationHandler: read body fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), bucket, err)
		return
	}

	config, err := ParseNotificationConfig(body, o.region, sysNotifier)
	if err != nil {
		log.LogErrorf("putBucketNotificationHandler: parse config fail: requestID(%v) config(%v) err(%v)",
			GetRequestID(r), string(body), err)
		erc = &ErrorCode{
			ErrorCode:    "MalformedXML",
			ErrorMessage: fmt.Sprintf("The XML you provided was not well-formed or did not validate against our published schema: %v.", err),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}

	if config == nil || config.IsEmpty() {
		if err = deleteBucketNotification(volume); err != nil {
			log.LogErrorf("putBucketNotificationHandler: delete config fail: requestID(%v) volume(%v) err(%v)",
				GetRequestID(r), bucket, err)
			return
		}
	} else {
		if err = storeBucketNotification(volume, config); err != nil {
			log.LogErrorf("putBucketNotificationHandler: store config fail: requestID(%v) volume(%v) err(%v)",
				GetRequestID(r), bucket, err)
			return
		}
	}
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html
func (o *ObjectNode) getBucketNotificationConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	param := ParseRequestParam(r)
	bucket := param.Bucket()
	if bucket == "" {
		erc = InvalidBucketName
		return
	}

	volume, err := o.getVol(bucket)
	if err != nil {
		log.LogErrorf("getBucketNotificationHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), bucket, err)
		return
	}

	config, err := getBucketNotification(volume)
	if err != nil {
		log.LogErrorf("getBucketNotificationHandler: get config fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), bucket, err)
		return
	}

	response, err := MarshalXMLEntity(config)
	if err != nil {
		log.LogErrorf("getBucketNotificationHandler: xml marshal fail: requestID(%v) config(%v) err(%v)",
			GetRequestID(r), config, err)
		return
	}

	writeSuccessResponseXML(w, response)
	return
}
