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
	"io/ioutil"
	"net/http"

	"github.com/cubefs/cubefs/util/log"
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLogging.html
func (o *ObjectNode) getBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		erc = InvalidBucketName
		return
	}

	vol, err := o.getVol(param.bucket)
	if err != nil {
		log.LogErrorf("getBucketLoggingHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.bucket, err)
		return
	}
	logging, err := getBucketLogging(vol)
	if err != nil {
		log.LogErrorf("getBucketLoggingHandler: get bucket logging fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), vol.Name(), err)
		return
	}
	response, err := MarshalXMLEntity(logging)
	if err != nil {
		log.LogErrorf("getBucketLoggingHandler: xml marshal fail: requestID(%v) volume(%v) logging(%v) err(%v)",
			GetRequestID(r), vol.Name(), logging, err)
		return
	}

	writeSuccessResponseXML(w, response)
	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLogging.html
func (o *ObjectNode) putBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		erc *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, erc)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		erc = InvalidBucketName
		return
	}

	vol, err := o.getVol(param.bucket)
	if err != nil {
		log.LogErrorf("putBucketLoggingHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.bucket, err)
		return
	}
	requestMD5 := r.Header.Get(ContentMD5)
	if requestMD5 == "" {
		erc = MissingContentMD5
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.LogErrorf("putBucketLoggingHandler: read request body fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), vol.Name(), err)
		return
	}
	if requestMD5 != GetMD5(body) {
		erc = InvalidDigest
		return
	}

	var logging *Logging
	if err = UnmarshalXMLEntity(body, &logging); err != nil {
		log.LogErrorf("putBucketLoggingHandler: xml unmarshal fail: requestID(%v) volume(%v) body(%v) err(%v)",
			GetRequestID(r), vol.Name(), string(body), err)
		erc = MalformedXML
		return
	}

	if logging.LoggingEnabled != nil {
		if !isLoggingPrefixValid(logging.LoggingEnabled.TargetPrefix) {
			erc = ErrInvalidTargetPrefixForLogging
			return
		}
		targetBucket := logging.LoggingEnabled.TargetBucket
		if targetBucket == "" {
			erc = ErrInvalidTargetBucketForLogging
			return
		}
		var targetVol *Volume
		if targetVol, err = o.getVol(targetBucket); err != nil {
			log.LogErrorf("putBucketLoggingHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
				GetRequestID(r), targetBucket, err)
			erc = ErrInvalidTargetBucketForLogging
			return
		}
		if vol.GetOwner() != targetVol.GetOwner() {
			erc = ErrInvalidTargetBucketForLogging
			return
		}
		if err = storeBucketLogging(vol, logging); err != nil {
			log.LogErrorf("putBucketLoggingHandler: store logging fail: requestID(%v) volume(%v) err(%v)",
				GetRequestID(r), vol.Name(), err)
			return
		}
		vol.metaLoader.storeLogging(logging)
	} else {
		if err = deleteBucketLogging(vol); err != nil {
			log.LogErrorf("putBucketLoggingHandler: delete logging fail: requestID(%v) volume(%v) err(%v)",
				GetRequestID(r), vol.Name(), err)
			return
		}
		vol.metaLoader.storeLogging(nil)
	}

	return
}
