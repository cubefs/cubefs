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
	"net/http"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/keystore"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
)

type CreateBucketConfiguration struct {
	xmlns              string `xml:"xmlns"` //todo ???
	locationConstraint string `xml:"locationConstraint"`
}

// Head bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
func (o *ObjectNode) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	// do nothing
}

// Create bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (o *ObjectNode) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	var (
		mc  *master.MasterClient
		err error
		ec  *ErrorCode
	)
	defer o.errorResponse(w, r, err, ec)

	log.LogInfof("Create bucket")
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		ec = &InvalidBucketName
		return
	}
	auth := parseRequestAuthInfo(r)
	var akCaps *keystore.AccessKeyCaps
	if akCaps, err = o.authStore.GetAkCaps(auth.accessKey); err != nil {
		log.LogInfof("get user info from authnode error: accessKey(%v), err(%v)", auth.accessKey, err)
		return
	}
	//todo required error code？
	if mc, err = o.vm.GetMasterClient(); err != nil {
		log.LogInfof("get master client error: err(%v)", err)
		return
	}
	//todo what params to createVol？
	if err = mc.AdminAPI().CreateDefaultVolume(bucket, akCaps.ID); err != nil {
		log.LogInfof("create bucket[%v] error: accessKey(%v), err(%v)", bucket, auth.accessKey, err)
		return
	}
	//todo parse body
	//todo add const
	cap := "{\"OwnerVOL\":[\"*:" + bucket + ":*\"]}"
	if _, err = o.authStore.authClient.API().OSSAddCaps(proto.ObjectServiceID, o.authStore.authKey, auth.accessKey, []byte(cap)); err != nil {
		return
	}
	w.Header().Set("Location", o.region)
}

// List buckets
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (o *ObjectNode) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: implement 'listBucketsHandler'
}

// Get bucket location
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (o *ObjectNode) getBucketLocation(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("getBucketLocation: get bucket location: requestID(%v)", RequestIDFromRequest(r))
	var output = &GetBucketLocationOutput{
		LocationConstraint: o.region,
	}
	var marshaled []byte
	var err error
	if marshaled, err = MarshalXMLEntity(output); err != nil {
		log.LogErrorf("getBucketLocation: marshal result fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		ServeInternalStaticErrorResponse(w, r)
		return
	}
	if _, err = w.Write(marshaled); err != nil {
		log.LogErrorf("getBucketLocation: write response body fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
	}
	return
}
