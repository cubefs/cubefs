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
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
)

// Head bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
func (o *ObjectNode) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	w.Header()[HeaderNameXAmzBucketRegion] = []string{o.region}
}

// Create bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (o *ObjectNode) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		errorCode = InvalidBucketName
		return
	}

	if vol, _ := o.vm.VolumeWithoutBlacklist(bucket); vol != nil {
		log.LogInfof("createBucketHandler: duplicated bucket name: requestID(%v) bucket(%v)", GetRequestID(r), bucket)
		errorCode = BucketAlreadyOwnedByYou
		return
	}

	auth := parseRequestAuthInfo(r)
	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKeyV2(auth.accessKey); err != nil {
		log.LogErrorf("createBucketHandler: get user info from master fail: requestID(%v) accessKey(%v) err(%v)",
			GetRequestID(r), auth.accessKey, err)
		return
	}

	// get LocationConstraint if any
	contentLenStr := r.Header.Get(HeaderNameContentLength)
	if contentLen, errConv := strconv.Atoi(contentLenStr); errConv == nil && contentLen > 0 {
		var requestBytes []byte
		requestBytes, err = ioutil.ReadAll(r.Body)
		if err != nil && err != io.EOF {
			log.LogErrorf("createBucketHandler: read request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
			return
		}

		createBucketRequest := &CreateBucketRequest{}
		err = UnmarshalXMLEntity(requestBytes, createBucketRequest)
		if err != nil {
			log.LogErrorf("createBucketHandler: unmarshal xml fail: requestID(%v) err(%v)",
				GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if createBucketRequest.LocationConstraint != o.region {
			log.LogErrorf("createBucketHandler: location constraint not match the service: requestID(%v) LocationConstraint(%v) region(%v)",
				GetRequestID(r), createBucketRequest.LocationConstraint, o.region)
			errorCode = InvalidLocationConstraint
			return
		}
	}

	var acl *AccessControlPolicy
	if acl, err = ParseACL(r, userInfo.UserID, false); err != nil {
		log.LogErrorf("createBucketHandler: parse acl fail: requestID(%v) err(%v)", GetRequestID(r), err)
		return
	}

	if err = o.mc.AdminAPI().CreateDefaultVolume(bucket, userInfo.UserID); err != nil {
		log.LogErrorf("createBucketHandler: create bucket fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), bucket, auth.accessKey, err)
		return
	}
	//todo parse body
	//w.Header()[HeaderNameLocation] = []string{o.region}
	w.Header()[HeaderNameLocation] = []string{"/" + bucket}
	w.Header()[HeaderNameConnection] = []string{"close"}

	vol, err1 := o.vm.VolumeWithoutBlacklist(bucket)
	if err1 != nil {
		log.LogWarnf("createBucketHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), bucket, err1)
		return
	}
	if err1 = putBucketACL(vol, acl); err1 != nil {
		log.LogWarnf("createBucketHandler: put acl fail: requestID(%v) volume(%v) acl(%+v) err(%v)",
			GetRequestID(r), bucket, acl, err1)
	}
	vol.metaLoader.storeACL(acl)

	return
}

// Delete bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
func (o *ObjectNode) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		errorCode = InvalidBucketName
		return
	}
	auth := parseRequestAuthInfo(r)
	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKeyV2(auth.accessKey); err != nil {
		log.LogErrorf("deleteBucketHandler: get user info fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), bucket, auth.accessKey, err)
		return
	}

	var vol *Volume
	if vol, err = o.getVol(bucket); err != nil {
		log.LogErrorf("deleteBucketHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), bucket, err)
		return
	}
	if !vol.IsEmpty() {
		errorCode = BucketNotEmpty
		return
	}

	// delete Volume from master
	var authKey string
	if authKey, err = calculateAuthKey(userInfo.UserID); err != nil {
		log.LogErrorf("deleteBucketHandler: calculate authKey fail: requestID(%v) volume(%v) authKey(%v) err(%v)",
			GetRequestID(r), bucket, userInfo.UserID, err)
		errorCode = InternalErrorCode(err)
		return
	}
	if err = o.mc.AdminAPI().DeleteVolume(bucket, authKey); err != nil {
		log.LogErrorf("deleteBucketHandler: delete volume fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), bucket, auth.accessKey, err)
		errorCode = InternalErrorCode(err)
		return
	}
	log.LogDebugf("deleteBucketHandler: delete bucket success: requestID(%v) volume(%v) accessKey(%v)",
		GetRequestID(r), bucket, auth.accessKey)
	// release Volume from Volume manager
	o.vm.Release(bucket)
	w.WriteHeader(http.StatusNoContent)
	return
}

// List buckets
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (o *ObjectNode) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	auth := parseRequestAuthInfo(r)
	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKeyV2(auth.accessKey); err != nil {
		log.LogErrorf("listBucketsHandler: get user info fail: requestID(%v) accessKey(%v) err(%v)",
			GetRequestID(r), auth.accessKey, err)
		return
	}

	type bucket struct {
		XMLName      xml.Name `xml:"Bucket"`
		CreationDate string   `xml:"CreationDate"`
		Name         string   `xml:"Name"`
	}

	type listBucketsOutput struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Owner   Owner    `xml:"Owner"`
		Buckets []bucket `xml:"Buckets>Bucket"`
	}

	output := listBucketsOutput{}
	ownVols := userInfo.Policy.OwnVols
	for _, ownVol := range ownVols {
		var vol *Volume
		if vol, err = o.getVol(ownVol); err != nil {
			log.LogErrorf("listBucketsHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
				GetRequestID(r), ownVol, err)
			continue
		}
		output.Buckets = append(output.Buckets, bucket{
			Name:         ownVol,
			CreationDate: formatTimeISO(vol.CreateTime()),
		})
	}
	output.Owner = Owner{DisplayName: userInfo.UserID, Id: userInfo.UserID}

	var bytes []byte
	if bytes, err = MarshalXMLEntity(&output); err != nil {
		log.LogErrorf("listBucketsHandler: marshal result fail: requestID(%v) result(%v) err(%v)",
			GetRequestID(r), output, err)
		return
	}
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("listBucketsHandler: write response body fail: requestID(%v) body(%v) err(%v)",
			GetRequestID(r), string(bytes), err)
	}
	return
}

// Get bucket location
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (o *ObjectNode) getBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if _, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getBucketLocationHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	location := LocationResponse{Location: o.region}
	response, err := MarshalXMLEntity(location)
	if err != nil {
		log.LogErrorf("getBucketLocationHandler: xml marshal fail: requestID(%v) location(%v) err(%v)",
			GetRequestID(r), location, err)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	if _, err = w.Write(response); err != nil {
		log.LogErrorf("getBucketLocationHandler: write response body fail: requestID(%v) body(%v) err(%v)",
			GetRequestID(r), string(response), err)
	}

	return
}

// Get bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
func (o *ObjectNode) getBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getBucketTaggingHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	var xattrInfo *proto.XAttrInfo
	if xattrInfo, err = vol.GetXAttr("/", XAttrKeyOSSTagging); err != nil {
		log.LogErrorf("getBucketTaggingHandler: Volume get XAttr fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}
	ossTaggingData := xattrInfo.Get(XAttrKeyOSSTagging)
	var output, _ = ParseTagging(string(ossTaggingData))

	var encoded []byte
	if nil == output || len(output.TagSet) == 0 {
		errorCode = NoSuchTagSetError
		return
	} else {
		if encoded, err = MarshalXMLEntity(output); err != nil {
			log.LogErrorf("getBucketTaggingHandler: encode output fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InternalErrorCode(err)
			return
		}
	}
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	if _, err = w.Write(encoded); err != nil {
		log.LogErrorf("getBucketTaggingHandler: write response fail: requestID(%v) err（%v)", GetRequestID(r), err)
	}
	return
}

// Put bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
func (o *ObjectNode) putBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
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
		log.LogErrorf("putBucketTaggingHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	var requestBody []byte
	if requestBody, err = ioutil.ReadAll(r.Body); err != nil {
		log.LogErrorf("putBucketTaggingHandler: read request body data fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}

	var tagging = NewTagging()
	if err = UnmarshalXMLEntity(requestBody, tagging); err != nil {
		log.LogWarnf("putBucketTaggingHandler: decode request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}
	validateRes, errorCode := tagging.Validate()
	if !validateRes {
		log.LogErrorf("putBucketTaggingHandler: tagging validate fail: requestID(%v) tagging(%v) err(%v)", GetRequestID(r), tagging, err)
		return
	}

	if err = vol.SetXAttr("/", XAttrKeyOSSTagging, []byte(tagging.Encode()), false); err != nil {
		errorCode = InternalErrorCode(err)
	}

	return
}

// Delete bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
func (o *ObjectNode) deleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("deleteBucketTaggingHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}
	if err = vol.DeleteXAttr("/", XAttrKeyOSSTagging); err != nil {
		log.LogErrorf("deleteBucketTaggingHandler: delete tagging xattr fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		errorCode = InternalErrorCode(err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	return
}

func calculateAuthKey(key string) (authKey string, err error) {
	h := md5.New()
	_, err = h.Write([]byte(key))
	if err != nil {
		log.LogErrorf("calculateAuthKey: calculate auth key fail: key[%v] err[%v]", key, err)
		return
	}
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr)), nil
}

func (o *ObjectNode) getUserInfoByAccessKey(accessKey string) (userInfo *proto.UserInfo, err error) {
	userInfo, err = o.userStore.LoadUser(accessKey)
	return
}

func (o *ObjectNode) getUserInfoByAccessKeyV2(accessKey string) (userInfo *proto.UserInfo, err error) {
	userInfo, err = o.userStore.LoadUser(accessKey)
	if err == proto.ErrUserNotExists || err == proto.ErrAccessKeyNotExists || err == proto.ErrParamError {
		err = InvalidAccessKeyId
	}
	return
}
