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
	"io/ioutil"
	"net/http"
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
		err error
	)
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		_ = InvalidBucketName.ServeResponse(w, r)
		return
	}
	if vol, _ := o.getVol(bucket); vol != nil {
		log.LogInfof("create bucket failed: duplicated bucket name[%v]", bucket)
		_ = DuplicatedBucket.ServeResponse(w, r)
		return
	}
	auth := parseRequestAuthInfo(r)
	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(auth.accessKey); err != nil {
		log.LogErrorf("get user info from master error: accessKey(%v), err(%v)", auth.accessKey, err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}
	if err = o.mc.AdminAPI().CreateDefaultVolume(bucket, userInfo.UserID); err != nil {
		log.LogErrorf("create bucket[%v] failed: accessKey(%v), err(%v)", bucket, auth.accessKey, err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}
	//todo parse body
	w.Header()[HeaderNameLocation] = []string{o.region}
	return
}

// Delete bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
func (o *ObjectNode) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {

	var (
		volState *proto.VolStatInfo
		authKey  string
		err      error
	)
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		_ = InvalidBucketName.ServeResponse(w, r)
		return
	}
	auth := parseRequestAuthInfo(r)
	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(auth.accessKey); err != nil {
		log.LogErrorf("get user info from master error: accessKey(%v), err(%v)", auth.accessKey, err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}
	if volState, err = o.mc.ClientAPI().GetVolumeStat(bucket); err != nil {
		log.LogErrorf("get bucket state from master error: err(%v)", err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}
	if volState.UsedSize != 0 {
		_ = BucketNotEmpty.ServeResponse(w, r)
		return
	}
	// delete Volume from master
	if authKey, err = calculateAuthKey(userInfo.UserID); err != nil {
		log.LogErrorf("delete bucket[%v] error: calculate authKey(%v) err(%v)", bucket, userInfo.UserID, err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}
	if err = o.mc.AdminAPI().DeleteVolume(bucket, authKey); err != nil {
		log.LogErrorf("delete bucket[%v] error: accessKey(%v), err(%v)", bucket, auth.accessKey, err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}

	// release Volume from Volume manager
	o.vm.Release(bucket)
	w.WriteHeader(http.StatusNoContent)
	return
}

// List buckets
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (o *ObjectNode) listBucketsHandler(w http.ResponseWriter, r *http.Request) {

	var err error
	auth := parseRequestAuthInfo(r)
	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(auth.accessKey); err != nil {
		log.LogErrorf("get user info from master error: accessKey(%v), err(%v)", auth.accessKey, err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
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

	var output = listBucketsOutput{}

	ownVols := userInfo.Policy.OwnVols
	for _, ownVol := range ownVols {
		var vol *Volume
		if vol, err = o.getVol(ownVol); err != nil {
			log.LogErrorf("listBucketsHandler: load volume fail: volume(%v) err(%v)",
				ownVol, err)
			continue
		}
		output.Buckets = append(output.Buckets, bucket{
			Name:         ownVol,
			CreationDate: formatTimeISO(vol.CreateTime()),
		})
	}
	output.Owner = Owner{DisplayName: userInfo.UserID, Id: userInfo.UserID}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(&output); marshalError != nil {
		log.LogErrorf("listBucketsHandler: marshal result fail, requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("listBucketsHandler: write response body fail, requestID(%v) err(%v)", GetRequestID(r), err)
	}
	return
}

// Get bucket location
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (o *ObjectNode) getBucketLocation(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write(o.encodedRegion)
	return
}

// Get bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
func (o *ObjectNode) getBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var param = ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		_ = InvalidBucketName.ServeResponse(w, r)
		return
	}
	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	var xattrInfo *proto.XAttrInfo
	if xattrInfo, err = vol.GetXAttr("/", XAttrKeyOSSTagging); err != nil {
		log.LogErrorf("getBucketTaggingHandler: Volume get XAttr fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
	}
	ossTaggingData := xattrInfo.Get(XAttrKeyOSSTagging)
	var output, _ = ParseTagging(string(ossTaggingData))

	var encoded []byte
	if nil == output || len(output.TagSet) == 0 {
		sb := strings.Builder{}
		sb.WriteString(xml.Header)
		sb.WriteString("<Tagging></Tagging>")
		encoded = []byte(sb.String())
	} else {
		if encoded, err = MarshalXMLEntity(output); err != nil {
			log.LogErrorf("getBucketTaggingHandler: encode output fail: requestID(%v) err(%v)", GetRequestID(r), err)
			_ = InternalErrorCode(err).ServeResponse(w, r)
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
	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		errorCode = NoSuchBucket
		return
	}

	var requestBody []byte
	if requestBody, err = ioutil.ReadAll(r.Body); err != nil {
		log.LogErrorf("putBucketTaggingHandler: read request body data fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	var tagging = NewTagging()
	if err = UnmarshalXMLEntity(requestBody, tagging); err != nil {
		log.LogWarnf("putBucketTaggingHandler: decode request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}
	validateRes, errorCode := tagging.Validate()
	if !validateRes {
		log.LogErrorf("putBucketTaggingHandler: tagging validate fail: requestID(%v) tagging(%v) err(%v)", GetRequestID(r), tagging, err)
		return
	}

	if err = vol.SetXAttr("/", XAttrKeyOSSTagging, []byte(tagging.Encode()), false); err != nil {
		_ = InternalErrorCode(err).ServeResponse(w, r)
		return
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
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
	}()

	var param = ParseRequestParam(r)

	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		log.LogErrorf("deleteBucketTaggingHandler: load Volume fail: requestID(%v) Volume(%v) err(%v)", GetRequestID(r), param.bucket, err)
		errorCode = NoSuchBucket
		return
	}
	if err = vol.DeleteXAttr("/", XAttrKeyOSSTagging); err != nil {
		log.LogErrorf("deleteBucketTaggingHandler: Volume delete tagging xattr fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InternalErrorCode(err).ServeResponse(w, r)
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
