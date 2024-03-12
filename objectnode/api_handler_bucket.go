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
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/cubefs/cubefs/proto"
)

const (
	DefaultMinBucketLength = 3
	DefaultMaxBucketLength = 63
)

var regexBucketName = regexp.MustCompile(`^[0-9a-z][-0-9a-z]+[0-9a-z]$`)

// Head bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
func (o *ObjectNode) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(XAmzBucketRegion, o.region)
}

// Create bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
func (o *ObjectNode) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "CreateBucket")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	if o.disableCreateBucketByS3 {
		errorCode = DisableCreateBucketByS3
		return
	}

	param := ParseRequestParam(r)
	bucket := param.Bucket()
	if bucket == "" {
		errorCode = InvalidBucketName
		return
	}

	if !IsValidBucketName(bucket, DefaultMinBucketLength, DefaultMaxBucketLength) {
		errorCode = InvalidBucketName
		return
	}

	if vol, _ := o.vm.VolumeWithoutBlacklist(ctx, bucket); vol != nil {
		errorCode = BucketAlreadyOwnedByYou
		return
	}

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(ctx, param.AccessKey()); err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(userInfo.UserID, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(userInfo.UserID, param.apiName)

	length, errorCode := VerifyContentLength(r, BodyLimit)
	if errorCode != nil {
		return
	}
	if length > 0 {
		var requestBytes []byte
		requestBytes, err = io.ReadAll(r.Body)
		if err != nil && err != io.EOF {
			span.Errorf("read request body fail: %v", err)
			return
		}
		createBucketRequest := &CreateBucketRequest{}
		err = UnmarshalXMLEntity(requestBytes, createBucketRequest)
		if err != nil {
			span.Errorf("unmarshal xml body fail: body(%v) err(%v)", string(requestBytes), err)
			errorCode = InvalidArgument
			return
		}
		if createBucketRequest.LocationConstraint != o.region {
			errorCode = InvalidLocationConstraint
			return
		}
	}

	acl, err := ParseACL(r, userInfo.UserID, false, false)
	if err != nil {
		return
	}

	if err = o.mc.AdminAPI().CreateDefaultVolume(ctx, bucket, userInfo.UserID); err != nil {
		span.Errorf("create volume by master fail: reqUid(%v) volume(%v) err(%v)",
			userInfo.UserID, bucket, err)
		return
	}

	w.Header().Set(Location, "/"+bucket)
	w.Header().Set(Connection, "close")

	vol, err1 := o.vm.VolumeWithoutBlacklist(ctx, bucket)
	if err1 != nil {
		span.Warnf("load volume fail: volume(%v) err(%v)", bucket, err1)
		return
	}
	if acl != nil {
		if err1 = putBucketACL(ctx, vol, acl); err1 != nil {
			span.Warnf("put acl fail: volume(%v) acl(%+v) err(%v)", bucket, acl, err1)
		}
		vol.metaLoader.storeACL(acl)
	}
}

// Delete bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
func (o *ObjectNode) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "DeleteBucket")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	bucket := param.Bucket()
	if bucket == "" {
		errorCode = InvalidBucketName
		return
	}

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(ctx, param.AccessKey()); err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, bucket); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", bucket, err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	if !vol.IsEmpty() {
		errorCode = BucketNotEmpty
		return
	}

	// delete Volume from master
	var authKey string
	if authKey, err = calculateAuthKey(userInfo.UserID); err != nil {
		span.Errorf("calculate authKey fail: authKey(%v) err(%v)", userInfo.UserID, err)
		return
	}
	if err = o.mc.AdminAPI().DeleteVolume(ctx, bucket, authKey); err != nil {
		span.Errorf("delete volume by master fail: reqUid(%v) volume(%v) authKey(%v) err(%v)",
			userInfo.UserID, bucket, authKey, err)
		return
	}

	span.Infof("delete bucket success: reqUid(%v) volume(%v) accessKey(%v)",
		userInfo.UserID, bucket, param.AccessKey())

	// release Volume from Volume manager
	o.vm.Release(ctx, bucket)

	w.WriteHeader(http.StatusNoContent)
}

// List buckets
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (o *ObjectNode) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "ListBuckets")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(ctx, param.accessKey); err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(userInfo.UserID, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(userInfo.UserID, param.apiName)

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

	var output listBucketsOutput
	authVos := userInfo.Policy.AuthorizedVols
	ownVols := userInfo.Policy.OwnVols
	for vol := range authVos {
		ownVols = append(ownVols, vol)
	}
	for _, ownVol := range ownVols {
		var vol *Volume
		if vol, err = o.getVol(ctx, ownVol); err != nil {
			span.Errorf("load volume(%v) fail: %v", ownVol, err)
			continue
		}
		output.Buckets = append(output.Buckets, bucket{
			Name:         ownVol,
			CreationDate: formatTimeISO(vol.CreateTime()),
		})
	}
	output.Owner = Owner{DisplayName: userInfo.UserID, Id: userInfo.UserID}

	response, err := MarshalXMLEntity(&output)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", output, err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// Get bucket location
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (o *ObjectNode) getBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetBucketLocation")
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

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	location := LocationResponse{Location: o.region}
	response, err := MarshalXMLEntity(location)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", location, err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// Get bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
func (o *ObjectNode) getBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetBucketTagging")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	var xattrInfo *proto.XAttrInfo
	if xattrInfo, err = vol.GetXAttr(ctx, bucketRootPath, XAttrKeyOSSTagging); err != nil {
		span.Errorf("get xAttr fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}

	ossTaggingData := xattrInfo.Get(XAttrKeyOSSTagging)
	output, _ := ParseTagging(string(ossTaggingData))
	if nil == output || len(output.TagSet) == 0 {
		errorCode = NoSuchTagSetError
		return
	}

	response, err := MarshalXMLEntity(output)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", output, err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// Put bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
func (o *ObjectNode) putBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutBucketTagging")
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

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	_, errorCode = VerifyContentLength(r, BodyLimit)
	if errorCode != nil {
		return
	}

	var body []byte
	if body, err = io.ReadAll(r.Body); err != nil {
		span.Errorf("read request body fail: %v", err)
		errorCode = InvalidArgument
		return
	}

	tagging := NewTagging()
	if err = UnmarshalXMLEntity(body, tagging); err != nil {
		span.Errorf("unmarshal xml body fail: body(%v) err(%v)", string(body), err)
		errorCode = InvalidArgument
		return
	}
	validateRes, errorCode := tagging.Validate()
	if !validateRes {
		return
	}

	taggingStr := tagging.Encode()
	if err = vol.SetXAttr(ctx, bucketRootPath, XAttrKeyOSSTagging, []byte(taggingStr), false); err != nil {
		span.Errorf("set tagging xAttr fail: volume(%v) tagging(%v) err(%v)",
			vol.Name(), taggingStr, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Delete bucket tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
func (o *ObjectNode) deleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "DeleteBucketTagging")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	if err = vol.DeleteXAttr(ctx, bucketRootPath, XAttrKeyOSSTagging); err != nil {
		span.Errorf("delete tagging xAttr fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func calculateAuthKey(key string) (authKey string, err error) {
	h := md5.New()
	_, err = h.Write([]byte(key))
	if err != nil {
		return
	}
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr)), nil
}

// Put Object Lock Configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html
func (o *ObjectNode) putObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutObjectLockConfiguration")
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

	var body []byte
	if body, err = io.ReadAll(io.LimitReader(r.Body, MaxObjectLockSize+1)); err != nil {
		span.Errorf("read request body fail: %v", err)
		return
	}
	if len(body) > MaxObjectLockSize {
		errorCode = EntityTooLarge
		return
	}

	var config *ObjectLockConfig
	if config, err = ParseObjectLockConfigFromXML(body); err != nil {
		return
	}

	if body, err = json.Marshal(config); err != nil {
		span.Errorf("marshal json config fail: config(%+v) err(%v)", config, err)
		return
	}
	if err = storeObjectLock(ctx, body, vol); err != nil {
		span.Errorf("store object lock config fail: volume(%v) config(%v) err(%v)",
			vol.Name(), string(body), err)
		return
	}

	vol.metaLoader.storeObjectLock(config)

	w.WriteHeader(http.StatusNoContent)
}

// Get Object Lock Configuration
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html
func (o *ObjectNode) getObjectLockConfigurationHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetObjectLockConfiguration")
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

	var config *ObjectLockConfig
	if config, err = vol.metaLoader.loadObjectLock(ctx); err != nil {
		span.Errorf("load object lock fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}
	if config == nil || config.IsEmpty() {
		errorCode = ObjectLockConfigurationNotFound
		return
	}

	var data []byte
	if data, err = MarshalXMLEntity(config); err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", config, err)
		return
	}

	writeSuccessResponseXML(w, data)
}

func (o *ObjectNode) getUserInfoByAccessKey(ctx context.Context, accessKey string) (*proto.UserInfo, error) {
	userInfo, err := o.userStore.LoadUser(ctx, accessKey)
	switch err {
	case proto.ErrUserNotExists, proto.ErrAccessKeyNotExists, proto.ErrParamError:
		return nil, InvalidAccessKeyId
	case nil:
		return userInfo, nil
	default:
		return nil, err
	}
}

func IsValidBucketName(bucketName string, minBucketLength, maxBucketLength int) bool {
	if len(bucketName) < minBucketLength || len(bucketName) > maxBucketLength {
		return false
	}
	if !regexBucketName.MatchString(bucketName) {
		return false
	}
	return true
}
