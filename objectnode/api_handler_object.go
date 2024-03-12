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
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/cubefs/cubefs/proto"
)

var (
	rangeRegexp  = regexp.MustCompile("^bytes=(\\d)*-(\\d)*$")
	MaxKeyLength = 750
)

// Get object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (o *ObjectNode) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetObject")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
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

	// parse http range option
	var (
		revRange    bool
		isRangeRead bool
		rangeLower  uint64
		rangeUpper  uint64
		partSize    uint64
		partCount   uint64
	)
	rangeOpt := strings.TrimSpace(r.Header.Get(Range))
	if len(rangeOpt) > 0 {
		if !rangeRegexp.MatchString(rangeOpt) {
			errorCode = InvalidRange
			return
		}
		hyphenIndex := strings.Index(rangeOpt, "-")
		if hyphenIndex < 0 {
			errorCode = InvalidRange
			return
		}
		lowerPart := rangeOpt[len("bytes="):hyphenIndex]
		upperPart := ""
		if hyphenIndex+1 < len(rangeOpt) {
			// bytes=-5
			if hyphenIndex == len("bytes=") { // suffix range opt
				revRange = true
				rangeUpper = 1<<64 - 1
				lowerPart = rangeOpt[hyphenIndex+1:]
			} else { // bytes=1-10
				upperPart = rangeOpt[hyphenIndex+1:]
			}
		} else if hyphenIndex+1 == len(rangeOpt) { // bytes=1-
			rangeUpper = 1<<64 - 1
		}
		if len(lowerPart) > 0 {
			if rangeLower, err = strconv.ParseUint(lowerPart, 10, 64); err != nil {
				errorCode = InvalidRange
				return
			}
		}
		if len(upperPart) > 0 {
			if rangeUpper, err = strconv.ParseUint(upperPart, 10, 64); err != nil {
				errorCode = InvalidRange
				return
			}
		}
		if rangeUpper < rangeLower {
			errorCode = InvalidRange
			return
		}
		isRangeRead = true
	}

	responseCacheControl := r.URL.Query().Get(ParamResponseCacheControl)
	if len(responseCacheControl) > 0 && !ValidateCacheControl(responseCacheControl) {
		errorCode = InvalidCacheArgument
		return
	}
	responseExpires := r.URL.Query().Get(ParamResponseExpires)
	if len(responseExpires) > 0 && !ValidateCacheExpires(responseExpires) {
		errorCode = InvalidCacheArgument
		return
	}
	responseContentType := r.URL.Query().Get(ParamResponseContentType)
	responseContentDisposition := r.URL.Query().Get(ParamResponseContentDisposition)

	// get object meta
	fileInfo, xattr, err := vol.ObjectMeta(ctx, param.Object())
	if err != nil {
		span.Errorf("get object meta fail: volume(%v) path(%v) err(%v)",
			vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}

	// header condition check
	errorCode = CheckConditionInHeader(r, fileInfo)
	if errorCode != nil {
		return
	}

	// validate and fix range
	if isRangeRead && rangeUpper > uint64(fileInfo.Size)-1 {
		rangeUpper = uint64(fileInfo.Size) - 1
		if revRange && rangeLower > 0 {
			rangeLower = rangeUpper + 1 - rangeLower
		}
	}

	// compute content length
	contentLength := uint64(fileInfo.Size)
	if isRangeRead {
		contentLength = rangeUpper - rangeLower + 1
	}

	// get object tagging size
	ossTaggingData := xattr.Get(XAttrKeyOSSTagging)
	output, _ := ParseTagging(string(ossTaggingData))
	if output != nil && len(output.TagSet) > 0 {
		w.Header().Set(XAmzTaggingCount, strconv.Itoa(len(output.TagSet)))
	}

	// set response header for GetObject
	w.Header().Set(AcceptRanges, ValueAcceptRanges)
	w.Header().Set(LastModified, formatTimeRFC1123(fileInfo.ModifyTime))
	if len(responseContentType) > 0 {
		w.Header().Set(ContentType, responseContentType)
	} else if len(fileInfo.MIMEType) > 0 {
		w.Header().Set(ContentType, fileInfo.MIMEType)
	} else {
		w.Header().Set(ContentType, ValueContentTypeStream)
	}
	if len(responseContentDisposition) > 0 {
		w.Header().Set(ContentDisposition, responseContentDisposition)
	} else if len(fileInfo.Disposition) > 0 {
		w.Header().Set(ContentDisposition, fileInfo.Disposition)
	}
	if len(responseCacheControl) > 0 {
		w.Header().Set(CacheControl, responseCacheControl)
	} else if len(fileInfo.CacheControl) > 0 {
		w.Header().Set(CacheControl, fileInfo.CacheControl)
	}
	if len(responseExpires) > 0 {
		w.Header().Set(Expires, responseExpires)
	} else if len(fileInfo.Expires) > 0 {
		w.Header().Set(Expires, fileInfo.Expires)
	}
	if len(fileInfo.RetainUntilDate) > 0 {
		w.Header().Set(XAmzObjectLockMode, ComplianceMode)
		w.Header().Set(XAmzObjectLockRetainUntilDate, fileInfo.RetainUntilDate)
	}

	// check request is whether contain param : partNumber
	partNumber := r.URL.Query().Get(ParamPartNumber)
	if len(partNumber) > 0 && fileInfo.Size >= MinParallelDownloadFileSize {
		partNumberInt, err := strconv.ParseUint(partNumber, 10, 64)
		if err != nil {
			span.Errorf("parse request partNumber(%s) fail: %v", partNumber, err)
			errorCode = InvalidArgument
			return
		}
		partSize, partCount, rangeLower, rangeUpper = parsePartInfo(partNumberInt, uint64(fileInfo.Size))
		span.Debugf("partNumber(%v) fileSize(%v) parsed: partSize(%d) partCount(%d) rangeLower(%d) rangeUpper(%d)",
			partNumberInt, fileInfo.Size, partSize, partCount, rangeLower, rangeUpper)
		if partNumberInt > partCount {
			span.Errorf("invalid partNumber: partNumber(%d) > partCount(%d)", partNumberInt, partCount)
			errorCode = NoSuchKey
			return
		}
		// Header : Accept-Range, Content-Length, Content-Range, ETag, x-amz-mp-parts-count
		w.Header().Set(ContentLength, strconv.Itoa(int(partSize)))
		w.Header().Set(ContentRange, fmt.Sprintf("bytes %d-%d/%d", rangeLower, rangeUpper, fileInfo.Size))
		w.Header().Set(XAmzMpPartsCount, strconv.Itoa(int(partCount)))
		if len(fileInfo.ETag) > 0 && !strings.Contains(fileInfo.ETag, "-") {
			w.Header()[ETag] = []string{fmt.Sprintf("%s-%d", fileInfo.ETag, partCount)}
		}
	} else {
		w.Header().Set(ContentLength, strconv.FormatUint(contentLength, 10))
		if len(fileInfo.ETag) > 0 {
			w.Header()[ETag] = []string{wrapUnescapedQuot(fileInfo.ETag)}
		}
		if isRangeRead {
			w.Header().Set(ContentRange, fmt.Sprintf("bytes %d-%d/%d", rangeLower, rangeUpper, fileInfo.Size))
		}
	}

	// User-defined metadata
	for name, value := range fileInfo.Metadata {
		w.Header().Set(XAmzMetaPrefix+name, value)
	}

	if fileInfo.Mode.IsDir() {
		return
	}

	// get object content
	offset := rangeLower
	size, err := safeConvertInt64ToUint64(fileInfo.Size)
	fileSize := size
	if err != nil {
		return
	}
	if isRangeRead || len(partNumber) > 0 {
		size = rangeUpper - rangeLower + 1
		w.WriteHeader(http.StatusPartialContent)
	}

	// Flow Control
	var writer io.Writer
	if size > DefaultFlowLimitSize {
		writer = rateLimit.GetResponseWriter(vol.owner, param.apiName, w)
	} else {
		writer = w
	}

	// read file
	err = vol.readFile(ctx, fileInfo.Inode, fileSize, param.Object(), writer, offset, size)
	if err != nil {
		span.Errorf("read file fail: volume(%v) path(%v) inode(%v) offset(%v) size(%v) err(%v)",
			vol.Name(), param.Object(), fileInfo.Inode, offset, size, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}
}

func CheckConditionInHeader(r *http.Request, fileInfo *FSFileInfo) *ErrorCode {
	span := spanWithOperation(r.Context(), "CheckConditionInHeader")

	// parse request header
	match := r.Header.Get(IfMatch)
	noneMatch := r.Header.Get(IfNoneMatch)
	modified := r.Header.Get(IfModifiedSince)
	unmodified := r.Header.Get(IfUnmodifiedSince)
	// Checking precondition: If-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if match != "" {
		if matchEag := strings.Trim(match, "\""); matchEag != fileInfo.ETag {
			return PreconditionFailed
		}
	}
	// Checking precondition: If-Modified-Since
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if modified != "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(modified)
		if err != nil {
			span.Errorf("parse RFC1123 time for %v header fail: %v", IfModifiedSince, err)
			return InvalidArgument
		}
		if !fileModTime.After(modifiedTime) {
			return NotModified
		}
	}
	// Checking precondition: If-None-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if noneMatch != "" {
		if noneMatchEtag := strings.Trim(noneMatch, "\""); noneMatchEtag == fileInfo.ETag {
			return NotModified
		}
	}
	// Checking precondition: If-Unmodified-Since
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if unmodified != "" && match == "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(unmodified)
		if err != nil {
			span.Errorf("parse RFC1123 time for %v header fail: %v", IfUnmodifiedSince, err)
			return InvalidArgument
		}
		if fileModTime.After(modifiedTime) {
			return PreconditionFailed
		}
	}
	return nil
}

// Head object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
func (o *ObjectNode) headObjectHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "HeadObject")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	// check args
	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
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

	// get object meta
	fileInfo, _, err := vol.ObjectMeta(ctx, param.Object())
	if err != nil {
		span.Errorf("get object meta fail: volume(%v) path(%v) err(%v)",
			vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}

	// parse request header
	match := r.Header.Get(IfMatch)
	noneMatch := r.Header.Get(IfNoneMatch)
	modified := r.Header.Get(IfModifiedSince)
	unmodified := r.Header.Get(IfUnmodifiedSince)

	// Checking precondition: If-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
	if match != "" {
		if matchEag := strings.Trim(match, "\""); matchEag != fileInfo.ETag {
			errorCode = PreconditionFailed
			return
		}
	}
	// Checking precondition: If-Modified-Since
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
	if modified != "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(modified)
		if err != nil {
			span.Errorf("parse RFC1123 time for %v header fail: %v", IfModifiedSince, err)
			errorCode = InvalidArgument
			return
		}
		if !fileModTime.After(modifiedTime) {
			errorCode = NotModified
			return
		}
	}
	// Checking precondition: If-None-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
	if noneMatch != "" {
		if noneMatchEtag := strings.Trim(noneMatch, "\""); noneMatchEtag == fileInfo.ETag {
			errorCode = NotModified
			return
		}
	}
	// Checking precondition: If-Unmodified-Since
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
	if unmodified != "" && match == "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(unmodified)
		if err != nil {
			span.Errorf("parse RFC1123 time for %v header fail: %v", IfUnmodifiedSince, err)
			errorCode = InvalidArgument
			return
		}
		if fileModTime.After(modifiedTime) {
			errorCode = PreconditionFailed
			return
		}
	}

	// set response header
	w.Header().Set(AcceptRanges, ValueAcceptRanges)
	w.Header().Set(LastModified, formatTimeRFC1123(fileInfo.ModifyTime))
	w.Header().Set(ContentMD5, EmptyContentMD5String)
	if len(fileInfo.MIMEType) > 0 {
		w.Header().Set(ContentType, fileInfo.MIMEType)
	} else {
		w.Header().Set(ContentType, ValueContentTypeStream)
	}
	if len(fileInfo.Disposition) > 0 {
		w.Header().Set(ContentDisposition, fileInfo.Disposition)
	}
	if len(fileInfo.CacheControl) > 0 {
		w.Header().Set(CacheControl, fileInfo.CacheControl)
	}
	if len(fileInfo.Expires) > 0 {
		w.Header().Set(Expires, fileInfo.Expires)
	}
	if len(fileInfo.RetainUntilDate) > 0 {
		w.Header().Set(XAmzObjectLockMode, ComplianceMode)
		w.Header().Set(XAmzObjectLockRetainUntilDate, fileInfo.RetainUntilDate)
	}

	// check request is whether contain param : partNumber
	partNumber := r.URL.Query().Get(ParamPartNumber)
	if len(partNumber) > 0 && fileInfo.Size >= MinParallelDownloadFileSize {
		partNumberInt, err := strconv.ParseUint(partNumber, 10, 64)
		if err != nil {
			span.Errorf("parse request partNumber(%s) fail: %v", partNumber, err)
			errorCode = InvalidArgument
			return
		}
		partSize, partCount, rangeLower, rangeUpper := parsePartInfo(partNumberInt, uint64(fileInfo.Size))
		span.Debugf("partNumber(%v) fileSize(%v) parsed: partSize(%d) partCount(%d) rangeLower(%d) rangeUpper(%d)",
			partNumberInt, fileInfo.Size, partSize, partCount, rangeLower, rangeUpper)
		if partNumberInt > partCount {
			span.Errorf("invalid partNumber: partNumber(%d) > partCount(%d)", partNumberInt, partCount)
			errorCode = NoSuchKey
			return
		}
		w.Header().Set(ContentLength, strconv.Itoa(int(partSize)))
		w.Header().Set(ContentRange, fmt.Sprintf("bytes %d-%d/%d", rangeLower, rangeUpper, fileInfo.Size))
		w.Header().Set(XAmzMpPartsCount, strconv.Itoa(int(partCount)))
		if len(fileInfo.ETag) > 0 && !strings.Contains(fileInfo.ETag, "-") {
			w.Header()[ETag] = []string{fmt.Sprintf("%s-%d", fileInfo.ETag, partCount)}
		}
	} else {
		w.Header().Set(ContentLength, strconv.Itoa(int(fileInfo.Size)))
		if len(fileInfo.ETag) > 0 {
			w.Header()[ETag] = []string{wrapUnescapedQuot(fileInfo.ETag)}
		}
	}

	// User-defined metadata
	for name, value := range fileInfo.Metadata {
		w.Header().Set(XAmzMetaPrefix+name, value)
	}
}

// Delete objects (multiple objects)
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
func (o *ObjectNode) deleteObjectsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "DeleteObjects")
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

	requestMD5 := r.Header.Get(ContentMD5)
	if requestMD5 == "" {
		errorCode = MissingContentMD5
		return
	}

	_, errorCode = VerifyContentLength(r, BodyLimit)
	if errorCode != nil {
		return
	}
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		span.Errorf("read request body fail: %v", err)
		errorCode = UnexpectedContent
		return
	}
	if requestMD5 != GetMD5(bytes) {
		errorCode = BadDigest
		return
	}

	deleteReq := DeleteRequest{}
	err = UnmarshalXMLEntity(bytes, &deleteReq)
	if err != nil {
		span.Errorf("unmarshal xml body fail: body(%v) err(%v)", string(bytes), err)
		errorCode = MalformedXML
		return
	}
	if len(deleteReq.Objects) > 1000 {
		errorCode = EntityTooLarge
		return
	}

	// Sort the key values in reverse order.
	// The purpose of this is to delete the child leaf first and then the parent node.
	// Source:
	//  0. backup/
	//  1. backup/20200101.bak
	//  2. backup/20200102.bak
	// Result:
	//  0. backup/20200102.bak
	//  1. backup/20200101.bak
	//  2. backup/
	sort.SliceStable(deleteReq.Objects, func(i, j int) bool {
		return deleteReq.Objects[i].Key > deleteReq.Objects[j].Key
	})

	vol, acl, policy, err := o.loadBucketMeta(ctx, param.Bucket())
	if err != nil {
		span.Errorf("load bucket metadata fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	userInfo, err := o.getUserInfoByAccessKey(ctx, param.AccessKey())
	if err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}

	allowByAcl := false
	if acl == nil && userInfo.UserID == vol.owner {
		allowByAcl = true
	}
	if acl != nil && acl.IsAllowed(userInfo.UserID, param.Action()) {
		allowByAcl = true
	}

	deletedObjects := make([]Deleted, 0, len(deleteReq.Objects))
	deletedErrors := make([]Error, 0)
	objectKeys := make([]string, 0, len(deleteReq.Objects))
	start := time.Now()
	for _, object := range deleteReq.Objects {
		result := POLICY_UNKNOW
		if policy != nil && !policy.IsEmpty() {
			conditionCheck := map[string]string{
				SOURCEIP: param.sourceIP,
				REFERER:  param.r.Referer(),
				KEYNAME:  object.Key,
				HOST:     param.r.Host,
			}
			result = policy.IsAllowed(param, userInfo.UserID, vol.owner, conditionCheck)
		}
		if result == POLICY_DENY || (result == POLICY_UNKNOW && !allowByAcl) {
			deletedErrors = append(deletedErrors, Error{
				Key:     object.Key,
				Code:    "AccessDenied",
				Message: "Not Allowed By Policy",
			})
			continue
		}
		objectKeys = append(objectKeys, object.Key)
		span.Warnf("delete object: reqUid(%v) remote(%v) volume(%v) path(%v)",
			userInfo.UserID, getRequestIP(r), vol.Name(), object.Key)
		// QPS and Concurrency Limit
		rateLimit := o.AcquireRateLimiter()
		if err = rateLimit.AcquireLimitResource(vol.owner, DELETE_OBJECT); err != nil {
			return
		}
		if err1 := vol.DeletePath(ctx, object.Key); err1 != nil {
			span.Errorf("delete object failed: volume(%v) path(%v) err(%v)", vol.Name(), object.Key, err1)
			if !strings.Contains(err1.Error(), AccessDenied.ErrorMessage) {
				deletedErrors = append(deletedErrors, Error{Key: object.Key, Code: "InternalError", Message: err1.Error()})
			} else {
				deletedErrors = append(deletedErrors, Error{Key: object.Key, Code: "AccessDenied", Message: err1.Error()})
			}
		} else {
			deletedObjects = append(deletedObjects, Deleted{Key: object.Key})
		}
		rateLimit.ReleaseLimitResource(vol.owner, param.apiName)
	}
	span.AppendTrackLog("files.d", start, err)

	deleteResult := DeleteResult{
		Deleted: deletedObjects,
		Error:   deletedErrors,
	}
	response, err1 := MarshalXMLEntity(deleteResult)
	if err1 != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", deleteResult, err1)
	}

	writeSuccessResponseXML(w, response)
}

func extractSrcBucketKey(r *http.Request) (srcBucketId, srcKey, versionId string, err error) {
	copySource := r.Header.Get(XAmzCopySource)
	copySource, err = url.QueryUnescape(copySource)
	if err != nil {
		err = InvalidArgument
		return
	}
	// path could be /bucket/key or bucket/key
	copySource = strings.TrimPrefix(copySource, "/")
	elements := strings.SplitN(copySource, "?versionId=", 2)
	if len(elements) == 1 {
		versionId = ""
	} else {
		versionId = elements[1]
	}
	path := strings.SplitN(elements[0], "/", 2)
	if len(path) == 1 {
		err = InvalidArgument
		return
	}
	srcBucketId, srcKey = path[0], path[1]
	if srcBucketId == "" || srcKey == "" {
		err = InvalidArgument
		return
	}

	return
}

// Copy object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html .
func (o *ObjectNode) copyObjectHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "CopyObject")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
		return
	}
	if len(param.Object()) > MaxKeyLength {
		errorCode = KeyTooLong
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

	// client can reset these system metadata: Content-Type, Content-Disposition
	contentType := r.Header.Get(ContentType)
	contentDisposition := r.Header.Get(ContentDisposition)
	cacheControl := r.Header.Get(CacheControl)
	if len(cacheControl) > 0 && !ValidateCacheControl(cacheControl) {
		errorCode = InvalidCacheArgument
		return
	}
	expires := r.Header.Get(Expires)
	if len(expires) > 0 && !ValidateCacheExpires(expires) {
		errorCode = InvalidCacheArgument
		return
	}

	// metadata directive, direct object node use source file metadata or recreate metadata for target file
	metadataDirective := r.Header.Get(XAmzMetadataDirective)
	// metadata directive default value is COPY
	if len(metadataDirective) == 0 {
		metadataDirective = MetadataDirectiveCopy
	}
	if metadataDirective != MetadataDirectiveCopy && metadataDirective != MetadataDirectiveReplace {
		span.Errorf("invalid %v: %v", XAmzMetadataDirective, metadataDirective)
		errorCode = InvalidArgument
		return
	}
	// parse x-amz-copy-source header
	sourceBucket, sourceObject, _, err := extractSrcBucketKey(r)
	if err != nil {
		span.Errorf("invalid %v: %v", XAmzCopySource, r.Header.Get(XAmzCopySource))
		return
	}

	// check ACL
	userInfo, err := o.getUserInfoByAccessKey(ctx, param.AccessKey())
	if err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}
	acl, err := ParseACL(r, userInfo.UserID, false, vol.GetOwner() != userInfo.UserID)
	if err != nil {
		return
	}

	// get src object meta
	var sourceVol *Volume
	if sourceVol, err = o.getVol(ctx, sourceBucket); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", sourceBucket, err)
		return
	}

	// get object meta
	fileInfo, _, err := sourceVol.ObjectMeta(ctx, sourceObject)
	if err != nil {
		span.Errorf("get object meta fail: volume(%v) path(%v) err(%v)", sourceBucket, sourceObject, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}
	if fileInfo.Size > SinglePutLimit {
		errorCode = EntityTooLarge
		return
	}
	if fileInfo.Size > SinglePutLimit {
		errorCode = EntityTooLarge
		return
	}

	// get header
	copyMatch := r.Header.Get(XAmzCopySourceIfMatch)
	noneMatch := r.Header.Get(XAmzCopySourceIfNoneMatch)
	modified := r.Header.Get(XAmzCopySourceIfModifiedSince)
	unModified := r.Header.Get(XAmzCopySourceIfUnmodifiedSince)

	// response 412
	if modified != "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(modified)
		if err != nil {
			span.Errorf("parse RFC1123 time for %v header fail: %v", XAmzCopySourceIfModifiedSince, err)
			errorCode = InvalidArgument
			return
		}
		if fileModTime.Before(modifiedTime) {
			errorCode = PreconditionFailed
			return
		}
	}
	if unModified != "" {
		fileModTime := fileInfo.ModifyTime
		unmodifiedTime, err := parseTimeRFC1123(unModified)
		if err != nil {
			span.Errorf("parse RFC1123 time for %v header fail: %v", XAmzCopySourceIfUnmodifiedSince, err)
			errorCode = InvalidArgument
			return
		}
		if fileModTime.After(unmodifiedTime) {
			errorCode = PreconditionFailed
			return
		}
	}
	if copyMatch != "" && fileInfo.ETag != copyMatch {
		errorCode = PreconditionFailed
		return
	}
	if noneMatch != "" && fileInfo.ETag == noneMatch {
		errorCode = PreconditionFailed
		return
	}

	// ObjectLock  Config
	objetLock, err := vol.metaLoader.loadObjectLock(ctx)
	if err != nil {
		span.Errorf("load volume objetLock fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}

	// parse user-defined metadata
	metadata := ParseUserDefinedMetadata(r.Header)

	// copy file
	opt := &PutFileOption{
		MIMEType:     contentType,
		Disposition:  contentDisposition,
		Metadata:     metadata,
		CacheControl: cacheControl,
		Expires:      expires,
		ACL:          acl,
		ObjectLock:   objetLock,
	}
	fsFileInfo, err := vol.CopyFile(ctx, sourceVol, sourceObject, param.Object(), metadataDirective, opt)
	if err != nil && err != syscall.EINVAL && err != syscall.EFBIG {
		span.Errorf("copy file fail: srcVol(%v) srcKey(%v) dstVol(%v) dstKey(%v) err(%v)",
			sourceVol.Name(), sourceObject, vol.Name(), param.Object(), err)
		return
	}
	if err == syscall.EINVAL {
		span.Errorf("target file existed and mode conflict: srcVol(%v) srcKey(%v) dstVol(%v) dstKey(%v) err(%v)",
			sourceVol.Name(), sourceObject, vol.Name(), param.Object(), err)
		errorCode = ObjectModeConflict
		return
	}
	if err == syscall.EFBIG {
		span.Errorf("source file size greater than 5GB: srcVol(%v) srcKey(%v) dstVol(%v) dstKey(%v) err(%v)",
			sourceVol.Name(), sourceObject, vol.Name(), param.Object(), err)
		errorCode = CopySourceSizeTooLarge
		return
	}

	copyResult := CopyResult{
		ETag:         "\"" + fsFileInfo.ETag + "\"",
		LastModified: formatTimeISO(fsFileInfo.ModifyTime),
	}
	response, ierr := MarshalXMLEntity(copyResult)
	if ierr != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", copyResult, ierr)
	}

	writeSuccessResponseXML(w, response)
}

// List objects v1
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
func (o *ObjectNode) getBucketV1Handler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "ListObjects")
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

	// get options
	marker := r.URL.Query().Get(ParamMarker)
	prefix := r.URL.Query().Get(ParamPrefix)
	maxKeys := r.URL.Query().Get(ParamMaxKeys)
	delimiter := r.URL.Query().Get(ParamPartDelimiter)
	encodingType := r.URL.Query().Get(ParamEncodingType)

	var maxKeysInt uint64
	if maxKeys != "" {
		maxKeysInt, err = strconv.ParseUint(maxKeys, 10, 16)
		if err != nil {
			errorCode = InvalidArgument
			return
		}
		if maxKeysInt > MaxKeys {
			maxKeysInt = MaxKeys
		}
	} else {
		maxKeysInt = uint64(MaxKeys)
	}

	// Validate encoding type option
	if encodingType != "" && encodingType != "url" {
		errorCode = InvalidArgument
		return
	}

	if marker != "" && prefix != "" && !strings.HasPrefix(marker, prefix) {
		errorCode = InvalidArgument
		return
	}

	// list files
	option := &ListFilesV1Option{
		Prefix:     prefix,
		Delimiter:  delimiter,
		Marker:     marker,
		MaxKeys:    maxKeysInt,
		OnlyObject: true,
	}
	result, err := vol.ListFilesV1(ctx, option)
	if err != nil {
		span.Errorf("list files fail: volume(%v) option(%+v) err(%v)", vol.Name(), option, err)
		return
	}
	// The result of next list request should not include nextMarker.
	if result.Truncated && len(result.Files) != 0 {
		result.NextMarker = result.Files[len(result.Files)-1].Path
	}

	// get owner
	bucketOwner := NewBucketOwner(vol)
	contents := make([]*Content, 0, len(result.Files))
	for _, file := range result.Files {
		if file.Mode == 0 {
			// Invalid file mode, which means that the inode of the file may not exist.
			// Record and filter out the file.
			span.Warnf("invalid file found: volume(%v) path(%v) inode(%v)",
				vol.Name(), file.Path, file.Inode)
			continue
		}
		content := &Content{
			Key:          encodeKey(file.Path, encodingType),
			LastModified: formatTimeISO(file.ModifyTime),
			ETag:         wrapUnescapedQuot(file.ETag),
			Size:         int(file.Size),
			StorageClass: StorageClassStandard,
			Owner:        bucketOwner,
		}
		contents = append(contents, content)
	}

	commonPrefixes := make([]*CommonPrefix, 0)
	for _, prefix := range result.CommonPrefixes {
		commonPrefix := &CommonPrefix{
			Prefix: prefix,
		}
		commonPrefixes = append(commonPrefixes, commonPrefix)
	}

	listBucketResult := &ListBucketResult{
		Bucket:         param.Bucket(),
		Prefix:         prefix,
		Marker:         marker,
		MaxKeys:        int(maxKeysInt),
		Delimiter:      delimiter,
		IsTruncated:    result.Truncated,
		NextMarker:     result.NextMarker,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}
	response, err := MarshalXMLEntity(listBucketResult)
	if err != nil {
		span.Errorf("marshal xml response fail: %v", err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// List objects version 2
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func (o *ObjectNode) getBucketV2Handler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "ListObjectsV2")
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

	// get options
	prefix := r.URL.Query().Get(ParamPrefix)
	maxKeys := r.URL.Query().Get(ParamMaxKeys)
	delimiter := r.URL.Query().Get(ParamPartDelimiter)
	contToken := r.URL.Query().Get(ParamContToken)
	fetchOwner := r.URL.Query().Get(ParamFetchOwner)
	startAfter := r.URL.Query().Get(ParamStartAfter)
	encodingType := r.URL.Query().Get(ParamEncodingType)

	var maxKeysInt uint64
	if maxKeys != "" {
		maxKeysInt, err = strconv.ParseUint(maxKeys, 10, 16)
		if err != nil {
			errorCode = InvalidArgument
			return
		}
		if maxKeysInt > MaxKeys {
			maxKeysInt = MaxKeys
		}
	} else {
		maxKeysInt = MaxKeys
	}

	var fetchOwnerBool bool
	if fetchOwner != "" {
		fetchOwnerBool, err = strconv.ParseBool(fetchOwner)
		if err != nil {
			errorCode = InvalidArgument
			return
		}
	} else {
		fetchOwnerBool = false
	}

	// Validate encoding type option
	if encodingType != "" && encodingType != "url" {
		errorCode = InvalidArgument
		return
	}

	var marker string
	if contToken != "" {
		marker = contToken
	} else {
		marker = startAfter
	}
	if marker != "" && prefix != "" && !strings.HasPrefix(marker, prefix) {
		errorCode = InvalidArgument
		return
	}

	// list files
	option := &ListFilesV2Option{
		Delimiter:  delimiter,
		MaxKeys:    maxKeysInt,
		Prefix:     prefix,
		ContToken:  contToken,
		FetchOwner: fetchOwnerBool,
		StartAfter: startAfter,
	}
	result, err := vol.ListFilesV2(ctx, option)
	if err != nil {
		span.Errorf("list files fail: volume(%v) option(%+v) err(%v)", vol.Name(), option, err)
		return
	}
	// The result of next list request should not include continuationToken.
	if result.Truncated && len(result.Files) > 0 {
		result.NextToken = result.Files[len(result.Files)-1].Path
	}

	// get owner
	var bucketOwner *BucketOwner
	if fetchOwnerBool {
		bucketOwner = NewBucketOwner(vol)
	}

	contents := make([]*Content, 0)
	if len(result.Files) > 0 {
		for _, file := range result.Files {
			if file.Mode == 0 {
				// Invalid file mode, which means that the inode of the file may not exist.
				// Record and filter out the file.
				span.Warnf("invalid file found: volume(%v) path(%v) inode(%v)",
					vol.Name(), file.Path, file.Inode)
				result.KeyCount--
				continue
			}
			content := &Content{
				Key:          encodeKey(file.Path, encodingType),
				LastModified: formatTimeISO(file.ModifyTime),
				ETag:         wrapUnescapedQuot(file.ETag),
				Size:         int(file.Size),
				StorageClass: StorageClassStandard,
				Owner:        bucketOwner,
			}
			contents = append(contents, content)
		}
	}

	commonPrefixes := make([]*CommonPrefix, 0)
	for _, prefix := range result.CommonPrefixes {
		commonPrefix := &CommonPrefix{
			Prefix: prefix,
		}
		commonPrefixes = append(commonPrefixes, commonPrefix)
	}

	listBucketResult := ListBucketResultV2{
		Name:           param.Bucket(),
		Prefix:         prefix,
		Token:          contToken,
		NextToken:      result.NextToken,
		KeyCount:       result.KeyCount,
		MaxKeys:        maxKeysInt,
		Delimiter:      delimiter,
		IsTruncated:    result.Truncated,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}
	response, err := MarshalXMLEntity(listBucketResult)
	if err != nil {
		span.Errorf("marshal xml response fail: %v", err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// Put object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
func (o *ObjectNode) putObjectHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutObject")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
		return
	}
	if len(param.Object()) > MaxKeyLength {
		errorCode = KeyTooLong
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	// Get request MD5, if request MD5 is not empty, compute and verify it.
	requestMD5 := r.Header.Get(ContentMD5)
	if requestMD5 != "" {
		decoded, err := base64.StdEncoding.DecodeString(requestMD5)
		if err != nil {
			errorCode = InvalidDigest
			return
		}
		requestMD5 = hex.EncodeToString(decoded)
	}

	// ObjectLock  Config
	objetLock, err := vol.metaLoader.loadObjectLock(ctx)
	if err != nil {
		span.Errorf("load objetLock fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}
	if objetLock != nil && objetLock.ToRetention() != nil && requestMD5 == "" {
		errorCode = NoContentMd5HeaderErr
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	// Check 'x-amz-tagging' header
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
	var tagging *Tagging
	if xAmxTagging := r.Header.Get(XAmzTagging); xAmxTagging != "" {
		if tagging, err = ParseTagging(xAmxTagging); err != nil {
			errorCode = InvalidArgument
			return
		}
		if err = tagging.Validate(); err != nil {
			return
		}
	}

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(ctx, param.AccessKey()); err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}

	// Check ACL
	acl, err := ParseACL(r, userInfo.UserID, false, vol.GetOwner() != userInfo.UserID)
	if err != nil {
		return
	}

	// Verify ContentLength
	length := GetContentLength(r)
	if length > SinglePutLimit {
		errorCode = EntityTooLarge
		return
	}
	if length < 0 {
		errorCode = MissingContentLength
		return
	}

	// Get the requested content-type.
	// In addition to being used to manage data types, it is used to distinguish
	// whether the request is to create a directory.
	contentType := r.Header.Get(ContentType)
	// Get request header : content-disposition
	contentDisposition := r.Header.Get(ContentDisposition)
	// Get request header : Cache-Control
	cacheControl := r.Header.Get(CacheControl)
	if len(cacheControl) > 0 && !ValidateCacheControl(cacheControl) {
		errorCode = InvalidCacheArgument
		return
	}
	// Get request header : Expires
	expires := r.Header.Get(Expires)
	if len(expires) > 0 && !ValidateCacheExpires(expires) {
		errorCode = InvalidCacheArgument
		return
	}
	// Checking user-defined metadata
	metadata := ParseUserDefinedMetadata(r.Header)

	// Flow Control
	var reader io.Reader
	if length > DefaultFlowLimitSize {
		reader = rateLimit.GetReader(vol.owner, param.apiName, r.Body)
	} else {
		reader = r.Body
	}

	// Put Object
	opt := &PutFileOption{
		MIMEType:     contentType,
		Disposition:  contentDisposition,
		Tagging:      tagging,
		Metadata:     metadata,
		CacheControl: cacheControl,
		Expires:      expires,
		ACL:          acl,
		ObjectLock:   objetLock,
	}
	fsFileInfo, err := vol.PutObject(ctx, param.Object(), reader, opt)
	if err != nil {
		span.Errorf("put object fail: volume(%v) path(%v) option(%+v) err(%v)",
			vol.Name(), param.Object(), opt, err)
		err = handlePutObjectErr(err)
		return
	}

	// check content MD5
	if requestMD5 != "" && requestMD5 != fsFileInfo.ETag {
		errorCode = BadDigest
		return
	}

	// set response header
	w.Header()[ETag] = []string{wrapUnescapedQuot(fsFileInfo.ETag)}
}

// Post object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html
func (o *ObjectNode) postObjectHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PostObject")
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

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(ctx, param.AccessKey()); err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}

	// qps and concurrency limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	// parse the request form
	formReq := NewFormRequest(r)
	if err = formReq.ParseMultipartForm(); err != nil {
		span.Errorf("parse post form fail: volume(%v) err(%v)", param.Bucket(), err)
		errorCode = MalformedPOSTRequest.Copy()
		errorCode.ErrorMessage = fmt.Sprintf("%s (%v)", errorCode.ErrorMessage, err)
		return
	}

	// content-md5 check if specified in the request
	requestMD5 := formReq.MultipartFormValue(ContentMD5)
	if requestMD5 != "" {
		decoded, err := base64.StdEncoding.DecodeString(requestMD5)
		if err != nil {
			errorCode = MalformedPOSTRequest.Copy()
			errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "Invalid Content-MD5")
			return
		}
		requestMD5 = hex.EncodeToString(decoded)
	}

	// object lock check
	objetLock, err := vol.metaLoader.loadObjectLock(ctx)
	if err != nil {
		span.Errorf("load objetLock fail: volume(%v) err(%v)", vol.Name(), err)
		return
	}
	if objetLock != nil && objetLock.ToRetention() != nil && requestMD5 == "" {
		errorCode = MalformedPOSTRequest.Copy()
		errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "No Content-MD5 with Object Lock")
		return
	}

	// other form fields check
	key := formReq.MultipartFormValue("key")
	if key == "" {
		errorCode = MalformedPOSTRequest.Copy()
		errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "Missing key")
		return
	}
	key = strings.Replace(key, "${filename}", formReq.FileName(), -1)
	if !utf8.ValidString(key) || len(key) > MaxKeyLength {
		errorCode = MalformedPOSTRequest.Copy()
		errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "Invalid utf8 string or the key is too long")
		return
	}

	var aclInfo *AccessControlPolicy
	if acl := formReq.MultipartFormValue("acl"); acl != "" {
		if aclInfo, err = ParseCannedAcl(acl, userInfo.UserID); err != nil {
			errorCode = MalformedPOSTRequest.Copy()
			errorCode.ErrorMessage = fmt.Sprintf("%s (%v)", errorCode.ErrorMessage, err)
			return
		}
	}

	var tagging *Tagging
	if taggingRaw := formReq.MultipartFormValue("tagging"); taggingRaw != "" {
		tagging = NewTagging()
		if err = xml.Unmarshal([]byte(taggingRaw), tagging); err != nil {
			errorCode = MalformedPOSTRequest.Copy()
			errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "Invalid tagging")
			return
		}
		if err = tagging.Validate(); err != nil {
			errorCode = MalformedPOSTRequest.Copy()
			errorCode.ErrorMessage = fmt.Sprintf("%s (%v)", errorCode.ErrorMessage, err)
			return
		}
	}

	successStatus := formReq.MultipartFormValue("success_action_status")
	successRedirect := formReq.MultipartFormValue("success_action_redirect")
	var successRedirectURL *url.URL
	if successRedirect != "" {
		if successRedirectURL, err = url.Parse(successRedirect); err != nil {
			errorCode = MalformedPOSTRequest.Copy()
			errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "Invalid success_action_redirect")
			return
		}
	}

	contentType := formReq.MultipartFormValue("content-type")
	contentDisposition := formReq.MultipartFormValue("content-disposition")
	cacheControl := formReq.MultipartFormValue("cache-control")
	if cacheControl != "" && !ValidateCacheControl(cacheControl) {
		errorCode = MalformedPOSTRequest.Copy()
		errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "Invalid cache-control")
		return
	}

	expires := formReq.MultipartFormValue("expires")
	if expires != "" && !ValidateCacheExpires(expires) {
		errorCode = MalformedPOSTRequest.Copy()
		errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "Invalid expires")
		return
	}

	policy := formReq.MultipartFormValue("policy")
	if policy == "" {
		errorCode = MalformedPOSTRequest.Copy()
		errorCode.ErrorMessage = fmt.Sprintf("%s (%s)", errorCode.ErrorMessage, "Missing policy")
		return
	}

	// read the file, the rest will be written to a temporary file if exceed
	f, size, err := formReq.FormFile(10 << 20)
	if err != nil {
		errorCode = MalformedPOSTRequest.Copy()
		errorCode.ErrorMessage = fmt.Sprintf("%s (%v)", errorCode.ErrorMessage, err)
		return
	}
	defer f.Close()
	if size > SinglePutLimit {
		errorCode = EntityTooLarge
		return
	}

	metadata := make(map[string]string)
	// policy match forms
	forms := make(map[string]string)
	for name, values := range formReq.MultipartForm.Value {
		name = strings.ToLower(name)
		if strings.HasPrefix(name, XAmzMetaPrefix) {
			forms[name] = strings.Join(values, ",")
			metadata[name[len(XAmzMetaPrefix):]] = strings.Join(values, ",")
		} else if len(values) > 0 {
			forms[name] = values[0]
		}
	}
	forms["bucket"] = param.Bucket()
	forms["key"] = key
	forms["content-length"] = strconv.FormatInt(size, 10)

	// policy condition check
	if err = PolicyConditionMatch(policy, forms); err != nil {
		return
	}

	// flow control
	var reader io.Reader
	if size > DefaultFlowLimitSize {
		reader = rateLimit.GetReader(vol.owner, param.apiName, f)
	} else {
		reader = f
	}

	// put object
	putOpt := &PutFileOption{
		MIMEType:     contentType,
		Disposition:  contentDisposition,
		Tagging:      tagging,
		Metadata:     metadata,
		CacheControl: cacheControl,
		Expires:      expires,
		ACL:          aclInfo,
		ObjectLock:   objetLock,
	}
	fsFileInfo, err := vol.PutObject(ctx, key, reader, putOpt)
	if err != nil {
		span.Errorf("put object fail: volume(%v) path(%v) option(%+v) err(%v)",
			vol.Name(), key, putOpt, err)
		err = handlePutObjectErr(err)
		return
	}

	// check content-md5 of actual data if specified in the request
	if requestMD5 != "" && requestMD5 != fsFileInfo.ETag {
		errorCode = BadDigest
		return
	}

	// set response header
	etag := wrapUnescapedQuot(fsFileInfo.ETag)
	w.Header()[ETag] = []string{etag}

	// return response depending on success_action_xxx parameter
	if successRedirectURL != nil {
		query := successRedirectURL.Query()
		query.Set("bucket", param.Bucket())
		query.Set("key", key)
		query.Set("etag", etag)
		successRedirectURL.RawQuery = query.Encode()
		w.Header().Set(Location, successRedirectURL.String())
		w.WriteHeader(http.StatusSeeOther)
		return
	}
	switch successStatus {
	case "200":
		w.WriteHeader(http.StatusOK)
	case "201":
		response := NewS3UploadObject()
		response.Bucket = param.Bucket()
		response.Key = key
		response.ETag = etag
		response.Location = "/" + response.Bucket + "/" + response.Key
		writeResponse(w, http.StatusCreated, []byte(response.String()), ValueContentTypeXML)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func handlePutObjectErr(err error) error {
	if err == syscall.EINVAL {
		return ObjectModeConflict
	}
	if err == syscall.EEXIST {
		return ConflictUploadRequest
	}
	if err == io.ErrUnexpectedEOF {
		return EntityTooSmall
	}
	return err
}

// Delete object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html .
func (o *ObjectNode) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "DeleteObject")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
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

	// Delete file
	start := time.Now()
	err = vol.DeletePath(ctx, param.Object())
	span.AppendTrackLog("file.d", start, err)
	if err != nil {
		span.Errorf("delete file fail: volume(%v) path(%v) err(%v)", vol.Name(), param.Object(), err)
		if strings.Contains(err.Error(), AccessDenied.ErrorMessage) {
			err = AccessDenied
		}
		return
	}
	span.Warnf("delete object success: remote(%v) volume(%v) path(%v)",
		getRequestIP(r), vol.Name(), param.Object())

	w.WriteHeader(http.StatusNoContent)
}

// Get object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
func (o *ObjectNode) getObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetObjectTagging")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
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

	// get xattr
	xattrInfo, err := vol.GetXAttr(ctx, param.object, XAttrKeyOSSTagging)
	if err != nil {
		span.Errorf("get volume XAttr fail: volume(%v) path(%v) err(%v)",
			vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}

	output, _ := ParseTagging(string(xattrInfo.Get(XAttrKeyOSSTagging)))
	response, err := MarshalXMLEntity(output)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", output, err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// Put object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
func (o *ObjectNode) putObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutObjectTagging")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
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
	var requestBody []byte
	if requestBody, err = io.ReadAll(r.Body); err != nil {
		span.Errorf("read request body fail: %v", err)
		errorCode = InvalidArgument
		return
	}

	tagging := NewTagging()
	if err = xml.Unmarshal(requestBody, tagging); err != nil {
		span.Errorf("unmarshal xml body fail: body(%v) err(%v)", string(requestBody), err)
		errorCode = InvalidArgument
		return
	}
	if err = tagging.Validate(); err != nil {
		return
	}

	err = vol.SetXAttr(ctx, param.object, XAttrKeyOSSTagging, []byte(tagging.Encode()), false)
	if err != nil {
		span.Errorf("set tagging xattr fail: volume(%v) path(%v) err(%v)",
			vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}
}

// Delete object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
func (o *ObjectNode) deleteObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "DeleteObjectTagging")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
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

	if err = vol.DeleteXAttr(ctx, param.object, XAttrKeyOSSTagging); err != nil {
		span.Errorf("delete tagging xAttr fail: volume(%v) path(%v) err(%v)",
			vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Put object extend attribute (xattr)
func (o *ObjectNode) putObjectXAttrHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "PutObjectXAttr")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.bucket); err != nil {
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
	var requestBody []byte
	if requestBody, err = io.ReadAll(r.Body); err != nil {
		span.Errorf("read request body fail: %v", err)
		errorCode = &ErrorCode{
			ErrorCode:    "BadRequest",
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}

	putXAttrRequest := PutXAttrRequest{}
	if err = xml.Unmarshal(requestBody, &putXAttrRequest); err != nil {
		span.Errorf("unmarshal xml body fail: body(%v) err(%v)", string(requestBody), err)
		errorCode = &ErrorCode{
			ErrorCode:    "BadRequest",
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}
	key, value := putXAttrRequest.XAttr.Key, putXAttrRequest.XAttr.Value
	if len(key) == 0 {
		return
	}

	if err = vol.SetXAttr(ctx, param.object, key, []byte(value), true); err != nil {
		span.Errorf("set %v xAttr fail: volume(%v) path(%v) err(%v)",
			key, vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}
}

// Get object extend attribute (xattr)
func (o *ObjectNode) getObjectXAttrHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetObjectXAttr")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
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

	var xattrKey string
	if xattrKey = param.GetVar(ParamKey); len(xattrKey) == 0 {
		errorCode = InvalidArgument
		return
	}

	info, err := vol.GetXAttr(ctx, param.object, xattrKey)
	if err != nil {
		span.Errorf("get %v xAttr fail: volume(%v) path(%v) err(%v)",
			xattrKey, vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}

	output := GetXAttrOutput{
		XAttr: &XAttr{
			Key:   xattrKey,
			Value: string(info.Get(xattrKey)),
		},
	}
	response, err := MarshalXMLEntity(&output)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", output, err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// Delete object extend attribute (xattr)
func (o *ObjectNode) deleteObjectXAttrHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "DeleteObjectXAttr")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
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

	var xattrKey string
	if xattrKey = param.GetVar(ParamKey); len(xattrKey) == 0 {
		errorCode = InvalidArgument
		return
	}

	if err = vol.DeleteXAttr(ctx, param.object, xattrKey); err != nil {
		span.Errorf("delete %v xAttr fail: volume(%v) path(%v) err(%v)",
			xattrKey, vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}
}

// List object xattrs
func (o *ObjectNode) listObjectXAttrs(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "ListObjectXAttrs")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.bucket); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	keys, err := vol.ListXAttrs(ctx, param.object)
	if err != nil {
		span.Errorf("list object xAttrs fail: volume(%v) object(%v) err(%v)",
			vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}

	output := ListXAttrsOutput{
		Keys: keys,
	}
	response, err := MarshalXMLEntity(&output)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", output, err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// GetObjectRetention
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html
func (o *ObjectNode) getObjectRetentionHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "GetObjectRetention")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	// check args
	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
		return
	}

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
	}

	// get object meta
	_, xattrs, err := vol.ObjectMeta(ctx, param.Object())
	if err != nil {
		span.Errorf("get object meta fail: volume(%v) path(%v) err(%v)",
			vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}
	retainUntilDate := string(xattrs.Get(XAttrKeyOSSLock))
	if retainUntilDate == "" {
		errorCode = NoSuchObjectLockConfiguration
		return
	}
	retainUntilDateInt64, err := strconv.ParseInt(retainUntilDate, 10, 64)
	if err != nil {
		span.Errorf("parse save retainUntilDate fail: volume(%v) path(%v) retainUntilDate(%v) err(%v)",
			vol.Name(), param.Object(), retainUntilDate, err)
		return
	}

	var objectRetention ObjectRetention
	objectRetention.Mode = ComplianceMode
	objectRetention.RetainUntilDate = RetentionDate{Time: time.Unix(0, retainUntilDateInt64).UTC()}
	b, err := xml.Marshal(objectRetention)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%+v) err(%v)", objectRetention, err)
		return
	}

	writeSuccessResponseXML(w, b)
}

func parsePartInfo(partNumber uint64, fileSize uint64) (partSize, partCount, rangeLower, rangeUpper uint64) {
	partSizeConst := ParallelDownloadPartSize
	partCount = fileSize / uint64(partSizeConst)
	lastSize := fileSize % uint64(partSizeConst)
	if lastSize > 0 {
		partCount += 1
	}

	rangeLower = uint64(partSizeConst) * (partNumber - 1)
	if lastSize > 0 && partNumber == partCount {
		partSize = lastSize
		rangeUpper = fileSize - 1
	} else {
		partSize = uint64(partSizeConst)
		rangeUpper = (partSize * partNumber) - 1
	}
	if partNumber > partCount {
		partSize, partCount, rangeLower, rangeUpper = 0, 0, 0, 0
	}

	return
}

func GetContentLength(r *http.Request) int64 {
	dcl := r.Header.Get(HeaderNameXAmzDecodedContentLength)
	if dcl != "" {
		length, err := strconv.ParseInt(dcl, 10, 64)
		if err == nil {
			return length
		}
	}
	return r.ContentLength
}

func VerifyContentLength(r *http.Request, bodyLimit int64) (int64, *ErrorCode) {
	dcl := r.Header.Get(HeaderNameXAmzDecodedContentLength)
	length := r.ContentLength
	if dcl != "" {
		l, err := strconv.ParseInt(dcl, 10, 64)
		if err == nil {
			length = l
		}
	}
	if length > bodyLimit {
		return 0, EntityTooLarge
	}
	if length < 0 {
		return 0, MissingContentLength
	}
	return length, nil
}
