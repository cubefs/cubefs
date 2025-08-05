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

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

var (
	rangeRegexp  = regexp.MustCompile(`^bytes=(\d)*-(\d)*$`)
	MaxKeyLength = 750
)

// Get object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (o *ObjectNode) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getObjectHandler: load volume fail: requestID(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
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
			log.LogErrorf("getObjectHandler: invalid range header: requestID(%v) volume(%v) path(%v) rangeOpt(%v)",
				GetRequestID(r), param.Bucket(), param.Object(), rangeOpt)
			errorCode = InvalidRange
			return
		}
		hyphenIndex := strings.Index(rangeOpt, "-")
		if hyphenIndex < 0 {
			log.LogErrorf("getObjectHandler: invalid range header: requestID(%v) volume(%v) path(%v) rangeOpt(%v)",
				GetRequestID(r), param.Bucket(), param.Object(), rangeOpt)
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
				log.LogErrorf("getObjectHandler: parse range lower fail: requestID(%v) volume(%v) path(%v) rangeOpt(%v) err(%v)",
					GetRequestID(r), param.Bucket(), param.Object(), rangeOpt, err)
				errorCode = InvalidRange
				return
			}
		}
		if len(upperPart) > 0 {
			if rangeUpper, err = strconv.ParseUint(upperPart, 10, 64); err != nil {
				log.LogErrorf("getObjectHandler: parse range upper fail: requestID(%v) volume(%v) path(%v) rangeOpt(%v) err(%v)",
					GetRequestID(r), param.Bucket(), param.Object(), rangeOpt, err)
				errorCode = InvalidRange
				return
			}
		}
		if rangeUpper < rangeLower {
			// upper enabled and lower than lower side
			log.LogErrorf("getObjectHandler: invalid range header: requestID(%v) volume(%v) path(%v) rangeOpt(%v)",
				GetRequestID(r), param.Bucket(), param.Object(), rangeOpt)
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
	start := time.Now()
	fileInfo, xattr, err := vol.ObjectMeta(param.Object())
	span.AppendTrackLog("meta.r", start, err)
	if err != nil {
		log.LogErrorf("getObjectHandler: get file meta fail: requestId(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), err)
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
			if rangeLower > rangeUpper+1 {
				rangeLower = 0
			} else {
				rangeLower = rangeUpper + 1 - rangeLower
			}
		}
	}

	// compute content length
	contentLength := uint64(fileInfo.Size)
	if isRangeRead {
		if rangeUpper < rangeLower {
			log.LogErrorf("getObjectHandler: invalid range header: requestID(%v) volume(%v) path(%v) rangeOpt(%v)",
				GetRequestID(r), param.Bucket(), param.Object(), rangeOpt)
			errorCode = InvalidRange
			return
		}
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
			log.LogErrorf("getObjectHandler: parse partNumber(%v) fail: requestID(%v) volume(%v) path(%v) err(%v)",
				partNumber, GetRequestID(r), param.Bucket(), param.Object(), err)
			errorCode = InvalidArgument
			return
		}
		partSize, partCount, rangeLower, rangeUpper = parsePartInfo(partNumberInt, uint64(fileInfo.Size))
		log.LogDebugf("getObjectHandler: partNumber(%v) fileSize(%v) parsed: partSize(%d) partCount(%d) rangeLower(%d) rangeUpper(%d)",
			partNumberInt, fileInfo.Size, partSize, partCount, rangeLower, rangeUpper)
		if partNumberInt > partCount {
			log.LogErrorf("getObjectHandler: partNumber(%d) > partCount(%d): requestID(%v) volume(%v) path(%v)",
				partNumberInt, partCount, GetRequestID(r), param.Bucket(), param.Object())
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
	start = time.Now()
	err = vol.readFile(fileInfo.Inode, fileSize, param.Object(), writer, offset, size, fileInfo.StorageClass)
	span.AppendTrackLog("file.r", start, err)
	if err != nil {
		log.LogErrorf("getObjectHandler: read file fail: requestID(%v) volume(%v) path(%v) offset(%v) size(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), offset, size, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}
}

func CheckConditionInHeader(r *http.Request, fileInfo *FSFileInfo) *ErrorCode {
	// parse request header
	match := r.Header.Get(IfMatch)
	noneMatch := r.Header.Get(IfNoneMatch)
	modified := r.Header.Get(IfModifiedSince)
	unmodified := r.Header.Get(IfUnmodifiedSince)
	// Checking precondition: If-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if match != "" {
		if matchEag := strings.Trim(match, "\""); matchEag != fileInfo.ETag {
			log.LogDebugf("getObjectHandler: object eTag(%s) not match If-Match header value(%s), requestId(%v)",
				fileInfo.ETag, matchEag, GetRequestID(r))
			return PreconditionFailed

		}
	}
	// Checking precondition: If-Modified-Since
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if modified != "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(modified)
		if err != nil {
			log.LogErrorf("getObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			return InvalidArgument
		}
		if !fileModTime.After(modifiedTime) {
			log.LogInfof("getObjectHandler: file modified time not after than specified time: requestID(%v)", GetRequestID(r))
			return NotModified
		}
	}
	// Checking precondition: If-None-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if noneMatch != "" {
		if noneMatchEtag := strings.Trim(noneMatch, "\""); noneMatchEtag == fileInfo.ETag {
			log.LogErrorf("getObjectHandler: object eTag(%s) match If-None-Match header value(%s), requestId(%v)",
				fileInfo.ETag, noneMatchEtag, GetRequestID(r))
			return NotModified
		}
	}
	// Checking precondition: If-Unmodified-Since
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if unmodified != "" && match == "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(unmodified)
		if err != nil {
			log.LogErrorf("getObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			return InvalidArgument
		}
		if fileModTime.After(modifiedTime) {
			log.LogInfof("getObjectHandler: file modified time after than specified time: requestID(%v)", GetRequestID(r))
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("headObjectHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	// get object meta
	start := time.Now()
	fileInfo, _, err := vol.ObjectMeta(param.Object())
	span.AppendTrackLog("meta.r", start, err)
	if err != nil {
		log.LogErrorf("headObjectHandler: get file meta fail: requestId(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), err)
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
			log.LogDebugf("headObjectHandler: object eTag(%s) not match If-Match header value(%s), requestId(%v)",
				fileInfo.ETag, matchEag, GetRequestID(r))
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
			log.LogDebugf("headObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if !fileModTime.After(modifiedTime) {
			log.LogDebugf("headObjectHandler: file modified time not after than specified time: requestID(%v)", GetRequestID(r))
			errorCode = NotModified
			return
		}
	}
	// Checking precondition: If-None-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
	if noneMatch != "" {
		if noneMatchEtag := strings.Trim(noneMatch, "\""); noneMatchEtag == fileInfo.ETag {
			log.LogDebugf("headObjectHandler: object eTag(%s) match If-None-Match header value(%s), requestId(%v)",
				fileInfo.ETag, noneMatchEtag, GetRequestID(r))
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
			log.LogDebugf("headObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if fileModTime.After(modifiedTime) {
			log.LogDebugf("headObjectHandler: file modified time after than specified time: requestID(%v)", GetRequestID(r))
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
			log.LogErrorf("headObjectHandler: parse param partNumber(%s) fail: requestID(%v) err(%v)", partNumber, GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		partSize, partCount, rangeLower, rangeUpper := parsePartInfo(partNumberInt, uint64(fileInfo.Size))
		log.LogDebugf("headObjectHandler: parsed partSize(%d), partCount(%d), rangeLower(%d), rangeUpper(%d)", partSize, partCount, rangeLower, rangeUpper)
		if partNumberInt > partCount {
			log.LogErrorf("headObjectHandler: param partNumber(%d) is more then partCount(%d): requestID(%v)", partNumberInt, partCount, GetRequestID(r))
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

	span := trace.SpanFromContextSafe(r.Context())
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}

	if _, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("deleteObjectsHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
		log.LogErrorf("deleteObjectsHandler: read request body fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
		log.LogErrorf("deleteObjectsHandler: unmarshal xml fail: requestID(%v) volume(%v) request(%v) err(%v)",
			GetRequestID(r), param.Bucket(), string(bytes), err)
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

	vol, acl, policy, err := o.loadBucketMeta(param.Bucket())
	if err != nil {
		log.LogErrorf("deleteObjectsHandler: load bucket metadata fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	userInfo, err := o.getUserInfoByAccessKeyV2(param.AccessKey())
	if err != nil {
		log.LogErrorf("deleteObjectsHandler: get userinfo fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.AccessKey(), err)
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
		log.LogWarnf("deleteObjectsHandler: delete path: requestID(%v) remote(%v) volume(%v) path(%v)",
			GetRequestID(r), getRequestIP(r), vol.Name(), object.Key)
		// QPS and Concurrency Limit
		rateLimit := o.AcquireRateLimiter()
		if err = rateLimit.AcquireLimitResource(vol.owner, DELETE_OBJECT); err != nil {
			return
		}
		if err1 := vol.DeletePath(object.Key); err1 != nil {
			log.LogErrorf("deleteObjectsHandler: delete object failed: requestID(%v) volume(%v) path(%v) err(%v)",
				GetRequestID(r), vol.Name(), object.Key, err1)
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
		log.LogErrorf("deleteObjectsHandler: xml marshal fail: requestID(%v) volume(%v) result(%+v) err(%v)",
			GetRequestID(r), param.Bucket(), deleteResult, err1)
	}

	writeSuccessResponseXML(w, response)
}

func extractSrcBucketKey(r *http.Request) (srcBucketId, srcKey, versionId string, err error) {
	copySource := r.Header.Get(XAmzCopySource)
	copySource, err = url.QueryUnescape(copySource)
	if err != nil {
		return "", "", "", InvalidArgument
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
		return "", "", "", InvalidArgument
	}
	srcBucketId, srcKey = path[0], path[1]
	if srcBucketId == "" || srcKey == "" {
		return "", "", "", InvalidArgument
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("copyObjectHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
		log.LogErrorf("copyObjectHandler: x-amz-metadata-directive invalid: requestID(%v) volume(%v) x-amz-metadata-directive(%v) err(%v)",
			GetRequestID(r), param.Bucket(), metadataDirective, err)
		errorCode = InvalidArgument
		return
	}
	// parse x-amz-copy-source header
	sourceBucket, sourceObject, _, err := extractSrcBucketKey(r)
	if err != nil {
		log.LogErrorf("copyObjectHandler: copySource(%v) argument invalid: requestID(%v) volume(%v) err(%v)",
			r.Header.Get(XAmzCopySource), GetRequestID(r), param.Bucket(), err)
		return
	}

	// check ACL
	userInfo, err := o.getUserInfoByAccessKeyV2(param.AccessKey())
	if err != nil {
		log.LogErrorf("copyObjectHandler: get user info fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.AccessKey(), err)
		return
	}
	acl, err := ParseACL(r, userInfo.UserID, false, vol.GetOwner() != userInfo.UserID)
	if err != nil {
		log.LogErrorf("copyObjectHandler: parse acl fail: requestID(%v) volume(%v) acl(%+v) err(%v)",
			GetRequestID(r), param.Bucket(), acl, err)
		return
	}

	// get src object meta
	var sourceVol *Volume
	if sourceVol, err = o.getVol(sourceBucket); err != nil {
		log.LogErrorf("copyObjectHandler: load source volume fail: requestID(%v) srcVolume(%v) err(%v)",
			GetRequestID(r), sourceBucket, err)
		return
	}

	// get object meta
	start := time.Now()
	fileInfo, _, err := sourceVol.ObjectMeta(sourceObject)
	span.AppendTrackLog("meta.r", start, err)
	if err != nil {
		log.LogErrorf("copyObjectHandler: get object meta fail: requestID(%v) srcVolume(%v) srcObject(%v) err(%v)",
			GetRequestID(r), sourceBucket, sourceObject, err)
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
			log.LogErrorf("copyObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if fileModTime.Before(modifiedTime) {
			log.LogInfof("copyObjectHandler: file modified time not after than specified time: requestID(%v)", GetRequestID(r))
			errorCode = PreconditionFailed
			return
		}
	}
	if unModified != "" {
		fileModTime := fileInfo.ModifyTime
		unmodifiedTime, err := parseTimeRFC1123(unModified)
		if err != nil {
			log.LogErrorf("copyObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if fileModTime.After(unmodifiedTime) {
			log.LogInfof("copyObjectHandler: file modified time not before than specified time: requestID(%v)", GetRequestID(r))
			errorCode = PreconditionFailed
			return
		}
	}
	if copyMatch != "" && fileInfo.ETag != copyMatch {
		log.LogInfof("copyObjectHandler: eTag mismatched with specified: requestID(%v)", GetRequestID(r))
		errorCode = PreconditionFailed
		return
	}
	if noneMatch != "" && fileInfo.ETag == noneMatch {
		log.LogInfof("copyObjectHandler: eTag same with specified: requestID(%v)", GetRequestID(r))
		errorCode = PreconditionFailed
		return
	}

	// ObjectLock  Config
	objetLock, err := vol.metaLoader.loadObjectLock()
	if err != nil {
		log.LogErrorf("copyObjectHandler: load volume objetLock: requestID(%v)  volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
	start = time.Now()
	fsFileInfo, err := vol.CopyFile(sourceVol, sourceObject, param.Object(), metadataDirective, opt)
	span.AppendTrackLog("file.c", start, err)
	if err != nil && err != syscall.EINVAL && err != syscall.EFBIG {
		log.LogErrorf("copyObjectHandler: Volume copy file fail: requestID(%v) Volume(%v) source(%v) target(%v) err(%v)",
			GetRequestID(r), param.Bucket(), sourceObject, param.Object(), err)
		return
	}
	if err == syscall.EINVAL {
		log.LogErrorf("copyObjectHandler: target file existed, and mode conflict: requestID(%v) Volume(%v) source(%v) target(%v) err(%v)",
			GetRequestID(r), param.Bucket(), sourceObject, param.Object(), err)
		errorCode = ObjectModeConflict
		return
	}
	if err == syscall.EFBIG {
		log.LogErrorf("copyObjectHandler: source file size greater than 5GB: requestID(%v) Volume(%v) source(%v) target(%v) err(%v)",
			GetRequestID(r), param.Bucket(), sourceObject, param.Object(), err)
		errorCode = CopySourceSizeTooLarge
		return
	}

	copyResult := CopyResult{
		ETag:         "\"" + fsFileInfo.ETag + "\"",
		LastModified: formatTimeISO(fsFileInfo.ModifyTime),
	}
	response, ierr := MarshalXMLEntity(copyResult)
	if ierr != nil {
		log.LogErrorf("copyObjectHandler: marshal xml result fail: requestID(%v) result(%v) err(%v)",
			GetRequestID(r), copyResult, ierr)
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

	span := trace.SpanFromContextSafe(r.Context())
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getBucketV1Handler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
			log.LogErrorf("getBucketV1Handler: parse max key fail: requestID(%v) volume(%v) maxKeys(%v) err(%v)",
				GetRequestID(r), vol.Name(), maxKeys, err)
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
	start := time.Now()
	result, err := vol.ListFilesV1(option)
	span.AppendTrackLog("file.l", start, err)
	if err != nil {
		log.LogErrorf("getBucketV1Handler: list files fail: requestID(%v) volume(%v) option(%v) err(%v)",
			GetRequestID(r), vol.Name(), option, err)
		return
	}
	// The result of next list request should not include nextMarker.
	if result.Truncated && len(result.Files) != 0 {
		result.NextMarker = result.Files[len(result.Files)-1].Path
	}

	// get owner
	bucketOwner := NewBucketOwner(vol)
	log.LogDebugf("Owner: %v", bucketOwner)
	contents := make([]*Content, 0, len(result.Files))
	for _, file := range result.Files {
		if file.Mode == 0 {
			// Invalid file mode, which means that the inode of the file may not exist.
			// Record and filter out the file.
			log.LogWarnf("getBucketV1Handler: invalid file found: requestID(%v) volume(%v) path(%v) inode(%v)",
				GetRequestID(r), vol.Name(), file.Path, file.Inode)
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
		log.LogErrorf("getBucketV1Handler: xml marshal result fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), vol.Name(), err)
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

	span := trace.SpanFromContextSafe(r.Context())
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getBucketV2Handler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
			log.LogErrorf("getBucketV2Handler: parse max keys fail: requestID(%v) volume(%v) maxKeys(%v) err(%v)",
				GetRequestID(r), vol.Name(), maxKeys, err)
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
			log.LogErrorf("getBucketV2Handler: parse fetch owner fail: requestID(%v) volume(%v) fetchOwner(%v) err(%v)",
				GetRequestID(r), vol.Name(), fetchOwner, err)
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
	start := time.Now()
	result, err := vol.ListFilesV2(option)
	span.AppendTrackLog("file.l", start, err)
	if err != nil {
		log.LogErrorf("getBucketV2Handler: list files fail, requestID(%v) volume(%v) option(%v) err(%v)",
			GetRequestID(r), vol.Name(), option, err)
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
				log.LogWarnf("getBucketV2Handler: invalid file found: requestID(%v) volume(%v) path(%v) inode(%v)",
					GetRequestID(r), vol.Name(), file.Path, file.Inode)
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
		log.LogErrorf("getBucketV2Handler: xml marshal result fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), vol.Name(), err)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("putObjectHandler: load volume fail: requestID(%v)  volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
	objetLock, err := vol.metaLoader.loadObjectLock()
	if err != nil {
		log.LogErrorf("putObjectHandler: load volume objetLock: requestID(%v)  volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
			log.LogErrorf("putObjectHandler: tagging validate fail: requestID(%v) volume(%v) path(%v) tagging(%v) err(%v)",
				GetRequestID(r), vol.Name(), param.Object(), tagging, err)
			return
		}
	}

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKeyV2(param.AccessKey()); err != nil {
		log.LogErrorf("putObjectHandler: get user info fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.AccessKey(), err)
		return
	}

	// Check ACL
	acl, err := ParseACL(r, userInfo.UserID, false, vol.GetOwner() != userInfo.UserID)
	if err != nil {
		log.LogErrorf("putObjectHandler: parse acl fail: requestID(%v) volume(%v) path(%v) acl(%+v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), acl, err)
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
	// Audit file write
	log.LogInfof("Audit: put object: requestID(%v) remote(%v) volume(%v) path(%v) type(%v)",
		GetRequestID(r), getRequestIP(r), vol.Name(), param.Object(), contentType)

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
	start := time.Now()
	fsFileInfo, err := vol.PutObject(param.Object(), reader, opt)
	span.AppendTrackLog("file.w", start, err)
	if err != nil {
		log.LogErrorf("putObjectHandler: put object fail: requestId(%v) volume(%v) path(%v) remote(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), getRequestIP(r), err)
		err = handlePutObjectErr(err)
		return
	}

	// check content MD5
	if requestMD5 != "" && requestMD5 != fsFileInfo.ETag {
		log.LogErrorf("putObjectHandler: MD5 validate fail: requestID(%v) volume(%v) path(%v) requestMD5(%v) serverMD5(%v)",
			GetRequestID(r), vol.Name(), param.Object(), requestMD5, fsFileInfo.ETag)
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

	span := trace.SpanFromContextSafe(r.Context())
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}

	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("postObjectHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKeyV2(param.AccessKey()); err != nil {
		log.LogErrorf("postObjectHandler: get user info fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.AccessKey(), err)
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
		log.LogErrorf("postObjectHandler: parse form fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
	objetLock, err := vol.metaLoader.loadObjectLock()
	if err != nil {
		log.LogErrorf("postObjectHandler: load volume objetLock fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
			log.LogErrorf("postObjectHandler: parse canned acl fail: requestID(%v) volume(%v) acl(%v) err(%v)",
				GetRequestID(r), param.Bucket(), acl, err)
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
		log.LogErrorf("postObjectHandler: form file fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
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
		log.LogErrorf("postObjectHandler: policy match fail: requestID(%v) volume(%v) forms(%v) err(%v)",
			GetRequestID(r), param.Bucket(), forms, err)
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
	start := time.Now()
	fsFileInfo, err := vol.PutObject(key, reader, putOpt)
	span.AppendTrackLog("file.w", start, err)
	if err != nil {
		log.LogErrorf("postObjectHandler: put object fail: requestId(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), vol.Name(), key, err)
		err = handlePutObjectErr(err)
		return
	}

	// check content-md5 of actual data if specified in the request
	if requestMD5 != "" && requestMD5 != fsFileInfo.ETag {
		log.LogErrorf("postObjectHandler: MD5 validate fail: requestID(%v) volume(%v) path(%v) requestMD5(%v) serverMD5(%v)",
			GetRequestID(r), vol.Name(), key, requestMD5, fsFileInfo.ETag)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("deleteObjectHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	// Audit deletion
	log.LogInfof("Audit: delete object: requestID(%v) remote(%v) volume(%v) path(%v)",
		GetRequestID(r), getRequestIP(r), vol.Name(), param.Object())

	// Delete file
	start := time.Now()
	err = vol.DeletePath(param.Object())
	span.AppendTrackLog("file.d", start, err)
	if err != nil {
		log.LogErrorf("deleteObjectHandler: Volume delete file fail: "+
			"requestID(%v) volume(%v) path(%v) err(%v)", GetRequestID(r), vol.Name(), param.Object(), err)
		if strings.Contains(err.Error(), AccessDenied.ErrorMessage) {
			err = AccessDenied
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Get object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
func (o *ObjectNode) getObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getObjectTaggingHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	// get xattr
	start := time.Now()
	xattrInfo, err := vol.GetXAttr(param.object, XAttrKeyOSSTagging)
	span.AppendTrackLog("xattr.r", start, err)
	if err != nil {
		log.LogErrorf("getObjectTaggingHandler: get volume XAttr fail: requestID(%v) err(%v)", GetRequestID(r), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}

	ossTaggingData := xattrInfo.Get(XAttrKeyOSSTagging)

	output, _ := ParseTagging(string(ossTaggingData))
	response, err := MarshalXMLEntity(output)
	if err != nil {
		log.LogErrorf("getObjectTaggingHandler: xml marshal result fail: requestID(%v) result(%v) err(%v)",
			GetRequestID(r), output, err)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("putObjectTaggingHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
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
		log.LogErrorf("putObjectTaggingHandler: read request body data fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}

	tagging := NewTagging()
	if err = xml.Unmarshal(requestBody, tagging); err != nil {
		log.LogWarnf("putObjectTaggingHandler: decode request body fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}
	if err = tagging.Validate(); err != nil {
		log.LogErrorf("putObjectTaggingHandler: tagging validate fail: requestID(%v) tagging(%v) err(%v)",
			GetRequestID(r), tagging, err)
		return
	}

	start := time.Now()
	err = vol.SetXAttr(param.object, XAttrKeyOSSTagging, []byte(tagging.Encode()), false)
	span.AppendTrackLog("xattr.w", start, err)
	if err != nil {
		log.LogErrorf("pubObjectTaggingHandler: set tagging xattr fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("deleteObjectTaggingHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	start := time.Now()
	err = vol.DeleteXAttr(param.object, XAttrKeyOSSTagging)
	span.AppendTrackLog("xattr.d", start, err)
	if err != nil {
		log.LogErrorf("deleteObjectTaggingHandler: volume delete tagging fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("pubObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
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
		errorCode = &ErrorCode{
			ErrorCode:    "BadRequest",
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}
	putXAttrRequest := PutXAttrRequest{}
	if err = xml.Unmarshal(requestBody, &putXAttrRequest); err != nil {
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

	start := time.Now()
	err = vol.SetXAttr(param.object, key, []byte(value), true)
	span.AppendTrackLog("xattr.w", start, err)
	if err != nil {
		log.LogErrorf("pubObjectXAttrHandler: volume set extend attribute fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
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

	start := time.Now()
	info, err := vol.GetXAttr(param.object, xattrKey)
	span.AppendTrackLog("xattr.r", start, err)
	if err != nil {
		log.LogErrorf("getObjectXAttrHandler: get extend attribute fail: requestID(%v) volume(%v) object(%v) key(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), xattrKey, err)
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
		log.LogErrorf("getObjectXAttrHandler: xml marshal result fail: requestID(%v) result(%v) err(%v)",
			GetRequestID(r), output, err)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("deleteObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
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

	start := time.Now()
	err = vol.DeleteXAttr(param.object, xattrKey)
	span.AppendTrackLog("xattr.d", start, err)
	if err != nil {
		log.LogErrorf("deleteObjectXAttrHandler: delete extend attribute fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("listObjectXAttrs: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	// QPS and Concurrency Limit
	rateLimit := o.AcquireRateLimiter()
	if err = rateLimit.AcquireLimitResource(vol.owner, param.apiName); err != nil {
		return
	}
	defer rateLimit.ReleaseLimitResource(vol.owner, param.apiName)

	start := time.Now()
	keys, err := vol.ListXAttrs(param.object)
	span.AppendTrackLog("xattr.l", start, err)
	if err != nil {
		log.LogErrorf("listObjectXAttrs: volume list extend attributes fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
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
		log.LogErrorf("listObjectXAttrs: marshal response body fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
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

	span := trace.SpanFromContextSafe(r.Context())
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getObjectRetentionHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	// get object meta
	start := time.Now()
	_, xattrs, err := vol.ObjectMeta(param.Object())
	span.AppendTrackLog("meta.r", start, err)
	if err != nil {
		log.LogErrorf("getObjectRetentionHandler: get file meta fail: requestId(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), err)
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
		log.LogErrorf("getObjectRetentionHandler: parse retainUntilDate fail: requestId(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), err)
		return
	}
	var objectRetention ObjectRetention
	objectRetention.Mode = ComplianceMode
	objectRetention.RetainUntilDate = RetentionDate{Time: time.Unix(0, retainUntilDateInt64).UTC()}
	b, err := xml.Marshal(objectRetention)
	if err != nil {
		log.LogErrorf("getObjectRetentionHandler: xml marshal fail: requestId(%v) volume(%v) result(%v) err(%v)",
			GetRequestID(r), vol.Name(), objectRetention, err)
		return
	}

	writeSuccessResponseXML(w, b)
}

func parsePartInfo(partNumber uint64, fileSize uint64) (uint64, uint64, uint64, uint64) {
	var partSize uint64
	var partCount uint64
	var rangeLower uint64
	var rangeUpper uint64
	// partSize, partCount, rangeLower, rangeUpper
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
		return 0, 0, 0, 0
	}
	return partSize, partCount, rangeLower, rangeUpper
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
