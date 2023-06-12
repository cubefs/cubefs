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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
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
	var startGet = time.Now()
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
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
	// parse http range option
	var rangeOpt = strings.TrimSpace(r.Header.Get(HeaderNameRange))
	var rangeLower uint64
	var rangeUpper uint64
	var isRangeRead bool
	var partSize uint64
	var partCount uint64
	revRange := false
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
		log.LogDebugf("getObjectHandler: parse range header: requestID(%v) volume(%v) path(%v) rangeOpt(%v) lower(%v) upper(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), rangeOpt, rangeLower, rangeUpper)
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
	var fileInfo *FSFileInfo
	var xattr *proto.XAttrInfo
	var start = time.Now()
	fileInfo, xattr, err = vol.ObjectMeta(param.Object())
	log.LogDebugf("getObjectHandler: get object meta cost: %v", time.Since(start))
	if err == syscall.ENOENT {
		errorCode = NoSuchKey
		return
	}
	if err != nil {
		log.LogErrorf("getObjectHandler: get file meta fail: requestId(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), err)
		errorCode = InternalErrorCode(err)
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
	var contentLength = uint64(fileInfo.Size)
	if isRangeRead {
		contentLength = rangeUpper - rangeLower + 1
	}

	// get object tagging size
	ossTaggingData := xattr.Get(XAttrKeyOSSTagging)
	output, _ := ParseTagging(string(ossTaggingData))
	if output != nil && len(output.TagSet) > 0 {
		w.Header()[HeaderNameXAmzTaggingCount] = []string{strconv.Itoa(len(output.TagSet))}
	}

	// set response header for GetObject
	w.Header()[HeaderNameAcceptRange] = []string{HeaderValueAcceptRange}
	w.Header()[HeaderNameLastModified] = []string{formatTimeRFC1123(fileInfo.ModifyTime)}
	if len(responseContentType) > 0 {
		w.Header()[HeaderNameContentType] = []string{responseContentType}
	} else if len(fileInfo.MIMEType) > 0 {
		w.Header()[HeaderNameContentType] = []string{fileInfo.MIMEType}
	} else {
		w.Header()[HeaderNameContentType] = []string{HeaderValueTypeStream}
	}
	if len(responseContentDisposition) > 0 {
		w.Header()[HeaderNameContentDisposition] = []string{responseContentDisposition}
	} else if len(fileInfo.Disposition) > 0 {
		w.Header()[HeaderNameContentDisposition] = []string{fileInfo.Disposition}
	}
	if len(responseCacheControl) > 0 {
		w.Header()[HeaderNameCacheControl] = []string{responseCacheControl}
	} else if len(fileInfo.CacheControl) > 0 {
		w.Header()[HeaderNameCacheControl] = []string{fileInfo.CacheControl}
	}
	if len(responseExpires) > 0 {
		w.Header()[HeaderNameExpires] = []string{responseExpires}
	} else if len(fileInfo.Expires) > 0 {
		w.Header()[HeaderNameExpires] = []string{fileInfo.Expires}
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
		partSize, partCount, rangeLower, rangeUpper, err = parsePartInfo(partNumberInt, uint64(fileInfo.Size))
		log.LogDebugf("getObjectHandler: partNumber(%v) fileSize(%v) parsed: partSize(%d) partCount(%d) rangeLower(%d) rangeUpper(%d)",
			partNumberInt, fileInfo.Size, partSize, partCount, rangeLower, rangeUpper)
		if err != nil {
			errorCode = InternalErrorCode(err)
			return
		}

		if partNumberInt > partCount {
			log.LogErrorf("getObjectHandler: partNumber(%d) > partCount(%d): requestID(%v) volume(%v) path(%v)",
				partNumberInt, partCount, GetRequestID(r), param.Bucket(), param.Object())
			errorCode = NoSuchKey
			return
		}
		// Header : Accept-Range, Content-Length, Content-Range, ETag, x-amz-mp-parts-count
		w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(int(partSize))}
		w.Header()[HeaderNameContentRange] = []string{fmt.Sprintf("bytes %d-%d/%d", rangeLower, rangeUpper, fileInfo.Size)}
		w.Header()[HeaderNameXAmzDownloadPartCount] = []string{strconv.Itoa(int(partCount))}
		if len(fileInfo.ETag) > 0 && !strings.Contains(fileInfo.ETag, "-") {
			w.Header()[HeaderNameETag] = []string{fmt.Sprintf("%s-%d", fileInfo.ETag, partCount)}
		}
	} else {
		w.Header()[HeaderNameContentLength] = []string{strconv.FormatUint(contentLength, 10)}
		if len(fileInfo.ETag) > 0 {
			w.Header()[HeaderNameETag] = []string{wrapUnescapedQuot(fileInfo.ETag)}
		}
		if isRangeRead {
			w.Header()[HeaderNameContentRange] = []string{fmt.Sprintf("bytes %d-%d/%d", rangeLower, rangeUpper, fileInfo.Size)}
		}
	}

	// User-defined metadata
	for name, value := range fileInfo.Metadata {
		w.Header()[HeaderNameXAmzMetaPrefix+name] = []string{value}
	}

	if fileInfo.Mode.IsDir() {
		return
	}

	// get object content
	var offset = rangeLower
	var size = uint64(fileInfo.Size)
	if isRangeRead || len(partNumber) > 0 {
		size = rangeUpper - rangeLower + 1
	}
	if isRangeRead {
		w.WriteHeader(http.StatusPartialContent)
	}
	err = vol.readFile(fileInfo.Inode, uint64(fileInfo.Size), param.Object(), w, offset, size)
	if err == syscall.ENOENT {
		errorCode = NoSuchKey
		return
	}
	if err != nil {
		log.LogErrorf("getObjectHandler: read file fail: requestID(%v) volume(%v) path(%v) offset(%v) size(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), offset, size, err)
		return
	}
	log.LogDebugf("getObjectHandler: read file success: requestID(%v) volume(%v) path(%v) offset(%v) size(%v) cost(%v)",
		GetRequestID(r), param.Bucket(), param.Object(), offset, size, time.Since(startGet))

	return
}

func CheckConditionInHeader(r *http.Request, fileInfo *FSFileInfo) *ErrorCode {
	// parse request header
	match := r.Header.Get(HeaderNameIfMatch)
	noneMatch := r.Header.Get(HeaderNameIfNoneMatch)
	modified := r.Header.Get(HeaderNameIfModifiedSince)
	unmodified := r.Header.Get(HeaderNameIfUnmodifiedSince)
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
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	// check args
	var param = ParseRequestParam(r)
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

	// get object meta
	var fileInfo *FSFileInfo
	fileInfo, _, err = vol.ObjectMeta(param.Object())
	if err != nil {
		log.LogErrorf("headObjectHandler: get file meta fail: requestId(%v) volume(%v) path(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
			return
		}
		errorCode = InternalErrorCode(err)
		return
	}

	// parse request header
	match := r.Header.Get(HeaderNameIfMatch)
	noneMatch := r.Header.Get(HeaderNameIfNoneMatch)
	modified := r.Header.Get(HeaderNameIfModifiedSince)
	unmodified := r.Header.Get(HeaderNameIfUnmodifiedSince)

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
	w.Header()[HeaderNameAcceptRange] = []string{HeaderValueAcceptRange}
	w.Header()[HeaderNameLastModified] = []string{formatTimeRFC1123(fileInfo.ModifyTime)}
	w.Header()[HeaderNameContentMD5] = []string{EmptyContentMD5String}
	if len(fileInfo.MIMEType) > 0 {
		w.Header()[HeaderNameContentType] = []string{fileInfo.MIMEType}
	} else {
		w.Header()[HeaderNameContentType] = []string{HeaderValueTypeStream}
	}
	if len(fileInfo.Disposition) > 0 {
		w.Header()[HeaderNameContentDisposition] = []string{fileInfo.Disposition}
	}
	if len(fileInfo.CacheControl) > 0 {
		w.Header()[HeaderNameCacheControl] = []string{fileInfo.CacheControl}
	}
	if len(fileInfo.Expires) > 0 {
		w.Header()[HeaderNameExpires] = []string{fileInfo.Expires}
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
		partSize, partCount, rangeLower, rangeUpper, err := parsePartInfo(partNumberInt, uint64(fileInfo.Size))
		log.LogDebugf("headObjectHandler: parsed partSize(%d), partCount(%d), rangeLower(%d), rangeUpper(%d)", partSize, partCount, rangeLower, rangeUpper)
		if err != nil {
			errorCode = InternalErrorCode(err)
			return
		}
		if partNumberInt > partCount {
			log.LogErrorf("headObjectHandler: param partNumber(%d) is more then partCount(%d): requestID(%v)", partNumberInt, partCount, GetRequestID(r))
			errorCode = NoSuchKey
			return
		}
		w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(int(partSize))}
		w.Header()[HeaderNameContentRange] = []string{fmt.Sprintf("bytes %d-%d/%d", rangeLower, rangeUpper, fileInfo.Size)}
		w.Header()[HeaderNameXAmzDownloadPartCount] = []string{strconv.Itoa(int(partCount))}
		if len(fileInfo.ETag) > 0 && !strings.Contains(fileInfo.ETag, "-") {
			w.Header()[HeaderNameETag] = []string{fmt.Sprintf("%s-%d", fileInfo.ETag, partCount)}
		}
	} else {
		w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(int(fileInfo.Size))}
		if len(fileInfo.ETag) > 0 {
			w.Header()[HeaderNameETag] = []string{wrapUnescapedQuot(fileInfo.ETag)}
		}
	}

	// User-defined metadata
	for name, value := range fileInfo.Metadata {
		w.Header()[HeaderNameXAmzMetaPrefix+name] = []string{value}
	}
	return
}

// Delete objects (multiple objects)
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
func (o *ObjectNode) deleteObjectsHandler(w http.ResponseWriter, r *http.Request) {
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
		log.LogErrorf("deleteObjectsHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	var cmd5 string
	if cmd5 = r.Header.Get(HeaderNameContentMD5); cmd5 == "" {
		errorCode = MissingContentMD5
		return
	}

	var bytes []byte
	bytes, err = io.ReadAll(r.Body)
	if err != nil {
		log.LogErrorf("deleteObjectsHandler: read request body fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		errorCode = UnexpectedContent
		return
	}
	if cmd5 != GetMD5(bytes) {
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
	objectKeys := make([]string, 0, len(deleteReq.Objects))
	for _, object := range deleteReq.Objects {
		result := POLICY_UNKNOW
		if policy != nil && !policy.IsEmpty() {
			log.LogDebugf("deleteObjectsHandler: policy check: requestID(%v) volume(%v) path(%v) policy(%v)",
				GetRequestID(r), param.Bucket(), object.Key, policy)
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
		log.LogWarnf("deleteObjectsHandler: delete path: requestID(%v) remote(%v) volume(%v) path(%v)",
			GetRequestID(r), getRequestIP(r), vol.Name(), object.Key)
		if err = vol.DeletePath(object.Key); err != nil {
			log.LogErrorf("deleteObjectsHandler: delete object failed: requestID(%v) volume(%v) path(%v) err(%v)",
				GetRequestID(r), vol.Name(), object.Key, err)
			deletedErrors = append(deletedErrors, Error{Key: object.Key, Code: "InternalError", Message: err.Error()})
		} else {
			log.LogDebugf("deleteObjectsHandler: delete object success: requestID(%v) volume(%v) path(%v)",
				GetRequestID(r), vol.Name(), object.Key)
			deletedObjects = append(deletedObjects, Deleted{Key: object.Key})
		}
	}

	deleteResult := DeleteResult{
		Deleted: deletedObjects,
		Error:   deletedErrors,
	}
	bytesRes, err1 := MarshalXMLEntity(deleteResult)
	if err1 != nil {
		log.LogErrorf("deleteObjectsHandler: xml marshal fail: requestID(%v) volume(%v) result(%+v) err(%v)",
			GetRequestID(r), param.Bucket(), deleteResult, err1)
	}

	// set response header
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(len(bytesRes))}
	if _, err1 = w.Write(bytesRes); err1 != nil {
		log.LogErrorf("deleteObjectsHandler: write response body fail: requestID(%v) volume(%v) body(%v) err(%v)",
			GetRequestID(r), param.Bucket(), string(bytesRes), err1)
	}

	return
}

func extractSrcBucketKey(r *http.Request) (srcBucketId, srcKey, versionId string, err error) {
	copySource := r.Header.Get(HeaderNameXAmzCopySource)
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

	// client can reset these system metadata: Content-Type, Content-Disposition
	contentType := r.Header.Get(HeaderNameContentType)
	contentDisposition := r.Header.Get(HeaderNameContentDisposition)
	cacheControl := r.Header.Get(HeaderNameCacheControl)
	if len(cacheControl) > 0 && !ValidateCacheControl(cacheControl) {
		errorCode = InvalidCacheArgument
		return
	}
	expires := r.Header.Get(HeaderNameExpires)
	if len(expires) > 0 && !ValidateCacheExpires(expires) {
		errorCode = InvalidCacheArgument
		return
	}

	// metadata directive, direct object node use source file metadata or recreate metadata for target file
	metadataDirective := r.Header.Get(HeaderNameXAmzMetadataDirective)
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
			r.Header.Get(HeaderNameXAmzCopySource), GetRequestID(r), param.Bucket(), err)
		return
	}

	// check ACL
	userInfo, err := o.getUserInfoByAccessKeyV2(param.AccessKey())
	if err != nil {
		log.LogErrorf("copyObjectHandler: get user info fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.AccessKey(), err)
		return
	}
	acl, err := ParseACL(r, userInfo.UserID, false)
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
	var fileInfo *FSFileInfo
	fileInfo, _, err = sourceVol.ObjectMeta(sourceObject)
	if err != nil {
		log.LogErrorf("copyObjectHandler: get object meta fail: requestID(%v) srcVolume(%v) srcObject(%v) err(%v)",
			GetRequestID(r), sourceBucket, sourceObject, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
			return
		}
		errorCode = InternalErrorCode(err)
		return
	}

	// get header
	copyMatch := r.Header.Get(HeaderNameXAmzCopyMatch)
	noneMatch := r.Header.Get(HeaderNameXAmzCopyNoneMatch)
	modified := r.Header.Get(HeaderNameXAmzCopyModified)
	unModified := r.Header.Get(HeaderNameXAmzCopyUnModified)

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
	}
	fsFileInfo, err := vol.CopyFile(sourceVol, sourceObject, param.Object(), metadataDirective, opt)
	if err != nil && err != syscall.EINVAL && err != syscall.EFBIG {
		log.LogErrorf("copyObjectHandler: Volume copy file fail: requestID(%v) Volume(%v) source(%v) target(%v) err(%v)",
			GetRequestID(r), param.Bucket(), sourceObject, param.Object(), err)
		errorCode = InternalErrorCode(err)
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

	var bytes []byte
	if bytes, err = MarshalXMLEntity(copyResult); err != nil {
		log.LogErrorf("copyObjectHandler: marshal xml entity fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	// set response header
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(len(bytes))}
	if _, err1 := w.Write(bytes); err1 != nil {
		log.LogWarnf("copyObjectHandler: write response body fail: requestID(%v) volume(%v) body(%v) err(%v)",
			GetRequestID(r), param.Bucket(), string(bytes), err)
	}

	return
}

// List objects v1
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
func (o *ObjectNode) getBucketV1Handler(w http.ResponseWriter, r *http.Request) {
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
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getBucketV1Handler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}
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

	var option = &ListFilesV1Option{
		Prefix:     prefix,
		Delimiter:  delimiter,
		Marker:     marker,
		MaxKeys:    maxKeysInt,
		OnlyObject: true,
	}

	var result *ListFilesV1Result
	result, err = vol.ListFilesV1(option)
	if err != nil {
		log.LogErrorf("getBucketV1Handler: list files fail: requestID(%v) volume(%v) option(%v) err(%v)",
			GetRequestID(r), vol.Name(), option, err)
		errorCode = InternalErrorCode(err)
		return
	}
	// The result of next list request should not include nextMarker.
	if result.Truncated {
		result.NextMarker = result.Files[len(result.Files)-1].Path
	}

	// get owner
	var bucketOwner = NewBucketOwner(vol)
	log.LogDebugf("Owner: %v", bucketOwner)
	var contents = make([]*Content, 0, len(result.Files))
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

	var commonPrefixes = make([]*CommonPrefix, 0)
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

	var bytes []byte
	if bytes, err = MarshalXMLEntity(listBucketResult); err != nil {
		log.LogErrorf("getBucketV1Handler: marshal result fail: requestID(%v) volume(%v) result(%v) err(%v)",
			GetRequestID(r), vol.Name(), listBucketResult, err)
		errorCode = InternalErrorCode(err)
		return
	}

	// set response header
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(len(bytes))}
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("getBucketV1Handler: write response body fail: requestID(%v) volume(%v) body(%v) err(%v)",
			GetRequestID(r), vol.Name(), string(bytes), err)
		errorCode = InternalErrorCode(err)
	}

	return
}

// List objects version 2
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func (o *ObjectNode) getBucketV2Handler(w http.ResponseWriter, r *http.Request) {
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
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getBucketV2Handler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

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

	var option = &ListFilesV2Option{
		Delimiter:  delimiter,
		MaxKeys:    maxKeysInt,
		Prefix:     prefix,
		ContToken:  contToken,
		FetchOwner: fetchOwnerBool,
		StartAfter: startAfter,
	}

	var result *ListFilesV2Result
	result, err = vol.ListFilesV2(option)
	if err != nil {
		log.LogErrorf("getBucketV2Handler: list files fail, requestID(%v) volume(%v) option(%v) err(%v)",
			GetRequestID(r), vol.Name(), option, err)
		errorCode = InternalErrorCode(err)
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

	var contents = make([]*Content, 0)
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

	var commonPrefixes = make([]*CommonPrefix, 0)
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

	var bytes []byte
	if bytes, err = MarshalXMLEntity(listBucketResult); err != nil {
		log.LogErrorf("getBucketV2Handler: marshal result fail: requestID(%v) volume(%v) result(%v) err(%v)",
			GetRequestID(r), vol.Name(), listBucketResult, err)
		errorCode = InternalErrorCode(err)
		return
	}

	// set response header
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(len(bytes))}
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("getBucketV2Handler: write response body fail: requestID(%v) volume(%v) body(%v) err(%v)",
			GetRequestID(r), vol.Name(), string(bytes), err)
		errorCode = InternalErrorCode(err)
	}
	return
}

// Put object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
func (o *ObjectNode) putObjectHandler(w http.ResponseWriter, r *http.Request) {
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

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKeyV2(param.AccessKey()); err != nil {
		log.LogErrorf("putObjectHandler: get user info fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.AccessKey(), err)
		return
	}

	// Check 'x-amz-tagging' header
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#API_PutObject_RequestSyntax
	var tagging *Tagging
	if xAmxTagging := r.Header.Get(HeaderNameXAmzTagging); xAmxTagging != "" {
		if tagging, err = ParseTagging(xAmxTagging); err != nil {
			errorCode = InvalidArgument
			return
		}
		var validateRes bool
		if validateRes, errorCode = tagging.Validate(); !validateRes {
			log.LogErrorf("putObjectHandler: tagging validate fail: requestID(%v) volume(%v) path(%v) tagging(%v) err(%v)",
				GetRequestID(r), vol.Name(), param.Object(), tagging, errorCode)
			return
		}
	}

	// Check ACL
	acl, err := ParseACL(r, userInfo.UserID, false)
	if err != nil {
		log.LogErrorf("putObjectHandler: parse acl fail: requestID(%v) volume(%v) path(%v) acl(%+v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), acl, err)
		return
	}

	// Get request MD5, if request MD5 is not empty, compute and verify it.
	requestMD5 := r.Header.Get(HeaderNameContentMD5)
	if requestMD5 != "" {
		decoded, err := base64.StdEncoding.DecodeString(requestMD5)
		if err != nil {
			errorCode = InvalidDigest
			return
		}
		requestMD5 = hex.EncodeToString(decoded)
	}

	// Get the requested content-type.
	// In addition to being used to manage data types, it is used to distinguish
	// whether the request is to create a directory.
	contentType := r.Header.Get(HeaderNameContentType)
	// Get request header : content-disposition
	contentDisposition := r.Header.Get(HeaderNameContentDisposition)
	// Get request header : Cache-Control
	cacheControl := r.Header.Get(HeaderNameCacheControl)
	if len(cacheControl) > 0 && !ValidateCacheControl(cacheControl) {
		errorCode = InvalidCacheArgument
		return
	}
	// Get request header : Expires
	expires := r.Header.Get(HeaderNameExpires)
	if len(expires) > 0 && !ValidateCacheExpires(expires) {
		errorCode = InvalidCacheArgument
		return
	}
	// Checking user-defined metadata
	metadata := ParseUserDefinedMetadata(r.Header)
	// Audit file write
	log.LogInfof("Audit: put object: requestID(%v) remote(%v) volume(%v) path(%v) type(%v)",
		GetRequestID(r), getRequestIP(r), vol.Name(), param.Object(), contentType)

	var fsFileInfo *FSFileInfo
	var opt = &PutFileOption{
		MIMEType:     contentType,
		Disposition:  contentDisposition,
		Tagging:      tagging,
		Metadata:     metadata,
		CacheControl: cacheControl,
		Expires:      expires,
		ACL:          acl,
	}
	var startPut = time.Now()
	if fsFileInfo, err = vol.PutObject(param.Object(), r.Body, opt); err != nil {
		log.LogErrorf("putObjectHandler: put object fail: requestId(%v) volume(%v) path(%v) remote(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), getRequestIP(r), err)
		if err == syscall.EINVAL {
			errorCode = ObjectModeConflict
			return
		}
		if err == io.ErrUnexpectedEOF {
			errorCode = EntityTooSmall
			return
		}
		errorCode = InternalErrorCode(err)
		return
	}
	// check content MD5
	if requestMD5 != "" && requestMD5 != fsFileInfo.ETag {
		log.LogErrorf("putObjectHandler: MD5 validate fail: requestID(%v) volume(%v) path(%v) requestMD5(%v) serverMD5(%v)",
			GetRequestID(r), vol.Name(), param.Object(), requestMD5, fsFileInfo.ETag)
		errorCode = BadDigest
		return
	}
	log.LogDebugf("PutObject succeed, requestID(%v) volume(%v) key(%v) costTime: %v", GetRequestID(r),
		vol.Name(), param.Object(), time.Since(startPut))

	// set response header
	w.Header()[HeaderNameETag] = []string{wrapUnescapedQuot(fsFileInfo.ETag)}
	w.Header()[HeaderNameContentLength] = []string{"0"}
	return
}

// Delete object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html .
func (o *ObjectNode) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
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

	// Audit deletion
	log.LogInfof("Audit: delete object: requestID(%v) remote(%v) volume(%v) path(%v)",
		GetRequestID(r), getRequestIP(r), vol.Name(), param.Object())

	err = vol.DeletePath(param.Object())
	if err != nil {
		log.LogErrorf("deleteObjectHandler: Volume delete file fail: "+
			"requestID(%v) volume(%v) path(%v) err(%v)", GetRequestID(r), vol.Name(), param.Object(), err)
		errorCode = InternalErrorCode(err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	return
}

// Get object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
func (o *ObjectNode) getObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
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
	var xattrInfo *proto.XAttrInfo
	if xattrInfo, err = vol.GetXAttr(param.object, XAttrKeyOSSTagging); err != nil {
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
			return
		}
		log.LogErrorf("getObjectTaggingHandler: Volume get XAttr fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	ossTaggingData := xattrInfo.Get(XAttrKeyOSSTagging)

	var output, _ = ParseTagging(string(ossTaggingData))

	var encoded []byte
	if encoded, err = MarshalXMLEntity(output); err != nil {
		log.LogErrorf("getObjectTaggingHandler: encode output fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	if _, err = w.Write(encoded); err != nil {
		log.LogErrorf("getObjectTaggingHandler: write response fail: requestID(%v) err（%v)", GetRequestID(r), err)
	}
	return
}

// Put object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
func (o *ObjectNode) putObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
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

	var requestBody []byte
	if requestBody, err = io.ReadAll(r.Body); err != nil {
		log.LogErrorf("putObjectTaggingHandler: read request body data fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}

	var tagging = NewTagging()
	if err = xml.Unmarshal(requestBody, tagging); err != nil {
		log.LogWarnf("putObjectTaggingHandler: decode request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}
	validateRes, errorCode := tagging.Validate()
	if !validateRes {
		log.LogErrorf("putObjectTaggingHandler: tagging validate fail: requestID(%v) tagging(%v) err(%v)", GetRequestID(r), tagging, err)
		return
	}

	err = vol.SetXAttr(param.object, XAttrKeyOSSTagging, []byte(tagging.Encode()), false)

	if err != nil {
		log.LogErrorf("pubObjectTaggingHandler: volume set tagging fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		} else {
			errorCode = InternalErrorCode(err)
		}
		return
	}
	return
}

// Delete object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
func (o *ObjectNode) deleteObjectTaggingHandler(w http.ResponseWriter, r *http.Request) {
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
	if err = vol.DeleteXAttr(param.object, XAttrKeyOSSTagging); err != nil {
		log.LogErrorf("deleteObjectTaggingHandler: volume delete tagging fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
		errorCode = InternalErrorCode(err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	return
}

// Put object extend attribute (xattr)
func (o *ObjectNode) putObjectXAttrHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("pubObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
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
	var putXAttrRequest = PutXAttrRequest{}
	if err = xml.Unmarshal(requestBody, &putXAttrRequest); err != nil {
		errorCode = &ErrorCode{
			ErrorCode:    "BadRequest",
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}
		return
	}
	var key, value = putXAttrRequest.XAttr.Key, putXAttrRequest.XAttr.Value
	if len(key) == 0 {
		return
	}

	if err = vol.SetXAttr(param.object, key, []byte(value), true); err != nil {
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
			return
		}
		log.LogErrorf("pubObjectXAttrHandler: volume set extend attribute fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}
	return
}

// Get object extend attribute (xattr)
func (o *ObjectNode) getObjectXAttrHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode
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
		log.LogErrorf("getObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
		return
	}
	var xattrKey string
	if xattrKey = param.GetVar(ParamKey); len(xattrKey) == 0 {
		errorCode = InvalidArgument
		return
	}

	var info *proto.XAttrInfo
	if info, err = vol.GetXAttr(param.object, xattrKey); err != nil {
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
			return
		}
		log.LogErrorf("getObjectXAttrHandler: get extend attribute fail: requestID(%v) volume(%v) object(%v) key(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), xattrKey, err)
		errorCode = InternalErrorCode(err)
		return
	}
	var response = GetXAttrOutput{
		XAttr: &XAttr{
			Key:   xattrKey,
			Value: string(info.Get(xattrKey)),
		},
	}
	var marshaled []byte
	if marshaled, err = MarshalXMLEntity(&response); err != nil {
		log.LogErrorf("getObjectXAttrHandler: marshal response body fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}
	_, _ = w.Write(marshaled)
	return
}

// Delete object extend attribute (xattr)
func (o *ObjectNode) deleteObjectXAttrHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode
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
		log.LogErrorf("deleteObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
		return
	}
	var xattrKey string
	if xattrKey = param.GetVar(ParamKey); len(xattrKey) == 0 {
		errorCode = InvalidArgument
		return
	}

	if err = vol.DeleteXAttr(param.object, xattrKey); err != nil {
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
			return
		}
		log.LogErrorf("deleteObjectXAttrHandler: delete extend attribute fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}
	return
}

// List object xattrs
func (o *ObjectNode) listObjectXAttrs(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	var param = ParseRequestParam(r)
	if len(param.Bucket()) == 0 {
		errorCode = InvalidBucketName
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("listObjectXAttrs: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
		return
	}

	var keys []string
	if keys, err = vol.ListXAttrs(param.object); err != nil {
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
			return
		}
		log.LogErrorf("listObjectXAttrs: volume list extend attributes fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
		errorCode = InternalErrorCode(err)
		return
	}

	var response = ListXAttrsOutput{
		Keys: keys,
	}
	var marshaled []byte
	if marshaled, err = MarshalXMLEntity(&response); err != nil {
		log.LogErrorf("listObjectXAttrs: marshal response body fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
		errorCode = InternalErrorCode(err)
		return
	}
	if _, err = w.Write(marshaled); err != nil {
		log.LogErrorf("listObjectXAttrs: write response fail: requestID(%v) err(%v)", GetRequestID(r), err)
	}
	return
}

func parsePartInfo(partNumber uint64, fileSize uint64) (uint64, uint64, uint64, uint64, error) {
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
		return 0, 0, 0, 0, nil
	}
	return partSize, partCount, rangeLower, rangeUpper, nil
}
