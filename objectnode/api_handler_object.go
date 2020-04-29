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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/chubaofs/chubaofs/proto"

	"syscall"

	"github.com/chubaofs/chubaofs/util/log"
)

var (
	rangeRegexp = regexp.MustCompile("^bytes=(\\d)+-(\\d)*$")
)

// Get object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (o *ObjectNode) getObjectHandler(w http.ResponseWriter, r *http.Request) {
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

	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}
	if param.Object() == "" {
		errorCode = InvalidKey
		return
	}
	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		log.LogErrorf("getObjectHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}
	// parse http range option
	var rangeOpt = strings.TrimSpace(r.Header.Get(HeaderNameRange))
	var rangeLower uint64
	var rangeUpper uint64
	var isRangeRead bool
	if len(rangeOpt) > 0 && rangeRegexp.MatchString(rangeOpt) {

		var hyphenIndex = strings.Index(rangeOpt, "-")
		if hyphenIndex < 0 {
			errorCode = InvalidArgument
			return
		}

		var lowerPart = rangeOpt[len("bytes="):hyphenIndex]
		var upperPart = ""
		if hyphenIndex+1 < len(rangeOpt) {
			upperPart = rangeOpt[hyphenIndex+1:]
		}

		if len(lowerPart) > 0 {
			if rangeLower, err = strconv.ParseUint(lowerPart, 10, 64); err != nil {
				log.LogErrorf("getObjectHandler: parse range lower fail: requestID(%v) rangeOpt(%v) err(%v)",
					GetRequestID(r), rangeOpt, err)
				ServeInternalStaticErrorResponse(w, r)
				return
			}
		}
		if len(upperPart) > 0 {
			if rangeUpper, err = strconv.ParseUint(upperPart, 10, 64); err != nil {
				log.LogErrorf("getObjectHandler: parse range upper fail: requestID(%v) rangeOpt(%v) err(%v)",
					GetRequestID(r), rangeOpt, err)
				ServeInternalStaticErrorResponse(w, r)
				return
			}
		}
		if rangeUpper > 0 && rangeUpper < rangeLower {
			// upper enabled and lower than lower side
			if err = InvalidArgument.ServeResponse(w, r); err != nil {
				log.LogErrorf("getObjectHandler: serve response fail: requestID(%v) err(%v)",
					GetRequestID(r), err)
				return
			}
		}

		isRangeRead = true
		log.LogDebugf("getObjectHandler: parse range option: requestID(%v) rangeOpt(%v) rangeLower(%v) rangeUpper(%v)",
			GetRequestID(r), rangeOpt, rangeLower, rangeUpper)
	}

	// get object meta
	var fileInfo *FSFileInfo
	fileInfo, err = vol.ObjectMeta(param.Object())
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
			errorCode = PreconditionFailed
			return
		}
	}
	// Checking precondition: If-Modified-Since
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if modified != "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(modified)
		if err != nil {
			log.LogErrorf("getObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if !fileModTime.After(modifiedTime) {
			log.LogInfof("getObjectHandler: file modified time not after than specified time: requestID(%v)", GetRequestID(r))
			errorCode = NotModified
			return
		}
	}
	// Checking precondition: If-None-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if noneMatch != "" {
		if noneMatchEtag := strings.Trim(noneMatch, "\""); noneMatchEtag == fileInfo.ETag {
			log.LogErrorf("getObjectHandler: object eTag(%s) match If-None-Match header value(%s), requestId(%v)",
				fileInfo.ETag, noneMatchEtag, GetRequestID(r))
			errorCode = NotModified
			return
		}
	}
	// Checking precondition: If-Unmodified-Since
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	if unmodified != "" && match == "" {
		fileModTime := fileInfo.ModifyTime
		modifiedTime, err := parseTimeRFC1123(unmodified)
		if err != nil {
			log.LogErrorf("getObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if fileModTime.After(modifiedTime) {
			log.LogInfof("getObjectHandler: file modified time after than specified time: requestID(%v)", GetRequestID(r))
			errorCode = PreconditionFailed
			return
		}
	}

	// validate and fix range
	if isRangeRead && rangeUpper > uint64(fileInfo.Size)-1 {
		rangeUpper = uint64(fileInfo.Size) - 1
	}

	// compute content length
	var contentLength = uint64(fileInfo.Size)
	if isRangeRead {
		contentLength = rangeUpper - rangeLower + 1
	}

	// set response header for GetObject
	if len(fileInfo.ETag) > 0 {
		w.Header().Set(HeaderNameETag, fileInfo.ETag)
	}
	w.Header().Set(HeaderNameAcceptRange, HeaderValueAcceptRange)
	w.Header().Set(HeaderNameLastModified, formatTimeRFC1123(fileInfo.ModifyTime))
	if len(fileInfo.MIMEType) > 0 {
		w.Header().Set(HeaderNameContentType, fileInfo.MIMEType)
	} else {
		w.Header().Set(HeaderNameContentType, HeaderValueTypeStream)
	}
	w.Header().Set(HeaderNameContentLength, strconv.FormatUint(contentLength, 10))

	if isRangeRead {
		w.Header().Set(HeaderNameContentRange, fmt.Sprintf("bytes %d-%d/%d", rangeLower, rangeUpper, fileInfo.Size))
	}

	if fileInfo.Mode.IsDir() {
		return
	}

	// get object content
	var offset = rangeLower
	var size = uint64(fileInfo.Size)
	if isRangeRead {
		if rangeUpper == 0 {
			size = uint64(fileInfo.Size) - rangeLower
		} else {
			size = rangeUpper - rangeLower + 1
		}
	}
	if err = vol.ReadFile(param.Object(), w, offset, size); err != nil {
		log.LogErrorf("getObjectHandler: read from Volume fail: requestId(%v) volume(%v) path(%v) offset(%v) size(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), offset, size, err)
		errorCode = InternalErrorCode(err)
		return
	}
	log.LogDebugf("getObjectHandler: Volume read file: requestID(%v) Volume(%v) path(%v) offset(%v) size(%v)",
		GetRequestID(r), param.Bucket(), param.Object(), offset, size)
	return
}

// Head object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
func (o *ObjectNode) headObjectHandler(w http.ResponseWriter, r *http.Request) {
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
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		log.LogErrorf("headObjectHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	// get object meta
	var fileInfo *FSFileInfo
	fileInfo, err = vol.ObjectMeta(param.Object())
	if err == syscall.ENOENT {
		errorCode = NoSuchKey
		return
	}
	if err != nil {
		log.LogErrorf("headObjectHandler: get file meta fail: requestId(%v) volume(%v) path(%v)err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), err)
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
			log.LogErrorf("headObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if !fileModTime.After(modifiedTime) {
			log.LogInfof("headObjectHandler: file modified time not after than specified time: requestID(%v)", GetRequestID(r))
			errorCode = NotModified
			return
		}
	}
	// Checking precondition: If-None-Match
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
	if noneMatch != "" {
		if noneMatchEtag := strings.Trim(noneMatch, "\""); noneMatchEtag == fileInfo.ETag {
			log.LogErrorf("headObjectHandler: object eTag(%s) match If-None-Match header value(%s), requestId(%v)",
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
			log.LogErrorf("headObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
		if fileModTime.After(modifiedTime) {
			log.LogInfof("headObjectHandler: file modified time after than specified time: requestID(%v)", GetRequestID(r))
			errorCode = PreconditionFailed
			return
		}
	}

	// set response header
	if len(fileInfo.ETag) > 0 {
		w.Header().Set(HeaderNameETag, fileInfo.ETag)
	}
	w.Header().Set(HeaderNameAcceptRange, HeaderValueAcceptRange)
	if len(fileInfo.MIMEType) > 0 {
		w.Header().Set(HeaderNameContentType, fileInfo.MIMEType)
	} else {
		w.Header().Set(HeaderNameContentType, HeaderValueTypeStream)
	}
	w.Header().Set(HeaderNameLastModified, formatTimeRFC1123(fileInfo.ModifyTime))
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(int(fileInfo.Size)))
	w.Header().Set(HeaderNameContentMD5, EmptyContentMD5String)
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
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
	}()

	var param = ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}

	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		log.LogErrorf("deleteObjectsHandler: load volume fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	var bytes []byte
	bytes, err = ioutil.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		log.LogErrorf("deleteObjectsHandler: read request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	deleteReq := DeleteRequest{}
	err = UnmarshalXMLEntity(bytes, &deleteReq)
	if err != nil {
		log.LogErrorf("deleteObjectsHandler: unmarshal xml fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}

	if len(deleteReq.Objects) <= 0 {
		log.LogDebugf("deleteObjectsHandler: non objects found in request: requestID(%v)", GetRequestID(r))
		errorCode = InvalidArgument
		return
	}

	var (
		deletedObjects = make([]Deleted, 0, len(deleteReq.Objects))
		deletedErrors  = make([]Error, 0)
	)

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

	var objectKeys = make([]string, 0, len(deleteReq.Objects))
	for _, object := range deleteReq.Objects {
		objectKeys = append(objectKeys, object.Key)
		err = vol.DeletePath(object.Key)
		log.LogWarnf("deleteObjectsHandler: delete: requestID(%v) volume(%v) path(%v)",
			GetRequestID(r), vol.Name(), object.Key)
		if err != nil {
			deletedErrors = append(deletedErrors, Error{Key: object.Key, Message: err.Error()})
			log.LogErrorf("deleteObjectsHandler: delete object failed: requestID(%v) volume(%v) path(%v) err(%v)",
				GetRequestID(r), vol.Name(), object.Key, err)
		} else {
			deletedObjects = append(deletedObjects, Deleted{Key: object.Key})
			log.LogDebugf("deleteObjectsHandler: delete object success: requestID(%v) volume(%v) path(%v)", GetRequestID(r),
				vol.Name(), object.Key)
		}
	}

	// Audit bulk delete behavior
	log.LogInfof("Audit: delete multiple objects: requestID(%v) remote(%v) volume(%v) objects(%v)",
		GetRequestID(r), getRequestIP(r), vol.Name(), strings.Join(objectKeys, ","))

	deleteResult := DeleteResult{
		Deleted: deletedObjects,
		Error:   deletedErrors,
	}

	log.LogDebugf("deleteObjectsHandler: delete objects: deletes(%v) errors(%v)",
		len(deleteResult.Deleted), len(deleteResult.Error))

	var bytesRes []byte
	var marshalError error
	if bytesRes, marshalError = MarshalXMLEntity(deleteResult); marshalError != nil {
		log.LogErrorf("deleteObjectsHandler: marshal xml entity fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytesRes)))
	if _, err = w.Write(bytesRes); err != nil {
		log.LogErrorf("deleteObjectsHandler: write response body fail: requestID(%v) err(%v)", GetRequestID(r), err)
	}
	return
}

func parseCopySourceInfo(r *http.Request) (sourceBucket, sourceObject string) {
	var copySource = r.Header.Get(HeaderNameCopySource)
	if strings.HasPrefix(copySource, "/") {
		copySource = copySource[1:]
	}
	position := strings.Index(copySource, "/")
	var bucket, object string
	if position >= 0 {
		bucket = copySource[:position]
		if position+1 <= len(copySource) {
			object = copySource[position+1:]
		}
	}
	sourceBucket = bucket
	sourceObject = object
	return
}

// Copy object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html .
func (o *ObjectNode) copyObjectHandler(w http.ResponseWriter, r *http.Request) {

	var err error
	var errorCode *ErrorCode

	defer func() {
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
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
		log.LogErrorf("copyObjectHandler: load volume fail: requestID(%v) err(%v)",
			getRequestIP(r), err)
		errorCode = NoSuchBucket
		return
	}

	sourceBucket, sourceObject := parseCopySourceInfo(r)
	if param.Bucket() != sourceBucket {
		log.LogDebugf("copyObjectHandler: source bucket is not same with bucket: requestID(%v) target(%v) source(%v)",
			GetRequestID(r), param.Bucket(), sourceBucket)
		errorCode = UnsupportedOperation
		return
	}

	if sourceObject == param.Object() {
		log.LogErrorf("copyObjectHandler: source object same with target object: requestID(%v) target(%v) source(%v)",
			GetRequestID(r), param.Object(), sourceObject)
		errorCode = InvalidArgument
		return
	}

	// get object meta
	var fileInfo *FSFileInfo
	fileInfo, err = vol.ObjectMeta(sourceObject)
	if err != nil {
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
			return
		}
		log.LogErrorf("copyObjectHandler: volume get file info fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	// get header
	copyMatch := r.Header.Get(HeaderNameCopyMatch)
	noneMatch := r.Header.Get(HeaderNameCopyNoneMatch)
	modified := r.Header.Get(HeaderNameCopyModified)
	unModified := r.Header.Get(HeaderNameCopyUnModified)

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

	fsFileInfo, err := vol.CopyFile(param.Object(), sourceObject)
	if err != nil {
		log.LogErrorf("copyObjectHandler: Volume copy file fail: requestID(%v) Volume(%v) source(%v) target(%v) err(%v)",
			GetRequestID(r), param.Bucket(), sourceObject, param.Object(), err)
		errorCode = InternalErrorCode(err)
		return
	}

	copyResult := CopyResult{
		ETag:         fsFileInfo.ETag,
		LastModified: formatTimeISO(fsFileInfo.ModifyTime),
	}

	var bytes []byte
	if bytes, err = MarshalXMLEntity(copyResult); err != nil {
		log.LogErrorf("copyObjectHandler: marshal xml entity fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
	_, _ = w.Write(bytes)
	return
}

// List objects v1
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
func (o *ObjectNode) getBucketV1Handler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode
	defer func() {
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
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
		errorCode = NoSuchBucket
		return
	}
	// get options
	marker := r.URL.Query().Get(ParamMarker)
	prefix := r.URL.Query().Get(ParamPrefix)
	maxKeys := r.URL.Query().Get(ParamMaxKeys)
	delimiter := r.URL.Query().Get(ParamPartDelimiter)

	var maxKeysInt uint64
	if maxKeys != "" {
		maxKeysInt, err = strconv.ParseUint(maxKeys, 10, 16)
		if err != nil {
			log.LogErrorf("getBucketV1Handler: parse max key fail, requestID(%v) err(%v)", GetRequestID(r), err)
			_ = InvalidArgument.ServeResponse(w, r)
			return
		}
		if maxKeysInt > MaxKeys {
			maxKeysInt = MaxKeys
		}
	} else {
		maxKeysInt = uint64(MaxKeys)
	}

	listBucketRequest := &ListBucketRequestV1{
		prefix:    prefix,
		delimiter: delimiter,
		marker:    marker,
		maxKeys:   maxKeysInt,
	}

	fsFileInfos, nextMarker, isTruncated, prefixes, err := vol.ListFilesV1(listBucketRequest)
	if err != nil {
		log.LogErrorf("getBucketV1Handler: list file fail: requestID(%v) volume(%v) err(%v)",
			getRequestIP(r), vol.name, err)
		errorCode = InvalidArgument
		return
	}

	// get owner
	bucketOwner := NewBucketOwner(vol)
	var contents = make([]*Content, 0)
	if len(fsFileInfos) > 0 {
		for _, fsFileInfo := range fsFileInfos {
			if fsFileInfo.Mode == 0 {
				// Invalid file mode, which means that the inode of the file may not exist.
				// Record and filter out the file.
				log.LogWarnf("getBucketV2Handler: invalid file found: volume(%v) path(%v) inode(%v)",
					vol.Name(), fsFileInfo.Path, fsFileInfo.Inode)
				continue
			}
			content := &Content{
				Key:          fsFileInfo.Path,
				LastModified: formatTimeISO(fsFileInfo.ModifyTime),
				ETag:         wrapUnescapedQuot(fsFileInfo.ETag),
				Size:         int(fsFileInfo.Size),
				StorageClass: StorageClassStandard,
				Owner:        bucketOwner,
			}
			contents = append(contents, content)
		}
	}

	var commonPrefixes = make([]*CommonPrefix, 0)
	for _, prefix := range prefixes {
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
		IsTruncated:    isTruncated,
		NextMarker:     nextMarker,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(listBucketResult); marshalError != nil {
		log.LogErrorf("getBucketV1Handler: marshal result fail, requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
	_, _ = w.Write(bytes)

	return
}

// List objects version 2
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func (o *ObjectNode) getBucketV2Handler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode
	defer func() {
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
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
		errorCode = NoSuchBucket
		return
	}

	// get options
	prefix := r.URL.Query().Get(ParamPrefix)
	maxKeys := r.URL.Query().Get(ParamMaxKeys)
	delimiter := r.URL.Query().Get(ParamPartDelimiter)
	contToken := r.URL.Query().Get(ParamContToken)
	fetchOwner := r.URL.Query().Get(ParamFetchOwner)
	startAfter := r.URL.Query().Get(ParamStartAfter)

	var maxKeysInt uint64
	if maxKeys != "" {
		maxKeysInt, err = strconv.ParseUint(maxKeys, 10, 16)
		if err != nil {
			log.LogErrorf("getBucketV2Handler: parse max keys fail, requestID(%v) err(%v)",
				GetRequestID(r), err)
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
			log.LogErrorf("getBucketV2Handler: requestID(%v) err(%v)", GetRequestID(r), err)
			errorCode = InvalidArgument
			return
		}
	} else {
		fetchOwnerBool = false
	}

	request := &ListBucketRequestV2{
		delimiter:  delimiter,
		maxKeys:    maxKeysInt,
		prefix:     prefix,
		contToken:  contToken,
		fetchOwner: fetchOwnerBool,
		startAfter: startAfter,
	}

	fsFileInfos, keyCount, nextToken, isTruncated, prefixes, err := vol.ListFilesV2(request)
	if err != nil {
		log.LogErrorf("getBucketV2Handler: list files fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}
	// get owner
	var bucketOwner *BucketOwner
	if fetchOwnerBool {
		bucketOwner = NewBucketOwner(vol)
	}

	var contents = make([]*Content, 0)
	if len(fsFileInfos) > 0 {
		for _, fsFileInfo := range fsFileInfos {
			if fsFileInfo.Mode == 0 {
				// Invalid file mode, which means that the inode of the file may not exist.
				// Record and filter out the file.
				log.LogWarnf("getBucketV2Handler: invalid file found: volume(%v) path(%v) inode(%v)",
					vol.Name(), fsFileInfo.Path, fsFileInfo.Inode)
				continue
			}
			content := &Content{
				Key:          fsFileInfo.Path,
				LastModified: formatTimeISO(fsFileInfo.ModifyTime),
				ETag:         wrapUnescapedQuot(fsFileInfo.ETag),
				Size:         int(fsFileInfo.Size),
				StorageClass: StorageClassStandard,
				Owner:        bucketOwner,
			}
			contents = append(contents, content)
		}
	}

	var commonPrefixes = make([]*CommonPrefix, 0)
	for _, prefix := range prefixes {
		commonPrefix := &CommonPrefix{
			Prefix: prefix,
		}
		commonPrefixes = append(commonPrefixes, commonPrefix)
	}

	listBucketResult := ListBucketResultV2{
		Name:           param.Bucket(),
		Prefix:         prefix,
		Token:          contToken,
		NextToken:      nextToken,
		KeyCount:       keyCount,
		MaxKeys:        maxKeysInt,
		Delimiter:      delimiter,
		IsTruncated:    isTruncated,
		Contents:       contents,
		CommonPrefixes: commonPrefixes,
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(listBucketResult); marshalError != nil {
		log.LogErrorf("getBucketV2Handler: marshal result fail, requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("getBucketVeHandler: write response body fail, requestID(%v) err(%v)", GetRequestID(r), err)
	}
	return
}

// Put object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
func (o *ObjectNode) putObjectHandler(w http.ResponseWriter, r *http.Request) {

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
	if param.Object() == "" {
		errorCode = InvalidKey
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("putObjectHandler: load volume fail: requestID(%v)  volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		errorCode = NoSuchBucket
		return
	}

	// Get request MD5, if request MD5 is not empty, compute and verify it.
	requestMD5 := r.Header.Get(HeaderNameContentMD5)

	var checkMD5 bool
	if len(requestMD5) > 0 {
		checkMD5 = true
	}

	// Get the requested content-type.
	// In addition to being used to manage data types, it is used to distinguish
	// whether the request is to create a directory.
	contentType := r.Header.Get(HeaderNameContentType)

	// Audit file write
	log.LogInfof("Audit: put object: requestID(%v) remote(%v) volume(%v) path(%v) type(%v)",
		GetRequestID(r), getRequestIP(r), vol.Name(), param.Object(), contentType)

	var fsFileInfo *FSFileInfo
	fsFileInfo, err = vol.WriteObject(param.Object(), r.Body, contentType)
	if err == syscall.EISDIR || err == syscall.ENOTDIR {
		errorCode = Conflict
		return
	}
	if err != nil {
		errorCode = InternalErrorCode(err)
		return
	}

	// validate content MD5 value
	if strings.HasSuffix(requestMD5, "==") {
		var decoded []byte
		if decoded, err = base64.StdEncoding.DecodeString(requestMD5); err != nil {
			log.LogErrorf("putObjectHandler: decode request MD5 value fail: requestID(%v) raw(%v) err(%v)",
				GetRequestID(r), requestMD5, err)
			errorCode = InternalErrorCode(err)
			return
		}
		requestMD5 = hex.EncodeToString(decoded)
	}
	// check content MD5
	if checkMD5 && requestMD5 != fsFileInfo.ETag {
		log.LogErrorf("putObjectHandler: MD5 validate fail: requestID(%v) requestMD5(%v) serverMD5(%v)",
			r.URL.EscapedPath(), requestMD5, fsFileInfo.ETag)
		errorCode = BadDigest
		return
	}

	// set response header
	w.Header().Set(HeaderNameETag, fsFileInfo.ETag)
	w.Header().Set(HeaderNameContentLength, "0")
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
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
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
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		log.LogErrorf("deleteObjectHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		errorCode = NoSuchBucket
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
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
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
		errorCode = NoSuchBucket
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

	var output = NewTagging()
	if len(ossTaggingData) > 0 {
		if err = json.Unmarshal(ossTaggingData, output); err != nil {
			log.LogErrorf("getObjectTaggingHandler: decode tagging from json fail: requestID(%v) raw(%v) err(%v)",
				GetRequestID(r), string(ossTaggingData), err)
		}
	}

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
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
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
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		log.LogErrorf("putObjectTaggingHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	var requestBody []byte
	if requestBody, err = ioutil.ReadAll(r.Body); err != nil {
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

	var encoded []byte
	if encoded, err = json.Marshal(tagging); err != nil {
		log.LogWarnf("putObjectTaggingHandler: encode tagging data fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	if err = vol.SetXAttr(param.object, XAttrKeyOSSTagging, encoded); err != nil {
		log.LogErrorf("pubObjectTaggingHandler: volume set tagging fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
		errorCode = InternalErrorCode(err)
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
		if errorCode != nil {
			_ = errorCode.ServeResponse(w, r)
			return
		}
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
		errorCode = NoSuchBucket
		return
	}
	if err = vol.DeleteXAttr(param.object, XAttrKeyOSSTagging); err != nil {
		log.LogErrorf("deleteObjectTaggingHandler: volume delete tagging fail: requestID(%v) volume(%v) object(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.Object(), err)
		errorCode = InternalErrorCode(err)
		return
	}
	return
}

// Put object extend attribute (xattr)
func (o *ObjectNode) putObjectXAttrHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

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
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("pubObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}
	if len(param.Object()) == 0 {
		errorCode = InvalidKey
		return
	}
	var requestBody []byte
	if requestBody, err = ioutil.ReadAll(r.Body); err != nil {
		_ = ErrorCode{
			ErrorCode:    "BadRequest",
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}.ServeResponse(w, r)
		return
	}
	var putXAttrRequest = PutXAttrRequest{}
	if err = xml.Unmarshal(requestBody, &putXAttrRequest); err != nil {
		_ = ErrorCode{
			ErrorCode:    "BadRequest",
			ErrorMessage: err.Error(),
			StatusCode:   http.StatusBadRequest,
		}.ServeResponse(w, r)
		return
	}
	var key, value = putXAttrRequest.XAttr.Key, putXAttrRequest.XAttr.Value
	if len(key) == 0 {
		return
	}

	if err = vol.SetXAttr(param.object, key, []byte(value)); err != nil {
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("getObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("deleteObjectXAttrHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
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
	if vol, err = o.getVol(param.bucket); err != nil {
		log.LogErrorf("listObjectXAttrs: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}
	if len(param.Object()) == 0 {
		_ = &InvalidKey
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
