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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"sync"
	"syscall"

	"github.com/chubaofs/chubaofs/util/log"
)

var (
	rangeRegexp = regexp.MustCompile("^bytes=(\\d)+-(\\d)*$")
)

// Get object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (o *ObjectNode) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: range read support
	log.LogInfof("getObjectHandler: get object, requestID(%v) remote(%v)", RequestIDFromRequest(r), r.RemoteAddr)

	_, _, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("getObjectHandler: parse request parameters fail, requestId(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
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
			_ = InvalidArgument.ServeResponse(w, r)
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
					RequestIDFromRequest(r), rangeOpt, err)
				ServeInternalStaticErrorResponse(w, r)
				return
			}
		}
		if len(upperPart) > 0 {
			if rangeUpper, err = strconv.ParseUint(upperPart, 10, 64); err != nil {
				log.LogErrorf("getObjectHandler: parse range upper fail: requestID(%v) rangeOpt(%v) err(%v)",
					RequestIDFromRequest(r), rangeOpt, err)
				ServeInternalStaticErrorResponse(w, r)
				return
			}
		}
		if rangeUpper > 0 && rangeUpper < rangeLower {
			// upper enabled and lower than lower side
			if err = InvalidArgument.ServeResponse(w, r); err != nil {
				log.LogErrorf("getObjectHandler: serve response fail: requestID(%v) err(%v)",
					RequestIDFromRequest(r), err)
				return
			}
		}

		isRangeRead = true
		log.LogDebugf("getObjectHandler: parse range option: requestID(%v) rangeOpt(%v) rangeLower(%v) rangeUpper(%v)",
			RequestIDFromRequest(r), rangeOpt, rangeLower, rangeUpper)
	}

	// get object meta
	fileInfo, err := vl.FileInfo(object)
	if err != nil {
		log.LogErrorf("getObjectHandler: volume get file info fail, requestId(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = NoSuchKey.ServeResponse(w, r)
		return
	}

	// validate and fix range
	if isRangeRead && rangeUpper > uint64(fileInfo.Size) {
		rangeUpper = uint64(fileInfo.Size)
	}

	// compute content length
	var contentLength = uint64(fileInfo.Size)
	if isRangeRead {
		contentLength = rangeUpper - rangeLower
	}

	// set response header for GetObject
	w.Header().Set(HeaderNameETag, fileInfo.ETag)
	w.Header().Set(HeaderNameAcceptRange, HeaderValueAcceptRange)
	w.Header().Set(HeaderNameLastModified, formatTimeRFC1123(fileInfo.ModifyTime))
	w.Header().Set(HeaderNameContentType, HeaderValueTypeStream)
	w.Header().Set(HeaderNameContentLength, strconv.FormatUint(contentLength, 10))

	if isRangeRead {
		w.Header().Set(HeaderNameContentRange, fmt.Sprintf("bytes %d-%d/%d", rangeLower, rangeUpper, fileInfo.Size))
	}

	// get object content
	var offset = rangeLower
	var size = uint64(fileInfo.Size)
	if isRangeRead {
		if rangeUpper == 0 {
			size = uint64(fileInfo.Size) - rangeLower
		} else {
			size = rangeUpper - rangeLower
		}
	}
	if err = vl.ReadFile(object, w, offset, size); err != nil {
		log.LogErrorf("getObjectHandler: read from volume fail: requestId(%v) volume(%v) path(%v) offset(%v) size(%v) err(%v)",
			RequestIDFromRequest(r), vl.name, object, offset, size, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	log.LogDebugf("getObjectHandler: volume read file: requestID(%v) volume(%v) path(%v) offset(%v) size(%v)",
		RequestIDFromRequest(r), vl.name, object, offset, size)
	return
}

// Head object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
func (o *ObjectNode) headObjectHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("headObjectHandler: get object meta, requestID(%v) remote(%v)", RequestIDFromRequest(r), r.RemoteAddr)

	// check args
	_, _, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("headObjectHandler: parse request parameters fail, requestId(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}
	log.LogInfof("headObjectHandler: parse request params result in header object handler, object(%v) vl(%v) err(%v)", object, vl.name, err)

	// get object meta
	fileInfo, err := vl.FileInfo(object)
	if err != nil && err == syscall.ENOENT {
		log.LogErrorf("headObjectHandler: get file meta fail, requestId(%v), err(%v)", RequestIDFromRequest(r), err)
		_ = NoSuchKey.ServeResponse(w, r)
		return
	}
	if err != nil {
		log.LogErrorf("headObjectHandler: get file meta fail, requestId(%v), err(%v)", RequestIDFromRequest(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	// set response header
	w.Header().Set(HeaderNameETag, fileInfo.ETag)
	w.Header().Set(HeaderNameAcceptRange, HeaderValueAcceptRange)
	w.Header().Set(HeaderNameContentType, HeaderValueTypeStream)
	w.Header().Set(HeaderNameLastModified, formatTimeRFC1123(fileInfo.ModifyTime))
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(int(fileInfo.Size)))
	w.Header().Set(HeaderNameContentMD5, EmptyContentMD5String)
	return
}

// Delete objects (multiple objects)
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
func (o *ObjectNode) deleteObjectsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("deleteObjectsHandler: delete multiple objects, requestID(%v) remote(%v)",
		RequestIDFromRequest(r), r.RemoteAddr)
	// check args
	_, _, _, vl, err := o.parseRequestParams(r)

	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		log.LogErrorf("deleteObjectsHandler: read request body fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	deleteReq := DeleteRequest{}
	err = UnmarshalXMLEntity(bytes, &deleteReq)
	if err != nil {
		log.LogErrorf("deleteObjectsHandler: unmarshal xml fail: requestID(%v) err(%v)",
			RequestIDFromRequest(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	if len(deleteReq.Objects) <= 0 {
		log.LogDebugf("deleteObjectsHandler: non objects found in request: requestID(%v)", RequestIDFromRequest(r))
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	var wg sync.WaitGroup
	deletesResult := DeletesResult{
		DeletedObjects: make([]Deleted, 0),
		DeletedErrors:  make([]Error, 0),
	}
	deletedObjectsCh := make(chan *Deleted, len(deleteReq.Objects))
	deletedErrorsCh := make(chan *Error, len(deleteReq.Objects))

	for _, object := range deleteReq.Objects {
		wg.Add(1)
		go func(obj Object) {
			defer func() {
				wg.Done()
				if errDelete := recover(); errDelete != nil {
					deletedError := Error{
						Key:     obj.Key,
						Code:    strconv.Itoa(InternalError.StatusCode),
						Message: InternalError.ErrorMessage,
					}
					deletedErrorsCh <- &deletedError
				}
			}()

			err = vl.DeleteFile(obj.Key)
			if err != nil {
				ossError := transferError(obj.Key, err)
				deletedErrorsCh <- &ossError
			} else {
				deleted := Deleted{Key: obj.Key}
				deletedObjectsCh <- &deleted
				log.LogDebugf("deleteObjectsHandler: delete object: requestID(%v) key(%v)", RequestIDFromRequest(r),
					deleted.Key)
			}
		}(object)
	}

	wg.Wait()
	close(deletedObjectsCh)
	close(deletedErrorsCh)

	for {
		deletedObject := <-deletedObjectsCh
		if deletedObject == nil {
			break
		}
		deletesResult.DeletedObjects = append(deletesResult.DeletedObjects, *deletedObject)
	}
	for {
		deletedError := <-deletedErrorsCh
		if deletedError == nil {
			break
		}
		deletesResult.DeletedErrors = append(deletesResult.DeletedErrors, *deletedError)
	}

	log.LogDebugf("deleteObjectsHandler: delete objects: deletes(%v) errors(%v)", len(deletesResult.DeletedObjects), len(deletesResult.DeletedErrors))
	var bytesRes []byte
	var marshalError error
	if bytesRes, marshalError = MarshalXMLEntity(deletesResult); marshalError != nil {
		log.LogErrorf("deleteObjectsHandler: marshal xml entity fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		if respErr := InternalError.ServeResponse(w, r); respErr != nil {
			log.LogErrorf("deleteObjectsHandler: write response fail: requestID(%v) err(%v)", RequestIDFromRequest(r), respErr)
			return
		}
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytesRes)))
	if _, err = w.Write(bytesRes); err != nil {
		log.LogErrorf("deleteObjectsHandler: write response body fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
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
	// check args
	_, bucket, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("copyObjectHandler: parse request params fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	sourceBucket, sourceObject := parseCopySourceInfo(r)
	if bucket != sourceBucket {
		log.LogDebugf("copyObjectHandler: source bucket is not same with bucket: requestID(%v) target(%v) source(%v)",
			RequestIDFromRequest(r), bucket, sourceBucket)
		_ = UnsupportedOperation.ServeResponse(w, r)
		return
	}

	if sourceObject == object {
		log.LogErrorf("copyObjectHandler: source object same with target object: requestID(%v) target(%v) source(%v)",
			RequestIDFromRequest(r), object, sourceObject)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	// get object meta
	fileInfo, err := vl.FileInfo(sourceObject)
	if err != nil {
		log.LogErrorf("copyObjectHandler: volume get file info fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = NoSuchKey.ServeResponse(w, r)
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
			log.LogErrorf("copyObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
			_ = InvalidArgument.ServeResponse(w, r)
			return
		}
		if fileModTime.Before(modifiedTime) {
			log.LogInfof("copyObjectHandler: file modified time not after than specified time: requestID(%v)", RequestIDFromRequest(r))
			_ = PreconditionFailed.ServeResponse(w, r)
			return
		}
	}
	if unModified != "" {
		fileModTime := fileInfo.ModifyTime
		unmodifiedTime, err := parseTimeRFC1123(unModified)
		if err != nil {
			log.LogErrorf("copyObjectHandler: parse RFC1123 time fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
			_ = InvalidArgument.ServeResponse(w, r)
			return
		}
		if fileModTime.After(unmodifiedTime) {
			log.LogInfof("copyObjectHandler: file modified time not before than specified time: requestID(%v)", RequestIDFromRequest(r))
			_ = PreconditionFailed.ServeResponse(w, r)
			return
		}
	}
	if copyMatch != "" && fileInfo.ETag != copyMatch {
		log.LogInfof("copyObjectHandler: eTag mismatched with specified: requestID(%v)", RequestIDFromRequest(r))
		_ = PreconditionFailed.ServeResponse(w, r)
		return
	}
	if noneMatch != "" && fileInfo.ETag == noneMatch {
		log.LogInfof("copyObjectHandler: eTag same with specified: requestID(%v)", RequestIDFromRequest(r))
		_ = PreconditionFailed.ServeResponse(w, r)
		return
	}

	fsFileInfo, err := vl.CopyFile(object, sourceObject)
	if err != nil {
		log.LogErrorf("copyObjectHandler: volume copy file fail: requestID(%v) volume(%v) source(%v) target(%v) err(%v)",
			RequestIDFromRequest(r), vl.name, sourceObject, object, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	copyResult := CopyResult{
		ETag:         fsFileInfo.ETag,
		LastModified: formatTimeISO(fsFileInfo.ModifyTime),
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(copyResult); marshalError != nil {
		log.LogErrorf("copyObjectHandler: marshal xml entity fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = InternalError.ServeResponse(w, r)
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
	// check args
	_, bucket, _, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("getBucketV1Handler: parse request parameters fail, requestID(%v) err(%v)",
			RequestIDFromRequest(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
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
			log.LogErrorf("getBucketV1Handler: parse max key fail, requestID(%v) err(%v)", RequestIDFromRequest(r), err)
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

	fsFileInfos, nextMarker, isTruncated, prefixes, err := vl.ListFilesV1(listBucketRequest)
	if err != nil {
		log.LogErrorf("getBucketV1Handler: list file fail, requestID(%v), err(%v)", getRequestIP(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	// get owner
	aceesKey, _ := vl.OSSSecure()
	bucketOwner := NewBucketOwner(aceesKey)
	var contents = make([]*Content, 0)
	if len(fsFileInfos) > 0 {
		for _, fsFileInfo := range fsFileInfos {
			content := &Content{
				Key:          fsFileInfo.Path,
				LastModified: formatTimeISO(fsFileInfo.ModifyTime),
				ETag:         fsFileInfo.ETag,
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
		Bucket:         bucket,
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
		log.LogErrorf("getBucketV1Handler: marshal result fail, requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
	w.Write(bytes)

	return
}

// List objects version 2
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func (o *ObjectNode) getBucketV2Handler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("getBucketV2Handler: get bucket, requestID(%v) remote(%v)", RequestIDFromRequest(r), r.RemoteAddr)

	// check args
	_, bucket, _, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("getBucketV2Handler: parse request parameters fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
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
				RequestIDFromRequest(r), err)
			_ = InvalidArgument.ServeResponse(w, r)
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
			log.LogErrorf("getBucketV2Handler: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
			_ = InvalidArgument.ServeResponse(w, r)
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

	fsFileInfos, keyCount, nextToken, isTruncated, prefixes, err := vl.ListFilesV2(request)
	if err != nil {
		log.LogErrorf("getBucketV2Handler: request id [%v], Get files list failed cause : %v", r.URL, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	// get owner
	var bucketOwner *BucketOwner
	if fetchOwnerBool {
		accessKey, _ := vl.OSSSecure()
		bucketOwner = NewBucketOwner(accessKey)
	}

	var contents = make([]*Content, 0)
	if len(fsFileInfos) > 0 {
		for _, fsFileInfo := range fsFileInfos {
			content := &Content{
				Key:          fsFileInfo.Path,
				LastModified: formatTimeISO(fsFileInfo.ModifyTime),
				ETag:         fsFileInfo.ETag,
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
		Name:           bucket,
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
		log.LogErrorf("getBucketV2Handler: marshal result fail, requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("getBucketVeHandler: write response body fail, requestID(%v) err(%v)", RequestIDFromRequest(r), err)
	}
	return
}

// Put object
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
func (o *ObjectNode) putObjectHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("putObjectHandler: put object, requestID(%v) remote(%v)", RequestIDFromRequest(r), r.RemoteAddr)

	_, bucket, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("putObjectHandler: parser request parameters fail, requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	// check args
	if bucket == "" || object == "" {
		log.LogErrorf("putObjectHandler: illegal bucket or object found: requestID(%v)", RequestIDFromRequest(r))
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	// Get request MD5, if request MD5 is not empty, compute and verify it.
	requestMD5 := r.Header.Get(HeaderNameContentMD5)

	var checkMD5 bool
	if len(requestMD5) > 0 {
		checkMD5 = true
	}

	var multipartID string
	if multipartID, err = vl.InitMultipart(object); err != nil {
		log.LogErrorf("putObjectHandler: volume init multipart fail: requestID(%v) path(%v) err(%v)",
			RequestIDFromRequest(r), object, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	defer func() {
		// rollback policy
		if err != nil {
			if abortErr := vl.AbortMultipart(object, multipartID); abortErr != nil {
				log.LogErrorf("putObjectHandler: volume abort multipart fail: requestID(%v) path(%v) multipartID(%v) err(%v)",
					RequestIDFromRequest(r), object, multipartID, err)
			}
		}
	}()
	const partID uint16 = 1
	if _, err = vl.WritePart(object, multipartID, partID, r.Body); err != nil {
		log.LogErrorf("putObjectHandler: volume write part fail: requestID(%v) path(%v) multipartID(%v) err(%v)",
			RequestIDFromRequest(r), object, multipartID, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	var fsFileInfo *FSFileInfo
	if fsFileInfo, err = vl.CompleteMultipart(object, multipartID); err != nil {
		log.LogErrorf("putObjectHandler: volume complete multipart fail: requestID(%v) path(%v) multipartID(%v) err(%v)",
			RequestIDFromRequest(r), object, multipartID, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	// validate content MD5 value
	if strings.HasSuffix(requestMD5, "==") {
		var decoded []byte
		if decoded, err = base64.StdEncoding.DecodeString(requestMD5); err != nil {
			log.LogErrorf("putObjectHandler: decode request MD5 value fail: requestID(%v) raw(%v) err(%v)",
				RequestIDFromRequest(r), requestMD5, err)
			_ = InternalError.ServeResponse(w, r)
			return
		}
		requestMD5 = hex.EncodeToString(decoded)
	}
	// check content MD5
	if checkMD5 && requestMD5 != fsFileInfo.ETag {
		log.LogErrorf("putObjectHandler: MD5 validate fail: requestID(%v) requestMD5(%v) serverMD5(%v)",
			r.URL.EscapedPath(), requestMD5, fsFileInfo.ETag)
		_ = BadDigest.ServeResponse(w, r)
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
	log.LogInfof("Delete object...")

	_, _, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("deleteObjectHandler: parse request params fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	err = vl.DeleteFile(object)
	if err != nil {
		log.LogErrorf("deleteObjectHandler: volume delete file fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	return
}

// Get object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
func (o *ObjectNode) getObjectTagging(w http.ResponseWriter, r *http.Request) {
	// TODO: implement handler 'GetObjectTagging'
	return
}

// Put object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
func (o *ObjectNode) putObjectTagging(w http.ResponseWriter, r *http.Request) {
	// TODO: implement handler 'PutObjectTagging'
	return
}

// Delete object tagging
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
func (o *ObjectNode) deleteObjectTagging(w http.ResponseWriter, r *http.Request) {
	// TODO: implement handler 'DeleteObjectTagging'
	return
}

// Put object extend attribute (xattr)
func (o *ObjectNode) putObjectXAttr(w http.ResponseWriter, r *http.Request) {
	// TODO: implement 'putObjectXAttr'
}

// Get object extend attribute (xattr)
func (o *ObjectNode) getObjectXAttr(w http.ResponseWriter, r *http.Request) {
	// TODO: implement 'getObjectXAttr'
}

// Delete object extend attribute (xattr)
func (o *ObjectNode) deleteObjectXAttr(w http.ResponseWriter, r *http.Request) {
	// TODO: implement 'deleteObjectXAttr'
}

// List object xattrs
func (o *ObjectNode) listObjectXAttrs(w http.ResponseWriter, r *http.Request) {
	// TODO: implement 'listObjectXAttrs'
}
