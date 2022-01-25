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
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"syscall"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// Create multipart upload
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
func (o *ObjectNode) createMultipleUploadHandler(w http.ResponseWriter, r *http.Request) {

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
		log.LogErrorf("createMultipleUploadHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	// system metadata
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
	var metadata = ParseUserDefinedMetadata(r.Header)

	// Check 'x-amz-tagging' header
	var tagging *Tagging
	if xAmxTagging := r.Header.Get(HeaderNameXAmzTagging); xAmxTagging != "" {
		if tagging, err = ParseTagging(xAmxTagging); err != nil {
			errorCode = InvalidArgument
			return
		}
	}
	var opt = &PutFileOption{
		MIMEType:     contentType,
		Disposition:  contentDisposition,
		Tagging:      tagging,
		Metadata:     metadata,
		CacheControl: cacheControl,
		Expires:      expires,
	}

	var uploadID string
	if uploadID, err = vol.InitMultipart(param.Object(), opt); err != nil {
		log.LogErrorf("createMultipleUploadHandler:  init multipart fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	initResult := InitMultipartResult{
		Bucket:   param.Bucket(),
		Key:      param.Object(),
		UploadId: uploadID,
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(initResult); marshalError != nil {
		log.LogErrorf("createMultipleUploadHandler: marshal result fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InternalErrorCode(marshalError)
		return
	}

	// set response header
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(len(bytes))}
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("createMultipleUploadHandler: write response body fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
	}
	return
}

// Upload part
// Uploads a part in a multipart upload.
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html .
func (o *ObjectNode) uploadPartHandler(w http.ResponseWriter, r *http.Request) {

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

	//// get upload id and part number
	uploadId := param.GetVar(ParamUploadId)
	partNumber := param.GetVar(ParamPartNumber)
	if uploadId == "" || partNumber == "" {
		log.LogErrorf("uploadPartHandler: illegal uploadID or partNumber, requestID(%v)", GetRequestID(r))
		errorCode = InvalidArgument
		return
	}

	var partNumberInt uint64
	if partNumberInt, err = strconv.ParseUint(partNumber, 10, 64); err != nil {
		log.LogErrorf("uploadPartHandler: parse part number fail, requestID(%v) raw(%v) err(%v)",
			GetRequestID(r), partNumber, err)
		errorCode = InvalidArgument
		return
	}

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
		log.LogErrorf("uploadPartHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	// handle exception
	var fsFileInfo *FSFileInfo
	fsFileInfo, err = vol.WritePart(param.Object(), uploadId, uint16(partNumberInt), r.Body)
	if err == syscall.ENOENT {
		errorCode = NoSuchUpload
		return
	}
	if err == io.ErrUnexpectedEOF {
		log.LogWarnf("uploadPartHandler: write part fail cause unexpected EOF: requestID(%v) volume(%v) path(%v) uploadId(%v) part(%v) remote(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), uploadId, partNumberInt, getRequestIP(r), err)
		errorCode = EntityTooSmall
		return
	}
	if err != nil {
		log.LogErrorf("uploadPartHandler: write part fail: requestID(%v) volume(%v) path(%v) uploadId(%v) part(%v) remote(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), uploadId, partNumberInt, getRequestIP(r), err)
		if !r.Close {
			errorCode = InternalErrorCode(err)
		}
		return
	}
	log.LogDebugf("uploadPartHandler: write part success: requestID(%v) volume(%v) path(%v) uploadId(%v) part(%v) fsFileInfo(%v)",
		GetRequestID(r), vol.Name(), param.Object(), uploadId, partNumberInt, fsFileInfo)

	// write header to response
	w.Header()[HeaderNameContentLength] = []string{"0"}
	w.Header()[HeaderNameETag] = []string{fsFileInfo.ETag}
	return
}

// List parts
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
func (o *ObjectNode) listPartsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("listPartsHandler: list parts, requestID(%v) remote(%v)", GetRequestID(r), r.RemoteAddr)

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

	// get upload id and part number
	uploadId := param.GetVar(ParamUploadId)
	maxParts := param.GetVar(ParamMaxParts)
	partNoMarker := param.GetVar(ParamPartNoMarker)

	var maxPartsInt uint64
	var partNoMarkerInt uint64

	if uploadId == "" {
		log.LogErrorf("listPartsHandler: illegal update ID, requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	if maxParts == "" {
		maxPartsInt = MaxParts
	} else {
		maxPartsInt, err = strconv.ParseUint(maxParts, 10, 64)
		if err != nil {
			log.LogErrorf("listPartsHandler: parse max parts fail, requestID(%v) raw(%v) err(%v)", GetRequestID(r), maxParts, err)
			_ = InvalidArgument.ServeResponse(w, r)
			return
		}
		if maxPartsInt > MaxParts {
			maxPartsInt = MaxParts
		}
	}
	if partNoMarker != "" {
		res, err := strconv.ParseUint(partNoMarker, 10, 64)
		if err != nil {
			log.LogErrorf("listPatsHandler: parse part number marker fail, requestID(%v) raw(%v) err(%v)", GetRequestID(r), partNoMarker, err)
			_ = InvalidArgument.ServeResponse(w, r)
			return
		}
		partNoMarkerInt = res
	}

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
		log.LogErrorf("listPartsHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	fsParts, nextMarker, isTruncated, err := vol.ListParts(param.Object(), uploadId, maxPartsInt, partNoMarkerInt)
	if err == syscall.ENOENT {
		errorCode = NoSuchUpload
		return
	}
	if err != nil {
		log.LogErrorf("listPartsHandler: Volume list parts fail, requestID(%v) uploadID(%v) maxParts(%v) partNoMarker(%v) err(%v)",
			GetRequestID(r), uploadId, maxPartsInt, partNoMarkerInt, err)
		errorCode = InternalErrorCode(err)
		return
	}
	log.LogDebugf("listPartsHandler: Volume list parts, "+
		"requestID(%v) uploadID(%v) maxParts(%v) partNoMarker(%v) numFSParts(%v) nextMarker(%v) isTruncated(%v)",
		GetRequestID(r), uploadId, maxPartsInt, partNoMarkerInt, len(fsParts), nextMarker, isTruncated)

	// get owner
	bucketOwner := NewBucketOwner(vol)

	// get parts
	parts := NewParts(fsParts)

	listPartsResult := ListPartsResult{
		Bucket:       param.Bucket(),
		Key:          param.Object(),
		UploadId:     uploadId,
		StorageClass: StorageClassStandard,
		NextMarker:   int(nextMarker),
		MaxParts:     int(maxPartsInt),
		IsTruncated:  isTruncated,
		Parts:        parts,
		Owner:        bucketOwner,
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(listPartsResult); marshalError != nil {
		log.LogErrorf("listPartsHandler: marshal result fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}

	// set response header
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(len(bytes))}
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("listPartsHandler: write response body fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
	}
	return
}

// Complete multipart
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
func (o *ObjectNode) completeMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("completeMultipartUploadHandler: complete multiple upload, requestID(%v) remote(%v)", GetRequestID(r), r.RemoteAddr)

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

	// get upload id and part number
	uploadId := param.GetVar(ParamUploadId)
	if uploadId == "" {
		log.LogErrorf("completeMultipartUploadHandler: non upload ID specified: requestID(%v)", GetRequestID(r))
		errorCode = InvalidArgument
		return
	}

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
		log.LogErrorf("completeMultipartUploadHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	// get uploaded part info in request
	var requestBytes []byte
	requestBytes, err = ioutil.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		log.LogErrorf("completeMultipartUploadHandler: read request body fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(err)
		return
	}
	multipartUploadRequest := &CompleteMultipartUploadRequest{}
	err = UnmarshalXMLEntity(requestBytes, multipartUploadRequest)
	if err != nil {
		log.LogErrorf("completeMultipartUploadHandler: unmarshal xml fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InvalidArgument
		return
	}

	// check uploaded part info
	if len(multipartUploadRequest.Parts) <= 0 {
		log.LogErrorf("completeMultipartUploadHandler: upload part is empty: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = InvalidPart
		return
	}
	// upload part info list must be in ascending order
	var partIndex int
	for _, partRequest := range multipartUploadRequest.Parts {
		partIndex++
		if partRequest.PartNumber != partIndex {
			log.LogErrorf("completeMultipartUploadHandler: the list of parts was not in ascending order: requestID(%v) err(%v)",
				GetRequestID(r), err)
			errorCode = InvalidPartOrder
			return
		}
	}

	// get multipart info
	var multipartInfo *proto.MultipartInfo
	if multipartInfo, err = vol.mw.GetMultipart_ll(param.object, uploadId); err != nil {
		log.LogErrorf("CompleteMultipart: meta get multipart fail: volume(%v) multipartID(%v) path(%v) err(%v)",
			vol.name, uploadId, param.object, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchUpload
			return
		}
		if err == syscall.EINVAL {
			errorCode = ObjectModeConflict
			return
		}
		errorCode = InternalErrorCode(err)
		return
	}

	// check request part info with every part wrote in previous WritePart request
	if len(multipartUploadRequest.Parts) != len(multipartInfo.Parts) {
		log.LogErrorf("CompleteMultipart: upload part size is not equal received part size: volume(%v) multipartID(%v) path(%v) err(%v)",
			vol.name, uploadId, param.object, err)
		errorCode = InvalidPart
		return
	}
	for index := 0; index < len(multipartInfo.Parts); index++ {
		eTag := multipartInfo.Parts[index].MD5
		if strings.Contains(eTag, "\"") {
			eTag = strings.ReplaceAll(eTag, "\"", "")
		}
		if multipartUploadRequest.Parts[index].ETag != eTag {
			log.LogErrorf("CompleteMultipart: upload part ETag not equal received part ETag: volume(%v) multipartID(%v) path(%v) err(%v)",
				vol.name, uploadId, param.object, err)
			errorCode = InvalidPart
			return
		}
	}

	fsFileInfo, err := vol.CompleteMultipart(param.Object(), uploadId, multipartInfo)
	if err == syscall.ENOENT {
		errorCode = NoSuchUpload
		return
	}
	if err == syscall.EINVAL {
		errorCode = ObjectModeConflict
		return
	}
	if err != nil {
		log.LogErrorf("completeMultipartUploadHandler: complete multipart fail, requestID(%v) uploadID(%v) err(%v)",
			GetRequestID(r), uploadId, err)
		errorCode = InternalErrorCode(err)
		return
	}
	log.LogDebugf("completeMultipartUploadHandler: complete multipart, requestID(%v) uploadID(%v) path(%v)",
		GetRequestID(r), uploadId, param.Object())

	// write response
	completeResult := CompleteMultipartResult{
		Bucket: param.Bucket(),
		Key:    param.Object(),
		ETag:   wrapUnescapedQuot(fsFileInfo.ETag),
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(completeResult); marshalError != nil {
		log.LogErrorf("completeMultipartUploadHandler: marshal result fail, requestID(%v) err(%v)", GetRequestID(r), marshalError)
		errorCode = InternalErrorCode(marshalError)
		return
	}

	// set response header
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(len(bytes))}
	if _, err = w.Write(bytes); err != nil {
		log.LogErrorf("completeMultipartUploadHandler: write response body fail, requestID(%v) err(%v)", GetRequestID(r), err)
		return
	}
	return
}

// Abort multipart
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html .
func (o *ObjectNode) abortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("abortMultipartUploadHandler: abort multiple upload, requestID(%v) remote(%v)", GetRequestID(r), r.RemoteAddr)

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

	uploadId := param.GetVar(ParamUploadId)
	if uploadId == "" {
		errorCode = InvalidArgument
		return
	}
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
		log.LogErrorf("abortMultipartUploadHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	// Abort multipart upload
	err = vol.AbortMultipart(param.Object(), uploadId)
	if err != nil && err != syscall.ENOENT {
		log.LogErrorf("abortMultipartUploadHandler: Volume abort multipart fail, requestID(%v) uploadID(%v) err(%v)", GetRequestID(r), uploadId, err)
		errorCode = InternalErrorCode(err)
		return
	}
	log.LogDebugf("abortMultipartUploadHandler: Volume abort multipart, requestID(%v) uploadID(%v) path(%v)", GetRequestID(r), uploadId, param.Object())
	return
}

// List multipart uploads
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
func (o *ObjectNode) listMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("listMultipartUploadsHandler: list multipart uploads, requestID(%v) remote(%v)", GetRequestID(r), r.RemoteAddr)

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

	// get list uploads parameter
	prefix := param.GetVar(ParamPrefix)
	keyMarker := param.GetVar(ParamKeyMarker)
	delimiter := param.GetVar(ParamPartDelimiter)
	maxUploads := param.GetVar(ParamPartMaxUploads)
	uploadIdMarker := param.GetVar(ParamUploadIdMarker)

	var maxUploadsInt uint64
	if maxUploads == "" {
		maxUploadsInt = MaxUploads
	} else {
		maxUploadsInt, err = strconv.ParseUint(maxUploads, 10, 64)
		if err != nil {
			log.LogErrorf("listMultipartUploadsHandler: parse max uploads option fail: requestID(%v), err(%v)", GetRequestID(r), err)
			_ = InvalidArgument.ServeResponse(w, r)
			return
		}
		if maxUploadsInt > MaxUploads {
			maxUploadsInt = MaxUploads
		}
	}

	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}

	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		log.LogErrorf("listMultipartUploadsHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	fsUploads, nextKeyMarker, nextUploadIdMarker, IsTruncated, prefixes, err := vol.ListMultipartUploads(prefix, delimiter, keyMarker, uploadIdMarker, maxUploadsInt)
	if err != nil {
		log.LogErrorf("listMultipartUploadsHandler: Volume list multipart uploads fail: requestID(%v), err(%v)", GetRequestID(r), err)
		errorCode = NoSuchBucket
		return
	}

	uploads := NewUploads(fsUploads, param.AccessKey())

	var commonPrefixes = make([]*CommonPrefix, 0)
	for _, prefix := range prefixes {
		commonPrefix := &CommonPrefix{
			Prefix: prefix,
		}
		commonPrefixes = append(commonPrefixes, commonPrefix)
	}

	listUploadsResult := ListUploadsResult{
		Bucket:             param.Bucket(),
		KeyMarker:          keyMarker,
		UploadIdMarker:     uploadIdMarker,
		NextKeyMarker:      nextKeyMarker,
		NextUploadIdMarker: nextUploadIdMarker,
		Delimiter:          delimiter,
		Prefix:             prefix,
		MaxUploads:         int(maxUploadsInt),
		IsTruncated:        IsTruncated,
		Uploads:            uploads,
		CommonPrefixes:     commonPrefixes,
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(listUploadsResult); marshalError != nil {
		log.LogErrorf("listMultipartUploadsHandler: marshal xml entity fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = InternalErrorCode(marshalError)
		return
	}

	// set response header
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.Header()[HeaderNameContentLength] = []string{strconv.Itoa(len(bytes))}
	_, _ = w.Write(bytes)
	return
}
