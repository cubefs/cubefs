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
	"strconv"

	"github.com/chubaofs/chubaofs/util/log"
)

// Create multipart upload
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
func (o *ObjectNode) createMultipleUploadHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("createMultipleUploadHandler: init multiple upload, requestID(%v) remote(%v)",
		GetRequestID(r), r.RemoteAddr)

	_, bucket, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("createMultipleUploadHandler: parse request parameters fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}
	uploadId, initErr := vl.InitMultipart(object)
	if initErr != nil {
		log.LogErrorf("createMultipleUploadHandler:  init multipart fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	initResult := InitMultipartResult{
		Bucket:   bucket,
		Key:      object,
		UploadId: uploadId,
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(initResult); marshalError != nil {
		log.LogErrorf("createMultipleUploadHandler: marshal result fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
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
	log.LogInfof("uploadPartHandler: upload part, requestID(%v) remote(%v)",
		GetRequestID(r), r.RemoteAddr)
	// check args
	params, _, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("uploadPartHandler: parse request parameters fail, requestID(%v) err(%v)", GetRequestID(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	//// get upload id and part number
	uploadId := params[ParamUploadId]
	partNumber := params[ParamPartNumber]
	if uploadId == "" || partNumber == "" {
		log.LogErrorf("uploadPartHandler: illegal uploadID or partNumber, requestID(%v)", GetRequestID(r))
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	var partNumberInt uint64
	if partNumberInt, err = strconv.ParseUint(partNumber, 10, 64); err != nil {
		log.LogErrorf("uploadPartHandler: parse part number fail, requestID(%v) raw(%v) err(%v)",
			GetRequestID(r), partNumber, err)
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	// handle exception

	var fsFileInfo *FSFileInfo
	if fsFileInfo, err = vl.WritePart(object, uploadId, uint16(partNumberInt), r.Body); err != nil {
		log.LogErrorf("uploadPartHandler: write part fail, requestID(%v) err(%v)", GetRequestID(r), err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	log.LogDebugf("uploadPartHandler: write part, requestID(%v) fsFileInfo(%v)", GetRequestID(r), fsFileInfo)

	// write header to response
	w.Header().Set(HeaderNameContentLength, "0")
	w.Header().Set(HeaderNameETag, fsFileInfo.ETag)
	return
}

// List parts
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
func (o *ObjectNode) listPartsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("listPartsHandler: list parts, requestID(%v) remote(%v)", GetRequestID(r), r.RemoteAddr)

	// check args
	params, bucket, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("listPartsHandler: parse request parameters fail, requestID(%v) err(%v)", GetRequestID(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}
	//// get upload id and part number
	uploadId := params[ParamUploadId]
	maxParts := params[ParamMaxParts]
	partNoMarker := params[ParamPartNoMarker]

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
		res, err := strconv.ParseUint(uploadId, 10, 64)
		if err != nil {
			log.LogErrorf("listPatsHandler: parse update ID fail, requestID(%v) raw(%v) err(%v)", GetRequestID(r), uploadId, err)
			_ = InvalidArgument.ServeResponse(w, r)
			return
		}
		partNoMarkerInt = res
	}

	fsParts, nextMarker, isTruncated, err := vl.ListParts(object, uploadId, maxPartsInt, partNoMarkerInt)
	if err != nil {
		log.LogErrorf("listPartsHandler: volume list parts fail, requestID(%v) uploadID(%v) maxParts(%v) partNoMarker(%v) err(%v)",
			GetRequestID(r), uploadId, maxPartsInt, partNoMarkerInt, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	log.LogDebugf("listPartsHandler: volume list parts, "+
		"requestID(%v) uploadID(%v) maxParts(%v) partNoMarker(%v) numFSParts(%v) nextMarker(%v) isTruncated(%v)",
		GetRequestID(r), uploadId, maxPartsInt, partNoMarkerInt, len(fsParts), nextMarker, isTruncated)

	// get owner
	accessKey, _ := vl.OSSSecure()
	bucketOwner := NewBucketOwner(accessKey)

	// get parts
	parts := NewParts(fsParts)

	listPartsResult := ListPartsResult{
		Bucket:       bucket,
		Key:          object,
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
		_ = InternalError.ServeResponse(w, r)
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
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

	params, bucket, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("completeMultipartUploadHandler: parse request params fail: requestID(%v) err(%v)", GetRequestID(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	// get upload id and part number
	uploadId := params[ParamUploadId]
	if uploadId == "" {
		log.LogErrorf("completeMultipartUploadHandler: non upload ID specified: requestID(%v)", GetRequestID(r))
		_ = InvalidArgument.ServeResponse(w, r)
		return
	}

	fsFileInfo, err := vl.CompleteMultipart(object, uploadId)
	if err != nil {
		log.LogErrorf("completeMultipartUploadHandler: complete multipart fail, requestID(%v) uploadID(%v) err(%v)",
			GetRequestID(r), uploadId, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	log.LogDebugf("completeMultipartUploadHandler: complete multipart, requestID(%v) uploadID(%v) path(%v)",
		GetRequestID(r), uploadId, object)

	// write response
	completeResult := CompleteMultipartResult{
		Bucket: bucket,
		Key:    object,
		ETag:   fsFileInfo.ETag,
	}

	var bytes []byte
	var marshalError error
	if bytes, marshalError = MarshalXMLEntity(completeResult); marshalError != nil {
		log.LogErrorf("completeMultipartUploadHandler: marshal result fail, requestID(%v) err(%v)", GetRequestID(r), marshalError)
		_ = InternalError.ServeResponse(w, r)
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
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

	// check args
	params, _, object, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("abortMultipartUploadHandler: parse request parameters fail, requestID(%v) err(%v)", GetRequestID(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	uploadId := params["uploadId"]
	//// Abort multipart upload
	err = vl.AbortMultipart(object, uploadId)
	if err != nil {
		log.LogErrorf("abortMultipartUploadHandler: volume abort multipart fail, requestID(%v) uploadID(%v) err(%v)", GetRequestID(r), uploadId, err)
		_ = InternalError.ServeResponse(w, r)
		return
	}
	log.LogDebugf("abortMultipartUploadHandler: volume abort multipart, requestID(%v) uploadID(%v) path(%v)", GetRequestID(r), uploadId, object)
	return
}

// List multipart uploads
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
func (o *ObjectNode) listMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("abortMultipartUploadHandler: list multipart uploads, requestID(%v) remote(%v)", GetRequestID(r), r.RemoteAddr)
	// check args
	params, bucket, _, vl, err := o.parseRequestParams(r)
	if err != nil {
		log.LogErrorf("listMultipartUploadsHandler: parse request parameters fail, requestID(%v) err(%v)", GetRequestID(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
	}

	// get list uploads parameter
	prefix := params[ParamPrefix]
	keyMarker := params[ParamKeyMarker]
	delimiter := params[ParamPartDelimiter]
	maxUploads := params[ParamPartMaxUploads]
	uploadIdMarker := params[ParamUploadIdMarker]

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

	fsUploads, nextKeyMarker, nextUploadIdMarker, IsTruncated, prefixes, err := vl.ListMultipartUploads(prefix, delimiter, keyMarker, uploadIdMarker, maxUploadsInt)
	if err != nil {
		log.LogErrorf("listMultipartUploadsHandler: volume list multipart uploads fail: requestID(%v), err(%v)", GetRequestID(r), err)
		_ = NoSuchBucket.ServeResponse(w, r)
		return
	}

	accessKey, _ := vl.OSSSecure()
	uploads := NewUploads(fsUploads, accessKey)

	var commonPrefixes = make([]*CommonPrefix, 0)
	for _, prefix := range prefixes {
		commonPrefix := &CommonPrefix{
			Prefix: prefix,
		}
		commonPrefixes = append(commonPrefixes, commonPrefix)
	}

	listUploadsResult := ListUploadsResult{
		Bucket:             bucket,
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
		_ = InternalError.ServeResponse(w, r)
		return
	}

	// set response header
	w.Header().Set(HeaderNameContentType, HeaderValueContentTypeXML)
	w.Header().Set(HeaderNameContentLength, strconv.Itoa(len(bytes)))
	_, _ = w.Write(bytes)
	return
}
