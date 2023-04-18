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
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

var (
	MinPartNumberValid        = 1
	MaxPartNumberValid        = 10000
	MinPartSizeBytes   uint64 = 1024 * 1024
	MaxPartCopySize    int64  = 5 << 30 // 5GBytes
)

// Create multipart upload
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
func (o *ObjectNode) createMultipleUploadHandler(w http.ResponseWriter, r *http.Request) {
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
	if len(param.Object()) > MaxKeyLength {
		errorCode = KeyTooLong
		return
	}
	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("createMultipleUploadHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKeyV2(param.AccessKey()); err != nil {
		log.LogErrorf("createMultipleUploadHandler: get user info fail: requestID(%v) volume(%v) accessKey(%v) err(%v)",
			GetRequestID(r), param.Bucket(), param.AccessKey(), err)
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
	// Check ACL
	var acl *AccessControlPolicy
	acl, err = ParseACL(r, userInfo.UserID, false)
	if err != nil {
		log.LogErrorf("createMultipleUploadHandler: parse acl fail: requestID(%v) acl(%+v) err(%v)",
			GetRequestID(r), acl, err)
		return
	}
	var opt = &PutFileOption{
		MIMEType:     contentType,
		Disposition:  contentDisposition,
		Tagging:      tagging,
		Metadata:     metadata,
		CacheControl: cacheControl,
		Expires:      expires,
		ACL:          acl,
	}

	var uploadID string
	if uploadID, err = vol.InitMultipart(param.Object(), opt); err != nil {
		log.LogErrorf("createMultipleUploadHandler:  init multipart fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	initResult := InitMultipartResult{
		Bucket:   param.Bucket(),
		Key:      param.Object(),
		UploadId: uploadID,
	}

	var bytes []byte
	if bytes, err = MarshalXMLEntity(initResult); err != nil {
		log.LogErrorf("createMultipleUploadHandler: marshal result fail, requestID(%v) err(%v)",
			GetRequestID(r), err)
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
		o.errorResponse(w, r, err, errorCode)
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("uploadPartHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	var fsFileInfo *FSFileInfo
	if fsFileInfo, err = vol.WritePart(param.Object(), uploadId, uint16(partNumberInt), r.Body); err != nil {
		log.LogErrorf("uploadPartHandler: write part fail: requestID(%v) volume(%v) path(%v) uploadId(%v) part(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), uploadId, partNumberInt, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchUpload
			return
		}
		if err == io.ErrUnexpectedEOF {
			errorCode = EntityTooSmall
			return
		}
		errorCode = InternalErrorCode(err)
		return
	}
	log.LogDebugf("uploadPartHandler: write part success: requestID(%v) volume(%v) path(%v) uploadId(%v) part(%v) fsFileInfo(%v)",
		GetRequestID(r), vol.Name(), param.Object(), uploadId, partNumberInt, fsFileInfo)
	// write header to response
	w.Header()[HeaderNameContentLength] = []string{"0"}
	w.Header()[HeaderNameETag] = []string{"\"" + fsFileInfo.ETag + "\""}
	return
}

// Upload part copy
// Uploads a part in a multipart upload by copying a existed object.
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html .
func (o *ObjectNode) uploadPartCopyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	//step1: check args
	var param = ParseRequestParam(r)
	uploadId := param.GetVar(ParamUploadId)
	partNumber := param.GetVar(ParamPartNumber)
	if uploadId == "" || partNumber == "" {
		log.LogErrorf("uploadPartCopyHandler: illegal uploadID or partNumber, requestID(%v)", GetRequestID(r))
		errorCode = InvalidArgument
		return
	}
	var partNumberInt uint64
	if partNumberInt, err = strconv.ParseUint(partNumber, 10, 64); err != nil {
		log.LogErrorf("uploadPartCopyHandler: parse part number fail, requestID(%v) raw(%v) err(%v)",
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("partCopyHandler: load volume fail: requestID(%v) err(%v)", GetRequestID(r), err)
		return
	}

	//step2: extract params from req
	srcBucket, srcObject, _, err := extractSrcBucketKey(r)
	if err != nil {
		log.LogDebugf("copySource(%v) argument invalid: requestID(%v)", r.Header.Get(HeaderNameXAmzCopySource), GetRequestID(r))
		return
	}

	// step3: get srcObject metadata
	var srcVol *Volume
	if srcVol, err = o.getVol(srcBucket); err != nil {
		log.LogErrorf("partCopyHandler: load src volume fail: requestID(%v) err(%v)", GetRequestID(r), err)
		return
	}
	srcFileInfo, _, err := srcVol.ObjectMeta(srcObject)
	if err == syscall.ENOENT {
		errorCode = NoSuchKey
		return
	}
	if err != nil {
		log.LogErrorf("partCopyHandler: get fileMeta fail: requestId(%v) srcVol(%v) path(%v) err(%v)", GetRequestID(r), srcBucket, srcObject, err)
		errorCode = InternalErrorCode(err)
		return
	}
	errorCode = CheckConditionInHeader(r, srcFileInfo)
	if errorCode != nil {
		return
	}

	//step4: extract range params
	copyRange := r.Header.Get(HeaderNameXAmzCopyRange)
	firstByte, copyLength, errorCode := determineCopyRange(copyRange, srcFileInfo.Size)
	if errorCode != nil {
		return
	}
	reader, writer := io.Pipe()
	go func() {
		err = srcVol.readFile(srcFileInfo.Inode, uint64(srcFileInfo.Size), srcObject, writer, uint64(firstByte), uint64(copyLength))
		if err != nil {
			log.LogErrorf("partCopyHandler: read srcObj err: requestId(%v) srcVol(%v) path(%v)", err, GetRequestID(r), srcBucket, srcObject)
		}
		writer.CloseWithError(err)
	}()

	// step5: upload part by copy
	var fsFileInfo *FSFileInfo
	fsFileInfo, err = vol.WritePart(param.Object(), uploadId, uint16(partNumberInt), reader)
	if err == syscall.ENOENT {
		errorCode = NoSuchUpload
		return
	}
	if err == io.ErrUnexpectedEOF {
		log.LogWarnf("partCopyHandler: write part fail cause unexpected EOF: requestID(%v) volume(%v) path(%v) uploadId(%v) part(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), uploadId, partNumberInt, err)
		errorCode = EntityTooSmall
		return
	}
	if err != nil {
		log.LogErrorf("partCopyHandler: write part fail: requestID(%v) volume(%v) path(%v) uploadId(%v) part(%v) err(%v)",
			GetRequestID(r), vol.Name(), param.Object(), uploadId, partNumberInt, err)
		errorCode = InternalErrorCode(err)
		return
	}
	log.LogDebugf("partCopyHandler: write part success: requestID(%v) volume(%v) path(%v) uploadId(%v) part(%v) fsFileInfo(%+v)",
		GetRequestID(r), vol.Name(), param.Object(), uploadId, partNumberInt, fsFileInfo)
	Etag := "\"" + fsFileInfo.ETag + "\""
	w.Header()[HeaderNameETag] = []string{Etag}
	cpr := NewS3CopyPartResult(Etag, fsFileInfo.CreateTime.UTC().Format(time.RFC3339))
	w.Write([]byte(cpr.String()))
	return
}

// List parts
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
func (o *ObjectNode) listPartsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
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
		errorCode = InvalidArgument
		return
	}

	if maxParts == "" {
		maxPartsInt = MaxParts
	} else {
		maxPartsInt, err = strconv.ParseUint(maxParts, 10, 64)
		if err != nil {
			log.LogErrorf("listPartsHandler: parse max parts fail, requestID(%v) raw(%v) err(%v)", GetRequestID(r), maxParts, err)
			errorCode = InvalidArgument
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
			errorCode = InvalidArgument
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("listPartsHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	fsParts, nextMarker, isTruncated, err := vol.ListParts(param.Object(), uploadId, maxPartsInt, partNoMarkerInt)
	if err != nil {
		log.LogErrorf("listPartsHandler: Volume list parts fail, requestID(%v) uploadID(%v) maxParts(%v) partNoMarker(%v) err(%v)",
			GetRequestID(r), uploadId, maxPartsInt, partNoMarkerInt, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchUpload
			return
		}
		errorCode = InternalErrorCode(err)
		return
	}
	log.LogDebugf("listPartsHandler: Volume list parts, "+
		"requestID(%v) uploadID(%v) maxParts(%v) partNoMarker(%v) numFSParts(%v) nextMarker(%v) isTruncated(%v)",
		GetRequestID(r), uploadId, maxPartsInt, partNoMarkerInt, len(fsParts), nextMarker, isTruncated)

	// get owner
	bucketOwner := NewBucketOwner(vol)
	initiator := NewInitiator(vol)

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
		Initiator:    initiator,
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

func (o *ObjectNode) checkReqParts(reqParts *CompleteMultipartUploadRequest, multipartInfo *proto.MultipartInfo) (discardedPartInodes map[uint64]uint16, committedPartInfo *proto.MultipartInfo, errCode *ErrorCode) {
	if len(reqParts.Parts) <= 0 {
		log.LogErrorf("isReqPartsValid: upload part is empty")
		return nil, nil, InvalidPart
	}

	reqInfo := make(map[int]int, 0)
	for _, reqPart := range reqParts.Parts {
		reqInfo[reqPart.PartNumber] = 0
	}

	committedPartInfo = &proto.MultipartInfo{
		ID:       multipartInfo.ID,
		Path:     multipartInfo.Path,
		InitTime: multipartInfo.InitTime,
		Parts:    make([]*proto.MultipartPartInfo, 0),
		Extend:   make(map[string]string),
	}
	for key, val := range multipartInfo.Extend {
		committedPartInfo.Extend[key] = val
	}
	uploadedInfo := make(map[uint16]string, 0)
	discardedPartInodes = make(map[uint64]uint16, 0)
	for _, uploadedPart := range multipartInfo.Parts {
		eTag := uploadedPart.MD5
		if strings.Contains(eTag, "\"") {
			eTag = strings.ReplaceAll(eTag, "\"", "")
		}
		uploadedInfo[uploadedPart.ID] = eTag

		if _, existed := reqInfo[int(uploadedPart.ID)]; !existed {
			discardedPartInodes[uploadedPart.Inode] = uploadedPart.ID
		} else {
			committedPartInfo.Parts = append(committedPartInfo.Parts, uploadedPart)
		}
	}

	for idx, reqPart := range reqParts.Parts {
		if reqPart.PartNumber > len(multipartInfo.Parts) {
			return nil, nil, InvalidPart
		}
		if multipartInfo.Parts[reqPart.PartNumber-1].Size < MinPartSizeBytes && idx < len(reqParts.Parts)-1 {
			return nil, nil, EntityTooSmall
		}

		if eTag, existed := uploadedInfo[uint16(reqPart.PartNumber)]; !existed {
			log.LogErrorf("isReqPartsValid: part number(%v) not existed", reqPart.PartNumber)
			return nil, nil, InvalidPart
		} else {
			reqEtag := reqPart.ETag
			if strings.Contains(reqEtag, "\"") {
				reqEtag = strings.ReplaceAll(reqEtag, "\"", "")
			}
			if eTag != reqEtag {
				log.LogErrorf("isReqPartsValid: part number(%v) md5 not matched, reqPart.ETag(%v), eTag(%v)",
					reqPart.PartNumber, reqEtag, eTag)
				return nil, nil, InvalidPart
			}
		}
	}
	return discardedPartInodes, committedPartInfo, nil
}

// Complete multipart
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
func (o *ObjectNode) completeMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
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
	if len(param.Object()) > MaxKeyLength {
		errorCode = KeyTooLong
		return
	}

	var vol *Volume
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("completeMultipartUploadHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
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
		log.LogErrorf("completeMultipartUploadHandler: unmarshal xml fail: requestID(%v) err(%v)", GetRequestID(r), err)
		errorCode = MalformedXML
		return
	}
	// check part parameter
	partsLen := len(multipartUploadRequest.Parts)
	if partsLen > MaxPartNumberValid {
		errorCode = InvalidMaxPartNumber
		return
	}
	if partsLen < MinPartNumberValid {
		errorCode = InvalidMinPartNumber
		return
	}
	previousPartNum := 0
	for _, p := range multipartUploadRequest.Parts {
		if p.PartNumber < previousPartNum {
			log.LogDebugf("CompletedParts invalid part order with previousPartNum=%d partNum=%d, requestID(%v)", previousPartNum, p.PartNumber, GetRequestID(r))
			errorCode = InvalidPartOrder
			return
		}
		previousPartNum = p.PartNumber
		etag := strings.ReplaceAll(p.ETag, "\"", "")
		if etag == "" {
			errorCode = InvalidPart
			return
		}
	}
	// get multipart info
	var multipartInfo *proto.MultipartInfo
	if multipartInfo, err = vol.mw.GetMultipart_ll(param.object, uploadId); err != nil {
		log.LogErrorf("CompleteMultipart: meta get multipart fail: requestID(%v) path(%v) err(%v)", GetRequestID(r), param.object, err)
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

	discardedInods, committedPartInfo, errorCode := o.checkReqParts(multipartUploadRequest, multipartInfo)
	if errorCode != nil {
		log.LogWarnf("CompleteMultipart: checkReqParts err requestID(%v) path(%v) err(%v)", GetRequestID(r), param.object, errorCode)
		return
	}
	//todo
	fsFileInfo, err := vol.CompleteMultipart(param.Object(), uploadId, committedPartInfo, discardedInods)
	if err != nil {
		log.LogErrorf("completeMultipartUploadHandler: complete multipart fail, requestID(%v) uploadID(%v) err(%v)",
			GetRequestID(r), uploadId, err)
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
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("abortMultipartUploadHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		return
	}

	// Abort multipart upload
	err = vol.AbortMultipart(param.Object(), uploadId)
	if err != nil && err != syscall.ENOENT {
		log.LogErrorf("abortMultipartUploadHandler: Volume abort multipart fail, requestID(%v) uploadID(%v) err(%v)", GetRequestID(r), uploadId, err)
		errorCode = InternalErrorCode(err)
		return
	}
	if err == syscall.ENOENT {
		log.LogWarnf("abortMultipartUploadHandler: Volume abort multipart fail, requestID(%v) uploadID(%v) err(%v)", GetRequestID(r), uploadId, err)
		errorCode = NoSuchUpload
		return
	}
	log.LogDebugf("abortMultipartUploadHandler: Volume abort multipart, requestID(%v) uploadID(%v) path(%v)", GetRequestID(r), uploadId, param.Object())
	w.WriteHeader(http.StatusNoContent)
	return
}

// List multipart uploads
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
func (o *ObjectNode) listMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)
	defer func() {
		o.errorResponse(w, r, err, errorCode)
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
			errorCode = InvalidArgument
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
	if vol, err = o.getVol(param.Bucket()); err != nil {
		log.LogErrorf("listMultipartUploadsHandler: load volume fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
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

func determineCopyRange(copyRange string, fsize int64) (firstByte, copyLength int64, err *ErrorCode) {
	if copyRange == "" { // whole file
		return 0, fsize, nil
	}
	firstByte, lastByte, err := extractCopyRangeParam(copyRange)
	if err != nil {
		return
	}
	if !(0 <= firstByte && firstByte <= lastByte && lastByte < fsize) {
		err = InvalidArgument
		return
	}
	copyLength = lastByte + 1 - firstByte
	if copyLength > MaxPartCopySize {
		err = EntityTooLarge
		return
	}
	return
}

func extractCopyRangeParam(copRange string) (firstByte, lastByte int64, err *ErrorCode) {
	//copRange must use the form : bytes=first-last
	strs := strings.SplitN(copRange, "=", 2)
	if len(strs) < 2 {
		err = InvalidArgument
		return
	}
	byteRange := strings.SplitN(strs[1], "-", 2)
	if len(byteRange) < 2 {
		err = InvalidArgument
		return
	}
	firstByteStr, lastByteStr := byteRange[0], byteRange[1]
	firstByte, err1 := strconv.ParseInt(firstByteStr, 10, 64)
	lastByte, err2 := strconv.ParseInt(lastByteStr, 10, 64)
	if err1 != nil || err2 != nil {
		err = InvalidArgument
		return
	}
	return
}

type S3CopyPartResult struct {
	XMLName      xml.Name
	ETag         string `xml:"ETag"`
	LastModified string `xml:"LastModified"`
}

func NewS3CopyPartResult(etag, lastModified string) *S3CopyPartResult {
	return &S3CopyPartResult{
		XMLName: xml.Name{
			Space: S3Namespace,
			Local: "CopyPartResult",
		},
		ETag:         etag,
		LastModified: lastModified,
	}
}

func (s *S3CopyPartResult) String() string {
	b, _ := xml.Marshal(s)
	return string(b)
}
