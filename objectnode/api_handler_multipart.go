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
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
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

	ctx := r.Context()
	span := spanWithOperation(ctx, "CreateMultipartUpload")
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

	var userInfo *proto.UserInfo
	if userInfo, err = o.getUserInfoByAccessKey(ctx, param.AccessKey()); err != nil {
		span.Errorf("get user info fail: accessKey(%v) err(%v)", param.AccessKey(), err)
		return
	}

	// metadata
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

	// Checking user-defined metadata
	metadata := ParseUserDefinedMetadata(r.Header)

	// Check 'x-amz-tagging' header
	var tagging *Tagging
	if xAmxTagging := r.Header.Get(XAmzTagging); xAmxTagging != "" {
		if tagging, err = ParseTagging(xAmxTagging); err != nil {
			errorCode = InvalidArgument
			return
		}
	}

	// Check ACL
	acl, err := ParseACL(r, userInfo.UserID, false, vol.GetOwner() != userInfo.UserID)
	if err != nil {
		return
	}

	opt := &PutFileOption{
		MIMEType:     contentType,
		Disposition:  contentDisposition,
		Tagging:      tagging,
		Metadata:     metadata,
		CacheControl: cacheControl,
		Expires:      expires,
		ACL:          acl,
	}
	var uploadID string
	if uploadID, err = vol.InitMultipart(ctx, param.Object(), opt); err != nil {
		span.Errorf("init multipart fail: volume(%v) path(%v) option(%+v) err(%v)",
			vol.Name(), param.Object(), opt, err)
		return
	}

	initResult := InitMultipartResult{
		Bucket:   param.Bucket(),
		Key:      param.Object(),
		UploadId: uploadID,
	}
	response, err := MarshalXMLEntity(initResult)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%v) err(%v)", initResult, err)
		return
	}

	writeSuccessResponseXML(w, response)
}

// Upload part
// Uploads a part in a multipart upload.
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html .
func (o *ObjectNode) uploadPartHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "UploadPart")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	// check args
	param := ParseRequestParam(r)
	uploadId := param.GetVar(ParamUploadId)
	partNumber := param.GetVar(ParamPartNumber)
	if uploadId == "" || partNumber == "" {
		errorCode = InvalidArgument
		return
	}

	var partNumberInt uint16
	if partNumberInt, err = safeConvertStrToUint16(partNumber); err != nil {
		errorCode = InvalidPartNumber
		return
	}
	if partNumberInt < uint16(MinPartNumberValid) || partNumberInt > uint16(MaxPartNumberValid) {
		errorCode = InvalidPartNumber
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

	var vol *Volume
	if vol, err = o.getVol(ctx, param.Bucket()); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", param.Bucket(), err)
		return
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

	// Flow Control
	var reader io.Reader
	if length > DefaultFlowLimitSize {
		reader = rateLimit.GetReader(vol.owner, param.apiName, r.Body)
	} else {
		reader = r.Body
	}

	// Write Part
	fsFileInfo, err := vol.WritePart(ctx, param.Object(), uploadId, partNumberInt, reader)
	if err != nil {
		span.Errorf("write part fail: volume(%v) path(%v) uploadId(%v) partNum(%v) err(%v)",
			vol.Name(), param.Object(), uploadId, partNumberInt, err)
		err = handleWritePartErr(err)
		return
	}

	// check content MD5
	if requestMD5 != "" && requestMD5 != fsFileInfo.ETag {
		errorCode = BadDigest
		return
	}

	// write header to response
	w.Header()[ETag] = []string{"\"" + fsFileInfo.ETag + "\""}
}

// Upload part copy
// Uploads a part in a multipart upload by copying a existed object.
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html .
func (o *ObjectNode) uploadPartCopyHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "UploadPartCopy")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	// step1: check args
	param := ParseRequestParam(r)
	uploadId := param.GetVar(ParamUploadId)
	partNumber := param.GetVar(ParamPartNumber)
	if uploadId == "" || partNumber == "" {
		errorCode = InvalidArgument
		return
	}
	var partNumberInt uint16
	if partNumberInt, err = safeConvertStrToUint16(partNumber); err != nil {
		errorCode = InvalidPartNumber
		return
	}
	if partNumberInt < uint16(MinPartNumberValid) || partNumberInt > uint16(MaxPartNumberValid) {
		errorCode = InvalidPartNumber
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

	// step2: extract params from req
	srcBucket, srcObject, _, err := extractSrcBucketKey(r)
	if err != nil {
		span.Errorf("invalid %v: %v", XAmzCopySource, r.Header.Get(XAmzCopySource))
		return
	}

	// step3: get srcObject metadata
	var srcVol *Volume
	if srcVol, err = o.getVol(ctx, srcBucket); err != nil {
		span.Errorf("load volume fail: volume(%v) err(%v)", srcBucket, err)
		return
	}
	srcFileInfo, _, err := srcVol.ObjectMeta(ctx, srcObject)
	if err != nil {
		span.Errorf("get object meta fail: volume(%v) path(%v) err(%v)", srcBucket, srcObject, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchKey
		}
		return
	}

	errorCode = CheckConditionInHeader(r, srcFileInfo)
	if errorCode != nil {
		return
	}

	// step4: extract range params
	copyRange := r.Header.Get(XAmzCopySourceRange)
	firstByte, copyLength, errorCode := determineCopyRange(copyRange, srcFileInfo.Size)
	if errorCode != nil {
		span.Errorf("invalid %v(%v) with fsize(%v): %v", XAmzCopySourceRange, copyRange, srcFileInfo.Size, err)
		return
	}
	size, err := safeConvertInt64ToUint64(srcFileInfo.Size)
	if err != nil {
		return
	}
	fb, err := safeConvertInt64ToUint64(firstByte)
	if err != nil {
		return
	}
	cl, err := safeConvertInt64ToUint64(copyLength)
	if err != nil {
		return
	}

	reader, writer := io.Pipe()
	go func() {
		err = srcVol.readFile(ctx, srcFileInfo.Inode, size, srcObject, writer, fb, cl)
		if err != nil {
			span.Errorf("read src file fail: srcVol(%v) path(%v) inode(%v) err(%v)",
				srcBucket, srcObject, srcFileInfo.Inode, err)
		}
		writer.CloseWithError(err)
	}()

	// step5: upload part by copy and flow control
	var rd io.Reader
	if copyLength > DefaultFlowLimitSize {
		rd = rateLimit.GetReader(vol.owner, param.apiName, reader)
	} else {
		rd = reader
	}
	fsFileInfo, err := vol.WritePart(ctx, param.Object(), uploadId, partNumberInt, rd)
	if err != nil {
		span.Errorf("write part fail: volume(%v) path(%v) uploadId(%v) partNum(%v) err(%v)",
			vol.Name(), param.Object(), uploadId, partNumberInt, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchUpload
			return
		}
		if err == syscall.EAGAIN {
			errorCode = ConflictUploadRequest
			return
		}
		if err == io.ErrUnexpectedEOF {
			errorCode = EntityTooSmall
		}
		return
	}

	Etag := "\"" + fsFileInfo.ETag + "\""
	w.Header()[ETag] = []string{Etag}
	response := NewS3CopyPartResult(Etag, fsFileInfo.CreateTime.UTC().Format(time.RFC3339)).String()

	writeSuccessResponseXML(w, []byte(response))
}

func handleWritePartErr(err error) error {
	if err == syscall.ENOENT {
		return NoSuchUpload
	}
	if err == syscall.EEXIST {
		return ConflictUploadRequest
	}
	if err == io.ErrUnexpectedEOF {
		return EntityTooSmall
	}
	return err
}

// List parts
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
func (o *ObjectNode) listPartsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "ListParts")
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

	uploadId := param.GetVar(ParamUploadId)
	if uploadId == "" {
		errorCode = InvalidArgument
		return
	}

	var maxPartsInt uint64
	maxParts := param.GetVar(ParamMaxParts)
	if maxParts == "" {
		maxPartsInt = MaxParts
	} else {
		maxPartsInt, err = strconv.ParseUint(maxParts, 10, 64)
		if err != nil {
			errorCode = InvalidArgument
			return
		}
		if maxPartsInt > MaxParts {
			maxPartsInt = MaxParts
		}
	}

	var partNoMarkerInt uint64
	partNoMarker := param.GetVar(ParamPartNoMarker)
	if partNoMarker != "" {
		res, err := strconv.ParseUint(partNoMarker, 10, 64)
		if err != nil {
			errorCode = InvalidArgument
			return
		}
		partNoMarkerInt = res
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

	// List Parts
	fsParts, nextMarker, isTruncated, err := vol.ListParts(ctx, param.Object(), uploadId, maxPartsInt, partNoMarkerInt)
	if err != nil {
		span.Errorf("list parts fail: volume(%v) path(%v) uploadID(%v) maxParts(%v) partNumMarker(%v) err(%v)",
			vol.Name(), param.Object(), uploadId, maxPartsInt, partNoMarkerInt, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchUpload
		}
		return
	}

	listPartsResult := ListPartsResult{
		Bucket:       param.Bucket(),
		Key:          param.Object(),
		UploadId:     uploadId,
		StorageClass: StorageClassStandard,
		NextMarker:   nextMarker,
		MaxParts:     maxPartsInt,
		IsTruncated:  isTruncated,
		Parts:        NewParts(fsParts),
		Owner:        NewBucketOwner(vol),
		Initiator:    NewInitiator(vol),
	}
	response, err := MarshalXMLEntity(listPartsResult)
	if err != nil {
		span.Errorf("marshal xml response fail: %v", err)
		return
	}

	writeSuccessResponseXML(w, response)
}

func (o *ObjectNode) checkReqParts(
	ctx context.Context,
	reqParts *CompleteMultipartUploadRequest,
	multipartInfo *proto.MultipartInfo,
) (discardedPartInodes map[uint64]uint16, committedPartInfo *proto.MultipartInfo, err error,
) {
	span := spanWithOperation(ctx, "CheckParts")
	if len(reqParts.Parts) <= 0 {
		err = InvalidPart
		return
	}

	reqInfo := make(map[int]int)
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

	saveParts := multipartInfo.Parts
	sort.SliceStable(saveParts, func(i, j int) bool { return saveParts[i].ID < saveParts[j].ID })

	maxPartNum := saveParts[len(saveParts)-1].ID
	allSaveParts := make([]*proto.MultipartPartInfo, maxPartNum+1)
	uploadedInfo := make(map[uint16]string)
	discardedPartInodes = make(map[uint64]uint16)
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
		allSaveParts[uploadedPart.ID] = uploadedPart
	}

	for idx, reqPart := range reqParts.Parts {
		if reqPart.PartNumber >= len(allSaveParts) {
			err = InvalidPart
			return
		}
		if allSaveParts[reqPart.PartNumber].Size < MinPartSizeBytes && idx < len(reqParts.Parts)-1 {
			span.Errorf("size of uploaded part %v is smaller than the minimum: size(%v) minimum(%v)",
				reqPart.PartNumber, multipartInfo.Parts[reqPart.PartNumber-1].Size, MinPartSizeBytes)
			err = EntityTooSmall
			return
		}
		eTag, existed := uploadedInfo[uint16(reqPart.PartNumber)]
		if !existed {
			span.Errorf("request part not existed: part(%v)", reqPart)
			err = InvalidPart
			return
		}
		reqEtag := reqPart.ETag
		if strings.Contains(reqEtag, "\"") {
			reqEtag = strings.ReplaceAll(reqEtag, "\"", "")
		}
		if eTag != reqEtag {
			span.Errorf("md5 of part %v not matched: reqETag(%v) srvETag(%v)",
				reqPart.PartNumber, reqEtag, eTag)
			err = InvalidPart
			return
		}
	}

	return
}

// Complete multipart
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
func (o *ObjectNode) completeMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "CompleteMultipartUpload")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
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

	// get uploaded part info in request
	_, errorCode = VerifyContentLength(r, BodyLimit)
	if errorCode != nil {
		return
	}
	requestBytes, err := io.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		span.Errorf("read request body fail: %v", err)
		return
	}
	multipartUploadRequest := &CompleteMultipartUploadRequest{}
	err = UnmarshalXMLEntity(requestBytes, multipartUploadRequest)
	if err != nil {
		span.Errorf("unmarshal xml body fail: body(%v) err(%v)", string(requestBytes), err)
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
		if p.PartNumber < MinPartNumberValid || p.PartNumber > MaxPartNumberValid {
			errorCode = InvalidPartNumber
			return
		}
		if p.PartNumber < previousPartNum {
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
	multipartInfo, err := vol.mw.GetMultipart_ll(ctx, param.Object(), uploadId)
	if err != nil {
		span.Errorf("meta get multipart fail: volume(%v) path(%v) uploadId(%v) err(%v)",
			vol.Name(), param.Object(), uploadId, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchUpload
			return
		}
		if err == syscall.EINVAL {
			errorCode = ObjectModeConflict
		}
		return
	}

	discardedInods, committedPartInfo, err := o.checkReqParts(ctx, multipartUploadRequest, multipartInfo)
	if err != nil {
		return
	}

	// complete multipart
	fsFileInfo, err := vol.CompleteMultipart(ctx, param.Object(), uploadId, committedPartInfo, discardedInods)
	if err != nil {
		span.Errorf("complete multipart fail: volume(%v) path(%v) uploadId(%v) err(%v)",
			vol.Name(), param.Object(), uploadId, err)
		if err == syscall.EINVAL {
			errorCode = ObjectModeConflict
		}
		return
	}

	completeResult := CompleteMultipartResult{
		Bucket: param.Bucket(),
		Key:    param.Object(),
		ETag:   wrapUnescapedQuot(fsFileInfo.ETag),
	}
	response, ierr := MarshalXMLEntity(completeResult)
	if ierr != nil {
		span.Warnf("marshal xml response fail: response(%+v) err(%v)", completeResult, ierr)
	}

	writeSuccessResponseXML(w, response)
}

// Abort multipart
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html .
func (o *ObjectNode) abortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "AbortMultipartUpload")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	// check args
	param := ParseRequestParam(r)
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

	// Abort multipart upload
	if err = vol.AbortMultipart(ctx, param.Object(), uploadId); err != nil {
		span.Errorf("abort multipart fail: volume(%v) path(%v) uploadId(%v) err(%v)",
			vol.Name(), param.Object(), uploadId, err)
		if err == syscall.ENOENT {
			errorCode = NoSuchUpload
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// List multipart uploads
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
func (o *ObjectNode) listMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err       error
		errorCode *ErrorCode
	)

	ctx := r.Context()
	span := spanWithOperation(ctx, "ListMultipartUploads")
	defer func() {
		o.errorResponse(w, r, err, errorCode)
	}()

	param := ParseRequestParam(r)
	if param.Bucket() == "" {
		errorCode = InvalidBucketName
		return
	}

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
			errorCode = InvalidArgument
			return
		}
		if maxUploadsInt > MaxUploads {
			maxUploadsInt = MaxUploads
		}
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

	// List multipart uploads
	fsUploads, nextKeyMarker, nextUploadIdMarker, IsTruncated, prefixes, err := vol.ListMultipartUploads(ctx, prefix,
		delimiter, keyMarker, uploadIdMarker, maxUploadsInt)
	if err != nil {
		span.Errorf("list multipart uploads fail: volume(%v) prefix(%v) delimiter(%v) keyMarker(%v) "+
			"uploadIdMarker(%v) maxUploads(%v) err(%v)",
			vol.Name(), prefix, delimiter, keyMarker, uploadIdMarker, maxUploadsInt, err)
		return
	}
	uploads := NewUploads(fsUploads, param.AccessKey())

	commonPrefixes := make([]*CommonPrefix, 0)
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
		MaxUploads:         maxUploadsInt,
		IsTruncated:        IsTruncated,
		Uploads:            uploads,
		CommonPrefixes:     commonPrefixes,
	}
	response, err := MarshalXMLEntity(listUploadsResult)
	if err != nil {
		span.Errorf("marshal xml response fail: response(%v) err(%v)", listUploadsResult, err)
		return
	}

	writeSuccessResponseXML(w, response)
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
	// copRange must use the form : bytes=first-last
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
