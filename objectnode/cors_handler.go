package objectnode

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/EnableCorsUsingREST.html

import (
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/cubefs/cubefs/util/log"
)

const (
	MaxCORSSize = 1 << 16 // 64KB
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html
func (o *ObjectNode) getBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
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
		log.LogErrorf("getBucketCorsHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}

	var cors *CORSConfiguration
	if cors, err = vol.metaLoader.loadCors(); err != nil {
		log.LogErrorf("getBucketCorsHandler: load cors fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), vol.Name(), err)
		return
	}
	if cors == nil || len(cors.CORSRule) == 0 {
		errorCode = NoSuchCORSConfiguration
		return
	}
	var data []byte
	if data, err = xml.Marshal(cors); err != nil {
		log.LogErrorf("getBucketCorsHandler: xml marshal fail: requestID(%v) volume(%v) cors(%+v) err(%v)",
			GetRequestID(r), vol.Name(), cors, err)
		return
	}
	if _, err = w.Write(data); err != nil {
		log.LogErrorf("getBucketCorsHandler: write response body fail: requestID(%v) volume(%v) body(%v) err(%v)",
			GetRequestID(r), vol.Name(), string(data), err)
	}

	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html
func (o *ObjectNode) putBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
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
		log.LogErrorf("putBucketCorsHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}
	md5 := r.Header.Get(HeaderNameContentMD5)
	if md5 == "" {
		errorCode = MissingContentMD5
		return
	}
	var body []byte
	if body, err = ioutil.ReadAll(io.LimitReader(r.Body, MaxCORSSize+1)); err != nil {
		log.LogErrorf("putBucketCorsHandler: read request body fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), vol.Name(), err)
		return
	}
	if len(body) > MaxCORSSize {
		errorCode = EntityTooLarge
		return
	}
	if md5 != GetMD5(body) {
		errorCode = InvalidDigest
		return
	}
	var corsConfig *CORSConfiguration
	if corsConfig, errorCode = parseCorsConfig(body); errorCode != nil {
		log.LogErrorf("putBucketCorsHandler: parse cors config fail: requestID(%v) volume(%v) config(%v) err(%v)",
			GetRequestID(r), vol.Name(), string(body), errorCode)
		return
	}
	if err = storeBucketCors(body, vol); err != nil {
		log.LogErrorf("putBucketCorsHandler: store cors config fail: requestID(%v) volume(%v) config(%v) err(%v)",
			GetRequestID(r), vol.Name(), string(body), err)
		return
	}
	vol.metaLoader.storeCors(corsConfig)

	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html
func (o *ObjectNode) deleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
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
		log.LogErrorf("deleteBucketCorsHandler: load volume fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), param.Bucket(), err)
		return
	}
	if err = deleteBucketCors(vol); err != nil {
		log.LogErrorf("deleteBucketCorsHandler: delete bucket cors fail: requestID(%v) volume(%v) err(%v)",
			GetRequestID(r), vol.Name(), err)
		return
	}
	vol.metaLoader.storeCors(nil)
	w.WriteHeader(http.StatusNoContent)

	return
}

// Option object
// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTOPTIONSobject.html
func (o *ObjectNode) optionsObjectHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("optionsObjectHandler: OPTIONS object, requestID(%v) remote(%v)", GetRequestID(r), r.RemoteAddr)
	// Already done in methods 'corsMiddleware'.
	return
}
