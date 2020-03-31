package objectnode

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/EnableCorsUsingREST.html

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/chubaofs/chubaofs/util/log"
)

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html
func (o *ObjectNode) getBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("Get bucket cors")
	var (
		err error
		ec  *ErrorCode
	)
	defer o.errorResponse(w, r, err, ec)

	var param *RequestParam
	param = ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = NoSuchBucket
		return
	}

	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		ec = NoSuchBucket
		return
	}

	var output = CORSConfiguration{}

	cors := vol.loadCors()
	if cors != nil {
		output.CORSRule = cors.CORSRule
	}
	var corsData []byte
	if corsData, err = xml.Marshal(output); err != nil {
		ec = InternalErrorCode(err)
		return
	}

	_, _ = w.Write(corsData)
	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html
func (o *ObjectNode) putBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("Put bucket cors")

	var (
		err error
		ec  *ErrorCode
	)
	defer o.errorResponse(w, r, err, ec)

	var param *RequestParam
	param = ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = NoSuchBucket
		return
	}
	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		ec = NoSuchBucket
		return
	}

	var bytes []byte
	if bytes, err = ioutil.ReadAll(r.Body); err != nil && err != io.EOF {
		return
	}

	var corsConfig *CORSConfiguration
	if corsConfig, err = ParseCorsConfig(bytes); err != nil {
		return
	}
	if corsConfig == nil {
		return
	}

	var newBytes []byte
	if newBytes, err = json.Marshal(corsConfig); err != nil {
		return
	}
	if err = storeBucketCors(newBytes, vol); err != nil {
		return
	}
	vol.storeCors(corsConfig)

	return
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html
func (o *ObjectNode) deleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("Delete bucket cors")

	var (
		err error
		ec  *ErrorCode
	)
	defer o.errorResponse(w, r, err, ec)

	var param *RequestParam
	param = ParseRequestParam(r)
	if param.Bucket() == "" {
		ec = NoSuchBucket
		return
	}
	var vol *Volume
	if vol, err = o.vm.Volume(param.Bucket()); err != nil {
		ec = NoSuchBucket
		return
	}

	if err = deleteBucketCors(vol); err != nil {
		return
	}
	vol.storeCors(nil)

	w.WriteHeader(http.StatusNoContent)
	return
}
