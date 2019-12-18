package objectnode

import (
	"net/http"

	"github.com/chubaofs/chubaofs/util/log"
)

// Head bucket
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
func (o *ObjectNode) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	// do nothing
}

// List buckets
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (o *ObjectNode) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: implement 'listBucketsHandler'
}

// Get bucket location
// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (o *ObjectNode) getBucketLocation(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("getBucketLocation: get bucket location: requestID(%v)", RequestIDFromRequest(r))
	// TODO: implement 'getBucketLocation'
	var output = &GetBucketLocationOutput{
		LocationConstraint: o.region,
	}
	var marshaled []byte
	var err error
	if marshaled, err = MarshalXMLEntity(output); err != nil {
		log.LogErrorf("getBucketLocation: marshal result fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
		ServeInternalStaticErrorResponse(w, r)
		return
	}
	if _, err = w.Write(marshaled); err != nil {
		log.LogErrorf("getBucketLocation: write response body fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
	}
	return
}
