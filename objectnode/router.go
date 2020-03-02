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

	"github.com/gorilla/mux"
)

// register api routers
func (o *ObjectNode) registerApiRouters(router *mux.Router) {

	var bucketRouters []*mux.Router
	bRouter := router.PathPrefix("/").Subrouter()
	for _, d := range o.domains {
		bucketRouters = append(bucketRouters, bRouter.Host("{bucket:.+}."+d).Subrouter())
		bucketRouters = append(bucketRouters, bRouter.Host("{bucket:.+}."+d+":{port:[0-9]+}").Subrouter())
	}
	bucketRouters = append(bucketRouters, bRouter.PathPrefix("/{bucket}").Subrouter())

	var registerBucketHttpHeadRouters = func(r *mux.Router) {
		// Head object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
		r.NewRoute().Name(HeadObjectAction.UniqueRouteName()).
			Methods(http.MethodHead).
			Path("/{object:.+}").
			HandlerFunc(o.headObjectHandler)

		// Head bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
		r.NewRoute().Name(HeadBucketAction.UniqueRouteName()).
			Methods(http.MethodHead).
			HandlerFunc(o.headBucketHandler)
	}

	var registerBucketHttpGetRouters = func(r *mux.Router) {
		// Get object with pre-signed auth signature v2
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.NewRoute().Name(GetObjectAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("AWSAccessKeyId", "{accessKey:.+}",
				"Expires", "{expires:[0-9]+}", "Signature", "{signature:.+}").
			HandlerFunc(o.getObjectHandler)

		// Get object with pre-signed auth signature v4
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.NewRoute().Name(GetObjectAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("X-Amz-Credential", "{creadential:.+}",
				"X-Amz-Algorithm", "{algorithm:.+}", "X-Amz-Signature", "{signature:.+}",
				"X-Amz-Date", "{date:.+}", "X-Amz-SignedHeaders", "{signedHeaders:.+}",
				"X-Amz-Expires", "{expires:[0-9]+}").
			HandlerFunc(o.getObjectHandler)

		// Get object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
		r.NewRoute().Name(GetObjectTaggingAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("tagging", "").
			HandlerFunc(o.getObjectTaggingHandler)

		// Get object XAttr
		// Notes: ChubaoFS owned API for XAttr operation
		r.NewRoute().Name(GetObjectXAttrAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("xattr", "", "key", "{key:.+}").
			HandlerFunc(o.getObjectXAttr)

		// List object XAttrs
		r.NewRoute().Name(ListObjectXAttrsAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("xattr", "").
			HandlerFunc(o.listObjectXAttrs)

		// Get object acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
		r.NewRoute().Name(GetObjectAclAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Path("/{objject:.+}").
			Queries("acl", "").
			HandlerFunc(o.getObjectACLHandler)

		// Get object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.NewRoute().Name(GetObjectAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.getObjectHandler)

		// List objects version 2
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
		r.NewRoute().Name(ListObjectsAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Queries("list-type", "2").
			HandlerFunc(o.getBucketV2Handler)

		// List multipart uploads
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
		r.NewRoute().Name(ListMultipartUploadsAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Queries("uploads", "").
			HandlerFunc(o.listMultipartUploadsHandler)

		// List parts
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
		r.NewRoute().Name(ListPartsAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Queries("uploadId", "{uploadId:.*}").
			HandlerFunc(o.listPartsHandler)

		// Get bucket location
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
		r.NewRoute().Name(GetBucketLocationAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Queries("location", "").
			HandlerFunc(o.getBucketLocation)

		// Get bucket policy
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
		r.NewRoute().Name(GetBucketPolicyAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Queries("policy", "").
			HandlerFunc(o.getBucketPolicyHandler)

		// Get bucket acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
		r.NewRoute().Name(GetBucketAclAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Queries("acl", "").
			HandlerFunc(o.getBucketACLHandler)

		// Get bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
		r.NewRoute().Name(GetBucketTaggingAction.UniqueRouteName()).
			Methods(http.MethodGet).
			Queries("tagging", "").
			HandlerFunc(o.getBucketTaggingHandler)

		// List objects version 1
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
		r.NewRoute().Name(ListObjectsAction.UniqueRouteName()).
			Methods(http.MethodGet).
			HandlerFunc(o.getBucketV1Handler)
	}

	var registerBucketHttpPostRouters = func(r *mux.Router) {
		// Create multipart upload
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
		r.NewRoute().Name(CreateMultipartUploadAction.UniqueRouteName()).
			Methods(http.MethodPost).
			Path("/{object:.+}").
			Queries("uploads", "").
			HandlerFunc(o.createMultipleUploadHandler)

		// Complete multipart
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
		r.NewRoute().Name(CompleteMultipartUploadAction.UniqueRouteName()).
			Methods(http.MethodPost).
			Path("/{object:.+}").
			Queries("uploadId", "{uploadId:.*}").
			HandlerFunc(o.completeMultipartUploadHandler)

		// Delete objects (multiple objects)
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
		r.NewRoute().Name(DeleteObjectsAction.UniqueRouteName()).
			Methods(http.MethodPost).
			Queries("delete", "").
			HandlerFunc(o.deleteObjectsHandler)
	}

	var registerBucketHttpPutRouters = func(r *mux.Router) {
		// Upload part
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html .
		r.NewRoute().Name(UploadPartAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}").
			HandlerFunc(o.uploadPartHandler)

		// Copy object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html .
		r.NewRoute().Name(CopyObjectAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			HeadersRegexp(HeaderNameCopySource, ".*?(\\/|%2F).*?").
			HandlerFunc(o.copyObjectHandler)

		// Put object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
		r.NewRoute().Name(PutObjectTaggingAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("tagging", "").
			HandlerFunc(o.putObjectTaggingHandler)

		// Put object xattrs
		// Notes: ChubaoFS owned API for XAttr operation
		r.NewRoute().Name(PutObjectXAttrAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("xattr", "").
			HandlerFunc(o.putObjectXAttrHandler)

		// Put object acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
		r.NewRoute().Name(PutObjectAclAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("acl", "").
			HandlerFunc(o.putObjectACLHandler)

		// Put object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
		r.NewRoute().Name(PutObjectAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.putObjectHandler)

		// Put bucket acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
		r.NewRoute().Name(PutBucketAclAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Queries("acl", "").
			HandlerFunc(o.putBucketACLHandler)

		// Put bucket policy
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
		r.NewRoute().Name(PutBucketPolicyAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Queries("policy", "").
			HandlerFunc(o.putBucketPolicyHandler)

		// Put bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
		r.NewRoute().Name(PutBucketTaggingAction.UniqueRouteName()).
			Methods(http.MethodPut).
			Queries("tagging", "").
			HandlerFunc(o.putBucketTaggingHandler)

		// Create bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
		r.NewRoute().Name(CreateBucketAction.UniqueRouteName()).
			Methods(http.MethodPut).
			HandlerFunc(o.createBucketHandler)
	}

	var registerBucketHttpDeleteRouters = func(r *mux.Router) {
		// Abort multipart upload
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html .
		r.NewRoute().Name(AbortMultipartUploadAction.UniqueRouteName()).
			Methods(http.MethodDelete).
			Path("/{object:.+}").
			Queries("uploadId", "{uploadId:.*}").
			HandlerFunc(o.abortMultipartUploadHandler)

		// Delete object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
		r.NewRoute().Name(DeleteObjectTaggingAction.UniqueRouteName()).
			Methods(http.MethodDelete).
			Path("/{object:.+").
			Queries("tagging", "").
			HandlerFunc(o.deleteObjectTaggingHandler)

		// Delete object xattrs
		// Notes: ChubaoFS owned API for XAttr operation
		r.NewRoute().Name(DeleteObjectXAttrAction.UniqueRouteName()).
			Methods(http.MethodDelete).
			Path("/{object:.+}").
			Queries("xattr", "", "key", "{key:.+}}").
			HandlerFunc(o.deleteObjectXAttrHandler)

		// Delete object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html .
		r.NewRoute().Name(DeleteObjectAction.UniqueRouteName()).
			Methods(http.MethodDelete).
			Path("/{object:.+}").
			HandlerFunc(o.deleteObjectHandler)

		// Delete bucket policy
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
		r.NewRoute().Name(DeleteBucketPolicyAction.UniqueRouteName()).
			Methods(http.MethodDelete).
			Queries("policy", "").
			HandlerFunc(o.deleteBucketPolicyHandler)

		// Delete bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
		r.NewRoute().Name(DeleteBucketTaggingAction.UniqueRouteName()).
			Methods(http.MethodDelete).
			Queries("tagging", "").
			HandlerFunc(o.deleteBucketTaggingHandler)

		// Delete bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
		r.NewRoute().Name(DeleteBucketAction.UniqueRouteName()).
			Methods(http.MethodDelete).
			HandlerFunc(o.deleteBucketHandler)

	}

	for _, r := range bucketRouters {
		registerBucketHttpHeadRouters(r)
		registerBucketHttpGetRouters(r)
		registerBucketHttpPostRouters(r)
		registerBucketHttpPutRouters(r)
		registerBucketHttpDeleteRouters(r)
	}

	// List buckets
	// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
	router.NewRoute().Name(ListBucketAction.UniqueRouteName()).
		Methods(http.MethodGet).
		HandlerFunc(o.listBucketsHandler)

	// Unsupported operation
	router.NotFoundHandler = http.HandlerFunc(o.unsupportedOperationHandler)
}
