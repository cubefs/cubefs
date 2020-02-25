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
	"strings"

	"github.com/chubaofs/chubaofs/util"
	"github.com/google/uuid"

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

	var unionRouteName = func(action Action) (name string) {
		var id uuid.UUID
		var err error
		if id, err = uuid.NewRandom(); err != nil {
			name = action.String() + ":" + util.RandomString(32, util.UpperLetter|util.LowerLetter)
			return
		}
		name = action.String() + ":" + strings.ReplaceAll(id.String(), "-", "")
		return
	}

	var registerBucketHttpHeadRouters = func(r *mux.Router) {
		// Head object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
		r.Methods(http.MethodHead).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.headObjectHandler, HeadObjectAction)).
			Name(unionRouteName(HeadObjectAction))

		// Head bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
		r.Methods(http.MethodHead).
			HandlerFunc(o.policyCheck(o.headBucketHandler, HeadBucketAction)).
			Name(unionRouteName(HeadBucketAction))
	}

	var registerBucketHttpGetRouters = func(r *mux.Router) {
		// Get object with pre-signed auth signature v2
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectHandler, GetObjectAction)).
			Queries("AWSAccessKeyId", "{accessKey:.+}",
				"Expires", "{expires:[0-9]+}", "Signature", "{signature:.+}").
			Name(unionRouteName(GetObjectAction))

		// Get object with pre-signed auth signature v4
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectHandler, GetObjectAction)).
			Queries("X-Amz-Credential", "{creadential:.+}",
				"X-Amz-Algorithm", "{algorithm:.+}", "X-Amz-Signature", "{signature:.+}",
				"X-Amz-Date", "{date:.+}", "X-Amz-SignedHeaders", "{signedHeaders:.+}",
				"X-Amz-Expires", "{expires:[0-9]+}")

		// Get object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectTaggingHandler, GetObjectTaggingAction)).
			Queries("tagging", "")

		// Get object XAttr
		// Notes: ChubaoFS owned API for XAttr operation
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectXAttr, GetObjectXAttrAction)).
			Queries("xattr", "", "key", "{key:.+}")

		// List object XAttrs
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.listObjectXAttrs, ListObjectXAttrsAction)).
			Queries("xattr", "")

		// Get object acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
		r.Methods(http.MethodGet).
			Path("/{objject:.+}").
			HandlerFunc(o.policyCheck(o.getObjectACLHandler, GetObjectAclAction)).
			Queries("acl", "")

		// Get object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectHandler, GetObjectAction))

		// List objects version 2
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketV2Handler, ListObjectsAction)).
			Queries("list-type", "2")

		// List multipart uploads
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.listMultipartUploadsHandler, ListMultipartUploadsAction)).
			Queries("uploads", "")

		// List parts
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.listPartsHandler, ListPartsAction)).
			Queries("uploadId", "{uploadId:.*}")

		// Get bucket location
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketLocation, GetBucketLocationAction)).
			Queries("location", "")

		// Get bucket policy
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketPolicyHandler, GetBucketPolicyAction)).
			Queries("policy", "")

		// Get bucket acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketACLHandler, GetBucketAclAction)).
			Queries("acl", "")

		// Get bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketTaggingHandler, GetBucketTaggingAction)).
			Queries("tagging", "")

		// List objects version 1
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketV1Handler, ListObjectsAction))
	}

	var registerBucketHttpPostRouters = func(r *mux.Router) {
		// Create multipart upload
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
		r.Methods(http.MethodPost).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.createMultipleUploadHandler, CreateMultipartUploadAction)).
			Queries("uploads", "")

		// Complete multipart
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
		r.Methods(http.MethodPost).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.completeMultipartUploadHandler, CompleteMultipartUploadAction)).
			Queries("uploadId", "{uploadId:.*}")

		// Delete objects (multiple objects)
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
		r.Methods(http.MethodPost).
			HandlerFunc(o.policyCheck(o.deleteObjectsHandler, DeleteObjectsAction)).
			Queries("delete", "")
	}

	var registerBucketHttpPutRouters = func(r *mux.Router) {
		// Upload part
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html .
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.uploadPartHandler, UploadPartAction)).
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")

		// Copy object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html .
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HeadersRegexp(HeaderNameCopySource, ".*?(\\/|%2F).*?").
			HandlerFunc(o.policyCheck(o.copyObjectHandler, CopyObjectAction))

		// Put object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.putObjectTaggingHandler, PutObjectTaggingAction)).
			Queries("tagging", "")

		// Put object xattrs
		// Notes: ChubaoFS owned API for XAttr operation
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.putObjectXAttr, PutObjectXAttrAction)).
			Queries("xattr", "")

		// Put object acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.putObjectACLHandler, PutObjectAclAction)).
			Queries("acl", "")

		// Put object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.putObjectHandler, PutObjectAction))

		// Put bucket acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
		r.Methods(http.MethodPut).
			HandlerFunc(o.policyCheck(o.putBucketACLHandler, PutBucketAclAction)).
			Queries("acl", "")

		// Put bucket policy
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
		r.Methods(http.MethodPut).
			HandlerFunc(o.policyCheck(o.putBucketPolicyHandler, PutBucketPolicyAction)).
			Queries("policy", "")

		// Put bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
		r.Methods(http.MethodPut).
			HandlerFunc(o.policyCheck(o.putBucketTaggingHandler, PutBucketTaggingAction)).
			Queries("tagging", "")

		// Create bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
		r.Methods(http.MethodPut).
			HandlerFunc(o.createBucketHandler)
	}

	var registerBucketHttpDeleteRouters = func(r *mux.Router) {
		// Abort multipart upload
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html .
		r.Methods(http.MethodDelete).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.abortMultipartUploadHandler, AbortMultipartUploadAction)).
			Queries("uploadId", "{uploadId:.*}")

		// Delete object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
		r.Methods(http.MethodDelete).
			Path("/{object:.+").
			HandlerFunc(o.policyCheck(o.deleteObjectTaggingHandler, DeleteObjectTaggingAction)).
			Queries("tagging", "")

		// Delete object xattrs
		// Notes: ChubaoFS owned API for XAttr operation
		r.Methods(http.MethodDelete).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.deleteObjectXAttr, DeleteObjectXAttrAction)).
			Queries("xattr", "key", "{key:.+}}")

		// Delete object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html .
		r.Methods(http.MethodDelete).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.deleteObjectHandler, DeleteObjectAction))

		// Delete bucket policy
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
		r.Methods(http.MethodDelete).
			HandlerFunc(o.policyCheck(o.deleteBucketPolicyHandler, DeleteBucketPolicyAction)).
			Queries("policy", "")

		// Delete bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
		r.Methods(http.MethodDelete).
			Handler(o.policyCheck(o.deleteBucketTaggingHandler, DeleteBucketTaggingAction)).
			Queries("tagging", "")

		// Delete bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
		r.Methods(http.MethodDelete).HandlerFunc(o.policyCheck(o.deleteBucketHandler, DeleteBucketAction))

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
	router.Methods(http.MethodGet).
		HandlerFunc(o.listBucketsHandler)

	// Unsupported operation
	router.NotFoundHandler = http.HandlerFunc(o.unsupportedOperationHandler)
}
