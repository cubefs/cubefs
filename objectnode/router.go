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

	var routers []*mux.Router
	bRouter := router.PathPrefix("/").Subrouter()
	for _, d := range o.domains {
		routers = append(routers, bRouter.Host("{bucket:.+}."+d).Subrouter())
		routers = append(routers, bRouter.Host("{bucket:.+}."+d+":{port:[0-9]+}").Subrouter())
	}
	routers = append(routers, bRouter.PathPrefix("/{bucket}").Subrouter())

	for _, r := range routers {

		// List parts
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.listPartsHandler, []Action{ListMultipartUploadPartsAction}, Read)).
			Queries("uploadId", "{uploadId:.*}")

		// Get object with presgined auth signature v2
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectHandler, []Action{ListBucketAction}, Read)).
			Queries("AWSAccessKeyId", "{accessKey:.+}",
				"Expires", "{expires:[0-9]+}", "Signature", "{signature:.+}")

		// Get object with presigned auth signature v4
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectHandler, []Action{ListBucketAction}, Read)).
			Queries("X-Amz-Credential", "{creadential:.+}",
				"X-Amz-Algorithm", "{algorithm:.+}", "X-Amz-Signature", "{signature:.+}",
				"X-Amz-Date", "{date:.+}", "X-Amz-SignedHeaders", "{signedHeaders:.+}",
				"X-Amz-Expires", "{expires:[0-9]+}")

		// Get object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectHandler, []Action{GetObjectAction}, Read))

		// Create multipart upload
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
		r.Methods(http.MethodPost).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.createMultipleUploadHandler, []Action{PutObjectAction}, Write)).
			Queries("uploads", "")

		// Complete multipart
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
		r.Methods(http.MethodPost).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.completeMultipartUploadHandler, []Action{PutObjectAction}, Write)).
			Queries("uploadId", "{uploadId:.*}")

		// Upload part
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html .
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.uploadPartHandler, []Action{PutObjectAction}, Write)).
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}")

		// Copy object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html .
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HeadersRegexp(HeaderNameCopySource, ".*?(\\/|%2F).*?").
			HandlerFunc(o.policyCheck(o.copyObjectHandler, []Action{PutObjectAction}, Write))

		// Put object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.putObjectHandler, []Action{PutObjectAction}, Write))

		// Abort multipart
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html .
		r.Methods(http.MethodDelete).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.abortMultipartUploadHandler, []Action{AbortMultipartUploadAction}, Write)).
			Queries("uploadId", "{uploadId:.*}")

		// Head object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
		r.Methods(http.MethodHead).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.headObjectHandler, []Action{GetObjectAction}, Read))

		// Get object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectTagging, []Action{GetBucketPolicyAction}, Read)).
			Queries("tagging", "")

		// Put object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.putObjectTagging, []Action{PutBucketPolicyAction}, Write)).
			Queries("tagging", "")

		// Delete object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
		r.Methods(http.MethodDelete).
			Path("/{object:.+").
			HandlerFunc(o.policyCheck(o.deleteObjectTagging, []Action{PutBucketPolicyAction}, Write)).
			Queries("tagging", "")

		// Put object xattrs
		// Notes: ChubaoFS owned API for XAttr operation
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.putObjectXAttr).
			Queries("xattr", "")

		// Delete object xattrs
		// Notes: ChubaoFS owned API for XAttr operation
		r.Methods(http.MethodDelete).
			Path("/{object:.+}").
			HandlerFunc(o.deleteObjectXAttr).
			Queries("xattr", "key", "{key:.+}}")

		// Get object XAttr
		// Notes: ChubaoFS owned API for XAttr operation
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.getObjectXAttr).
			Queries("xattr", "", "key", "{key:.+}")

		// List object XAttrs
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.listObjectXAttrs).
			Queries("xattr", "")

		// Get object acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
		r.Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.getObjectACLHandler, []Action{GetObjectAclAction}, Read)).
			Queries("acl", "")

		// Put object acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
		r.Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.putObjectACLHandler, []Action{PutObjectAclAction}, Write)).
			Queries("acl", "")

		// Delete object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html .
		r.Methods(http.MethodDelete).
			Path("/{object:.+}").
			HandlerFunc(o.policyCheck(o.deleteObjectHandler, []Action{DeleteObjectAction}, Write))

		// List multipart uploads
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.listMultipartUploadsHandler, []Action{ListMultipartUploadPartsAction}, Read)).
			Queries("uploads", "")

		// List parts
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.listPartsHandler, []Action{ListMultipartUploadPartsAction}, Read)).
			Queries("uploadId", "{uploadId:.*}")

		// Delete objects (multiple objects)
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
		r.Methods(http.MethodPost).
			HandlerFunc(o.policyCheck(o.deleteObjectsHandler, []Action{DeleteObjectAction}, Write)).
			Queries("delete", "")

		// Get bucket location
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketLocation, []Action{GetBucketLocationAction}, Read)).
			Queries("location", "")

		// Get bucket policy
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketPolicyHandler, []Action{GetBucketPolicyAction}, Read)).
			Queries("policy", "")

		// Put bucket policy
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
		r.Methods(http.MethodPut).
			HandlerFunc(o.policyCheck(o.putBucketPolicyHandler, []Action{PutBucketPolicyAction}, Write)).
			Queries("policy", "")

		// Delete bucket policy
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
		r.Methods(http.MethodDelete).
			HandlerFunc(o.policyCheck(o.deleteBucketPolicyHandler, []Action{DeleteBucketPolicyAction}, Write)).
			Queries("policy", "")

		// Get bucket acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketACLHandler, []Action{GetBucketAclAction}, Read)).
			Queries("acl", "")

		// Put bucket acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
		r.Methods(http.MethodPut).
			HandlerFunc(o.policyCheck(o.putBucketACLHandler, []Action{PutBucketAclAction}, Write)).
			Queries("acl", "")

		// List objects version 2
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketV2Handler, []Action{ListBucketAction}, Read)).
			Queries("list-type", "2")

		// Head bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
		r.Methods(http.MethodHead).
			HandlerFunc(o.policyCheck(o.headBucketHandler, []Action{ListBucketAction}, Read))

		// !MUST set latest loop
		// List objects version 1
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
		r.Methods(http.MethodGet).
			HandlerFunc(o.policyCheck(o.getBucketV1Handler, []Action{ListBucketAction}, Read))

		// Create bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
		r.Methods(http.MethodPut).
			HandlerFunc(o.createBucketHandler)

		// Delete bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
		r.Methods(http.MethodDelete).
			HandlerFunc(o.policyCheck(o.deleteBucketHandler, []Action{DeleteBucketAction}, Write))

	}

	// List buckets
	// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
	router.Methods(http.MethodGet).
		HandlerFunc(o.listBucketsHandler)

	// Unsupported operation
	router.NotFoundHandler = http.HandlerFunc(o.unsupportedOperationHandler)
}
