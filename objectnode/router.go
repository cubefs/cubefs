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
	"net/http"

	"github.com/cubefs/cubefs/proto"

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
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSHeadObjectAction)).
			Methods(http.MethodHead).
			Path("/{object:.+}").
			HandlerFunc(o.headObjectHandler)

		// Head bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSHeadBucketAction)).
			Methods(http.MethodHead).
			HandlerFunc(o.headBucketHandler)
	}

	var registerBucketHttpGetRouters = func(r *mux.Router) {

		// Get Object Lock configuration
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetObjectLockConfigurationAction)).
			Methods(http.MethodGet).
			Queries("object-lock", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// List parts
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSListPartsAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("uploadId", "{uploadId:.*}").
			HandlerFunc(o.listPartsHandler)

		// Get object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetObjectTaggingAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("tagging", "").
			HandlerFunc(o.getObjectTaggingHandler)

		// Get object XAttr
		// Notes: CubeFS owned API for XAttr operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetObjectXAttrAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("xattr", "", "key", "{key:.+}").
			HandlerFunc(o.getObjectXAttrHandler)

		// List object XAttrs
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSListObjectXAttrsAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("xattr", "").
			HandlerFunc(o.listObjectXAttrs)

		// Get object acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetObjectAclAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("acl", "").
			HandlerFunc(o.getObjectACLHandler)

		// Get object legal hold
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetObjectLegalHoldAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("legal-hold", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get object retention
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetObjectRetentionAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("retention", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get object torrent
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTorrent.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetObjectTorrentAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			Queries("torrent", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetObjectAction)).
			Methods(http.MethodGet).
			Path("/{object:.+}").
			HandlerFunc(o.getObjectHandler)

		// List objects version 2
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSListObjectsAction)).
			Methods(http.MethodGet).
			Queries("list-type", "2").
			HandlerFunc(o.getBucketV2Handler)

		// List multipart uploads
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSListMultipartUploadsAction)).
			Methods(http.MethodGet).
			Queries("uploads", "").
			HandlerFunc(o.listMultipartUploadsHandler)

		// Get bucket location
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketLocationAction)).
			Methods(http.MethodGet).
			Queries("location", "").
			HandlerFunc(o.getBucketLocation)

		// Get bucket policy
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketPolicyAction)).
			Methods(http.MethodGet).
			Queries("policy", "").
			HandlerFunc(o.getBucketPolicyHandler)

		// Get bucket policy status
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketPolicyStatusAction)).
			Methods(http.MethodGet).
			Queries("policyStatus", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get bucket acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketAclAction)).
			Methods(http.MethodGet).
			Queries("acl", "").
			HandlerFunc(o.getBucketACLHandler)

		// Get bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketTaggingAction)).
			Methods(http.MethodGet).
			Queries("tagging", "").
			HandlerFunc(o.getBucketTaggingHandler)

		// Get bucket encryption
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketEncryptionAction)).
			Methods(http.MethodGet).
			Queries("encryption", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get bucket cors
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketCorsAction)).
			Methods(http.MethodGet).
			Queries("cors", "").
			HandlerFunc(o.getBucketCorsHandler)

		// Get bucket website
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketWebsite.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketWebsiteAction)).
			Methods(http.MethodGet).
			Queries("website", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get public access block
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetPublicAccessBlock.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetPublicAccessBlockAction)).
			Methods(http.MethodGet).
			Queries("publicAccessBlock", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get bucket request payment
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketRequestPayment.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketRequestPaymentAction)).
			Methods(http.MethodGet).
			Queries("requestPayment", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get bucket replication
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketReplication.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketReplicationAction)).
			Methods(http.MethodGet).
			Queries("replication", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get bucket lifecycle
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycle.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketLifecycleAction)).
			Methods(http.MethodGet).
			Queries("lifecycle", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Get bucket versioning
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSGetBucketVersioningAction)).
			Methods(http.MethodGet).
			Queries("versioning", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// List object versions
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSListObjectVersionsAction)).
			Methods(http.MethodGet).
			Queries("versions", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// List objects version 1
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSListObjectsAction)).
			Methods(http.MethodGet).
			HandlerFunc(o.getBucketV1Handler)
	}

	var registerBucketHttpPostRouters = func(r *mux.Router) {
		// Create multipart upload
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSCreateMultipartUploadAction)).
			Methods(http.MethodPost).
			Path("/{object:.+}").
			Queries("uploads", "").
			HandlerFunc(o.createMultipleUploadHandler)

		// Complete multipart
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSCompleteMultipartUploadAction)).
			Methods(http.MethodPost).
			Path("/{object:.+}").
			Queries("uploadId", "{uploadId:.*}").
			HandlerFunc(o.completeMultipartUploadHandler)

		// Restore object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSRestoreObjectAction)).
			Methods(http.MethodPost).
			Path("/{object:.+}").
			Queries("restore", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Delete objects (multiple objects)
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteObjectsAction)).
			Methods(http.MethodPost).
			Queries("delete", "").
			HandlerFunc(o.deleteObjectsHandler)
	}

	var registerBucketHttpPutRouters = func(r *mux.Router) {
		// Put Object Lock configuration
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutObjectLockConfigurationAction)).
			Methods(http.MethodPut).
			Queries("object-lock", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Upload part copy
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSUploadPartCopyAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			HeadersRegexp(HeaderNameXAmzCopySource, ".*?(\\/|%2F).*?").
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}").
			HandlerFunc(o.uploadPartCopyHandler)

		// Upload part
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html .
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSUploadPartAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId:.*}").
			HandlerFunc(o.uploadPartHandler)

		// Copy object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html .
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSCopyObjectAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			HeadersRegexp(HeaderNameXAmzCopySource, ".*?(\\/|%2F).*?").
			HandlerFunc(o.copyObjectHandler)

		// Put object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutObjectTaggingAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("tagging", "").
			HandlerFunc(o.putObjectTaggingHandler)

		// Put object xattrs
		// Notes: CubeFS owned API for XAttr operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutObjectXAttrAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("xattr", "").
			HandlerFunc(o.putObjectXAttrHandler)

		// Put object acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutObjectAclAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("acl", "").
			HandlerFunc(o.putObjectACLHandler)

		// Put object legal hold
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutObjectLegalHoldAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("legal-hold", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Put object retention
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutObjectRetentionAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			Queries("retention", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Put object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutObjectAction)).
			Methods(http.MethodPut).
			Path("/{object:.+}").
			HandlerFunc(o.putObjectHandler)

		// Put bucket acl
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketAclAction)).
			Methods(http.MethodPut).
			Queries("acl", "").
			HandlerFunc(o.putBucketACLHandler)

		// Put bucket policy
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketPolicyAction)).
			Methods(http.MethodPut).
			Queries("policy", "").
			HandlerFunc(o.putBucketPolicyHandler)

		// Put bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketTaggingAction)).
			Methods(http.MethodPut).
			Queries("tagging", "").
			HandlerFunc(o.putBucketTaggingHandler)

		// Put bucket encryption
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketEncryptionAction)).
			Methods(http.MethodPut).
			Queries("encryption", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Put bucket cors
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketCorsAction)).
			Methods(http.MethodPut).
			Queries("cors", "").
			HandlerFunc(o.putBucketCorsHandler)

		// Put bucket website
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketWebsite.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketWebsiteAction)).
			Methods(http.MethodPut).
			Queries("website", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Put public access block
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutPublicAccessBlock.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutPublicAccessBlockAction)).
			Methods(http.MethodPut).
			Queries("publicAccessBlock", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Put bucket request payment
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketRequestPayment.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketRequestPaymentAction)).
			Methods(http.MethodPut).
			Queries("requestPayment", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Put bucket replication
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketReplicationAction)).
			Methods(http.MethodPut).
			Queries("replication", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Put bucket lifecycle
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycle.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketLifecycleAction)).
			Methods(http.MethodPut).
			Queries("lifecycle", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Put bucket versioning
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSPutBucketVersioningAction)).
			Methods(http.MethodPut).
			Queries("versioning", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Create bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSCreateBucketAction)).
			Methods(http.MethodPut).
			HandlerFunc(o.createBucketHandler)
	}

	var registerBucketHttpDeleteRouters = func(r *mux.Router) {
		// Abort multipart upload
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html .
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSAbortMultipartUploadAction)).
			Methods(http.MethodDelete).
			Path("/{object:.+}").
			Queries("uploadId", "{uploadId:.*}").
			HandlerFunc(o.abortMultipartUploadHandler)

		// Delete object tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteObjectTaggingAction)).
			Methods(http.MethodDelete).
			Path("/{object:.+}").
			Queries("tagging", "").
			HandlerFunc(o.deleteObjectTaggingHandler)

		// Delete object xattrs
		// Notes: CubeFS owned API for XAttr operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteObjectXAttrAction)).
			Methods(http.MethodDelete).
			Path("/{object:.+}").
			Queries("xattr", "", "key", "{key:.+}").
			HandlerFunc(o.deleteObjectXAttrHandler)

		// Delete object
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html .
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteObjectAction)).
			Methods(http.MethodDelete).
			Path("/{object:.+}").
			HandlerFunc(o.deleteObjectHandler)

		// Delete bucket policy
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteBucketPolicyAction)).
			Methods(http.MethodDelete).
			Queries("policy", "").
			HandlerFunc(o.deleteBucketPolicyHandler)

		// Delete bucket tagging
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteBucketTaggingAction)).
			Methods(http.MethodDelete).
			Queries("tagging", "").
			HandlerFunc(o.deleteBucketTaggingHandler)

		// Delete bucket encryption
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteBucketEncryptionAction)).
			Methods(http.MethodDelete).
			Queries("encryption", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Delete bucket cors
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteBucketCorsAction)).
			Methods(http.MethodDelete).
			Queries("cors", "").
			HandlerFunc(o.deleteBucketCorsHandler)

		// Delete bucket website
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketWebsite.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteBucketWebsiteAction)).
			Methods(http.MethodDelete).
			Queries("website", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Delete public access block
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletePublicAccessBlock.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeletePublicAccessBlockAction)).
			Methods(http.MethodDelete).
			Queries("publicAccessBlock", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Delete bucket replication
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketReplication.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteBucketReplicationAction)).
			Methods(http.MethodDelete).
			Queries("replication", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Delete bucket lifecycle
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
		// Notes: unsupported operation
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteBucketLifecycleAction)).
			Methods(http.MethodDelete).
			Queries("lifecycle", "").
			HandlerFunc(o.unsupportedOperationHandler)

		// Delete bucket
		// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSDeleteBucketAction)).
			Methods(http.MethodDelete).
			HandlerFunc(o.deleteBucketHandler)

	}

	var registerBucketHttpOptionsRouters = func(r *mux.Router) {
		// OPTIONS object
		// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTOPTIONSobject.html
		r.NewRoute().Name(ActionToUniqueRouteName(proto.OSSOptionsObjectAction)).
			Methods(http.MethodOptions).
			HandlerFunc(o.optionsObjectHandler)
	}

	for _, r := range bucketRouters {
		registerBucketHttpHeadRouters(r)
		registerBucketHttpGetRouters(r)
		registerBucketHttpPostRouters(r)
		registerBucketHttpPutRouters(r)
		registerBucketHttpDeleteRouters(r)
		registerBucketHttpOptionsRouters(r)
	}

	// List buckets
	// API reference: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
	router.NewRoute().Name(ActionToUniqueRouteName(proto.OSSListBucketsAction)).
		Methods(http.MethodGet).
		HandlerFunc(o.listBucketsHandler)

	// Unsupported operation
	router.NotFoundHandler = http.HandlerFunc(o.unsupportedOperationHandler)
}
