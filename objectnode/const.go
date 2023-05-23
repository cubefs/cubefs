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

import "os"

const (
	MaxRetry = 3
)

const (
	S3Namespace        = "http://s3.amazonaws.com/doc/2006-03-01/"
	Server             = "Server"
	Host               = "Host"
	LastModified       = "Last-Modified"
	ETag               = "ETag"
	Date               = "Date"
	ContentMD5         = "Content-MD5"
	ContentEncoding    = "Content-Encoding"
	ContentType        = "Content-Type"
	ContentLength      = "Content-Length"
	ContentRange       = "Content-Range"
	ContentDisposition = "Content-Disposition"
	Authorization      = "Authorization"
	AcceptRanges       = "Accept-Ranges"
	Range              = "Range"
	Expect             = "Expect"
	XForwardedExpect   = "X-Forwarded-Expect"
	Location           = "Location"
	CacheControl       = "Cache-Control"
	Expires            = "Expires"
	Connection         = "Connection"
	Signature          = "Signature"
	Origin             = "Origin"

	AccessControlRequestMethod    = "Access-Control-Request-Method"
	AccessControlRequestHeaders   = "Access-Control-Request-Headers"
	AccessControlAllowOrigin      = "Access-Control-Allow-Origin"
	AccessControlAllowCredentials = "Access-Control-Allow-Credentials"
	AccessControlMaxAge           = "Access-Control-Max-Age"
	AccessControlAllowMethods     = "Access-Control-Allow-Methods"
	AccessControlAllowHeaders     = "Access-Control-Allow-Headers"
	AccessControlExposeHeaders    = "Access-Control-Expose-Headers"

	IfMatch           = "If-Match"
	IfNoneMatch       = "If-None-Match"
	IfModifiedSince   = "If-Modified-Since"
	IfUnmodifiedSince = "If-Unmodified-Since"

	XAmzRequestId                   = "x-amz-request-id"
	XAmzCopySource                  = "x-amz-copy-source"
	XAmzCopySourceRange             = "x-amz-copy-source-range"
	XAmzCopySourceIfMatch           = "x-amz-copy-source-if-match"
	XAmzCopySourceIfNoneMatch       = "x-amz-copy-source-if-none-match"
	XAmzCopySourceIfModifiedSince   = "x-amz-copy-source-if-modified-since"
	XAmzCopySourceIfUnmodifiedSince = "x-amz-copy-source-if-unmodified-since"
	XAmzDecodedContentLength        = "x-amz-decoded-content-length"
	XAmzTagging                     = "x-amz-tagging"
	XAmzMetaPrefix                  = "x-amz-meta-"
	XAmzMpPartsCount                = "x-amz-mp-parts-count"
	XAmzMetadataDirective           = "x-amz-metadata-directive"
	XAmzBucketRegion                = "x-amz-bucket-region"
	XAmzStorageClass                = "x-amz-storage-class"
	XAmzTaggingCount                = "x-amz-tagging-count"
	XAmzContentSha256               = "X-Amz-Content-Sha256"
	XAmzCredential                  = "X-Amz-Credential"
	XAmzSignature                   = "X-Amz-Signature"
	XAmzSignedHeaders               = "X-Amz-SignedHeaders"
	XAmzAlgorithm                   = "X-Amz-Algorithm"
	XAmzDate                        = "X-Amz-Date"
	XAmzExpires                     = "X-Amz-Expires"
	XAmzSecurityToken               = "X-Amz-Security-Token"
)

const (
	ValueServer               = "CubeFS"
	ValueAcceptRanges         = "bytes"
	ValueContentTypeStream    = "application/octet-stream"
	ValueContentTypeXML       = "application/xml"
	ValueContentTypeJSON      = "application/json"
	ValueContentTypeDirectory = "application/directory"
	ValueMultipartFormData    = "multipart/form-data"
)

const (
	SubObjectDelete    = "delete"
	SubMultipartUpload = "uploads"
)

const (
	ParamUploadId   = "uploadId"
	ParamPartNumber = "partNumber"
	ParamKeyMarker  = "key-marker"
	ParamMarker     = "marker"
	ParamPrefix     = "prefix"
	ParamContToken  = "continuation-token"
	ParamFetchOwner = "fetch-owner"
	ParamMaxKeys    = "max-keys"
	ParamStartAfter = "start-after"
	ParamKey        = "key"

	ParamMaxParts       = "max-parts"
	ParamUploadIdMarker = "upload-id-marker"
	ParamPartNoMarker   = "part-number-marker"
	ParamPartMaxUploads = "max-uploads"
	ParamPartDelimiter  = "delimiter"
	ParamEncodingType   = "encoding-type"

	ParamResponseCacheControl       = "response-cache-control"
	ParamResponseContentType        = "response-content-type"
	ParamResponseContentDisposition = "response-content-disposition"
	ParamResponseExpires            = "response-expires"
)

const (
	MaxKeys    = 1000
	MaxParts   = 1000
	MaxUploads = 1000
)

const (
	StorageClassStandard = "STANDARD"
)

// XAttr keys for ObjectNode compatible feature
const (
	XAttrKeyOSSETag         = "oss:etag"
	XAttrKeyOSSTagging      = "oss:tagging"
	XAttrKeyOSSPolicy       = "oss:policy"
	XAttrKeyOSSACL          = "oss:acl"
	XAttrKeyOSSMIME         = "oss:mime"
	XAttrKeyOSSDISPOSITION  = "oss:disposition"
	XAttrKeyOSSCORS         = "oss:cors"
	XAttrKeyOSSCacheControl = "oss:cache"
	XAttrKeyOSSExpires      = "oss:expires"

	// Deprecated
	XAttrKeyOSSETagDeprecated = "oss:tag"
)

const (
	DateLayout              = "20060102"
	ISO8601Format           = "20060102T150405Z"
	ISO8601Layout           = "2006-01-02T15:04:05.000Z"
	ISO8601LayoutCompatible = "2006-01-02T15:04:05Z"
	RFC1123Format           = "Mon, 02 Jan 2006 15:04:05 GMT"
)

const (
	EmptyContentMD5String = "d41d8cd98f00b204e9800998ecf8427e"
)

const (
	DefaultFileMode = 0644
	DefaultDirMode  = DefaultFileMode | os.ModeDir
)

const (
	SplitFileRangeBlockSize     = 10 * 1024 * 1024 // 10MB
	ParallelDownloadPartSize    = 10 * 1024 * 1024
	MinParallelDownloadFileSize = 2 * ParallelDownloadPartSize
)

const (
	MaxCopyObjectSize = 5 * 1024 * 1024 * 1024
)

const (
	MetadataDirectiveCopy    = "COPY"
	MetadataDirectiveReplace = "REPLACE"
)

const (
	TaggingCounts         = 10
	TaggingKeyMaxLength   = 128
	TaggingValueMaxLength = 256
)
