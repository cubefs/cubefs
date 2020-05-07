// Copyright 2019 The ChubaoFS Authors.
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
	HeaderNameServer             = "Server"
	HeaderNameHost               = "Host"
	HeaderNameLastModified       = "Last-Modified"
	HeaderNameETag               = "ETag"
	HeaderNameDate               = "Date"
	HeaderNameContentMD5         = "Content-MD5"
	HeaderNameContentEnc         = "Content-Encoding"
	HeaderNameContentType        = "Content-Type"
	HeaderNameContentLength      = "Content-Length"
	HeaderNameContentRange       = "Content-Range"
	HeaderNameContentDisposition = "Content-Disposition"
	HeaderNameAuthorization      = "Authorization"
	HeaderNameAcceptRange        = "Accept-Ranges"
	HeaderNameRange              = "Range"
	HeaderNameExpect             = "Expect"
	HeaderNameXForwardedExpect   = "X-Forwarded-Expect"
	HeaderNameLocation           = "Location"

	// Headers for CORS validation
	HeaderNameAccessControlAllowOrigin  = "Access-Control-Allow-Origin"
	HeaderNameAccessControlAllowMethods = "Access-Control-Allow-Methods"
	HeaderNameAccessControlAllowHeaders = "Access-Control-Allow-Headers"
	HeaderNameAccessControlMaxAge       = "Access-Control-Max-Age"

	HeaderNameXAmzStartDate           = "x-amz-date"
	HeaderNameXAmzRequestId           = "x-amz-request-id"
	HeaderNameXAmzContentHash         = "x-amz-content-sha256"
	HeaderNameXAmzCopySource          = "x-amz-copy-source"
	HeaderNameXAmzCopyMatch           = "x-amz-copy-source-if-match"
	HeaderNameXAmzCopyNoneMatch       = "x-amz-copy-source-if-none-match"
	HeaderNameXAmzCopyModified        = "x-amz-copy-source-if-modified-since"
	HeaderNameXAmzCopyUnModified      = "x-amz-copy-source-if-unmodified-since"
	HeaderNameXAmzDecodeContentLength = "x-amz-decoded-content-length"
	HeaderNameXAmzTagging             = "x-amz-tagging"
	HeaderNameXAmzMetaPrefix          = "x-amz-meta-"
	HeaderNameXAmzDownloadPartCount   = "x-amz-mp-parts-count"
	HeaderNameXAmzMetadataDirective   = "x-amz-metadata-directive"

	HeaderNameIfMatch           = "If-Match"
	HeaderNameIfNoneMatch       = "If-None-Match"
	HeaderNameIfModifiedSince   = "If-Modified-Since"
	HeaderNameIfUnmodifiedSince = "If-Unmodified-Since"
)

const (
	HeaderValueServer               = "ChubaoFS"
	HeaderValueAcceptRange          = "bytes"
	HeaderValueTypeStream           = "application/octet-stream"
	HeaderValueContentTypeXML       = "application/xml"
	HeaderValueContentTypeDirectory = "application/directory"
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
	ParamUploadIdMarker = "upload-id-​marker"
	ParamPartNoMarker   = "part-number-marker"
	ParamPartMaxUploads = "max-uploads"
	ParamPartDelimiter  = "delimiter"
)

const (
	MaxKeys    = 1000
	MaxParts   = 1000
	MaxUploads = 1000
)

const (
	StorageClassStandard = "Standard"
)

// XAttr keys for ObjectNode compatible feature
const (
	XAttrKeyOSSETag        = "oss:etag"
	XAttrKeyOSSTagging     = "oss:tagging"
	XAttrKeyOSSPolicy      = "oss:policy"
	XAttrKeyOSSMIME        = "oss:mime"
	XAttrKeyOSSDISPOSITION = "oss:disposition"

	// Departure
	XAttrKeyOSSETagInvalid = "oss:tag"
)

const (
	AMZTimeFormat = "2006-01-02T15:04:05.000Z"
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
