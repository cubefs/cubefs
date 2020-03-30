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

import "os"

type OSSOperation string

const (
	HeaderNameServer        = "Server"
	HeaderNameHost          = "Host"
	HeaderNameLastModified  = "Last-Modified"
	HeaderNameETag          = "ETag"
	HeaderNameDate          = "Date"
	HeaderNameContentMD5    = "content-md5"
	HeaderNameContentEnc    = "content-encoding"
	HeaderNameContentType   = "Content-Type"
	HeaderNameContentLength = "Content-Length"
	HeaderNameContentRange  = "Content-Range"
	HeaderNameAuthorization = "Authorization"
	HeaderNameAcceptRange   = "Accept-Ranges"
	HeaderNameRange         = "Range"

	// Headers for CORS validation
	HeaderNameAccessControlAllowOrigin  = "Access-Control-Allow-Origin"
	HeaderNameAccessControlAllowMethods = "Access-Control-Allow-Methods"
	HeaderNameAccessControlAllowHeaders = "Access-Control-Allow-Headers"
	HeaderNameAccessControlMaxAge       = "Access-Control-Max-Age"

	HeaderNameStartDate           = "x-amz-date"
	HeaderNameRequestId           = "x-amz-request-id"
	HeaderNameContentHash         = "X-Amz-Content-SHA256"
	HeaderNameCopySource          = "X-Amz-Copy-Source"
	HeaderNameCopyMatch           = "x-amz-copy-source-if-match"
	HeaderNameCopyNoneMatch       = "x-amz-copy-source-if-none-match"
	HeaderNameCopyModified        = "x-amz-copy-source-if-modified-since"
	HeaderNameCopyUnModified      = "x-amz-copy-source-if-unmodified-since"
	HeaderNameDecodeContentLength = "X-Amz-Decoded-Content-Length"
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
	XAttrKeyOSSETag    = "oss:tag"
	XAttrKeyOSSTagging = "oss:tagging"
	XAttrKeyOSSPolicy  = "oss:policy"
	XAttrKeyOSSMIME    = "oss:mime"
)

const (
	AMZTimeFormat = "2006-01-02T15:04:05Z"
)

const (
	EmptyContentMD5String = "d41d8cd98f00b204e9800998ecf8427e"
)

const (
	DefaultFileMode = 0644
	DefaultDirMode  = DefaultFileMode | os.ModeDir
)
