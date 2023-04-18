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

import (
	"encoding/xml"
	"net/http"
	"strings"
)

type ErrorCode struct {
	ErrorCode    string
	ErrorMessage string
	StatusCode   int
}

func (code ErrorCode) ServeResponse(w http.ResponseWriter, r *http.Request) error {
	// write status code to request context,
	// traceMiddleWare send exception request to prometheus via status code
	SetResponseStatusCode(r, code.StatusCode)
	SetResponseErrorMessage(r, code.ErrorMessage)

	var err error
	var marshaled []byte
	var xmlError = struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Resource  string   `xml:"Resource"`
		RequestId string   `xml:"RequestId"`
	}{
		Code:      code.ErrorCode,
		Message:   code.ErrorMessage,
		Resource:  r.URL.String(),
		RequestId: GetRequestID(r),
	}
	if marshaled, err = xml.Marshal(&xmlError); err != nil {
		return err
	}
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.WriteHeader(code.StatusCode)
	if _, err = w.Write(marshaled); err != nil {
		return err
	}
	return nil
}

func ServeInternalStaticErrorResponse(w http.ResponseWriter, r *http.Request) {
	SetResponseStatusCode(r, http.StatusInternalServerError)
	w.Header()[HeaderNameContentType] = []string{HeaderValueContentTypeXML}
	w.WriteHeader(http.StatusInternalServerError)
	sb := strings.Builder{}
	sb.WriteString(xml.Header)
	sb.WriteString("<Error><Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message><Resource>")
	sb.WriteString(r.URL.String())
	sb.WriteString("</Resource><RequestId>")
	sb.WriteString(GetRequestID(r))
	sb.WriteString("</RequestId></Error>")
	_, _ = w.Write([]byte(sb.String()))
}

// Presets
var (
	UnsupportedOperation                = &ErrorCode{ErrorCode: "UnsupportedOperation", ErrorMessage: "Operation is not supported", StatusCode: http.StatusBadRequest}
	AccessDenied                        = &ErrorCode{ErrorCode: "AccessDenied", ErrorMessage: "Access Denied", StatusCode: http.StatusForbidden}
	BadDigest                           = &ErrorCode{ErrorCode: "BadDigest", ErrorMessage: "The Content-MD5 you specified did not match what we received.", StatusCode: http.StatusBadRequest}
	BucketNotExisted                    = &ErrorCode{ErrorCode: "BucketNotExisted", ErrorMessage: "The requested bucket name is not existed.", StatusCode: http.StatusNotFound}
	BucketNotExistedForHead             = &ErrorCode{ErrorCode: "BucketNotExisted", ErrorMessage: "The requested bucket name is not existed.", StatusCode: http.StatusConflict}
	BucketNotEmpty                      = &ErrorCode{ErrorCode: "BucketNotEmpty", ErrorMessage: "The bucket you tried to delete is not empty.", StatusCode: http.StatusConflict}
	BucketNotOwnedByYou                 = &ErrorCode{ErrorCode: "BucketNotOwnedByYou", ErrorMessage: "The bucket is not owned by you.", StatusCode: http.StatusConflict}
	KeyTooLongError                     = &ErrorCode{ErrorCode: "KeyTooLongError", ErrorMessage: "", StatusCode: http.StatusBadRequest}
	InvalidKey                          = &ErrorCode{ErrorCode: "InvalidKey", ErrorMessage: "Object key is Illegal", StatusCode: http.StatusBadRequest}
	EntityTooSmall                      = &ErrorCode{ErrorCode: "EntityTooSmall", ErrorMessage: "Your proposed upload is smaller than the minimum allowed object size.", StatusCode: http.StatusBadRequest}
	EntityTooLarge                      = &ErrorCode{ErrorCode: "EntityTooLarge", ErrorMessage: "Your proposed upload exceeds the maximum allowed object size.", StatusCode: http.StatusBadRequest}
	IncorrectNumberOfFilesInPostRequest = &ErrorCode{ErrorCode: "IncorrectNumberOfFilesInPostRequest", ErrorMessage: "POST requires exactly one file upload per request.", StatusCode: http.StatusBadRequest}
	InvalidArgument                     = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "Invalid Argument", StatusCode: http.StatusBadRequest}
	InvalidBucketName                   = &ErrorCode{ErrorCode: "InvalidBucketName", ErrorMessage: "The specified bucket is not valid.", StatusCode: http.StatusBadRequest}
	InvalidRange                        = &ErrorCode{ErrorCode: "InvalidRange", ErrorMessage: "The requested range cannot be satisfied.", StatusCode: http.StatusRequestedRangeNotSatisfiable}
	MissingContentLength                = &ErrorCode{ErrorCode: "MissingContentLength", ErrorMessage: "You must provide the Content-Length HTTP header.", StatusCode: http.StatusLengthRequired}
	NoSuchBucket                        = &ErrorCode{ErrorCode: "NoSuchBucket", ErrorMessage: "The specified bucket does not exist.", StatusCode: http.StatusNotFound}
	NoSuchKey                           = &ErrorCode{ErrorCode: "NoSuchKey", ErrorMessage: "The specified key does not exist.", StatusCode: http.StatusNotFound}
	PreconditionFailed                  = &ErrorCode{ErrorCode: "PreconditionFailed", ErrorMessage: "At least one of the preconditions you specified did not hold.", StatusCode: http.StatusPreconditionFailed}
	MaxContentLength                    = &ErrorCode{ErrorCode: "MaxContentLength", ErrorMessage: "Content-Length is bigger than 20KB.", StatusCode: http.StatusLengthRequired}
	DuplicatedBucket                    = &ErrorCode{ErrorCode: "CreateBucketFailed", ErrorMessage: "Duplicate bucket name.", StatusCode: http.StatusBadRequest}
	ObjectModeConflict                  = &ErrorCode{ErrorCode: "ObjectModeConflict", ErrorMessage: "Object already exists but file mode conflicts", StatusCode: http.StatusConflict}
	NotModified                         = &ErrorCode{ErrorCode: "MaxContentLength", ErrorMessage: "Not modified.", StatusCode: http.StatusNotModified}
	NoSuchUpload                        = &ErrorCode{ErrorCode: "NoSuchUpload", ErrorMessage: "The specified upload does not exist.", StatusCode: http.StatusNotFound}
	OverMaxRecordSize                   = &ErrorCode{ErrorCode: "OverMaxRecordSize", ErrorMessage: "The length of a record in the input or result is greater than maxCharsPerRecord of 1 MB.", StatusCode: http.StatusBadRequest}
	CopySourceSizeTooLarge              = &ErrorCode{ErrorCode: "InvalidRequest", ErrorMessage: "The specified copy source is larger than the maximum allowable size for a copy source: 5368709120", StatusCode: http.StatusBadRequest}
	InvalidPartOrder                    = &ErrorCode{ErrorCode: "InvalidPartOrder", ErrorMessage: "The list of parts was not in ascending order. Parts list must be specified in order by part number.", StatusCode: http.StatusBadRequest}
	InvalidPart                         = &ErrorCode{ErrorCode: "InvalidPart", ErrorMessage: "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.", StatusCode: http.StatusBadRequest}
	InvalidCacheArgument                = &ErrorCode{ErrorCode: "InvalidCacheArgument", ErrorMessage: "Invalid Cache-Control or Expires Argument", StatusCode: http.StatusBadRequest}
	TagsGreaterThen10                   = &ErrorCode{ErrorCode: "BadRequest", ErrorMessage: "Object tags cannot be greater than 10", StatusCode: http.StatusBadRequest}
	InvalidTagKey                       = &ErrorCode{ErrorCode: "InvalidTag", ErrorMessage: "The TagKey you have provided is invalid", StatusCode: http.StatusBadRequest}
	InvalidTagValue                     = &ErrorCode{ErrorCode: "InvalidTagValue", ErrorMessage: "The TagValue you have provided is invalid", StatusCode: http.StatusBadRequest}
	TooManyRequests                     = &ErrorCode{ErrorCode: "TooManyRequests", ErrorMessage: "Too many requests, reduce your request", StatusCode: http.StatusTooManyRequests}
	TooManyLevelsOfSymlinks             = &ErrorCode{ErrorCode: "TooManyLevelsOfSymlink", ErrorMessage: "Too many levels of symbolic links", StatusCode: http.StatusBadRequest}
)

func HttpStatusErrorCode(code int) *ErrorCode {
	statusText := http.StatusText(code)
	statusTextWithoutSpace := strings.ReplaceAll(statusText, " ", "")
	return &ErrorCode{
		ErrorCode:    statusTextWithoutSpace,
		ErrorMessage: statusText,
		StatusCode:   code,
	}
}

func InternalErrorCode(err error) *ErrorCode {
	var errorMessage string
	if err != nil {
		errorMessage = err.Error()
	} else {
		errorMessage = "Internal Server Error"
	}
	return &ErrorCode{
		ErrorCode:    "InternalError",
		ErrorMessage: errorMessage,
		StatusCode:   http.StatusInternalServerError,
	}
}
