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
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"
)

var (
	UnsupportedOperation                = &ErrorCode{ErrorCode: "NotImplemented", ErrorMessage: "A header you provided implies functionality that is not implemented.", StatusCode: http.StatusNotImplemented}
	AccessDenied                        = &ErrorCode{ErrorCode: "AccessDenied", ErrorMessage: "Access Denied", StatusCode: http.StatusForbidden}
	BadDigest                           = &ErrorCode{ErrorCode: "BadDigest", ErrorMessage: "The Content-MD5 you specified did not match what we received.", StatusCode: http.StatusBadRequest}
	BucketNotExisted                    = &ErrorCode{ErrorCode: "BucketNotExisted", ErrorMessage: "The requested bucket name is not existed.", StatusCode: http.StatusNotFound}
	BucketNotExistedForHead             = &ErrorCode{ErrorCode: "BucketNotExisted", ErrorMessage: "The requested bucket name is not existed.", StatusCode: http.StatusConflict}
	BucketNotEmpty                      = &ErrorCode{ErrorCode: "BucketNotEmpty", ErrorMessage: "The bucket you tried to delete is not empty.", StatusCode: http.StatusConflict}
	BucketNotOwnedByYou                 = &ErrorCode{ErrorCode: "BucketNotOwnedByYou", ErrorMessage: "The bucket is not owned by you.", StatusCode: http.StatusConflict}
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
	NoSuchBucketPolicy                  = &ErrorCode{ErrorCode: "NoSuchBucketPolicy", ErrorMessage: "The specified bucket does not have a bucket policy.", StatusCode: http.StatusNotFound}
	PreconditionFailed                  = &ErrorCode{ErrorCode: "PreconditionFailed", ErrorMessage: "At least one of the preconditions you specified did not hold.", StatusCode: http.StatusPreconditionFailed}
	MaxContentLength                    = &ErrorCode{ErrorCode: "MaxContentLength", ErrorMessage: "Content-Length is bigger than 20KB.", StatusCode: http.StatusLengthRequired}
	BucketAlreadyOwnedByYou             = &ErrorCode{ErrorCode: "BucketAlreadyOwnedByYou", ErrorMessage: "The bucket that you tried to create already exists, and you own it. ", StatusCode: http.StatusConflict}
	InvalidLocationConstraint           = &ErrorCode{ErrorCode: "CreateBucketFailed", ErrorMessage: "The specified location (Region) constraint is not valid.", StatusCode: http.StatusBadRequest}
	ObjectModeConflict                  = &ErrorCode{ErrorCode: "ObjectModeConflict", ErrorMessage: "Object already exists but file mode conflicts", StatusCode: http.StatusConflict}
	NotModified                         = &ErrorCode{ErrorCode: "MaxContentLength", ErrorMessage: "Not modified.", StatusCode: http.StatusNotModified}
	NoSuchUpload                        = &ErrorCode{ErrorCode: "NoSuchUpload", ErrorMessage: "The specified upload does not exist.", StatusCode: http.StatusNotFound}
	ConflictUploadRequest               = &ErrorCode{ErrorCode: "ConflictUploadRequest", ErrorMessage: "Conflict request, please try again later.", StatusCode: http.StatusConflict}
	OverMaxRecordSize                   = &ErrorCode{ErrorCode: "OverMaxRecordSize", ErrorMessage: "The length of a record in the input or result is greater than maxCharsPerRecord of 1 MB.", StatusCode: http.StatusBadRequest}
	CopySourceSizeTooLarge              = &ErrorCode{ErrorCode: "InvalidRequest", ErrorMessage: "The specified copy source is larger than the maximum allowable size for a copy source: 5368709120", StatusCode: http.StatusBadRequest}
	InvalidPartOrder                    = &ErrorCode{ErrorCode: "InvalidPartOrder", ErrorMessage: "The list of parts was not in ascending order. Parts list must be specified in order by part number.", StatusCode: http.StatusBadRequest}
	InvalidPart                         = &ErrorCode{ErrorCode: "InvalidPart", ErrorMessage: "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.", StatusCode: http.StatusBadRequest}
	InvalidCacheArgument                = &ErrorCode{ErrorCode: "InvalidCacheArgument", ErrorMessage: "Invalid Cache-Control or Expires Argument", StatusCode: http.StatusBadRequest}
	ExceedTagLimit                      = &ErrorCode{ErrorCode: "InvalidTagError", ErrorMessage: "Object tags cannot be greater than 10", StatusCode: http.StatusBadRequest}
	InvalidTagKey                       = &ErrorCode{ErrorCode: "InvalidTag", ErrorMessage: "The TagKey you have provided is invalid", StatusCode: http.StatusBadRequest}
	InvalidTagValue                     = &ErrorCode{ErrorCode: "InvalidTag", ErrorMessage: "The TagValue you have provided is invalid", StatusCode: http.StatusBadRequest}
	MissingContentMD5                   = &ErrorCode{ErrorCode: "InvalidRequest", ErrorMessage: "Missing required header for this request: Content-MD5.", StatusCode: http.StatusBadRequest}
	UnexpectedContent                   = &ErrorCode{ErrorCode: "UnexpectedContent", ErrorMessage: "This request does not support content.", StatusCode: http.StatusBadRequest}
	AccessDeniedBySTS                   = &ErrorCode{ErrorCode: "AccessDeniedBySTS", ErrorMessage: "Access Denied by STS.", StatusCode: http.StatusForbidden}
	InvalidToken                        = &ErrorCode{ErrorCode: "InvalidToken", ErrorMessage: "The provided token is malformed or otherwise invalid.", StatusCode: http.StatusBadRequest}
	ExpiredToken                        = &ErrorCode{ErrorCode: "ExpiredToken", ErrorMessage: "The provided token has expired.", StatusCode: http.StatusBadRequest}
	MissingSecurityElement              = &ErrorCode{ErrorCode: "MissingSecurityElement", ErrorMessage: "The request is missing a security element.", StatusCode: http.StatusBadRequest}
	RequestTimeTooSkewed                = &ErrorCode{ErrorCode: "RequestTimeTooSkewed", ErrorMessage: "The difference between the request time and the server's time is too large.", StatusCode: http.StatusBadRequest}
	NoSuchTagSetError                   = &ErrorCode{ErrorCode: "NoSuchTagSetError", ErrorMessage: "The TagSet does not exist.", StatusCode: http.StatusNotFound}
	InvalidTagError                     = &ErrorCode{ErrorCode: "InvalidTagError", ErrorMessage: "Missing tag in body.", StatusCode: http.StatusBadRequest}
	NoSuchCORSConfiguration             = &ErrorCode{ErrorCode: "NoSuchCORSConfiguration", ErrorMessage: "The CORS configuration does not exist.", StatusCode: http.StatusNotFound}
	CORSRuleNotMatch                    = &ErrorCode{ErrorCode: "AccessForbidden", ErrorMessage: "CORSResponse: This CORS request is not allowed.", StatusCode: http.StatusForbidden}
	ErrCORSNotEnabled                   = &ErrorCode{ErrorCode: "AccessForbidden", ErrorMessage: "CORSResponse: CORS is not enabled for this bucket.", StatusCode: http.StatusForbidden}
	MissingOriginHeader                 = &ErrorCode{ErrorCode: "MissingOriginHeader", ErrorMessage: "Missing Origin header.", StatusCode: http.StatusBadRequest}
	MalformedXML                        = &ErrorCode{ErrorCode: "MalformedXML", ErrorMessage: "The XML you provided was not well-formed or did not validate against our published schema.", StatusCode: http.StatusBadRequest}
	TooManyCorsRules                    = &ErrorCode{ErrorCode: "TooManyCorsRules", ErrorMessage: "Too many cors rules.", StatusCode: http.StatusBadRequest}
	InvalidDigest                       = &ErrorCode{ErrorCode: "InvalidDigest", ErrorMessage: "The Content-MD5 you specified is not valid.", StatusCode: http.StatusBadRequest}
	KeyTooLong                          = &ErrorCode{ErrorCode: "KeyTooLongError", ErrorMessage: "Your key is too long.", StatusCode: http.StatusBadRequest}
	InvalidAccessKeyId                  = &ErrorCode{ErrorCode: "InvalidAccessKeyId", ErrorMessage: "The access key Id you provided does not exist in our records.", StatusCode: http.StatusForbidden}
	SignatureDoesNotMatch               = &ErrorCode{ErrorCode: "SignatureDoesNotMatch", ErrorMessage: "The request signature we calculated does not match the signature you provided.", StatusCode: http.StatusForbidden}
	InvalidMaxPartNumber                = &ErrorCode{ErrorCode: "InvalidRequest", ErrorMessage: "The total part numbers exceed limit.", StatusCode: http.StatusBadRequest}
	InvalidMinPartNumber                = &ErrorCode{ErrorCode: "InvalidRequest", ErrorMessage: "You must specify at least one part.", StatusCode: http.StatusBadRequest}
	DiskQuotaExceeded                   = &ErrorCode{"DiskQuotaExceeded", "Disk Quota Exceeded.", http.StatusBadRequest}
)

type ErrorCode struct {
	ErrorCode    string
	ErrorMessage string
	StatusCode   int
}

type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestId string   `xml:"RequestId"`
}

func NewError(errCode, errMsg string, statusCode int) *ErrorCode {
	return &ErrorCode{
		ErrorCode:    errCode,
		ErrorMessage: errMsg,
		StatusCode:   statusCode,
	}
}

func (ec *ErrorCode) ServeResponse(w http.ResponseWriter, r *http.Request) {
	// write status code to request context,
	// traceMiddleWare send exception request to prometheus via status code
	SetResponseStatusCode(r, strconv.Itoa(ec.StatusCode))
	SetResponseErrorMessage(r, ec.ErrorMessage)

	errorResponse := ErrorResponse{
		Code:      ec.ErrorCode,
		Message:   ec.ErrorMessage,
		Resource:  r.URL.String(),
		RequestId: GetRequestID(r),
	}
	response, _ := MarshalXMLEntity(errorResponse)
	w.Header().Set(ContentType, ValueContentTypeXML)
	w.Header().Set(ContentLength, strconv.Itoa(len(response)))
	w.WriteHeader(ec.StatusCode)
	_, _ = w.Write(response)
}

func (ec *ErrorCode) Error() string {
	return ec.ErrorMessage
}

func ServeInternalStaticErrorResponse(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(ContentType, ValueContentTypeXML)
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
