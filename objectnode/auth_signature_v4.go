/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 * Modifications copyright 2019 The ChubaoFS Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package objectnode

import (
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
)

const (
	SignatureExpires    = time.Hour * 24 * 7     // Signature is valid for seven days after the specified date.
	MaxPresignedExpires = 3 * 365 * 24 * 60 * 60 //10years
	DateFormatISO8601   = "20060102T150405Z"     //"yyyyMMddTHHmmssZ"
	MaxSkewTime         = 15 * time.Minute

	XAmzContentSha256 = "X-Amz-Content-Sha256"
	XAmzCredential    = "X-Amz-Credential"
	XAmzSignature     = "X-Amz-Signature" //
	XAmzSignedHeaders = "X-Amz-SignedHeaders"
	XAmzAlgorithm     = "X-Amz-Algorithm"
	XAmzDate          = "X-Amz-Date"
	XAmzExpires       = "X-Amz-Expires"

	SignatureV4Algorithm = "AWS4-HMAC-SHA256"
	SignatureV4Request   = "aws4-request"
	SignedHeaderHost     = "host"
	UnsignedPayload      = "UNSIGNED-PAYLOAD"
	PresignedV4QueryAuth = "Authorization"

	credentialFlag    = "Credential="
	signatureFlag     = "Signature="
	signedHeadersFlag = "SignedHeaders="
)

var PresignedSignatureV4Queries = []string{
	XAmzCredential,
	XAmzSignature,
}

var AuthSignatureV4Headers = []string{
	PresignedV4QueryAuth,
	XAmzContentSha256,
}

// // isUrlUsingSignatureAlgorithmV4 checks if request is using signature algorithm V2 in url parameter.
// Example:
// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// url: "127.0.0.1:9000/umptest/b.tinyExtents
//      ?X-Amz-Algorithm=AWS4-HMAC-SHA256_CMD
//      &X-Amz-Credential=<your-access-key-id>/20130721/us-east-1/s3/aws4_request
//      &X-Amz-Date=20191111T112557Z
//      &X-Amz-Expires=432000
//      &X-Amz-SignedHeaders=host
//      &X-Amz-Signature=55c588734f017b861c24cbd69c203c283aad566cfe4ee712b7a3c846e1de151a"
func isUrlUsingSignatureAlgorithmV4(r *http.Request) bool {
	if u, err := url.Parse(strings.ToLower(r.URL.String())); err == nil {
		return isRequestQueryValid(u.Query(), PresignedSignatureV4Queries)
	}
	return false
}

// IsHeaderUsingSignatureAlgorithmV4 checks if request is using signature algorithm V4 in header.
func isHeaderUsingSignatureAlgorithmV4(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get(HeaderNameAuthorization), SignatureV4Algorithm)
}

// check request signature valid
func (o *ObjectNode) validateHeaderBySignatureAlgorithmV4(r *http.Request) (bool, error) {
	var err error

	var req *signatureRequestV4
	if req, err = parseRequestV4(r); err != nil {
		return false, err
	}

	// The signature is valid for seven days after the specified date.
	// Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
	var signatureTime time.Time
	if signatureTime, err = time.Parse("20060102", req.Credential.Date); err != nil {
		return false, err
	}
	if time.Since(signatureTime) > SignatureExpires {
		log.LogDebugf("expired signature: requestID(%v) remote(%v) scope(%v)",
			GetRequestID(r), getRequestIP(r), req.Credential.GetScopeString())
		return false, nil
	}

	var accessKey = req.Credential.AccessKey
	var secretKey string
	var bucket = mux.Vars(r)["bucket"]
	if userInfo, err := o.getUserInfoByAccessKey(accessKey); err == nil {
		secretKey = userInfo.SecretKey
	} else if (err == proto.ErrUserNotExists || err == proto.ErrAccessKeyNotExists) &&
		len(bucket) > 0 && GetActionFromContext(r) != proto.OSSCreateBucketAction {
		// In order to be directly compatible with the signature verification of version 1.5
		// (each volume has its own access key and secret key), if the user does not exist and
		// the request specifies a volume, try to use the accesskey and secret key bound in the
		// volume information for verification.
		var volume *Volume
		if volume, err = o.getVol(bucket); err != nil {
			return false, err
		}
		if ak, sk := volume.OSSSecure(); ak == accessKey {
			secretKey = sk
		} else {
			return false, nil
		}
	} else {
		log.LogErrorf("validateHeaderBySignatureAlgorithmV4: get secretKey from master fail: accessKey(%v) err(%v)",
			accessKey, err)
		return false, err
	}

	newSignature := calculateSignatureV4(r, req.Credential, secretKey, req.SignedHeaders)
	if req.Signature != newSignature {
		log.LogDebugf("validateHeaderBySignatureAlgorithmV4: invalid signature: requestID(%v) client(%v) server(%v)",
			GetRequestID(r), req.Signature, newSignature)
		return false, nil
	}

	return true, nil
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// url: "127.0.0.1:9000/umptest/b.tinyExtents
//      ?X-Amz-Algorithm=AWS4-HMAC-SHA256_CMD
//      &X-Amz-Credential=<your-access-key-id>/20130721/us-east-1/s3/aws4_request
//      &X-Amz-Date=20191111T112557Z
//      &X-Amz-Expires=432000
//      &X-Amz-SignedHeaders=host
//      &X-Amz-Signature=55c588734f017b861c24cbd69c203c283aad566cfe4ee712b7a3c846e1de151a"
//
func (o *ObjectNode) validateUrlBySignatureAlgorithmV4(r *http.Request) (pass bool, err error) {
	var req *signatureRequestV4
	req, err = parseRequestV4(r)
	if err != nil {
		log.LogErrorf("validateUrlBySignatureAlgorithmV4: parse request fail: requestID(%v) err(%v)", GetRequestID(r), err)
		return
	}

	//check req valid
	var ok bool
	if ok, err = req.isValid(); !ok {
		log.LogErrorf("validateUrlBySignatureAlgorithmV4: request invalid: requestID(%v) err(%v)", GetRequestID(r), err)
		return false, nil
	}

	var accessKey = req.Credential.AccessKey
	var secretKey string
	var bucket = mux.Vars(r)["bucket"]
	if userInfo, err := o.getUserInfoByAccessKey(accessKey); err == nil {
		secretKey = userInfo.SecretKey
	} else if (err == proto.ErrUserNotExists || err == proto.ErrAccessKeyNotExists) &&
		len(bucket) > 0 && GetActionFromContext(r) != proto.OSSCreateBucketAction {
		// In order to be directly compatible with the signature verification of version 1.5
		// (each volume has its own access key and secret key), if the user does not exist and
		// the request specifies a volume, try to use the accesskey and secret key bound in the
		// volume information for verification.
		var volume *Volume
		if volume, err = o.getVol(bucket); err != nil {
			return false, err
		}
		if ak, sk := volume.OSSSecure(); ak == accessKey {
			secretKey = sk
		} else {
			return false, nil
		}
	} else {
		log.LogErrorf("validateHeaderBySignatureAlgorithmV4: get secretKey from master fail: accessKey(%v) err(%v)",
			accessKey, err)
		return false, err
	}

	// create canonicalRequest
	var canonicalHeader http.Header
	canonicalHeader, err = req.createCanonicalHeaderV4()
	if err != nil {
		log.LogErrorf("validateUrlBySignatureAlgorithmV4: create canonical header fail: requestID(%v) err(%v)", GetRequestID(r), err)
		return
	}
	canonicalHeaderStr := buildCanonicalHeaderString(r.Host, canonicalHeader, req.SignedHeaders)
	headerNames := getCanonicalHeaderNames(req.SignedHeaders)
	payload := UnsignedPayload
	canonicalQuery := createCanonicalQueryV4(req)
	canonicalRequestString := createCanonicalRequestString(r.Method, getCanonicalURI(r), canonicalQuery, canonicalHeaderStr, headerNames, payload)

	log.LogDebugf("canonical request %v: %v",
		GetRequestID(r), strings.ReplaceAll(canonicalHeaderStr, "\n", "\\n"))

	// build signingKey
	signingKey := buildSigningKey(SCHEME, secretKey, req.Credential.Date, req.Credential.Region, req.Credential.Service, req.Credential.Request)

	// build stringToSign
	scope := buildScope(req.Credential.Date, req.Credential.Region, req.Credential.Service, req.Credential.Request)
	stringToSign := buildStringToSign(req.Algorithm, req.Timestamp, scope, canonicalRequestString)

	//sign stringToSign with signingKey
	newSignature := hex.EncodeToString(sign(stringToSign, signingKey))

	//compare newSignature with request signature
	pass = newSignature == req.Signature
	return
}

type credential struct {
	AccessKey string
	Date      string
	Region    string
	Service   string //s3
	Request   string
}

type signatureRequestV4 struct {
	r             *http.Request
	bucket        string
	URI           string
	Algorithm     string
	Timestamp     string
	Expires       string
	Signature     string
	SignedHeaders []string
	Credential    credential
}

func (c *credential) GetScopeString() string {
	return strings.Join([]string{
		c.Date,
		c.Region,
		c.Service,
		c.Request,
	}, "/")
}

//get presignedReq query
func (req *signatureRequestV4) Query() url.Values {
	return req.r.URL.Query()
}

// get Timestamp
func (req *signatureRequestV4) GetTimestamp() (time.Time, error) {
	return time.Parse(DateFormatISO8601, req.Timestamp)
}

// get
func (req *signatureRequestV4) GetExpires() (time.Duration, error) {
	return time.ParseDuration(req.Expires + "s")
}

//
func (req *signatureRequestV4) isValid() (bool, error) {
	expires, err := req.GetExpires()
	if err != nil {
		return false, errors.New("expires is invalid ")
	}
	if expires < 0 {
		return false, errors.New("expires < 0 ")
	}
	if expires.Seconds() > MaxPresignedExpires {
		return false, errors.New("expires > MaxPresignedExpires ")
	}
	utcNow := time.Now().UTC()
	ts, err1 := req.GetTimestamp()
	if err1 != nil {
		return false, errors.New("expires is invalid ")
	}
	if ts.After(utcNow.Add(MaxSkewTime)) {
		return false, errors.New("req date invalid ")
	}
	if utcNow.Sub(ts) > expires {
		return false, errors.New("expires time out ")
	}

	return true, nil
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
//
//  Authorization: AWS4-HMAC-SHA256\nCredential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-date,
//  Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024
//
func (req *signatureRequestV4) parseRequestHeaderV4(r *http.Request) (err error) {
	authorizationValue := r.Header.Get(HeaderNameAuthorization)
	if authorizationValue != "" {
		authorizationValue = strings.ReplaceAll(authorizationValue, " ", "")
		credentialIndex := strings.Index(authorizationValue, credentialFlag)
		algorithm := util.SubString(authorizationValue, len(HeaderNameAuthorization)+1, credentialIndex)
		if algorithm != "" {
			req.Algorithm = algorithm
		}
		credentialSignatureStr := util.SubString(authorizationValue, credentialIndex+len(credentialFlag), len(authorizationValue))
		authorizationValues := strings.Split(credentialSignatureStr, ",")
		if len(authorizationValues) < 2 {
			log.LogInfof("decode signature error: %v  ", authorizationValue)
			return errors.New("request header authorization parse error")
		}

		credentialStr := authorizationValues[0]
		if credentialStr != "" {
			req.parseCredential(credentialStr)
		}
		signedHeadersStr := authorizationValues[1]
		signatureStr := authorizationValues[2]
		req.Signature = util.SubString(signatureStr, len(signatureFlag), len(signatureStr))

		signedHeadersStr = util.SubString(signedHeadersStr, len(signedHeadersFlag), len(signedHeadersStr))
		req.SignedHeaders = strings.Split(signedHeadersStr, ";")
	}
	return
}

func (req *signatureRequestV4) parseCredential(credentialStr string) error {
	credentialStr = strings.TrimPrefix(credentialStr, credentialFlag)
	item := strings.Split(credentialStr, "/")
	if len(item) < 5 {
		return errors.New("x-amz-credential params invalid")
	}
	req.Credential = credential{
		AccessKey: item[0],
		Date:      item[1],
		Region:    item[2],
		Service:   item[3],
		Request:   item[4],
	}
	return nil
}

func (req *signatureRequestV4) parseRequestQueryV4(r *http.Request) (err error) {
	q := r.URL.Query()
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket != "" {
		req.bucket = bucket
	}
	algorithm := q.Get(XAmzAlgorithm)
	if algorithm != "" {
		req.Algorithm = algorithm
	}
	timestamp := q.Get(XAmzDate)
	if timestamp != "" {
		req.Timestamp = timestamp
		if _, err = req.GetTimestamp(); err != nil {
			return err
		}
	}
	expires := q.Get(XAmzExpires)
	if expires != "" {
		req.Expires = expires
		if _, err = req.GetExpires(); err != nil {
			return err
		}
	}
	credentialStr := q.Get(XAmzCredential)
	if credentialStr != "" {
		req.parseCredential(credentialStr)
	}
	signatrue := q.Get(XAmzSignature)
	if signatrue != "" {
		req.Signature = signatrue
	}
	signedHeaders := q.Get(XAmzSignedHeaders)
	if signedHeaders != "" {
		req.SignedHeaders = strings.Split(signedHeaders, ";")
	}
	return
}

// getPresignedV4 encodeQuery
// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// url: "127.0.0.1:9000/umptest/b.tinyExtents
//      ?X-Amz-Algorithm=AWS4-HMAC-SHA256_CMD
//      &X-Amz-Credential=<your-access-key-id>/20130721/us-east-1/s3/aws4_request
//      &X-Amz-Date=20191111T112557Z
//      &X-Amz-Expires=432000
//      &X-Amz-SignedHeaders=host
//      &X-Amz-Signature=55c588734f017b861c24cbd69c203c283aad566cfe4ee712b7a3c846e1de151a"
func parseRequestV4(r *http.Request) (req *signatureRequestV4, err error) {
	req = &signatureRequestV4{}
	req.r = r

	uri := strings.Split(r.RequestURI, "?")[0]
	if uri != "" {
		req.URI = uri
	} else {
		req.URI = "/"
	}

	//parseHeader
	err = req.parseRequestHeaderV4(r)
	if err != nil {
		return
	}

	//parseQuery
	err = req.parseRequestQueryV4(r)
	if err != nil {
		return
	}

	return
}

// create canonical query not contain X-Amz-Signature query
func createCanonicalQueryV4(req *signatureRequestV4) string {
	newQuery := make(url.Values)
	newQuery.Set(XAmzAlgorithm, req.Algorithm)
	newQuery.Set(XAmzExpires, req.Expires)
	newQuery.Set(XAmzDate, req.Timestamp)
	newQuery.Set(XAmzSignedHeaders, req.Query().Get(XAmzSignedHeaders))
	newQuery.Set(XAmzCredential, req.Query().Get(XAmzCredential))
	hashContent := req.Query().Get(XAmzContentSha256)
	if hashContent != "" {
		newQuery.Set(XAmzContentSha256, hashContent)
	} else {
	}

	for k, v := range req.Query() {
		key := strings.ToLower(k)
		if strings.Contains(key, "x-amz-meta-") {
			newQuery.Set(k, v[0])
			continue
		}
		if strings.Contains(key, "x-amz-server-side-") {
			newQuery.Set(k, v[0])
		}
		if strings.HasPrefix(key, "x-amz") {
			continue
		}
		newQuery[k] = v
	}

	return newQuery.Encode()
}

func buildSigningKey(scheme, secret, date, region, service, terminator string) []byte {
	secretKey := []byte(scheme + secret)
	dateKey := sign(date, secretKey)
	dateRegionKey := sign(region, dateKey)
	dateRegionServiceKey := sign(service, dateRegionKey)
	signingKey := sign(terminator, dateRegionServiceKey)
	return signingKey
}

func getContentHash(headers http.Header) (contentHash string) {
	for headerName := range headers {
		if strings.ToLower(headerName) == strings.ToLower(HeaderNameXAmzContentHash) {
			contentHash = headers.Get(headerName)
			break
		}
	}
	return
}

func getEncodeQuery(r *http.Request) string {
	return r.URL.Query().Encode()
}

// calculete signature v4
func calculateSignatureV4(r *http.Request, cred credential, secretKey string, signedHeaders []string) string {
	headers := r.Header

	// get request start time in ISO8601 type
	canonicalHeaderString := buildCanonicalHeaderString(r.Host, headers, signedHeaders)
	headerNames := getCanonicalHeaderNames(signedHeaders)
	contentHash := getContentHash(headers)
	encodeQuery := getEncodeQuery(r)
	canonicalURI := getCanonicalURI(r)
	canonicalRequest := createCanonicalRequestString(
		r.Method, canonicalURI, encodeQuery, canonicalHeaderString, headerNames, contentHash)

	signingKey := buildSigningKey(SCHEME, secretKey, cred.Date, cred.Region, SERVICE, TERMINATOR)
	scope := buildScope(cred.Date, cred.Region, SERVICE, TERMINATOR)

	var timestamp = getStartTime(headers)
	stringToSign := buildStringToSign(SignatureV4Algorithm, timestamp, scope, canonicalRequest)
	signature := sign(stringToSign, signingKey)

	log.LogDebugf("canonical request %v: %v",
		GetRequestID(r), strings.ReplaceAll(canonicalRequest, "\n", "\\n"))
	return hex.EncodeToString(signature)
}

func getCanonicalURI(r *http.Request) string {
	// If request path contain double slash, the java sdk of aws escape the second slash into '%2F' after computing signature,
	// so we received path is '/%2F', we have to unescape it to double slash again before computing signature.
	return strings.ReplaceAll(r.URL.EscapedPath(), "/%2F", "//")
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
func createCanonicalRequestString(httpMethod, canonicalURI, encodeQuery, canonicalHeaders, headerName, hashedPayload string) string {
	canonicalQueryString := strings.Replace(encodeQuery, "+", "%20", -1)
	return strings.Join([]string{
		httpMethod,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		headerName,
		hashedPayload,
	}, "\n")
}

// get headerNames string from signedHeaders
//  1. sort headers
//  2. lower
//  3. join with ;
func getCanonicalHeaderNames(signedHeaders []string) (headerName string) {
	sort.Strings(signedHeaders)

	lowerSignedHeaders := make([]string, 0)
	for _, headerName := range signedHeaders {
		lowerSignedHeaders = append(lowerSignedHeaders, headerName)
	}
	headerName = strings.Join(lowerSignedHeaders, ";")
	return
}

func contains(items []string, key string) bool {
	for _, s := range items {
		if s == key {
			return true
		}
	}
	return false
}

// canonical header
func (req *signatureRequestV4) createCanonicalHeaderV4() (canonicalHeader http.Header, err error) {
	if !contains(req.SignedHeaders, SignedHeaderHost) {
		return nil, errors.New("signedHeaders not contain host")
	}
	canonicalHeader = make(http.Header)
	reqHeader := req.r.Header
	reqQuery := req.r.URL.Query()
	for _, header := range req.SignedHeaders {
		vals, ok := reqHeader[http.CanonicalHeaderKey(header)] //
		if !ok {
			vals, ok = reqQuery[header] //
		}
		if ok {
			for _, val := range vals {
				canonicalHeader.Add(header, val)
			}
			continue
		}
		switch strings.ToLower(header) {
		case "content-length":
			canonicalHeader.Set(header, strconv.FormatInt(req.r.ContentLength, 10))
		case "expect":
			canonicalHeader.Set(header, "100-continue")
		case "transfer-encoding":
			canonicalHeader.Set(header, req.r.Host)
		default:
			return nil, nil
		}
	}

	return
}

// filter request headers by signedHeaders then gen join to a string
func buildCanonicalHeaderString(host string, headers http.Header, signedHeaders []string) (headerString string) {
	// copy a new http header from headers, because net/http package remove host header when parsing request
	newHeaders := make(http.Header)
	for n := range headers {
		newHeaders.Add(n, headers.Get(n))
	}
	newHeaders.Add(HeaderNameHost, host)

	canonicalHeaders := make([]string, 0)
	sort.Strings(signedHeaders)
	for _, signedHeaderName := range signedHeaders {
		vals, ok := newHeaders[(http.CanonicalHeaderKey(signedHeaderName))]
		if ok {
			sb := strings.Builder{}
			sb.WriteString(strings.ToLower(signedHeaderName))
			sb.WriteString(":")
			sb.WriteString(strings.Join(vals, ","))
			canonicalHeaders = append(canonicalHeaders, sb.String())
		}
	}
	headerString = strings.Join(canonicalHeaders, "\n") + "\n"
	return
}

// build scope string to sign
func buildScope(date, region, service, request string) string {
	return strings.Join([]string{
		date,
		region,
		service,
		request,
	}, "/")
}

// build string to sign
func buildStringToSign(algorithm, timeStamp, scope, canonicalRequestString string) string {
	return strings.Join([]string{
		algorithm,
		timeStamp,
		scope,
		calcHash(canonicalRequestString),
	}, "\n")
}
