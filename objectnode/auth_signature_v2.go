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
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/gorilla/mux"
)

//https://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html#ConstructingTheAuthenticationHeader

const (
	RequestHeaderV2Authorization       = "Authorization"
	RequestHeaderV2AuthorizationScheme = "AWS"
	RequestHeaderV2XAmzDate            = "X-Amz-Date"
)

var PresignedSignatureV2Queries = []string{
	"AWSAccessKeyId",
	"Signature",
}

var SignatureV2WhiteQueries = map[string]struct{}{
	"acl":                          struct{}{},
	"delete":                       struct{}{},
	"lifecycle":                    struct{}{},
	"location":                     struct{}{},
	"logging":                      struct{}{},
	"notification":                 struct{}{},
	"partNumber":                   struct{}{},
	"policy":                       struct{}{},
	"requestPayment":               struct{}{},
	"response-cache-control":       struct{}{},
	"response-content-disposition": struct{}{},
	"response-content-encoding":    struct{}{},
	"response-content-language":    struct{}{},
	"response-content-type":        struct{}{},
	"response-expires":             struct{}{},
	"torrent":                      struct{}{},
	"uploadId":                     struct{}{},
	"uploads":                      struct{}{},
	"versionId":                    struct{}{},
	"versioning":                   struct{}{},
	"versions":                     struct{}{},
}

//
type requestAuthInfoV2 struct {
	r           *http.Request
	authType    AuthType
	bucket      string
	accessKeyId string
	signature   string
	expires     string
}

// http://127.0.0.1:33032/ltptest/b.txt
//  ?AWSAccessKeyId=Yqnqp4v6q1fzNM2e
//  &Expires=1573369185
//  &Signature=GJCqOY0ahf1BdzJDjNnFWB7vfSc%3D
//
func parsePresignedV2AuthInfo(r *http.Request) (*requestAuthInfoV2, error) {
	//
	ai := new(requestAuthInfoV2)
	uris := strings.SplitN(r.RequestURI, "?", 2)
	if len(uris) < 2 {
		log.LogInfof("validateUrlBySignatureAlgorithmV2 error, request url invalid %v ", r.RequestURI)
		return nil, errors.New("uri is invalid")
	}

	vars := mux.Vars(r)
	ai.accessKeyId = vars["accessKey"]
	ai.signature = vars["signature"]
	ai.expires = vars["expires"]

	return ai, nil
}

// Authorization: AWS AWSAccessKeyId:Signature
func parseRequestAuthInfoV2(r *http.Request) (ra *requestAuthInfoV2, err error) {
	ra = &requestAuthInfoV2{r: r}

	vars := mux.Vars(r)
	ra.bucket = vars["bucket"]

	authStr := r.Header.Get(RequestHeaderV2Authorization)
	if authStr == "" {
		err = errors.New("header not found authentication")
		return nil, err
	}

	if !strings.HasPrefix(authStr, RequestHeaderV2AuthorizationScheme) {
		return nil, errors.New("header has no prefix ")
	}

	credentialStr := util.SubString(authStr, len(RequestHeaderV2AuthorizationScheme), len(authStr))
	credentialStr = strings.Trim(credentialStr, " ")
	credentials := strings.Split(credentialStr, ":")
	if len(credentials) < 2 {
		err = errors.New("")
		return nil, err
	}

	ra.accessKeyId = credentials[0]
	ra.signature = credentials[1]

	return
}

// IsHeaderUsingSignatureAlgorithmV2 checks if request is using signature algorithm V2 in header.
func isHeaderUsingSignatureAlgorithmV2(r *http.Request) bool {
	hasV2 := strings.HasPrefix(r.Header.Get(HeaderNameAuthorization), RequestHeaderV2AuthorizationScheme)
	hasV4 := strings.HasPrefix(r.Header.Get(HeaderNameAuthorization), SignatureV4Algorithm)
	if hasV2 && !hasV4 {
		return true
	}

	return false
}

func isRequestQueryValid(queries url.Values, neededQueries []string) bool {
	for _, q := range neededQueries {
		k := strings.ToLower(q)
		if _, ok := queries[k]; !ok {
			return false
		}
	}
	return true
}

// IsUrlUsingSignatureAlgorithmV2 checks if request is using signature algorithm V2 in url parameter.
// Example:
// http://127.0.0.1:33032/ltptest/b.txt
//  ?AWSAccessKeyId=Yqnqp4v6q1fzNM2e
//  &Expires=1573369185
//  &Signature=GJCqOY0ahf1BdzJDjNnFWB7vfSc%3D
func isUrlUsingSignatureAlgorithmV2(r *http.Request) bool {
	if u, err := url.Parse(strings.ToLower(r.URL.String())); err == nil {
		return isRequestQueryValid(u.Query(), PresignedSignatureV2Queries)
	}
	return false
}

func (o *ObjectNode) validateHeaderBySignatureAlgorithmV2(r *http.Request) (bool, error) {
	// parse v2 request header and query, and get reqSignature
	authInfo, err := parseRequestAuthInfoV2(r)
	if err != nil {
		log.LogInfof("parseRequestAuthInfoV2 error: %v, %v", authInfo.r, err)
		return false, err
	}

	var akPolicy *proto.AKPolicy
	if akPolicy, err = o.getAkInfo(authInfo.accessKeyId); err != nil {
		log.LogInfof("get secretKey from master error: accessKey(%v), err(%v)", authInfo.accessKeyId, err)
		return false, err
	}

	// 2. calculate new signature
	newSignature, err1 := calculateSignatureV2(authInfo, akPolicy.SecretKey, o.wildcards)
	if err1 != nil {
		log.LogInfof("calculute SignatureV2 error: %v, %v", authInfo.r, err)
		return false, err1
	}

	// 3. compare newSignatrue and reqSignature
	if authInfo.signature == newSignature {
		return true, nil
	}
	log.LogInfof("newSignature: %v, reqSignature: %v, %v", newSignature, authInfo.signature, authInfo.r)

	return false, nil
}

/*
Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;

Signature = Base64( HMAC-SHA1( YourSecretAccessKey, UTF-8-Encoding-Of( StringToSign ) ) );

StringToSign = HTTP-Verb + "\n" +
	Content-MD5 + "\n" +
	Content-Type + "\n" +
	Date + "\n" +
	CanonicalizedAmzHeaders +
	CanonicalizedResource;

CanonicalizedResource = [ "/" + Bucket ] +
	<HTTP-Request-URI, from the protocol name up to the query string> +
	[ subresource, if present. For example "?acl", "?location", "?logging", or "?torrent"];

CanonicalizedAmzHeaders = <described below>
*/
func calculateSignatureV2(authInfo *requestAuthInfoV2, secretKey string, wildcards Wildcards) (signature string, err error) {

	//encodedResource := strings.Split(authInfo.r.RequestURI, "?")[0]
	canonicalResource := getCanonicalizedResourceV2(authInfo.r, wildcards)

	canonicalResourceQuery := getCanonicalQueryV2(canonicalResource, authInfo.r.URL.Query().Encode())

	date := authInfo.r.Header.Get("Date")
	method := authInfo.r.Method
	canonicalHeaders := canonicalizedAmzHeadersV2(authInfo.r.Header)
	if len(canonicalHeaders) > 0 {
		canonicalHeaders += "\n"
	}
	contentHash := authInfo.r.Header.Get(HeaderNameContentMD5)
	contentType := authInfo.r.Header.Get(HeaderNameContentType)
	stringToSign := strings.Join([]string{
		method,
		contentHash,
		contentType,
		date,
		canonicalHeaders,
	}, "\n")

	stringToSign = stringToSign + canonicalResourceQuery

	hm := hmac.New(sha1.New, []byte(secretKey))
	hm.Write([]byte(stringToSign))

	signature = base64.StdEncoding.EncodeToString(hm.Sum(nil))

	return
}

func (o *ObjectNode) validateUrlBySignatureAlgorithmV2(r *http.Request) (bool, error) {

	var err error

	uris := strings.SplitN(r.RequestURI, "?", 2)
	if len(uris) < 2 {
		log.LogInfof("validateUrlBySignatureAlgorithmV2 error, request url invalid %v ", r.RequestURI)
		return false, nil
	}

	var param = ParseRequestParam(r)
	accessKey := param.GetVar("accessKey")
	signature := param.GetVar("signature")
	expires := param.GetVar("expires")
	if accessKey == "" || signature == "" || expires == "" {
		log.LogInfof("validateUrlBySignatureAlgorithmV2: incomplete authentication information: requestID(%v)",
			GetRequestID(r))
		return false, nil
	}

	log.LogDebugf("validateUrlBySignatureAlgorithmV2: parse signature info: requestID(%v) url(%v) accessKey(%v) signature(%v) expires(%v)",
		GetRequestID(r), r.URL.String(), accessKey, signature, expires)

	//check access key
	var akPolicy *proto.AKPolicy
	if akPolicy, err = o.getAkInfo(accessKey); err != nil {
		log.LogInfof("get secretKey from master error: accessKey(%v), err(%v)", accessKey, err)
		return false, err
	}

	// check expires
	if ok, _ := checkExpires(expires); !ok {
		log.LogDebugf("validateUrlBySignatureAlgorithmV2: signature expired: requestID(%v) expires(%v)", GetRequestID(r), expires)
		return false, nil
	}

	//calculatePresignedSignature
	var canonicalResource string
	canonicalResource = getCanonicalizedResourceV2(r, o.wildcards)
	canonicalResourceQuery := getCanonicalQueryV2(canonicalResource, r.URL.Query().Encode())
	calSignature := calPresignedSignatureV2(r.Method, canonicalResourceQuery, expires, akPolicy.SecretKey, r.Header)
	if calSignature != signature {
		log.LogDebugf("validateUrlBySignatureAlgorithmV2: invalid signature: requestID(%v) client(%v) server(%v)",
			GetRequestID(r), signature, calSignature)
		return false, nil
	}

	return true, nil
}

func checkExpires(expires string) (ok bool, err error) {
	expiresInt, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return false, err
	}
	now := time.Now().UTC().Unix()
	if now < expiresInt {
		log.LogInfof("validateUrlBySignatureAlgorithmV2 expired is out time %v, now: %v", expires, now)
		return true, nil
	}

	return false, nil
}

func getCanonicalQueryV2(encodeResource string, encodeQuery string) string {
	var canonicalQueries []string
	items := strings.Split(encodeQuery, "&")
	queries := make(map[string]string)
	for _, item := range items {
		k := item
		v := ""
		i := strings.Index(item, "=")
		if i != -1 {
			k = item[:i]
			v = item[i+1:]
		}
		queries[k] = v
	}

	for k, v := range queries {
		if _, ok := SignatureV2WhiteQueries[k]; !ok {
			continue
		}

		query := k
		if v != "" {
			query = k + "=" + v
		}
		canonicalQueries = append(canonicalQueries, query)
	}

	sort.Strings(canonicalQueries)

	canonicalQuery := strings.Join(canonicalQueries, "&")
	if canonicalQuery != "" {
		return encodeResource + "?" + canonicalQuery
	}
	return encodeResource
}

//
func canonicalizedAmzHeadersV2(headers http.Header) string {
	var keys []string
	keyval := make(map[string]string)
	for key := range headers {
		lkey := strings.ToLower(key)
		if !strings.HasPrefix(lkey, "x-amz-") {
			continue
		}
		keys = append(keys, lkey)
		keyval[lkey] = strings.Join(headers[key], ",")
	}
	sort.Strings(keys)
	var canonicalHeaders []string
	for _, key := range keys {
		canonicalHeaders = append(canonicalHeaders, key+":"+keyval[key])
	}
	return strings.Join(canonicalHeaders, "\n")
}

//
func calPresignedSignatureV2(method, canonicalQuery, expires, secretKey string, header http.Header) string {
	date := expires
	if date == "" {
		date = header.Get(HeaderNameDate)
	}
	canonicalHeaders := canonicalizedAmzHeadersV2(header)
	contentHash := header.Get(HeaderNameContentMD5)
	contentEnc := header.Get(HeaderNameContentEnc)
	stringToSign := strings.Join([]string{
		method,
		contentHash,
		contentEnc,
		date,
		canonicalHeaders,
	}, "\n") + canonicalQuery

	hm := hmac.New(sha1.New, []byte(secretKey))
	hm.Write([]byte(stringToSign))

	return base64.StdEncoding.EncodeToString(hm.Sum(nil))
}

func getCanonicalizedResourceV2(r *http.Request, ws Wildcards) (resource string) {
	// TODO: fix this
	path := r.URL.Path
	if bucket, wildcard := ws.Parse(r.Host); wildcard {
		resource = "/" + bucket + path
	} else {
		resource = path
	}
	return
}
