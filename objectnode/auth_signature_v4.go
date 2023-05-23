/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 * Modifications copyright 2019 The CubeFS Authors.
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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
)

// https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/sig-v4-header-based-auth.html

func buildSigningKey(scheme, secret, date, region, service, request string) []byte {
	dateKey := MakeHmacSha256([]byte(scheme+secret), []byte(date))
	dateRegionKey := MakeHmacSha256(dateKey, []byte(region))
	dateRegionServiceKey := MakeHmacSha256(dateRegionKey, []byte(service))

	return MakeHmacSha256(dateRegionServiceKey, []byte(request))
}

func getHashedPayload(r *http.Request) string {
	if hash := r.Header.Get(XAmzContentSha256); hash != "" {
		return hash
	}

	switch {
	case r.URL.Query().Get(XAmzAlgorithm) != "":
		if hash := r.URL.Query().Get(XAmzContentSha256); hash != "" {
			return hash
		}
		return UnsignedPayload
	case r.Body == nil:
		return EmptyStringSHA256
	default:
		hash := sha256.New()
		teeReader := io.TeeReader(r.Body, hash)
		buf := bytes.NewBuffer(nil)
		io.Copy(buf, teeReader)
		r.Body.Close()
		r.Body = io.NopCloser(buf)
		return hex.EncodeToString(hash.Sum(nil))
	}
}

func buildCanonialQueryString(r *http.Request, presign bool) string {
	query := r.URL.Query()
	if presign {
		query.Del(XAmzSignature)
	}

	return strings.ReplaceAll(query.Encode(), "+", "%20")
}

func buildCanonicalURI(r *http.Request) string {
	// If request path contain double slash, the java sdk of aws escape the second slash into '%2F' after computing signature,
	// so we received path is '/%2F', we have to unescape it to double slash again before computing signature.
	return strings.ReplaceAll(r.URL.EscapedPath(), "/%2F", "//")
}

func buildCanonicalRequest(r *http.Request, signedHeaders []string, presign bool) string {
	return strings.Join([]string{
		r.Method,
		buildCanonicalURI(r),
		buildCanonialQueryString(r, presign),
		buildCanonicalHeaders(r, signedHeaders),
		buildSignedHeaders(signedHeaders),
		getHashedPayload(r),
	}, "\n")
}

func buildSignedHeaders(signedHeaders []string) string {
	var headers []string
	for _, k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

func buildCanonicalHeaders(req *http.Request, signedHeaders []string) string {
	canonicalHeaders := make([]string, len(signedHeaders))
	sort.Strings(signedHeaders)

	for i, header := range signedHeaders {
		header = strings.ToLower(header)
		switch header {
		case "host":
			canonicalHeaders[i] = header + ":" + strings.TrimSpace(req.Host)
		case "expect":
			canonicalHeaders[i] = header + ":100-continue"
		case "transfer-encoding":
			canonicalHeaders[i] = header + ":" + strings.Join(req.TransferEncoding, ",")
		default:
			val := strings.Join(req.Header[http.CanonicalHeaderKey(header)], ",")
			val = regexp.MustCompile(` {2,}`).ReplaceAllString(val, " ")
			canonicalHeaders[i] = header + ":" + strings.TrimSpace(val)
		}
	}

	return strings.Join(canonicalHeaders, "\n") + "\n"
}

func buildScope(date, region, service, request string) string {
	return strings.Join([]string{
		date,
		region,
		service,
		request,
	}, "/")
}

func buildStringToSign(algorithm, timeStamp, scope, canonicalRequest string) string {
	return strings.Join([]string{
		algorithm,
		timeStamp,
		scope,
		hex.EncodeToString(MakeSha256([]byte(canonicalRequest))),
	}, "\n")
}

func calculateSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(MakeHmacSha256(signingKey, []byte(stringToSign)))
}
