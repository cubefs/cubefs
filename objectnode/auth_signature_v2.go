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
	"encoding/base64"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html

var signV2SubresourceQueries = map[string]bool{
	"acl":            true,
	"cors":           true,
	"delete":         true,
	"encryption":     true,
	"lifecycle":      true,
	"location":       true,
	"logging":        true,
	"notification":   true,
	"partNumber":     true,
	"policy":         true,
	"requestPayment": true,
	"restore":        true,
	"retention":      true,
	"tagging":        true,
	"torrent":        true,
	"uploadId":       true,
	"uploads":        true,
	"versionId":      true,
	"versioning":     true,
	"versions":       true,
	"website":        true,
}

var signV2ResponseHeaderQueries = map[string]bool{
	"response-cache-control":       true,
	"response-content-disposition": true,
	"response-content-encoding":    true,
	"response-content-language":    true,
	"response-content-type":        true,
	"response-expires":             true,
}

func buildStringToSignV2(method, date, canonicalizedResource string, header http.Header) string {
	canonicalAmzHeaders := buildCanonicalizedAmzHeadersV2(header)
	if len(canonicalAmzHeaders) > 0 {
		canonicalAmzHeaders += "\n"
	}

	return strings.Join([]string{
		method,
		header.Get(ContentMD5),
		header.Get(ContentType),
		date,
		canonicalAmzHeaders,
	}, "\n") + canonicalizedResource
}

func buildCanonicalizedAmzHeadersV2(headers http.Header) string {
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

func buildCanonicalizedResourceQueryV2(resource string, query url.Values) string {
	var queries []string
	for name := range query {
		if signV2SubresourceQueries[name] || signV2ResponseHeaderQueries[name] {
			var q string
			value := query.Get(name)
			if value == "" && signV2SubresourceQueries[name] {
				q = name
			} else {
				q = name + "=" + value
			}
			queries = append(queries, q)
		}
	}
	sort.Strings(queries)
	if q := strings.Join(queries, "&"); q != "" {
		return resource + "?" + q
	}

	return resource
}

func buildCanonicalizedResourceV2(r *http.Request, ws Wildcards) (resource string) {
	path := r.URL.EscapedPath()
	if bucket, wildcard := ws.Parse(r.Host); wildcard {
		resource = "/" + bucket + path
	} else {
		resource = path
	}
	return
}

func calculateSignatureV2(secretKey, stringToSign string) string {
	return base64.StdEncoding.EncodeToString(MakeHmacSha1([]byte(secretKey), []byte(stringToSign)))
}
