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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var SERVICE = "s3"
var SCHEME = "AWS4"
var ALGORITHM = "HMAC-SHA256"
var TERMINATOR = "aws4_request"

func getStartTime(headers http.Header) string {
	for headerName := range headers {
		if strings.ToLower(headerName) == strings.ToLower(HeaderNameXAmzStartDate) {
			return headers.Get(headerName)
		}
	}
	return ""
}

func getURIPath(u *url.URL) string {
	var uri string

	if len(u.Opaque) > 0 {
		uri = "/" + strings.Join(strings.Split(u.Opaque, "/")[3:], "/")
	} else {
		uri = u.EscapedPath()
	}

	if len(uri) == 0 {
		uri = "/"
	}

	return uri
}

func getCurrentDateStamp() string {
	// get current date in UTC time zone
	n := time.Now()
	return n.UTC().Format("20060102")
}

func calcHash(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[0:])
}

func sign(stringData string, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(stringData))
	return mac.Sum(nil)
}
