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
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/util/log"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

const (
	ctxKeyRequestID = "ctx_request_id"
)

func RequestIDFromRequest(r *http.Request) (id string) {
	return mux.Vars(r)[ctxKeyRequestID]
}

func (o *ObjectNode) traceMiddleware(next http.Handler) http.Handler {
	var generateRequestID = func() (string, error) {
		var uUID uuid.UUID
		var err error
		if uUID, err = uuid.NewRandom(); err != nil {
			return "", err
		}
		return strings.ReplaceAll(uUID.String(), "-", ""), nil
	}
	var handlerFunc http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {

		var err error
		var requestID string
		if requestID, err = generateRequestID(); err != nil {
			log.LogErrorf("traceMiddleware: generate request ID fail, remote(%v) url(%v) err(%v)",
				r.RemoteAddr, r.URL.String(), err)
			_ = InternalError.ServeResponse(w, r)
			return
		}
		mux.Vars(r)[ctxKeyRequestID] = requestID
		w.Header().Set(HeaderNameRequestId, requestID)

		var startTime = time.Now()

		next.ServeHTTP(w, r)

		var headerToString = func(header http.Header) string {
			var sb = strings.Builder{}
			for k := range header {
				if sb.Len() != 0 {
					sb.WriteString(",")
				}
				sb.WriteString(fmt.Sprintf("%o:[%o]", k, header.Get(k)))
			}
			return "{" + sb.String() + "}"
		}

		log.LogDebugf("traceMiddleware: trace request:\n"+
			"  requestID(%v) host(%v) method(%v) url(%v)\n"+
			"  header(%v)\n"+
			"  remote(%v) cost(%v)",
			requestID, r.Host, r.Method, r.URL.String(), headerToString(r.Header), getRequestIP(r), time.Since(startTime))

	}
	return handlerFunc
}

func (o *ObjectNode) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			//  1. check auth type
			if isSignaturedV4(r) {
				if ok, _ := o.checkSignatureV4(r); !ok {
					if err := AccessDenied.ServeResponse(w, r); err != nil {
						log.LogErrorf("authMiddleware: serve access denied response fail, requestID(%v) err(%v)", RequestIDFromRequest(r), err)
					}
					return
				}
			} else if isSignaturedV2(r) {
				if ok, _ := o.checkSignatureV2(r); !ok {
					if err := AccessDenied.ServeResponse(w, r); err != nil {
						log.LogErrorf("authMiddleware: serve access denied response fail, requestID(%v) err(%v)", RequestIDFromRequest(r), err)
					}
					return
				}
			} else if isPresignedSignaturedV2(r) {
				if ok, _ := o.checkPresignedSignatureV2(r); !ok {
					log.LogDebugf("authMiddleware: presigned v2 denied: requestID(%v)", RequestIDFromRequest(r))
					if err := AccessDenied.ServeResponse(w, r); err != nil {
						log.LogErrorf("authMiddleware: serve response fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
					}
					return
				}
			} else if isPresignedSignaturedV4(r) {
				if ok, _ := o.checkPresignedSignatureV4(r); !ok {
					log.LogDebugf("authMiddleware: presigned v4 denied: requestID(%v)", RequestIDFromRequest(r))
					if err := AccessDenied.ServeResponse(w, r); err != nil {
						log.LogErrorf("authMiddleware: serve response fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
					}
					return
				}
			} else {
				if err := AccessDenied.ServeResponse(w, r); err != nil {
					log.LogErrorf("authMiddleware: serve response fail: requestID(%v) err(%v)", RequestIDFromRequest(r), err)
				}
				return
			}

			next.ServeHTTP(w, r)
		})
}

func (o *ObjectNode) contentMiddleware(next http.Handler) http.Handler {
	var handlerFunc http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		if len(r.Header) > 0 && len(r.Header.Get(http.CanonicalHeaderKey(HeaderNameDecodeContentLength))) > 0 {
			r.Body = NewChunkedReader(r.Body)
			log.LogDebugf("contentMiddleware: chunk reader inited: requestID(%v)", RequestIDFromRequest(r))
		}
		next.ServeHTTP(w, r)
	}
	return handlerFunc
}
