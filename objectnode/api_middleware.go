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
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"

	"github.com/google/uuid"
)

var (
	routeSNRegexp = regexp.MustCompile(":(\\w){32}$")
)

// TraceMiddleware returns a middleware handler to trace request.
// After receiving the request, the handler will assign a unique RequestID to
// the request and record the processing time of the request.
// Workflow:
//   request → [pre-handle] → [next handler] → [post-handle] → response
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

		// ===== pre-handle start =====
		var requestID string
		if requestID, err = generateRequestID(); err != nil {
			log.LogErrorf("traceMiddleware: generate request ID fail, remote(%v) url(%v) err(%v)",
				r.RemoteAddr, r.URL.String(), err)
			_ = InternalErrorCode(err).ServeResponse(w, r)
			return
		}

		// store request ID to context and write to header
		SetRequestID(r, requestID)
		w.Header().Set(HeaderNameRequestId, requestID)
		w.Header().Set(HeaderNameServer, HeaderValueServer)

		var action = ActionFromRouteName(mux.CurrentRoute(r).GetName())
		SetRequestAction(r, action)
		// ===== pre-handle finish =====

		// check action is whether enable
		var disableFlag bool
		if !action.IsNone() && !o.disableActions.Contains(action) {
			// next
			next.ServeHTTP(w, r)
		} else {
			log.LogErrorf("traceMiddleware: action %s is disable: requestID(%v) ", requestID)
			disableFlag = true
		}

		var startTime = time.Now()
		metric := exporter.NewTPCnt(fmt.Sprintf("action_%v", action.Name()))
		defer metric.Set(err)

		// ===== post-handle start =====
		var headerToString = func(header http.Header) string {
			var sb = strings.Builder{}
			for k := range header {
				if sb.Len() != 0 {
					sb.WriteString(",")
				}
				sb.WriteString(fmt.Sprintf("%v:[%v]", k, header.Get(k)))
			}
			return "{" + sb.String() + "}"
		}

		log.LogDebugf("traceMiddleware: "+
			"action(%v) requestID(%v) host(%v) method(%v) url(%v) header(%v) "+
			"remote(%v) cost(%v)",
			action.Name(), requestID, r.Host, r.Method, r.URL.String(), headerToString(r.Header),
			getRequestIP(r), time.Since(startTime))
		// ==== post-handle finish =====

		// if action is disable, return access denied in response
		if disableFlag {
			_ = AccessDenied.ServeResponse(w, r)
		}
	}
	return handlerFunc
}

// AuthMiddleware returns a pre-handle middleware handler to perform user authentication.
func (o *ObjectNode) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			var currentAction = ActionFromRouteName(mux.CurrentRoute(r).GetName())
			if !currentAction.IsNone() && o.signatureIgnoredActions.Contains(currentAction) {
				next.ServeHTTP(w, r)
				return
			}

			//  check auth type
			if isHeaderUsingSignatureAlgorithmV4(r) {
				// using signature algorithm version 4 in header
				if ok, _ := o.validateHeaderBySignatureAlgorithmV4(r); !ok {
					if err := AccessDenied.ServeResponse(w, r); err != nil {
						log.LogErrorf("authMiddleware: serve access denied response fail, requestID(%v) err(%v)", GetRequestID(r), err)
					}
					return
				}
			} else if isHeaderUsingSignatureAlgorithmV2(r) {
				// using signature algorithm version 2 in header
				if ok, _ := o.validateHeaderBySignatureAlgorithmV2(r); !ok {
					if err := AccessDenied.ServeResponse(w, r); err != nil {
						log.LogErrorf("authMiddleware: serve access denied response fail, requestID(%v) err(%v)", GetRequestID(r), err)
					}
					return
				}
			} else if isUrlUsingSignatureAlgorithmV2(r) {
				// using signature algorithm version 2 in url parameter
				if ok, _ := o.validateUrlBySignatureAlgorithmV2(r); !ok {
					log.LogDebugf("authMiddleware: presigned v2 denied: requestID(%v)", GetRequestID(r))
					if err := AccessDenied.ServeResponse(w, r); err != nil {
						log.LogErrorf("authMiddleware: serve response fail: requestID(%v) err(%v)", GetRequestID(r), err)
					}
					return
				}
			} else if isUrlUsingSignatureAlgorithmV4(r) {
				// using signature algorithm version 4 in url parameter
				if ok, _ := o.validateUrlBySignatureAlgorithmV4(r); !ok {
					log.LogDebugf("authMiddleware: presigned v4 denied: requestID(%v)", GetRequestID(r))
					if err := AccessDenied.ServeResponse(w, r); err != nil {
						log.LogErrorf("authMiddleware: serve response fail: requestID(%v) err(%v)", GetRequestID(r), err)
					}
					return
				}
			} else {
				// no valid signature found
				if err := AccessDenied.ServeResponse(w, r); err != nil {
					log.LogErrorf("authMiddleware: serve response fail: requestID(%v) err(%v)", GetRequestID(r), err)
				}
				return
			}

			next.ServeHTTP(w, r)
		})
}

// PolicyCheckMiddleware returns a pre-handle middleware handler to process policy check.
// If action is configured in signatureIgnoreActions, then skip policy check.
func (o *ObjectNode) policyCheckMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			action := ActionFromRouteName(mux.CurrentRoute(r).GetName())
			if !action.IsNone() && o.signatureIgnoredActions.Contains(action) {
				next.ServeHTTP(w, r)
				return
			}
			wrappedNext := o.policyCheck(next.ServeHTTP)
			wrappedNext.ServeHTTP(w, r)
			return
		})
}

// ContentMiddleware returns a middleware handler to process reader for content.
// If the request contains the "X-amz-Decoded-Content-Length" header, it means that the data
// in the request body is chunked. Use ChunkedReader to parse the data.
// Workflow:
//   request → [pre-handle] → [next handler] → response
func (o *ObjectNode) contentMiddleware(next http.Handler) http.Handler {
	var handlerFunc http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		if len(r.Header) > 0 && len(r.Header.Get(http.CanonicalHeaderKey(HeaderNameDecodeContentLength))) > 0 {
			r.Body = NewChunkedReader(r.Body)
			log.LogDebugf("contentMiddleware: chunk reader inited: requestID(%v)", GetRequestID(r))
		}
		next.ServeHTTP(w, r)
	}
	return handlerFunc
}

// CORSMiddleware returns a middleware handler to support CORS request.
// This handler will write following header into response:
//   Access-Control-Allow-Origin [*]
//   Access-Control-Allow-Headers [*]
//   Access-Control-Allow-Methods [*]
//   Access-Control-Max-Age [0]
// Workflow:
//   request → [pre-handle] → [next handler] → response
func (o *ObjectNode) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// write access control allow headers
		w.Header().Set(HeaderNameAccessControlAllowOrigin, "*")
		w.Header().Set(HeaderNameAccessControlAllowHeaders, "*")
		w.Header().Set(HeaderNameAccessControlAllowMethods, "*")
		w.Header().Set(HeaderNameAccessControlMaxAge, "0")
		next.ServeHTTP(w, r)
	})
}
