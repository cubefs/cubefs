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
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

var (
	routeSNRegexp = regexp.MustCompile(":(\\w){32}$")
)

func IsMonitoredStatusCode(code int) bool {
	if code > http.StatusInternalServerError {
		return true
	}
	return false
}

func generateWarnDetail(r *http.Request, errorInfo string) string {
	var (
		action     proto.Action
		bucket     string
		object     string
		requestID  string
		statusCode int
	)

	var param = ParseRequestParam(r)
	bucket = param.Bucket()
	object = param.Object()
	action = GetActionFromContext(r)
	requestID = GetRequestID(r)
	statusCode = GetStatusCodeFromContext(r)

	return fmt.Sprintf("intenal error: status(%v) rerquestId(%v) action(%v) bucket(%v) object(%v) errorInfo(%v)",
		statusCode, requestID, action.Name(), bucket, object, errorInfo)
}

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
			// export ump warn info
			exporter.Warning(generateWarnDetail(r, err.Error()))
			return
		}

		// store request ID to context and write to header
		SetRequestID(r, requestID)
		w.Header()[HeaderNameXAmzRequestId] = []string{requestID}
		w.Header()[HeaderNameServer] = []string{HeaderValueServer}

		if connHeader := r.Header.Get(HeaderNameConnection); strings.EqualFold(connHeader, "close") {
			w.Header()[HeaderNameConnection] = []string{"close"}
		} else {
			w.Header()[HeaderNameConnection] = []string{"keep-alive"}
		}

		var action = ActionFromRouteName(mux.CurrentRoute(r).GetName())
		SetRequestAction(r, action)
		// ===== pre-handle finish =====

		var startTime = time.Now()
		metric := exporter.NewTPCnt(fmt.Sprintf("action_%v", action.Name()))
		defer func() {
			metric.Set(err)
		}()

		// Check action is whether enabled.
		if !action.IsNone() && !o.disabledActions.Contains(action) {
			// next
			next.ServeHTTP(w, r)
		} else {
			// If current action is disabled, return access denied in response.
			log.LogDebugf("traceMiddleware: disabled action: requestID(%v) action(%v)", requestID, action.Name())
			_ = AccessDenied.ServeResponse(w, r)
		}

		// failed request monitor
		var statusCode = GetStatusCodeFromContext(r)
		if IsMonitoredStatusCode(statusCode) {
			exporter.NewTPCnt(fmt.Sprintf("failed_%v", statusCode)).Set(nil)
			exporter.Warning(generateWarnDetail(r, getResponseErrorMessage(r)))
		}

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

			var (
				pass bool
				err  error
			)
			//  check auth type
			if isHeaderUsingSignatureAlgorithmV4(r) {
				// using signature algorithm version 4 in header
				pass, err = o.validateHeaderBySignatureAlgorithmV4(r)
			} else if isHeaderUsingSignatureAlgorithmV2(r) {
				// using signature algorithm version 2 in header
				pass, err = o.validateHeaderBySignatureAlgorithmV2(r)
			} else if isUrlUsingSignatureAlgorithmV2(r) {
				// using signature algorithm version 2 in url parameter
				pass, err = o.validateUrlBySignatureAlgorithmV2(r)
			} else if isUrlUsingSignatureAlgorithmV4(r) {
				// using signature algorithm version 4 in url parameter
				pass, err = o.validateUrlBySignatureAlgorithmV4(r)
			}

			if err != nil {
				if err == proto.ErrVolNotExists {
					_ = NoSuchBucket.ServeResponse(w, r)
					return
				}
				_ = InternalErrorCode(err).ServeResponse(w, r)
				return
			}
			authInfo := parseRequestAuthInfo(r)
			if !pass && !isAnonymous(authInfo.accessKey) {
				_ = AccessDenied.ServeResponse(w, r)
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
		if len(r.Header) > 0 && len(r.Header.Get(http.CanonicalHeaderKey(HeaderNameXAmzDecodeContentLength))) > 0 {
			r.Body = NewClosableChunkedReader(r.Body)
			log.LogDebugf("contentMiddleware: chunk reader inited: requestID(%v)", GetRequestID(r))
		}
		next.ServeHTTP(w, r)
	}
	return handlerFunc
}

// Http's Expect header is a special header. When nginx is used as the reverse proxy in the front
// end of ObjectNode, nginx will process the Expect header information in advance, send the http
// status code 100 to the client, and will not forward this header information to ObjectNode.
// At this time, if the client request uses the Expect header when signing, it will cause the
// ObjectNode to verify the signature.
// A workaround is used here to solve this problem. Add the following configuration in nginx:
//   proxy_set_header X-Forwarded-Expect $ http_Expect
// In this way, nginx will not only automatically handle the Expect handshake, but also send
// the original value of Expect to the ObjectNode through X-Forwarded-Expect. ObjectNode only
// needs to use the value of X-Forwarded-Expect.
func (o *ObjectNode) expectMiddleware(next http.Handler) http.Handler {
	var handlerFunc http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		if forwardedExpect, originExpect := r.Header.Get(HeaderNameXForwardedExpect), r.Header.Get(HeaderNameExpect); forwardedExpect != "" && originExpect == "" {
			r.Header.Set(HeaderNameExpect, forwardedExpect)
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

		var err error
		var param = ParseRequestParam(r)
		if param.Bucket() == "" {
			next.ServeHTTP(w, r)
			return
		}

		var vol *Volume
		if param.action == proto.OSSCreateBucketAction {
			if vol, err = o.vm.VolumeWithoutBlacklist(param.Bucket()); err != nil {
				next.ServeHTTP(w, r)
				return
			}
		} else {
			if vol, err = o.vm.Volume(param.Bucket()); err != nil {
				next.ServeHTTP(w, r)
				return
			}
		}

		var setupCORSHeader = func(volume *Volume, writer http.ResponseWriter, request *http.Request) {
			origin := request.Header.Get(Origin)
			method := request.Header.Get(HeaderNameAccessControlRequestMethod)
			headerStr := request.Header.Get(HeaderNameAccessControlRequestHeaders)
			if origin == "" || method == "" {
				return
			}
			cors, _ := volume.metaLoader.loadCors()
			if cors != nil {
				headers := strings.Split(headerStr, ",")
				for _, corsRule := range cors.CORSRule {
					if corsRule.match(origin, method, headers) {
						// write access control allow headers
						writer.Header()[HeaderNameAccessControlAllowOrigin] = []string{origin}
						writer.Header()[HeaderNameAccessControlMaxAge] = []string{strconv.Itoa(int(corsRule.MaxAgeSeconds))}
						writer.Header()[HeaderNameAccessControlAllowMethods] = []string{strings.Join(corsRule.AllowedMethod, ",")}
						writer.Header()[HeaderNameAccessControlAllowHeaders] = []string{strings.Join(corsRule.AllowedHeader, ",")}
						writer.Header()[HeaderNamrAccessControlExposeHeaders] = []string{strings.Join(corsRule.ExposeHeader, ",")}
						return
					}
				}
			}
		}
		setupCORSHeader(vol, w, r)
		next.ServeHTTP(w, r)
		return
	})
}
