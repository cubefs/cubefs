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
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"

	"github.com/gorilla/mux"
)

const StatusServerPanic = 597

var routeSNRegexp = regexp.MustCompile(":(\\w){32}$")

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

	param := ParseRequestParam(r)
	bucket = param.Bucket()
	object = param.Object()
	action = GetActionFromContext(r)
	requestID = GetRequestID(r)
	statusCode = GetStatusCodeFromContext(r)

	return fmt.Sprintf("intenal error: status(%v) rerquestId(%v) action(%v) bucket(%v) object(%v) errorInfo(%v)",
		statusCode, requestID, action.Name(), bucket, object, errorInfo)
}

// AuditMiddleware returns a middleware handler that writes the local audit log before returning response.
func (o *ObjectNode) auditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if o.localAuditHandler != nil {
				o.localAuditHandler.Handler(w, r, next.ServeHTTP)
			} else {
				next.ServeHTTP(w, r)
			}
		})
}

// TraceMiddleware returns a middleware handler to trace request.
// After receiving the request, the handler will write a unique RequestID to
// the request and record the processing status of the request.
func (o *ObjectNode) traceMiddleware(next http.Handler) http.Handler {
	var handlerFunc http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		var err error
		span := proto.SpanFromContext(r.Context())
		// wrapper for w to record its stats
		w = NewResponseStater(w)
		defer func() {
			p := ParseRequestParam(r)
			extraHeader := auditlog.ExtraHeader(w)
			extraHeader.Set("Tbl", p.Bucket())
			extraHeader.Set("Api", p.API())
			extraHeader.Set("Owner", p.Owner())
			extraHeader.Set("Requester", p.Requester())
			if o.externalAudit != nil {
				o.externalAudit.Logger(w, r)
			}
		}()

		// write to response header
		w.Header().Set(XAmzRequestId, span.TraceID())
		w.Header().Set(Server, ValueServer)
		if connHeader := r.Header.Get(Connection); strings.EqualFold(connHeader, "close") {
			w.Header().Set(Connection, "close")
		} else {
			w.Header().Set(Connection, "keep-alive")
		}

		// store request action to context
		action := ActionFromRouteName(mux.CurrentRoute(r).GetName())
		SetRequestAction(r, action)

		// request monitor
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
			span.Warnf("request with disabled action: %v", action.Name())
			AccessDenied.ServeResponse(w, r)
		}

		// failed request monitor
		statusCode := GetStatusCodeFromContext(r)
		if IsMonitoredStatusCode(statusCode) {
			exporter.NewTPCnt(fmt.Sprintf("failed_%v", statusCode)).Set(nil)
			exporter.Warning(generateWarnDetail(r, getResponseErrorMessage(r)))
		}
	}
	return handlerFunc
}

// AuthMiddleware returns a pre-handle middleware handler to perform user authentication.
func (o *ObjectNode) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			span := proto.SpanFromContext(r.Context())

			// parse authentication
			auth, err := NewAuth(r)
			if err != nil && err == MissingSecurityElement {
				// anonymous request will be authed in policy and acl check step
				next.ServeHTTP(w, r)
				return
			}
			if err != nil {
				span.Errorf("parse request auth failed: auth(%+v) err(%v)", auth, err)
				o.errorResponse(w, r, err, nil)
				return
			}
			// validate authentication information
			if err = o.validateAuthInfo(r, auth); err != nil {
				o.errorResponse(w, r, err, nil)
				return
			}

			next.ServeHTTP(w, r)
		})
}

// PolicyCheckMiddleware returns a pre-handle middleware handler to process policy check.
func (o *ObjectNode) policyCheckMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			o.policyCheck(next.ServeHTTP).ServeHTTP(w, r)
			return
		})
}

// ContentMiddleware returns a middleware handler to process reader for content.
// If the request contains the "X-amz-Decoded-Content-Length" header, it means that the data
// in the request body is chunked. Use ChunkedReader to parse the data.
func (o *ObjectNode) contentMiddleware(next http.Handler) http.Handler {
	var handlerFunc http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(XAmzDecodedContentLength) != "" && r.Header.Get(ContentEncoding) != streamingContentEncoding {
			r.Body = NewClosableChunkedReader(r.Body)
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
//
//	proxy_set_header X-Forwarded-Expect $ http_Expect
//
// In this way, nginx will not only automatically handle the Expect handshake, but also send
// the original value of Expect to the ObjectNode through X-Forwarded-Expect. ObjectNode only
// needs to use the value of X-Forwarded-Expect.
func (o *ObjectNode) expectMiddleware(next http.Handler) http.Handler {
	var handlerFunc http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		span := proto.SpanFromContext(r.Context())
		defer func() {
			// panic recover and response specific status to client
			p := recover()
			if p != nil {
				span.Errorf("panic(%v): %v", p, string(debug.Stack()))
				w.WriteHeader(StatusServerPanic)
			}
		}()

		if forwarded, origin := r.Header.Get(XForwardedExpect), r.Header.Get(Expect); forwarded != "" && origin == "" {
			r.Header.Set(Expect, forwarded)
		}
		next.ServeHTTP(w, r)
	}
	return handlerFunc
}

// CORSMiddleware returns a middleware handler to support CORS request.
// This handler will write following header into response:
//
//	Access-Control-Allow-Origin [*]
//	Access-Control-Allow-Headers [*]
//	Access-Control-Allow-Methods [*]
//	Access-Control-Max-Age [0]
func (o *ObjectNode) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		ctx := r.Context()
		span := proto.SpanFromContext(ctx)

		param := ParseRequestParam(r)
		if param.Bucket() == "" {
			next.ServeHTTP(w, r)
			return
		}

		var vol *Volume
		if param.action == proto.OSSCreateBucketAction {
			if vol, err = o.vm.VolumeWithoutBlacklist(ctx, param.Bucket()); err != nil {
				next.ServeHTTP(w, r)
				return
			}
		} else {
			if vol, err = o.vm.Volume(ctx, param.Bucket()); err != nil {
				next.ServeHTTP(w, r)
				return
			}
		}
		mux.Vars(r)[ContextKeyOwner] = vol.GetOwner()

		if IsAccountLevelApi(param.apiName) {
			next.ServeHTTP(w, r)
			return
		}

		isPreflight := param.apiName == OPTIONS_OBJECT
		w.Header().Add("Vary", "Origin,Access-Control-Request-Method,Access-Control-Request-Headers")
		cors, err := vol.metaLoader.loadCORS(ctx)
		if err != nil {
			span.Errorf("load cors failed: vol(%v) err(%v)", vol.name, err)
			InternalErrorCode(err).ServeResponse(w, r)
			return
		}

		if isPreflight {
			errCode := preflightProcess(cors, w, r)
			if errCode != nil {
				errCode.ServeResponse(w, r)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		errCode := simpleProcess(cors, w, r)
		if errCode != nil {
			errCode.ServeResponse(w, r)
			return
		}
		next.ServeHTTP(w, r)
		return
	})
}

func isMatchAndSetupCORSHeader(cors *CORSConfiguration, writer http.ResponseWriter, request *http.Request, isPreflight bool) (match bool) {
	origin := request.Header.Get(Origin)
	reqHeaders := request.Header.Get(AccessControlRequestHeaders)
	var reqMethod string
	if isPreflight {
		reqMethod = request.Header.Get(AccessControlRequestMethod)
	} else {
		reqMethod = request.Method
	}
	if cors != nil {
		for _, corsRule := range cors.CORSRule {
			if corsRule.match(origin, reqMethod, reqHeaders) {
				// write access control allow headers
				match = true
				if StringListContain(corsRule.AllowedOrigin, "*") {
					writer.Header().Set(AccessControlAllowOrigin, "*")
				} else {
					writer.Header().Set(AccessControlAllowOrigin, origin)
					writer.Header().Set(AccessControlAllowCredentials, "true")
				}
				writer.Header().Set(AccessControlAllowMethods, strings.Join(corsRule.AllowedMethod, ","))
				writer.Header().Set(AccessControlExposeHeaders, strings.Join(corsRule.ExposeHeader, ","))
				if corsRule.MaxAgeSeconds != 0 {
					writer.Header().Set(AccessControlMaxAge, strconv.Itoa(int(corsRule.MaxAgeSeconds)))
				}
				if reqHeaders != "" {
					writer.Header().Set(AccessControlAllowHeaders, strings.Join(corsRule.AllowedHeader, ","))
				}
				return
			}
		}
	}
	return
}

func preflightProcess(cors *CORSConfiguration, w http.ResponseWriter, r *http.Request) *ErrorCode {
	origin := r.Header.Get(Origin)
	if origin == "" {
		return MissingOriginHeader
	}

	if cors == nil || len(cors.CORSRule) == 0 {
		return ErrCORSNotEnabled
	}

	if !isMatchAndSetupCORSHeader(cors, w, r, true) {
		return CORSRuleNotMatch
	}
	return nil
}

func simpleProcess(cors *CORSConfiguration, w http.ResponseWriter, r *http.Request) *ErrorCode {
	origin := r.Header.Get(Origin)
	if origin == "" { // non-cors request
		return nil
	}

	if cors == nil || len(cors.CORSRule) == 0 {
		return nil
	}

	if !isMatchAndSetupCORSHeader(cors, w, r, false) {
		return nil
	}
	return nil
}
