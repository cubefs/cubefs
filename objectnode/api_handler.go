// Copyright 2019 The ChubaoFS Authors.
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
	"errors"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/proto"

	"github.com/gorilla/mux"

	"github.com/cubefs/cubefs/util/log"
)

type RequestParam struct {
	resource      string
	bucket        string
	object        string
	action        proto.Action
	sourceIP      string
	conditionVars map[string][]string
	vars          map[string]string
	accessKey     string
	r             *http.Request
}

func (p *RequestParam) Bucket() string {
	return p.bucket
}

func (p *RequestParam) Object() string {
	return p.object
}

func (p *RequestParam) Action() proto.Action {
	return p.action
}

func (p *RequestParam) GetVar(name string) string {
	if val, has := p.vars[name]; has {
		return val
	}
	return p.r.FormValue(name)
}

func (p *RequestParam) GetConditionVar(name string) []string {
	return p.conditionVars[name]
}

func (p *RequestParam) AccessKey() string {
	return p.accessKey
}

func ParseRequestParam(r *http.Request) *RequestParam {
	p := new(RequestParam)
	p.r = r
	p.vars = mux.Vars(r)
	p.bucket = p.vars["bucket"]
	p.object = p.vars["object"]
	p.sourceIP = getRequestIP(r)
	p.conditionVars = getCondtionValues(r)
	if len(p.bucket) > 0 {
		p.resource = p.bucket
		if len(p.object) > 0 {
			if strings.HasPrefix(p.object, "/") {
				p.resource = p.bucket + p.object
			} else {
				p.resource = p.bucket + "/" + p.object
			}
		}
	}
	auth := parseRequestAuthInfo(r)
	if auth != nil {
		p.accessKey = auth.accessKey
	}
	p.action = GetActionFromContext(r)
	if p.action.IsNone() {
		p.action = ActionFromRouteName(mux.CurrentRoute(r).GetName())
	}

	return p
}

func (o *ObjectNode) getVol(bucket string) (vol *Volume, err error) {
	if bucket == "" {
		return nil, errors.New("bucket name is empty")
	}
	vol, err = o.vm.Volume(bucket)
	if err != nil {
		log.LogErrorf("getVol: load Volume fail, bucket(%v) err(%v)", bucket, err)
		return nil, err
	}

	return vol, nil
}

func (o *ObjectNode) errorResponse(w http.ResponseWriter, r *http.Request, err error, ec *ErrorCode) {
	if err != nil || ec != nil {
		if err != nil {
			log.LogErrorf("errorResponse: found error: requestID(%v) err(%v)",
				GetRequestID(r), err)
		}
		if ec == nil {
			ec = InternalErrorCode(err)
		}
		_ = ec.ServeResponse(w, r)
	}
}

func (o *ObjectNode) unsupportedOperationHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("Audit: unsupported operation: requestID(%v) remote(%v) action(%v) userAgent(%v)",
		GetRequestID(r),
		getRequestIP(r),
		ActionFromRouteName(mux.CurrentRoute(r).GetName()),
		r.UserAgent())
	_ = UnsupportedOperation.ServeResponse(w, r)
	return
}
