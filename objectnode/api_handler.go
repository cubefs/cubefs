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
	"errors"
	"net/http"
	"strings"
	"syscall"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"

	"github.com/gorilla/mux"
)

type RequestParam struct {
	resource  string
	bucket    string
	object    string
	action    proto.Action
	apiName   string
	sourceIP  string
	vars      map[string]string
	accessKey string
	r         *http.Request
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

func (p *RequestParam) AccessKey() string {
	return p.accessKey
}

func ParseRequestParam(r *http.Request) *RequestParam {
	p := new(RequestParam)
	p.r = r
	p.vars = mux.Vars(r)
	p.bucket = p.vars[ContextKeyBucket]
	p.object = p.vars[ContextKeyObject]
	p.accessKey = p.vars[ContextKeyAccessKey]
	p.sourceIP = getRequestIP(r)
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
	p.action = GetActionFromContext(r)
	if p.action.IsNone() {
		p.action = ActionFromRouteName(mux.CurrentRoute(r).GetName())
	}
	p.apiName = strings.TrimPrefix(string(p.action), proto.OSSActionPrefix)

	return p
}

func (o *ObjectNode) getVol(bucket string) (vol *Volume, err error) {
	if bucket == "" {
		return nil, errors.New("bucket name is empty")
	}
	vol, err = o.vm.Volume(bucket)
	if err != nil {
		log.LogErrorf("getVol: load Volume fail, bucket(%v) err(%v)", bucket, err)
		if err == proto.ErrVolNotExists {
			err = NoSuchBucket
			return
		}
		err = InternalErrorCode(err)
		return
	}
	return vol, nil
}

func (o *ObjectNode) errorResponse(w http.ResponseWriter, r *http.Request, err error, ec *ErrorCode) {
	if err != nil || ec != nil {
		log.LogErrorf("errorResponse: found error: requestID(%v) err(%v) errCode(%v)", GetRequestID(r), err, ec)
		if err == syscall.EDQUOT || err == syscall.ENOSPC {
			ec = DiskQuotaExceeded
		}
		if ec1, ok := err.(*ErrorCode); ok && ec == nil {
			ec = ec1
		}
		if ec == nil {
			ec = InternalErrorCode(err)
		}
		ec.ServeResponse(w, r)
	}
}

func (o *ObjectNode) unsupportedOperationHandler(w http.ResponseWriter, r *http.Request) {
	log.LogInfof("Audit: unsupported operation: requestID(%v) remote(%v) action(%v) userAgent(%v)",
		GetRequestID(r),
		getRequestIP(r),
		ActionFromRouteName(mux.CurrentRoute(r).GetName()),
		r.UserAgent())
	UnsupportedOperation.ServeResponse(w, r)
	return
}
