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
	"errors"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"github.com/chubaofs/chubaofs/util/log"
)

type RequestParam struct {
	account   string
	resource  string
	bucket    string
	object    string
	actions   Action
	sourceIP  string
	vol       *volume
	condVals  map[string][]string
	isOwner   bool
	vars      map[string]string
	accessKey string
}

func (o *ObjectNode) parseRequestParam(r *http.Request) (*RequestParam, error) {
	p := new(RequestParam)
	p.vars = mux.Vars(r)
	p.bucket = p.vars["bucket"]
	p.object = p.vars["object"]
	p.vol, _ = o.getVol(p.bucket)
	p.sourceIP = getRequestIP(r)
	p.condVals = getCondtionValues(r)
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
	if auth != nil && p.vol != nil {
		accessKey, _ := p.vol.OSSSecure()
		p.account = accessKey
		if auth.accessKey == accessKey {
			p.isOwner = true
		}
		p.accessKey = auth.accessKey
	}

	return p, nil
}

//Deprecated:
func (o *ObjectNode) parseRequestParams(r *http.Request) (vars map[string]string, bucket, object string, vl *volume, err error) {
	vars = mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]
	if bucket != "" {
		if vm, ok := o.vm.(*volumeManager); ok {
			vl, err = vm.loadVolume(bucket)
			if err != nil {
				log.LogErrorf("parseRequestParams: load volume fail, requestId(%v) bucket(%v) err(%v)",
					GetRequestID(r), bucket, err)
			}
		} else {
			log.LogErrorf("parseRequestParams: load volume fail, requestId(%v) bucket(%v) err(%v)",
				GetRequestID(r), bucket, err)
		}
	}
	return
}

func (o *ObjectNode) getVol(bucket string) (vol *volume, err error) {
	if bucket == "" {
		return nil, errors.New("bucket name is empty")
	}
	vm, ok := o.vm.(*volumeManager)
	if !ok {
		return nil, errors.New("volumeManger is invalid")
	}

	vol, err = vm.loadVolume(bucket)
	if err != nil {
		log.LogErrorf("parseRequestParams: load volume fail, bucket(%v) err(%v)", bucket, err)
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
			ec = &InternalError
		}
		_ = ec.ServeResponse(w, r)
	}
}

func (o *ObjectNode) unsupportedOperationHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = UnsupportedOperation.ServeResponse(w, r); err != nil {
		log.LogErrorf("unsupportedOperationHandler: serve response fail: requestID(%v) err(%v)",
			GetRequestID(r), err)
		ServeInternalStaticErrorResponse(w, r)
	}
	return
}
