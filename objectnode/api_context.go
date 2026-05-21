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
	"encoding/json"
	"html"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"

	"github.com/gorilla/mux"
)

const (
	ContextKeyRequestID     = "ctx_request_id"
	ContextKeyRequestAction = "ctx_request_action"
	ContextKeyStatusCode    = "status_code"
	ContextKeyErrorMessage  = "error_message"
	ContextKeyBucket        = "bucket"
	ContextKeyObject        = "object"
	ContextKeyUid           = "uid"
	ContextKeyAccessKey     = "access_key"
	ContextKeyRequester     = "requester"
	ContextKeyOwner         = "owner"
	ContextKeyAuditFields   = "audit_fields"
)

func SetRequestID(r *http.Request, requestID string) {
	mux.Vars(r)[ContextKeyRequestID] = requestID
}

func GetRequestID(r *http.Request) (id string) {
	return html.EscapeString(mux.Vars(r)[ContextKeyRequestID])
}

func SetRequestAction(r *http.Request, action proto.Action) {
	mux.Vars(r)[ContextKeyRequestAction] = action.String()
}

func GetActionFromContext(r *http.Request) (action proto.Action) {
	return proto.ParseAction(mux.Vars(r)[ContextKeyRequestAction])
}

func SetResponseStatusCode(r *http.Request, code string) {
	mux.Vars(r)[ContextKeyStatusCode] = code
}

func GetStatusCodeFromContext(r *http.Request) int {
	code, err := strconv.Atoi(mux.Vars(r)[ContextKeyStatusCode])
	if err == nil {
		return code
	}
	return 0
}

func SetResponseErrorMessage(r *http.Request, message string) {
	mux.Vars(r)[ContextKeyErrorMessage] = message
}

func getResponseErrorMessage(r *http.Request) string {
	return mux.Vars(r)[ContextKeyErrorMessage]
}

type AuditFields struct {
	Size    int64    `json:"Size,omitempty"`
	ETag    string   `json:"ETag,omitempty"`
	Objects []string `json:"Objects,omitempty"`
}

func SetAuditFields(r *http.Request, fields AuditFields) {
	data, err := json.Marshal(fields)
	if err != nil {
		return
	}
	mux.Vars(r)[ContextKeyAuditFields] = string(data)
}

func GetAuditFields(r *http.Request) AuditFields {
	var fields AuditFields
	raw := mux.Vars(r)[ContextKeyAuditFields]
	if raw == "" {
		return fields
	}
	_ = json.Unmarshal([]byte(raw), &fields)
	return fields
}
