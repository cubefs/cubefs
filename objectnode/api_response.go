// Copyright 2023 The CubeFS Authors.
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
	"net/http"
	"strconv"
)

func writeResponse(w http.ResponseWriter, code int, response []byte, mime string) {
	if mime != "" {
		w.Header().Set(ContentType, mime)
	}
	w.Header().Set(ContentLength, strconv.Itoa(len(response)))
	w.WriteHeader(code)
	if response != nil {
		_, _ = w.Write(response)
	}
}

func writeSuccessResponseXML(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, ValueContentTypeXML)
}

func writeSuccessResponseJSON(w http.ResponseWriter, response []byte) {
	writeResponse(w, http.StatusOK, response, ValueContentTypeJSON)
}
