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

package flashnode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (f *FlashNode) registerAPIHandler() {
	http.HandleFunc("/stat", f.handleStat)
	http.HandleFunc("/evictVol", f.handleEvictVolume)
	http.HandleFunc("/evictAll", f.handleEvictAll)
}

func (f *FlashNode) handleStat(w http.ResponseWriter, r *http.Request) {
	replyOK(w, r, proto.FlashNodeStat{
		NodeLimit:   uint64(f.readLimiter.Limit()),
		CacheStatus: f.cacheEngine.Status(),
	})
}

func (f *FlashNode) handleEvictVolume(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	volume := r.FormValue("volume")
	if volume == "" {
		replyErr(w, r, proto.ErrCodeParamError, "volume name can not be empty", nil)
		return
	}
	replyOK(w, r, f.cacheEngine.EvictCacheByVolume(volume))
}

func (f *FlashNode) handleEvictAll(w http.ResponseWriter, r *http.Request) {
	f.cacheEngine.EvictCacheAll()
	replyOK(w, r, nil)
}

func replyOK(w http.ResponseWriter, r *http.Request, data interface{}) {
	replyErr(w, r, proto.ErrCodeSuccess, "OK", data)
}

func replyErr(w http.ResponseWriter, r *http.Request, code int32, msg string, data interface{}) {
	remote := fmt.Sprintf("url(%s) addr(%s)", r.URL.String(), r.RemoteAddr)
	reply := proto.HTTPReply{Code: code, Msg: msg, Data: data}
	replyBytes, err := json.Marshal(reply)
	if err != nil {
		log.LogErrorf("to %s marshal reply[%v] err:[%v]", remote, reply, err)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(replyBytes)))
	if _, err = w.Write(replyBytes); err != nil {
		log.LogErrorf("to %s write reply len[%d] err:[%v]", remote, len(replyBytes), err)
	}
	log.LogInfof("to %s respond", remote)
}
