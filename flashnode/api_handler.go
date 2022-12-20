// Copyright 2018 The CubeFS Authors.
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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
)

// register the APIs
func (f *FlashNode) registerAPIHandler() (err error) {
	http.HandleFunc(proto.VersionPath, f.getVersion)
	http.HandleFunc("/stat", f.getCacheStatHandler)
	http.HandleFunc("/evictVol", f.evictVolumeCacheHandler)
	http.HandleFunc("/evictAll", f.evictAllCacheHandler)

	return
}

func (f *FlashNode) getVersion(w http.ResponseWriter, _ *http.Request) {
	version := proto.MakeVersion("FlashNode")
	version.Version = NodeLatestVersion
	marshal, _ := json.Marshal(version)
	if _, err := w.Write(marshal); err != nil {
		log.LogErrorf("write version has err:[%s]", err.Error())
	}
}

func (f *FlashNode) getCacheStatHandler(w http.ResponseWriter, r *http.Request) {
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Data: f.cacheEngine.Status(), Msg: "ok"})
	return
}

func (f *FlashNode) evictVolumeCacheHandler(w http.ResponseWriter, r *http.Request) {
	var (
		volume     string
		failedKeys []interface{}
	)
	r.ParseForm()
	volume = r.FormValue(VolumePara)
	if volume == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusBadRequest, Msg: "volume name can not be empty"})
		return
	}
	failedKeys = f.cacheEngine.EvictCacheByVolume(volume)
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Data: failedKeys, Msg: "ok"})
	return
}

func (f *FlashNode) evictAllCacheHandler(w http.ResponseWriter, r *http.Request) {
	f.cacheEngine.EvictCacheAll()
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Msg: "ok"})
	return
}

func sendOkReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) (err error) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
		return
	}
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	return
}

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err[%v]", r.URL, r.RemoteAddr, httpReply)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
	}
	return
}
