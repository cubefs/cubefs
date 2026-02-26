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

package master

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func (m *Server) volAddRegion(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		err     error
		region  string
		vol     *Vol
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolAddRegion))
	defer func() {
		doStatAndMetric(proto.AdminVolAddRegion, metric, err, map[string]string{exporter.Vol: name})
		AuditLog(r, proto.AdminVolAddRegion, fmt.Sprintf("add region for %v", name), err)
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if authKey, err = extractAuthKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if region = r.FormValue(regionKey); region == "" {
		region = proto.DefaultRegion
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	// Validate region exists
	if !m.cluster.isValidRegion(region) {
		err = fmt.Errorf("region %v does not exist in cluster", region)
		log.LogErrorf("[volAddRegion] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	// Check if region already exists in allowed regions
	if vol.isRegionInAllowed(region) {
		err = fmt.Errorf("region(%v) already in vol allowed regions(%v)", region, vol.allowedRegions)
		log.LogErrorf("[volAddRegion] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	newArgs := getVolVarargs(vol)
	newArgs.allowedRegions = append(newArgs.allowedRegions, region)
	sort.Strings(newArgs.allowedRegions)

	// Set crossZone to true when adding region
	if !vol.crossZone {
		newArgs.crossZone = true
		log.LogWarnf("[volAddRegion] vol(%v) is not cross zone, set crossZone to true", name)
	}

	log.LogInfof("[volAddRegion] vol(%v) to add region, old(%v), add(%v)", name, vol.allowedRegions, region)

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg := fmt.Sprintf("add vol(%v) region successfully, new allowed regions: %v", name, newArgs.allowedRegions)
	log.LogInfof("[volAddRegion] %v", msg)
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

func (m *Server) volUpdateDefaultRegion(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		err     error
		region  string
		vol     *Vol
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolUpdateDefaultRegion))
	defer func() {
		doStatAndMetric(proto.AdminVolUpdateDefaultRegion, metric, err, map[string]string{exporter.Vol: name})
		AuditLog(r, proto.AdminVolUpdateDefaultRegion, fmt.Sprintf("update default region for %v", name), err)
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if authKey, err = extractAuthKey(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if region = r.FormValue(regionKey); region == "" {
		err = fmt.Errorf("region parameter is required")
		log.LogErrorf("[volUpdateDefaultRegion] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	// Validate region exists
	if !m.cluster.isValidRegion(region) {
		err = fmt.Errorf("region %v does not exist in cluster", region)
		log.LogErrorf("[volUpdateDefaultRegion] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	// Check if region is in allowed regions
	if !vol.isRegionInAllowed(region) {
		err = fmt.Errorf("region(%v) is not in vol allowed regions(%v)", region, vol.allowedRegions)
		log.LogErrorf("[volUpdateDefaultRegion] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	newArgs := getVolVarargs(vol)
	newArgs.defaultRegion = region

	log.LogInfof("[volUpdateDefaultRegion] vol(%v) to update default region, old(%v), new(%v)", name, vol.defaultRegion, region)

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg := fmt.Sprintf("update vol(%v) default region successfully, new default region: %v", name, region)
	log.LogInfof("[volUpdateDefaultRegion] %v", msg)
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}
