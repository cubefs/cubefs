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
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

// mpRegionPolicyFormValueMeansClear reports whether the policy form/query value requests clearing policy for the source region.
func mpRegionPolicyFormValueMeansClear(policy string) bool {
	s := strings.TrimSpace(policy)
	return strings.EqualFold(s, "empty")
}

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

func (m *Server) volUpdateMpRegionPolicy(w http.ResponseWriter, r *http.Request) {
	var (
		name    string
		authKey string
		err     error
		region  string
		policy  string
		vol     *Vol
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolUpdateMpRegionPolicy))
	defer func() {
		doStatAndMetric(proto.AdminVolUpdateMpRegionPolicy, metric, err, map[string]string{exporter.Vol: name})
		AuditLog(r, proto.AdminVolUpdateMpRegionPolicy, fmt.Sprintf("update mp region policy for %v", name), err)
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

	region = r.FormValue(regionKey)
	if region == "" {
		err = fmt.Errorf("region parameter is required")
		log.LogErrorf("[volUpdateMpRegionPolicy] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	policy = r.FormValue("policy")

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeVolNotExists, Msg: err.Error()})
		return
	}

	// Check if region is in allowed regions
	if !vol.isRegionInAllowed(region) {
		err = fmt.Errorf("region(%v) is not in vol allowed regions(%v)", region, vol.allowedRegions)
		log.LogErrorf("[volUpdateMpRegionPolicy] vol(%v), err: %v", name, err.Error())
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	// Parse policy string (empty or "empty" clears policy for this source region)
	var mpPolicy *proto.VolMpPolicy
	if mpRegionPolicyFormValueMeansClear(policy) {
		log.LogWarnf("[volUpdateMpRegionPolicy] to clear mp policy for region(%v)", region)
	} else {
		mpPolicy, err = parseMpRegionPolicy(policy, vol.allowedRegions, region)
		if err != nil {
			log.LogErrorf("[volUpdateMpRegionPolicy] vol(%v), err: %v", name, err.Error())
			sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
			return
		}
		mpPolicy.Name = region
	}

	newArgs := getVolVarargs(vol)
	if newArgs.mpPolicy == nil {
		newArgs.mpPolicy = make(map[string]*proto.VolMpPolicy)
	}

	if mpPolicy == nil {
		// Clear policy for this region
		delete(newArgs.mpPolicy, region)
		AuditLog(r, proto.AdminVolUpdateMpRegionPolicy, fmt.Sprintf("clear mp policy for region(%v), vol(%v)", region, name), nil)
	} else {
		// Set policy for this region
		newArgs.mpPolicy[region] = mpPolicy
		AuditLog(r, proto.AdminVolUpdateMpRegionPolicy, fmt.Sprintf("update mp policy for region(%v), vol(%v), policy(%v)", region, name, policy), nil)
	}

	if err = m.cluster.updateVol(name, authKey, newArgs); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	msg := fmt.Sprintf("update vol(%v) mp region policy successfully for region(%v), policy(%v)", name, region, newArgs.mpPolicy)
	log.LogInfof("[volUpdateMpRegionPolicy] %v", msg)
	sendOkReply(w, r, newSuccessHTTPReply("success"))
}

// parseMpRegionPolicy parses policy string like "r2:rocksdb; r3:mem" into VolMpPolicy
func parseMpRegionPolicy(policyStr string, allowedRegions []string, region string) (*proto.VolMpPolicy, error) {
	if mpRegionPolicyFormValueMeansClear(policyStr) {
		log.LogWarnf("[parseMpRegionPolicy] to clear mp policy")
		return nil, nil
	}

	policy := &proto.VolMpPolicy{
		Learner: make(map[string]*proto.LearnerPolicy),
	}

	// Split by semicolon
	parts := strings.Split(policyStr, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("invalid policy format, expected 'region:mode;region:mode', got: %s", policyStr)
		}

		// Split by colon
		kv := strings.Split(part, ":")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid policy format, expected 'region:mode', got: %s", part)
		}

		targetRegion := strings.TrimSpace(kv[0])
		modeStr := strings.TrimSpace(kv[1])

		if targetRegion == region {
			return nil, fmt.Errorf("learner region(%v) is the same as the normal region(%v)", targetRegion, region)
		}

		// Validate target region is in allowed regions
		isAllowed := false
		for _, r := range allowedRegions {
			if r == targetRegion {
				isAllowed = true
				break
			}
		}
		if !isAllowed {
			return nil, fmt.Errorf("target region(%v) is not in vol allowed regions(%v)", targetRegion, allowedRegions)
		}

		if _, dup := policy.Learner[targetRegion]; dup {
			return nil, fmt.Errorf("duplicate learner target region(%v) in policy", targetRegion)
		}

		// Parse store mode
		var mode proto.StoreMode
		switch strings.ToLower(modeStr) {
		case "rocksdb":
			mode = proto.StoreModeRocksDb
		case "memory":
			mode = proto.StoreModeMem
		default:
			return nil, fmt.Errorf("invalid store mode: %s, must be 'rocksdb' or 'memory'", modeStr)
		}

		policy.Learner[targetRegion] = &proto.LearnerPolicy{
			Mode: mode,
		}
	}

	return policy, nil
}

func (m *Server) volGetMpRegionPolicy(w http.ResponseWriter, r *http.Request) {
	var (
		name string
		err  error
		vol  *Vol
	)

	metric := exporter.NewTPCnt(apiToMetricsName(proto.AdminVolGetMpRegionPolicy))
	defer func() {
		doStatAndMetric(proto.AdminVolGetMpRegionPolicy, metric, err, map[string]string{exporter.Vol: name})
		AuditLog(r, proto.AdminVolGetMpRegionPolicy, fmt.Sprintf("get mp region policy for %v", name), err)
	}()

	if err = r.ParseForm(); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if name, err = extractName(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}

	statuses := vol.getMpRegionPolicyStatus(m.cluster)
	sendOkReply(w, r, newSuccessHTTPReply(statuses))
}
