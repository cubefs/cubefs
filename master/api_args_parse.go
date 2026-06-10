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

package master

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/compressor"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/log"
)

var (
	parseArgs = common.ParseArguments
	newArg    = common.NewArgument
)

// Parse the request that adds/deletes a raft node.
func parseRequestForRaftNode(r *http.Request) (id uint64, host string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if id, err = extractNodeID(r); err != nil {
		return
	}

	if host = r.FormValue(addrKey); host == "" {
		err = keyNotFound(addrKey)
		return
	}

	if arr := strings.Split(host, colonSplit); len(arr) < 2 {
		err = unmatchedKey(addrKey)
		return
	}
	return
}

func extractTxTimeout(r *http.Request, old int64) (timeout int64, err error) {
	var txTimeout uint64
	if txTimeout, err = extractUint64WithDefault(r, txTimeoutKey, uint64(old)); err != nil {
		return
	}

	if txTimeout == 0 || txTimeout > proto.MaxTransactionTimeout {
		return timeout, fmt.Errorf("txTimeout(%d) value range [1-%v] minutes", txTimeout, proto.MaxTransactionTimeout)
	}
	timeout = int64(txTimeout)
	return timeout, nil
}

func extractTxConflictRetryNum(r *http.Request, old int64) (retryNum int64, err error) {
	var txRetryNum uint64
	if txRetryNum, err = extractUint64WithDefault(r, txConflictRetryNumKey, uint64(old)); err != nil {
		return
	}

	if txRetryNum == 0 || txRetryNum > proto.MaxTxConflictRetryNum {
		return retryNum, fmt.Errorf("txRetryNum(%d) value range [1-%v]", txRetryNum, proto.MaxTxConflictRetryNum)
	}
	retryNum = int64(txRetryNum)
	return retryNum, nil
}

func extractTxConflictRetryInterval(r *http.Request, old int64) (interval int64, err error) {
	var txInterval uint64
	if txInterval, err = extractUint64WithDefault(r, txConflictRetryIntervalKey, uint64(old)); err != nil {
		return
	}

	if txInterval < proto.MinTxConflictRetryInterval || txInterval > proto.MaxTxConflictRetryInterval {
		return interval, fmt.Errorf("txInterval(%d) value range [%v-%v] ms",
			txInterval, proto.MinTxConflictRetryInterval, proto.MaxTxConflictRetryInterval)
	}
	interval = int64(txInterval)
	return interval, nil
}

func extractTxOpLimitInterval(r *http.Request, volLimit int) (limit int, err error) {
	return extractUintWithDefault(r, txOpLimitKey, volLimit)
}

func hasTxParams(r *http.Request) bool {
	return r.FormValue(enableTxMaskKey) != "" || r.FormValue(txTimeoutKey) != ""
}

func parseTxMask(r *http.Request, oldMask proto.TxOpMask) (mask proto.TxOpMask, err error) {
	maskStr := r.FormValue(enableTxMaskKey)
	if maskStr == "" {
		return oldMask, nil
	}

	var reset bool
	reset, err = extractBoolWithDefault(r, txForceResetKey, false)
	if err != nil {
		return
	}

	mask, err = proto.GetMaskFromString(maskStr)
	if err != nil {
		return
	}

	if reset || mask == proto.TxOpMaskOff {
		return
	}

	mask = mask | oldMask
	return
}

func parseRequestForAddNode(r *http.Request) (nodeAddr, raftHeartbeatPort, raftReplicaPort, zoneName, rack string, mediaType uint32, poolId uint8, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	zoneName = extractStrWithDefault(r, zoneNameKey, DefaultZoneName)
	rack = extractStrWithDefault(r, rackKey, "")
	// for old version node registration, heartbeat port and replica port may be empty
	raftHeartbeatPort = extractStr(r, heartbeatPortKey)
	raftReplicaPort = extractStr(r, replicaPortKey)

	if mediaType, err = extractMediaType(r); err != nil {
		return
	}

	// Parse poolId (optional, for backward compatibility)
	if poolIdStr := r.FormValue(poolIdKey); poolIdStr != "" {
		var poolIdVal uint64
		if poolIdVal, err = strconv.ParseUint(poolIdStr, 10, 8); err != nil {
			return "", "", "", "", "", 0, 0, fmt.Errorf("invalid poolId: %v", err)
		}
		poolId = uint8(poolIdVal)
	}

	return
}

func parseDecomNodeReq(r *http.Request) (nodeAddr string, limit int, err error) {
	nodeAddr, err = parseAndExtractNodeAddr(r)
	if err != nil {
		return
	}

	limit, err = extractUint(r, countKey)
	if err != nil {
		return
	}

	return
}

func parseDecomDataNodeReq(r *http.Request) (nodeAddr string, limit int, err error) {
	nodeAddr, err = parseAndExtractNodeAddr(r)
	if err != nil {
		return
	}
	limit, err = extractUint(r, countKey)
	if err != nil {
		return
	}
	return
}

func parseAndExtractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractNodeAddr(r)
}

func parseRequestToGetTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
	var body []byte
	if err = r.ParseForm(); err != nil {
		return
	}
	if body, err = io.ReadAll(r.Body); err != nil {
		return
	}
	tr = &proto.AdminTask{}
	decoder := json.NewDecoder(bytes.NewBuffer(body))
	decoder.UseNumber()
	err = decoder.Decode(tr)
	return
}

func parseVolName(r *http.Request) (name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractName(r)
}

func parseVolVerStrategy(r *http.Request) (strategy proto.VolumeVerStrategy, isForce bool, err error) {
	var value string
	if value = extractStr(r, enableKey); value == "" {
		strategy.Enable = true
	} else {
		if strategy.Enable, err = strconv.ParseBool(value); err != nil {
			log.LogErrorf("parseVolVerStrategy. strategy.Enable %v strategy %v", strategy.Enable, strategy)
			return
		}
	}

	strategy.KeepVerCnt, err = extractUint(r, countKey)
	if strategy.Enable && err != nil {
		log.LogErrorf("parseVolVerStrategy. strategy.Enable %v strategy %v", strategy.Enable, strategy)
		return
	}
	strategy.Periodic, err = extractUint(r, Periodic)
	if strategy.Enable && err != nil {
		log.LogErrorf("parseVolVerStrategy. strategy.Enable %v strategy %v", strategy.Enable, strategy)
		return
	}

	if value = r.FormValue(forceKey); value != "" {
		isForce = true
		strategy.ForceUpdate, _ = strconv.ParseBool(value)
	}

	log.LogDebugf("parseVolVerStrategy. strategy %v", strategy)
	return
}

func parseGetVolParameter(r *http.Request) (p *getVolParameter, err error) {
	p = &getVolParameter{}
	skipOwnerValidationVal := r.Header.Get(proto.SkipOwnerValidation)
	if len(skipOwnerValidationVal) > 0 {
		if p.skipOwnerValidation, err = strconv.ParseBool(skipOwnerValidationVal); err != nil {
			return
		}
	}
	if p.name, err = extractName(r); err != nil {
		return
	}
	if p.authKey = r.FormValue(volAuthKey); !p.skipOwnerValidation && len(p.authKey) == 0 {
		err = keyNotFound(volAuthKey)
		return
	}
	return
}

func parseRequestToDeleteVol(r *http.Request) (name, authKey string, status, force bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		return
	}

	if authKey, err = extractAuthKey(r); err != nil {
		return
	}

	if status, err = extractBoolWithDefault(r, deleteVolKey, true); err != nil {
		return
	}

	force, err = extractBoolWithDefault(r, forceDelVolKey, false)
	return
}

type updateVolReq struct {
	name                     string
	authKey                  string
	capacity                 uint64
	deleteLockTime           int64
	followerRead             bool
	metaFollowerRead         bool
	metaNearRead             bool
	directRead               bool
	ignoreTinyRecover        bool
	maximallyRead            bool
	leaderRetryTimeout       int64
	authenticate             bool
	enablePosixAcl           bool
	enableTransaction        proto.TxOpMask
	txTimeout                int64
	txConflictRetryNum       int64
	txConflictRetryInterval  int64
	txOpLimit                int
	zoneName                 string
	description              string
	dpSelectorName           string
	dpSelectorParm           string
	replicaNum               int
	coldArgs                 *coldVolArgs
	dpReadOnlyWhenVolFull    bool
	enableQuota              bool
	crossZone                bool
	trashInterval            int64
	enableAutoDpMetaRepair   bool
	enableAutoMpMetaRepair   bool
	accessTimeValidInterval  int64
	enablePersistAccessTime  bool
	volStorageClass          uint32
	forbidWriteOpOfProtoVer0 bool
	quotaOfClass             uint64
	quotaClass               uint32
	quotaOfPool              uint64
	quotaPool                uint8
	storeMode                int
	dpsSelectTag             string
	mpsSelectTag             string
	defaultPoolId            uint8
	allowedPools             []uint8
}

func parseColdVolUpdateArgs(r *http.Request, vol *Vol) (args *coldVolArgs, err error) {
	args = &coldVolArgs{}

	if args.objBlockSize, err = extractUintWithDefault(r, ebsBlkSizeKey, vol.EbsBlkSize); err != nil {
		return
	}

	if vol.volStorageClass != proto.StorageClass_BlobStore {
		log.LogInfof("[parseColdVolUpdateArgs] vol(%v) storageClass(%v) is not blobstore, skip parse cache args",
			vol.Name, proto.StorageClassString(vol.volStorageClass))
		return
	}

	return
}

func parseVolUpdateReq(r *http.Request, vol *Vol, req *updateVolReq) (err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	req.authKey = extractStr(r, volAuthKey)
	req.description = extractStrWithDefault(r, descriptionKey, vol.description)
	req.zoneName = extractStrWithDefault(r, zoneNameKey, vol.zoneName)
	if req.crossZone, err = extractBoolWithDefault(r, crossZoneKey, vol.crossZone); err != nil {
		return
	}

	if req.capacity, err = extractUint64WithDefault(r, volCapacityKey, vol.Capacity); err != nil {
		return
	}

	if req.deleteLockTime, err = extractInt64WithDefault(r, volDeleteLockTimeKey, vol.DeleteLockTime); err != nil {
		return
	}

	if req.leaderRetryTimeout, err = extractInt64WithDefault(r, proto.LeaderRetryTimeoutKey, vol.LeaderRetryTimeout); err != nil {
		return
	}

	if req.enablePosixAcl, err = extractBoolWithDefault(r, enablePosixAclKey, vol.enablePosixAcl); err != nil {
		return
	}

	var txMask proto.TxOpMask
	if txMask, err = parseTxMask(r, vol.enableTransaction); err != nil {
		return
	}
	req.enableTransaction = txMask

	if req.enableQuota, err = extractBoolWithDefault(r, enableQuota, vol.enableQuota); err != nil {
		return
	}

	var txTimeout int64
	if txTimeout, err = extractTxTimeout(r, vol.txTimeout); err != nil {
		return
	}
	req.txTimeout = txTimeout

	var txConflictRetryNum int64
	if txConflictRetryNum, err = extractTxConflictRetryNum(r, vol.txConflictRetryNum); err != nil {
		return
	}
	req.txConflictRetryNum = txConflictRetryNum

	var txConflictRetryInterval int64
	if txConflictRetryInterval, err = extractTxConflictRetryInterval(r, vol.txConflictRetryInterval); err != nil {
		return
	}
	req.txConflictRetryInterval = txConflictRetryInterval

	if req.txOpLimit, err = extractTxOpLimitInterval(r, vol.txOpLimit); err != nil {
		return
	}

	if req.authenticate, err = extractBoolWithDefault(r, authenticateKey, vol.authenticate); err != nil {
		return
	}

	if req.followerRead, err = extractBoolWithDefault(r, followerReadKey, vol.FollowerRead); err != nil {
		return
	}

	if req.metaFollowerRead, err = extractBoolWithDefault(r, proto.MetaFollowerReadKey, vol.MetaFollowerRead); err != nil {
		return
	}

	if req.metaNearRead, err = extractBoolWithDefault(r, proto.MetaNearReadKey, vol.MetaNearRead); err != nil {
		return
	}

	if req.maximallyRead, err = extractBoolWithDefault(r, proto.MaximallyReadKey, vol.MaximallyRead); err != nil {
		return
	}

	if req.directRead, err = extractBoolWithDefault(r, proto.VolEnableDirectRead, vol.DirectRead); err != nil {
		return
	}

	if req.ignoreTinyRecover, err = extractBoolWithDefault(r, proto.VolIgnoreTinyRecover, vol.IgnoreTinyRecover); err != nil {
		return
	}

	if req.dpReadOnlyWhenVolFull, err = extractBoolWithDefault(r, dpReadOnlyWhenVolFull, vol.DpReadOnlyWhenVolFull); err != nil {
		return
	}

	if req.trashInterval, err = extractInt64WithDefault(r, trashIntervalKey, vol.TrashInterval); err != nil {
		return
	}

	if req.trashInterval > maxTrashInterval {
		err = fmt.Errorf("trash interval can't be greater than %d, now %d", maxTrashInterval, req.trashInterval)
		return
	}

	if req.accessTimeValidInterval, err = extractInt64WithDefault(r, accessTimeIntervalKey, vol.AccessTimeValidInterval); err != nil {
		return
	}
	if req.enablePersistAccessTime, err = extractBoolWithDefault(r, enablePersistAccessTimeKey, vol.EnablePersistAccessTime); err != nil {
		return
	}

	if req.enableAutoDpMetaRepair, err = extractBoolWithDefault(r, autoDpMetaRepairKey, vol.EnableAutoDpMetaRepair.Load()); err != nil {
		return
	}
	if req.enableAutoMpMetaRepair, err = extractBoolWithDefault(r, autoMpMetaRepairKey, vol.EnableAutoMpMetaRepair.Load()); err != nil {
		return
	}

	if req.forbidWriteOpOfProtoVer0, err = extractBoolWithDefault(r, forbidWriteOpOfProtoVersion0, vol.ForbidWriteOpOfProtoVer0.Load()); err != nil {
		return
	}
	log.LogDebugf("[parseVolUpdateReq] vol(%v) forbidWriteOpOfProtoVer0: %v", vol.Name, req.forbidWriteOpOfProtoVer0)

	req.dpSelectorName = r.FormValue(dpSelectorNameKey)
	req.dpSelectorParm = r.FormValue(dpSelectorParmKey)

	if (req.dpSelectorName == "" && req.dpSelectorParm != "") || (req.dpSelectorName != "" && req.dpSelectorParm == "") {
		err = keyNotFound(dpSelectorNameKey + " or " + dpSelectorParmKey)
		return

	} else if req.dpSelectorParm == "" && req.dpSelectorName == "" {
		req.dpSelectorName = vol.dpSelectorName
		req.dpSelectorParm = vol.dpSelectorParm
	}

	if req.volStorageClass, err = extractUint32WithDefault(r, volStorageClassKey, vol.volStorageClass); err != nil {
		err = fmt.Errorf("failed to extract key: %v", volStorageClassKey)
		log.LogErrorf("[parseVolUpdateReq] vol(%v) err: %v", vol.Name, err.Error())
		return
	}

	req.quotaClass, err = extractUint32(r, quotaClass)
	if err != nil {
		log.LogErrorf("[parseVolUpdateReq] vol(%v) err: %v", vol.Name, err.Error())
		return
	}

	if req.quotaClass != 0 && (!proto.IsStorageClassReplica(req.quotaClass) ||
		!proto.IsVolSupportStorageClass(vol.allowedStorageClass, req.quotaClass)) {
		return fmt.Errorf("%s is not vaild, only support update replica mode, and need in allowd class, now %d",
			quotaClass, req.quotaClass)
	}

	if req.quotaClass != 0 && r.FormValue(quotaOfClass) == "" {
		return fmt.Errorf("%s can't be empty when set capacityClass info. ", quotaOfClass)
	}

	req.quotaOfClass, err = extractUint64(r, quotaOfClass)
	if err != nil {
		log.LogErrorf("[parseVolUpdateReq] vol(%v) err: %v", vol.Name, err.Error())
		return
	}

	if req.quotaOfClass > req.capacity {
		return fmt.Errorf("parseVolUpdateReq: quotaOfClass %d can't bigger than capacity %d", req.quotaOfClass, req.capacity)
	}

	req.quotaPool, err = extractUint8WithDefault(r, quotaPool, 0)
	if err != nil {
		log.LogErrorf("[parseVolUpdateReq] vol(%v) err: %v", vol.Name, err.Error())
		return
	}

	if req.quotaPool != 0 {
		// Validate pool exists and is in volume's allowed pools
		if len(vol.allowedPools) == 0 {
			return fmt.Errorf("%s is not valid, volume has no allowed pools", quotaPool)
		}

		if !vol.isPoolInAllowed(req.quotaPool) {
			return fmt.Errorf("%s is not valid, pool is not in volume's allowed pools", quotaPool)
		}

		if req.quotaPool != 0 && r.FormValue(quotaOfPool) == "" {
			return fmt.Errorf("%s can't be empty when set quotaPool info", quotaOfPool)
		}
	}

	req.quotaOfPool, err = extractUint64(r, quotaOfPool)
	if err != nil {
		log.LogErrorf("[parseVolUpdateReq] vol(%v) err: %v", vol.Name, err.Error())
		return
	}

	if req.quotaOfPool > req.capacity {
		return fmt.Errorf("parseVolUpdateReq: quotaOfPool %d can't bigger than capacity %d", req.quotaOfPool, req.capacity)
	}

	if vol.volStorageClass == proto.StorageClass_BlobStore {
		if req.volStorageClass != vol.volStorageClass {
			err = fmt.Errorf("volume volStorageClass is StorageClass_BlobStore, not allow to change it")
			log.LogErrorf("[parseVolUpdateReq] vol(%v) err: %v", vol.Name, err.Error())
			return
		}
	} else if proto.IsStorageClassReplica(vol.volStorageClass) {
		if !proto.IsStorageClassReplica(req.volStorageClass) {
			err = fmt.Errorf("volume volStorageClass is replica, not allow to change to: %v",
				proto.StorageClassString(req.volStorageClass))
			log.LogErrorf("[parseVolUpdateReq] vol(%v) err: %v", vol.Name, err.Error())
			return
		}

		volStorageClassAllowed := false
		for _, asc := range vol.allowedStorageClass {
			if asc == req.volStorageClass {
				volStorageClassAllowed = true
			}
		}
		if !volStorageClassAllowed {
			err = fmt.Errorf("requeset volStorageClass(%v) not in volume's allowedStorageClass",
				proto.StorageClassString(req.volStorageClass))
			log.LogErrorf("[parseVolUpdateReq] vol(%v) err: %v", vol.Name, err.Error())
			return
		}

		if req.volStorageClass != vol.volStorageClass {
			log.LogInfof("[parseVolUpdateReq] vol(%v) volStorageClass(%v) will be changed to: %v",
				vol.Name, proto.StorageClassString(vol.volStorageClass), proto.StorageClassString(req.volStorageClass))
		}
	}

	// Parse defaultPoolId (optional)
	if poolIdStr := r.FormValue(poolIdKey); poolIdStr != "" {
		var poolIdVal uint64
		if poolIdVal, err = strconv.ParseUint(poolIdStr, 10, 8); err != nil {
			return fmt.Errorf("invalid defaultPoolId: %v", err)
		}
		req.defaultPoolId = uint8(poolIdVal)
	}

	// Parse allowedPools (optional, comma-separated)
	if allowedPoolsStr := r.FormValue("allowedPools"); allowedPoolsStr != "" {
		allowedPoolsStrList := strings.Split(allowedPoolsStr, ",")
		encountered := map[uint8]bool{}
		for _, poolStr := range allowedPoolsStrList {
			var poolVal uint64
			if poolVal, err = strconv.ParseUint(strings.TrimSpace(poolStr), 10, 8); err != nil {
				return fmt.Errorf("invalid allowedPools: %v", err)
			}
			poolId := uint8(poolVal)
			if !encountered[poolId] {
				encountered[poolId] = true
				req.allowedPools = append(req.allowedPools, poolId)
			}
		}
	}

	if proto.IsStorageClassBlobStore(vol.volStorageClass) {
		req.followerRead = true
	}

	if proto.IsVolSupportStorageClass(vol.allowedStorageClass, proto.StorageClass_BlobStore) {
		req.coldArgs, err = parseColdVolUpdateArgs(r, vol)
		if err != nil {
			return
		}
	}

	return
}

func parseRequestToSetApiQpsLimit(r *http.Request) (name string, limit uint32, timeout uint32, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		return
	}

	if limit, err = extractUint32(r, Limit); err != nil {
		return
	}

	if timeout, err = extractUint32(r, TimeOut); err != nil {
		return
	}

	if timeout == 0 {
		err = fmt.Errorf("timeout(seconds) args must be larger than 0")
	}

	return
}

func parseRequestToSetVolCapacity(r *http.Request) (name, authKey string, capacity int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		return
	}

	if authKey, err = extractAuthKey(r); err != nil {
		return
	}

	capacity, err = extractUint(r, volCapacityKey)
	return
}

type qosArgs struct {
	qosEnable     bool
	diskQosEnable bool
	iopsRVal      uint64
	iopsWVal      uint64
	flowRVal      uint64
	flowWVal      uint64
}

func (qos *qosArgs) isArgsWork() bool {
	return (qos.iopsRVal | qos.iopsWVal | qos.flowRVal | qos.flowWVal) > 0
}

type coldVolArgs struct {
	objBlockSize            int
	accessTimeValidInterval int64
	trashInterval           int64
	enablePersistAccessTime bool
}

type createVolReq struct {
	name                    string
	owner                   string
	dpSize                  int
	mpCount                 int
	dpCount                 int
	dpReplicaNum            uint8
	capacity                int
	deleteLockTime          int64
	followerRead            bool
	metaFollowerRead        bool
	metaNearRead            bool
	maximallyRead           bool
	authenticate            bool
	crossZone               bool
	normalZonesFirst        bool
	domainId                uint64
	zoneName                string
	description             string
	volType                 int
	enablePosixAcl          bool
	DpReadOnlyWhenVolFull   bool
	enableTransaction       proto.TxOpMask
	enableQuota             bool
	txTimeout               int64
	txConflictRetryNum      int64
	txConflictRetryInterval int64
	qosLimitArgs            *qosArgs
	trashInterval           int64
	accessTimeValidInterval int64
	enablePersistAccessTime bool
	// cold vol args
	coldArgs coldVolArgs

	// hybrid cloud
	volStorageClass     uint32
	allowedStorageClass []uint32
	// remote cache
	remoteCacheEnable            bool
	remoteCacheAutoPrepare       bool
	remoteCachePath              string
	remoteCacheTTL               int64
	remoteCacheReadTimeout       int64
	remoteCacheMaxFileSizeGB     int64
	remoteCacheMaxFileSizeMB     int64
	minReadAheadSize             int64
	remoteCacheOnlyForNotSSD     bool
	remoteCacheMultiRead         bool
	flashNodeTimeoutCount        int64
	remoteCacheSameZoneTimeout   int64
	remoteCacheSameRegionTimeout int64
	remoteCacheDisableTTL        bool

	storeMode proto.StoreMode

	// Storage Pool
	defaultPoolId uint8
	allowedPools  []uint8

	// Meta Region
	defaultRegion  string
	allowedRegions []string
}

func parseColdArgs(r *http.Request) (args coldVolArgs, err error) {
	if args.objBlockSize, err = extractUint(r, ebsBlkSizeKey); err != nil {
		return
	}

	return
}

func parseRequestToCreateVol(r *http.Request, req *createVolReq, m *Server) (err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if req.name, err = extractName(r); err != nil {
		return
	}

	if req.owner, err = extractOwner(r); err != nil {
		return
	}

	if req.coldArgs, err = parseColdArgs(r); err != nil {
		return
	}

	if req.mpCount, err = extractUintWithDefault(r, metaPartitionCountKey, defaultInitMetaPartitionCount); err != nil {
		return
	}

	if req.dpCount, err = extractUintWithDefault(r, dataPartitionCountKey, defaultInitDataPartitionCnt); err != nil {
		return
	}

	var parsedDpReplicaNum int
	if parsedDpReplicaNum, err = extractUint(r, replicaNumKey); err != nil {
		return
	}
	if parsedDpReplicaNum < 0 || parsedDpReplicaNum > math.MaxUint8 {
		return fmt.Errorf("invalid arg dpReplicaNum: %v", parsedDpReplicaNum)
	}
	req.dpReplicaNum = uint8(parsedDpReplicaNum)

	if req.dpSize, err = extractUintWithDefault(r, dataPartitionSizeKey, 120); err != nil {
		return
	}

	// default capacity 120
	if req.capacity, err = extractUint(r, volCapacityKey); err != nil {
		return
	}

	if req.deleteLockTime, err = extractInt64WithDefault(r, volDeleteLockTimeKey, 0); err != nil {
		return
	}

	// Parse defaultPoolId (optional)

	if req.defaultPoolId, err = extractUint8WithDefault(r, poolIdKey, m.cluster.defaultPoolId); err != nil {
		return
	}

	req.allowedPools = append(req.allowedPools, req.defaultPoolId)

	if _, err = m.cluster.getStoragePool(req.defaultPoolId); err != nil {
		return fmt.Errorf("defaultPoolId[%d] not found", req.defaultPoolId)
	}

	// Parse allowedPools (optional, comma-separated)
	if allowedPoolsStr := r.FormValue(allowedPoolsKey); allowedPoolsStr != "" {
		allowedPoolsStrList := strings.Split(allowedPoolsStr, ",")
		encountered := map[uint8]bool{}
		encountered[req.defaultPoolId] = true

		for _, poolStr := range allowedPoolsStrList {

			poolIdUint64, err := strconv.ParseUint(poolStr, 10, 8)
			if err != nil {
				return fmt.Errorf("invalid allowedPools: %v, poolStr: %s, allowedPoolsStr: %s", err, poolStr, allowedPoolsStr)
			}
			poolId := uint8(poolIdUint64)
			if _, err = m.cluster.getStoragePool(poolId); err != nil {
				return fmt.Errorf("allowedPools[%d] not found, poolStr: %s, allowedPoolsStr: %s", poolId, poolStr, allowedPoolsStr)
			}
			if !encountered[poolId] {
				encountered[poolId] = true
				req.allowedPools = append(req.allowedPools, poolId)
			}
		}
	}

	pool, _ := m.cluster.getStoragePool(req.defaultPoolId)
	req.volStorageClass = uint32(pool.StorageClass)

	req.volType = proto.VolumeTypeHot
	if proto.IsStorageClassBlobStore(req.volStorageClass) {
		req.volType = proto.VolumeTypeCold
	}

	// Use a map to deduplicate StorageClass
	storageClassMap := make(map[uint32]bool)
	for _, poolId := range req.allowedPools {
		pool, _ := m.cluster.getStoragePool(poolId)
		storageClass := uint32(pool.StorageClass)
		if !storageClassMap[storageClass] {
			storageClassMap[storageClass] = true
			req.allowedStorageClass = append(req.allowedStorageClass, storageClass)
		}
	}

	followerRead, followerExist, err := extractFollowerRead(r)
	if err != nil {
		return
	}
	if followerExist && !followerRead && proto.IsHot(req.volType) &&
		(req.dpReplicaNum == 1 || req.dpReplicaNum == 2) {
		return fmt.Errorf("vol with 1 or 2 replica should enable followerRead")
	}
	req.followerRead = followerRead

	if !proto.IsStorageClassBlobStore(req.volStorageClass) && (req.dpReplicaNum == 1 || req.dpReplicaNum == 2) {
		req.followerRead = true
	}

	req.metaFollowerRead, err = extractBoolWithDefault(r, proto.MetaFollowerReadKey, false)
	if err != nil {
		return
	}

	req.metaNearRead, err = extractBoolWithDefault(r, proto.MetaNearReadKey, false)
	if err != nil {
		return
	}

	req.maximallyRead, err = extractBoolWithDefault(r, proto.MaximallyReadKey, false)
	if err != nil {
		return
	}

	if req.authenticate, err = extractBoolWithDefault(r, authenticateKey, false); err != nil {
		return
	}

	if req.crossZone, err = extractBoolWithDefault(r, crossZoneKey, false); err != nil {
		return
	}

	if req.normalZonesFirst, err = extractBoolWithDefault(r, normalZonesFirstKey, false); err != nil {
		return
	}

	if req.qosLimitArgs, err = parseRequestQos(r, false, false); err != nil {
		return err
	}

	req.zoneName = extractStr(r, zoneNameKey)
	req.description = extractStr(r, descriptionKey)

	// Parse default region (use cluster default if not specified)
	req.defaultRegion = extractStrWithDefault(r, defaultRegionKey, m.cluster.defaultMetaRegion)
	req.allowedRegions = []string{req.defaultRegion}
	if !m.cluster.isValidRegion(req.defaultRegion) {
		return fmt.Errorf("defaultRegion %q does not exist in cluster (no zone uses this meta region)", req.defaultRegion)
	}

	req.domainId, err = extractUint64WithDefault(r, domainIdKey, 0)
	if err != nil {
		return
	}

	req.enablePosixAcl, _ = extractPosixAcl(r)

	if req.DpReadOnlyWhenVolFull, err = extractBoolWithDefault(r, dpReadOnlyWhenVolFull, false); err != nil {
		return
	}

	var txMask proto.TxOpMask
	if txMask, err = parseTxMask(r, proto.TxOpMaskOff); err != nil {
		return
	}
	req.enableTransaction = txMask

	var txTimeout int64
	if txTimeout, err = extractTxTimeout(r, proto.DefaultTransactionTimeout); err != nil {
		return
	}
	req.txTimeout = txTimeout

	var txConflictRetryNum int64
	if txConflictRetryNum, err = extractTxConflictRetryNum(r, proto.DefaultTxConflictRetryNum); err != nil {
		return
	}
	req.txConflictRetryNum = txConflictRetryNum

	var txConflictRetryInterval int64
	if txConflictRetryInterval, err = extractTxConflictRetryInterval(r, proto.DefaultTxConflictRetryInterval); err != nil {
		return
	}
	req.txConflictRetryInterval = txConflictRetryInterval

	if req.enableQuota, err = extractBoolWithDefault(r, enableQuota, false); err != nil {
		return
	}

	if req.trashInterval, err = extractInt64WithDefault(r, trashIntervalKey, 0); err != nil {
		return
	}

	if req.trashInterval > maxTrashInterval {
		err = fmt.Errorf("trash interval can't be greater than %d, now %d", maxTrashInterval, req.trashInterval)
		return
	}

	if req.accessTimeValidInterval, err = extractInt64WithDefault(r, accessTimeIntervalKey, proto.DefaultAccessTimeValidInterval); err != nil {
		return
	}
	if req.enablePersistAccessTime, err = extractBoolWithDefault(r, enablePersistAccessTimeKey, false); err != nil {
		return
	}

	if req.remoteCacheEnable, err = extractBoolWithDefault(r, remoteCacheEnable, false); err != nil {
		return
	}
	if req.remoteCacheAutoPrepare, err = extractBoolWithDefault(r, remoteCacheAutoPrepare, false); err != nil {
		return
	}
	req.remoteCachePath = extractStrWithDefault(r, remoteCachePath, "")
	if req.remoteCacheTTL, err = extractInt64WithDefault(r, remoteCacheTTL, proto.DefaultRemoteCacheTTL); err != nil {
		return
	}
	if req.remoteCacheReadTimeout, err = extractInt64WithDefault(r, remoteCacheReadTimeout, proto.DefaultRemoteCacheClientReadTimeout); err != nil {
		return
	}

	if req.remoteCacheMaxFileSizeGB, err = extractInt64WithDefault(r, remoteCacheMaxFileSizeGB, proto.DefaultRemoteCacheMaxFileSizeGB); err != nil {
		return
	}
	if req.remoteCacheMaxFileSizeMB, err = extractInt64WithDefault(r, remoteCacheMaxFileSizeMB, proto.DefaultRemoteCacheMaxFileSizeMB); err != nil {
		return
	}
	if req.minReadAheadSize, err = extractInt64WithDefault(r, minReadAheadSize, proto.DefaultMinReadAheadSize); err != nil {
		return
	}
	req.remoteCacheOnlyForNotSSD = true
	if req.remoteCacheMultiRead, err = extractBoolWithDefault(r, remoteCacheMultiRead, false); err != nil {
		return
	}
	if req.flashNodeTimeoutCount, err = extractInt64WithDefault(r, flashNodeTimeoutCount, proto.DefaultFlashNodeTimeoutCount); err != nil {
		return
	}
	if req.remoteCacheSameZoneTimeout, err = extractInt64WithDefault(r, remoteCacheSameZoneTimeout, proto.DefaultRemoteCacheSameZoneTimeout); err != nil {
		return
	}
	if req.remoteCacheSameRegionTimeout, err = extractInt64WithDefault(r, remoteCacheSameRegionTimeout, proto.DefaultRemoteCacheSameRegionTimeout); err != nil {
		return
	}

	req.storeMode = m.config.DefaultVolStoreMode
	if storeModeStr := r.FormValue(StoreModeKey); storeModeStr != "" {
		var storeMode int
		storeMode, err = strconv.Atoi(storeModeStr)
		if err != nil {
			err = unmatchedKey(StoreModeKey)
			return
		}
		if storeMode != int(proto.StoreModeMem) && storeMode != int(proto.StoreModeRocksDb) {
			err = unmatchedKey(StoreModeKey)
			return
		}
		req.storeMode = proto.StoreMode(storeMode)
	}

	if req.remoteCacheDisableTTL, err = extractBoolWithDefault(r, remoteCacheDisableTTL, false); err != nil {
		return
	}
	return
}

func parseRequestToCreateDataPartition(r *http.Request) (count int, volName string, poolId uint8, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if countStr := r.FormValue(countKey); countStr == "" {
		err = keyNotFound(countKey)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = unmatchedKey(countKey)
		return
	}
	if volName, err = extractName(r); err != nil {
		return
	}

	if poolId, err = extractUint8WithDefault(r, poolIdKey, 0); err != nil {
		return
	}

	return
}

func parseRequestToGetDataPartition(r *http.Request) (ID uint64, volName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	volName = r.FormValue(nameKey)
	return
}

func parseRequestToBalanceMetaPartition(r *http.Request) (zones string, nodeSetIds string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	zones = r.FormValue(zoneNameKey)
	nodeSetIds = r.FormValue(nodesetIdKey)

	return
}

func parseRequestToLoadDataPartition(r *http.Request) (ID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	return
}

func parseRequestToAddMetaReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func parseRequestToRemoveMetaReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func parseRequestToAddMetaPartitionLearner(r *http.Request) (ID uint64, addr string, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func parseRequestToPromoteMetaReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func extractMetaPartitionIDAndAddr(r *http.Request) (ID uint64, addr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func parseRequestToAddDataReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func parseRequestToRemoveDataReplica(r *http.Request) (ID uint64, addr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func extractDataPartitionIDAndAddr(r *http.Request) (ID uint64, addr string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ID, err = extractDataPartitionID(r); err != nil {
		return
	}
	if addr, err = extractNodeAddr(r); err != nil {
		return
	}
	return
}

func extractDataPartitionID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseRequestToDecommissionDataPartition(r *http.Request) (ID uint64, nodeAddr string, err error) {
	return extractDataPartitionIDAndAddr(r)
}

func extractNodeAddr(r *http.Request) (nodeAddr string, err error) {
	if nodeAddr = r.FormValue(addrKey); nodeAddr == "" {
		err = keyNotFound(addrKey)
		return
	}
	if ipAddr, ok := util.ParseAddrToIpAddr(nodeAddr); ok {
		nodeAddr = ipAddr
	}
	return
}

func extractNodeID(r *http.Request) (ID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func extractNodesetID(r *http.Request) (ID uint64, err error) {
	// nodeset id use same form key with node id
	return extractNodeID(r)
}

func extractDiskPath(r *http.Request) (diskPath string, err error) {
	if diskPath = r.FormValue(diskPathKey); diskPath == "" {
		err = keyNotFound(diskPathKey)
		return
	}
	return
}

func extractDiskDisable(r *http.Request) (diskDisable bool, err error) {
	var value string
	if value = r.FormValue(DiskDisableKey); value == "" {
		diskDisable = true
		return
	}
	return strconv.ParseBool(value)
}

func parseRequestToLoadMetaPartition(r *http.Request) (partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}

func parseRequestToDecommissionMetaPartition(r *http.Request) (partitionID uint64, nodeAddr string, err error) {
	return extractMetaPartitionIDAndAddr(r)
}

func parseAndExtractStatus(r *http.Request) (status bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractStatus(r)
}

func parseAndExtractForbidden(r *http.Request) (forbidden bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractForbidden(r)
}

func parseAndExtractDpRepairBlockSize(r *http.Request) (size uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractDpRepairBlockSize(r)
}

func extractStatus(r *http.Request) (status bool, err error) {
	var value string
	if value = r.FormValue(enableKey); value == "" {
		err = keyNotFound(enableKey)
		return
	}
	if status, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractForbidden(r *http.Request) (forbidden bool, err error) {
	var value string
	if value = r.FormValue(forbiddenKey); value == "" {
		err = keyNotFound(forbiddenKey)
		return
	}
	if forbidden, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractDpRepairBlockSize(r *http.Request) (size uint64, err error) {
	var value string
	if value = r.FormValue(dpRepairBlockSizeKey); value == "" {
		err = keyNotFound(dpRepairBlockSizeKey)
		return
	}
	if size, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}
	return
}

func extractDataNodesetSelector(r *http.Request) string {
	return r.FormValue(dataNodesetSelectorKey)
}

func extractMetaNodesetSelector(r *http.Request) string {
	return r.FormValue(metaNodesetSelectorKey)
}

func extractDataNodeSelector(r *http.Request) string {
	return r.FormValue(dataNodeSelectorKey)
}

func extractMetaNodeSelector(r *http.Request) string {
	return r.FormValue(metaNodeSelectorKey)
}

func extractFollowerRead(r *http.Request) (followerRead bool, exist bool, err error) {
	var value string
	if value = r.FormValue(followerReadKey); value == "" {
		followerRead = false
		return
	}
	exist = true
	if followerRead, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func parseAndExtractDirLimit(r *http.Request) (limit uint32, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string

	value = r.FormValue(dirLimitKey)
	if value == "" {
		value = r.FormValue(dirQuotaKey)
		if value == "" {
			err = keyNotFound(dirLimitKey)
			return
		}
	}

	var tmpLimit uint64
	if tmpLimit, err = strconv.ParseUint(value, 10, 32); err != nil {
		return
	}

	limit = uint32(tmpLimit)
	return
}

func parseAndExtractThreshold(r *http.Request) (threshold float64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(thresholdKey); value == "" {
		err = keyNotFound(thresholdKey)
		return
	}
	if threshold, err = strconv.ParseFloat(value, 64); err != nil {
		return
	}
	return
}

func parseAndExtractVolDeletionDelayTime(r *http.Request) (volDeletionDelayTimeHour int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(volDeletionDelayTimeKey); value == "" {
		err = keyNotFound(volDeletionDelayTimeKey)
		return
	}
	if volDeletionDelayTimeHour, err = strconv.Atoi(value); err != nil {
		return
	}
	return
}

func parseAndExtractFlashTopoDeletionDelayTime(r *http.Request) (flashTopoDeletionDelayTimeHour int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(flashTopoDeletionDelayTimeKey); value == "" {
		err = keyNotFound(flashTopoDeletionDelayTimeKey)
		return
	}
	if flashTopoDeletionDelayTimeHour, err = strconv.Atoi(value); err != nil {
		return
	}
	return
}

func parseAndExtractMetaNodeGOGC(r *http.Request) (metaNodeGOGC int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(metaNodeGOGCKey); value == "" {
		err = keyNotFound(metaNodeGOGCKey)
		return
	}
	if metaNodeGOGC, err = strconv.Atoi(value); err != nil {
		return
	}
	return
}

func parseAndExtractDataNodeGOGC(r *http.Request) (dataNodeGOGC int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(dataNodeGOGCKey); value == "" {
		err = keyNotFound(dataNodeGOGCKey)
		return
	}
	if dataNodeGOGC, err = strconv.Atoi(value); err != nil {
		return
	}
	return
}

func parseAndExtractFileStatsThresholds(r *http.Request) (thresholds []uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue(thresholdKey); value == "" {
		err = keyNotFound(thresholdKey)
		return
	}
	thresholdsStr := strings.Split(value, ",")
	for _, t := range thresholdsStr {
		threshold, err := strconv.ParseUint(t, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid threshold value: %s", t)
		}
		thresholds = append(thresholds, threshold)
	}
	if len(thresholds) == 0 {
		err = fmt.Errorf("at least one threshold needs to be configured")
		return
	}
	return
}

func parseAndExtractSetNodeSetInfoParams(r *http.Request) (params map[string]interface{}, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	params = make(map[string]interface{})
	if value = r.FormValue(countKey); value != "" {
		count := uint64(0)
		count, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(countKey)
			return
		}
		params[countKey] = count
	} else {
		return nil, fmt.Errorf("not found %v", countKey)
	}
	var zoneName string
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	params[zoneNameKey] = zoneName

	if value = r.FormValue(idKey); value != "" {
		nodesetId := uint64(0)
		nodesetId, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(idKey)
			return
		}
		params[idKey] = nodesetId
	} else {
		return nil, fmt.Errorf("not found %v", idKey)
	}

	log.LogInfof("action[parseAndExtractSetNodeSetInfoParams]%v,%v,%v", params[zoneNameKey], params[idKey], params[countKey])

	return
}

func parseAndExtractSetNodeInfoParams(r *http.Request) (params map[string]interface{}, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	noParams := true
	params = make(map[string]interface{})
	if value = r.FormValue(nodeDeleteBatchCountKey); value != "" {
		noParams = false
		batchCount := uint64(0)
		batchCount, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeDeleteBatchCountKey)
			return
		}
		params[nodeDeleteBatchCountKey] = batchCount
	}

	if value = r.FormValue(followerReadLeaseTimeKey); value != "" {
		noParams = false
		followerReadLeaseTime := uint64(0)
		followerReadLeaseTime, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(followerReadLeaseTimeKey)
			return
		}
		if err = proto.ValidateFollowerReadLeaseTime(followerReadLeaseTime); err != nil {
			return
		}
		params[followerReadLeaseTimeKey] = followerReadLeaseTime
	}

	if value = r.FormValue(nodeMarkDeleteRateKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeMarkDeleteRateKey)
			return
		}
		params[nodeMarkDeleteRateKey] = val
	}

	if value = r.FormValue(nodeAutoRepairRateKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeAutoRepairRateKey)
			return
		}
		params[nodeAutoRepairRateKey] = val
	}

	if value = r.FormValue(nodeDeleteWorkerSleepMs); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeDeleteWorkerSleepMs)
			return
		}
		params[nodeDeleteWorkerSleepMs] = val
	}

	if value = r.FormValue(clusterLoadFactorKey); value != "" {
		noParams = false
		valF, err := strconv.ParseFloat(value, 64)
		if err != nil || valF < 0 {
			err = unmatchedKey(clusterLoadFactorKey)
			return params, err
		}

		params[clusterLoadFactorKey] = float32(valF)
	}

	if value = r.FormValue(maxDpCntLimitKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(maxDpCntLimitKey)
			return
		}
		params[maxDpCntLimitKey] = val
	}

	if value = r.FormValue(maxMpCntLimitKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(maxMpCntLimitKey)
			return
		}
		params[maxMpCntLimitKey] = val
	}

	if value = r.FormValue(maxDpTagDecommissionLimitKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(maxDpTagDecommissionLimitKey)
			return
		}
		params[maxDpTagDecommissionLimitKey] = val
	}

	if value = r.FormValue(maxMpTagDecommissionLimitKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(maxMpTagDecommissionLimitKey)
			return
		}
		params[maxMpTagDecommissionLimitKey] = val
	}

	if value = r.FormValue(nodeDpRepairTimeOutKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeDpRepairTimeOutKey)
			return
		}
		params[nodeDpRepairTimeOutKey] = val
	}
	if value = r.FormValue(nodeDpBackupKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeDpBackupKey)
			return
		}
		params[nodeDpBackupKey] = val
	}
	if value = r.FormValue(nodeDpMaxRepairErrCntKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeDpMaxRepairErrCntKey)
			return
		}
		params[nodeDpMaxRepairErrCntKey] = val
	}

	if value = r.FormValue(dpLimitSsdBaseCountKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(dpLimitSsdBaseCountKey)
			return
		}
		params[dpLimitSsdBaseCountKey] = val
	}
	if value = r.FormValue(dpLimitSsdFactorKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(dpLimitSsdFactorKey)
			return
		}
		params[dpLimitSsdFactorKey] = val
	}
	if value = r.FormValue(dpLimitHddBaseCountKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(dpLimitHddBaseCountKey)
			return
		}
		params[dpLimitHddBaseCountKey] = val
	}
	if value = r.FormValue(dpLimitHddFactorKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(dpLimitHddFactorKey)
			return
		}
		params[dpLimitHddFactorKey] = val
	}

	if value = r.FormValue(clusterCreateTimeKey); value != "" {
		noParams = false
		params[clusterCreateTimeKey] = value
	}

	if value = extractDataNodesetSelector(r); value != "" {
		noParams = false
		params[dataNodesetSelectorKey] = value
	}

	if value = extractMetaNodesetSelector(r); value != "" {
		noParams = false
		params[metaNodesetSelectorKey] = value
	}

	if value = extractDataNodeSelector(r); value != "" {
		noParams = false
		params[dataNodeSelectorKey] = value
	}

	if value = extractMetaNodeSelector(r); value != "" {
		noParams = false
		params[metaNodeSelectorKey] = value
	}

	if value = r.FormValue(markDiskBrokenThresholdKey); value != "" {
		noParams = false
		val := float64(0)
		val, err = strconv.ParseFloat(value, 64)
		if err != nil {
			err = unmatchedKey(markDiskBrokenThresholdKey)
			return
		}
		params[markDiskBrokenThresholdKey] = val
	}

	if value = r.FormValue(flashNodeHandleReadTimeout); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(flashNodeHandleReadTimeout)
			return
		}
		params[flashNodeHandleReadTimeout] = val
	}

	if value = r.FormValue(flashHotKeyMissCount); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(flashHotKeyMissCount)
			return
		}
		params[flashHotKeyMissCount] = val
	}

	if value = r.FormValue(preheatTotalTask); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(preheatTotalTask)
			return
		}
		params[preheatTotalTask] = val
	}

	if value = r.FormValue(maxDisableFlashGroupPercent); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(maxDisableFlashGroupPercent)
			return
		}
		params[maxDisableFlashGroupPercent] = val
	}

	if value = r.FormValue(flashNodeReadDataNodeTimeout); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(flashNodeReadDataNodeTimeout)
			return
		}
		params[flashNodeReadDataNodeTimeout] = val
	}

	if value = r.FormValue(autoDecommissionDiskKey); value != "" {
		noParams = false
		val := false
		val, err = strconv.ParseBool(value)
		if err != nil {
			err = unmatchedKey(autoDecommissionDiskKey)
			return
		}
		params[autoDecommissionDiskKey] = val
	}

	if value = r.FormValue(autoDecommissionDiskIntervalKey); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(autoDecommissionDiskIntervalKey)
			return
		}
		params[autoDecommissionDiskIntervalKey] = time.Duration(val)
	}

	if value = r.FormValue(autoDpMetaRepairKey); value != "" {
		noParams = false
		val := false
		val, err = strconv.ParseBool(value)
		if err != nil {
			err = unmatchedKey(autoDpMetaRepairKey)
			return
		}
		params[autoDpMetaRepairKey] = val
	}

	if value = r.FormValue(autoDpMetaRepairParallelCntKey); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(autoDpMetaRepairParallelCntKey)
			return
		}
		params[autoDpMetaRepairParallelCntKey] = int(val)
	}

	if value = r.FormValue(autoMpMetaRepairKey); value != "" {
		noParams = false
		val := false
		val, err = strconv.ParseBool(value)
		if err != nil {
			err = unmatchedKey(autoMpMetaRepairKey)
			return
		}
		params[autoMpMetaRepairKey] = val
	}

	if value = r.FormValue(autoMpMetaRepairParallelCntKey); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(autoMpMetaRepairParallelCntKey)
			return
		}
		params[autoMpMetaRepairParallelCntKey] = int(val)
	}

	if value = r.FormValue(autoDistributionOptimizationKey); value != "" {
		noParams = false
		val := false
		val, err = strconv.ParseBool(value)
		if err != nil {
			err = unmatchedKey(autoDistributionOptimizationKey)
			return
		}
		params[autoDistributionOptimizationKey] = val
	}

	if value = r.FormValue(enableMpDecommissionByLearnerKey); value != "" {
		noParams = false
		val := false
		val, err = strconv.ParseBool(value)
		if err != nil {
			err = unmatchedKey(enableMpDecommissionByLearnerKey)
			return
		}
		params[enableMpDecommissionByLearnerKey] = val
	}

	// distribution optimization configs
	if value = r.FormValue(distributionOptimizationConDpCntKey); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(distributionOptimizationConDpCntKey)
			return
		}
		params[distributionOptimizationConDpCntKey] = val
	}

	if value = r.FormValue(distributionOptimizationThresholdKey); value != "" {
		noParams = false
		val := float64(0)
		val, err = strconv.ParseFloat(value, 64)
		if err != nil {
			err = unmatchedKey(distributionOptimizationThresholdKey)
			return
		}
		params[distributionOptimizationThresholdKey] = val
	}

	if value = r.FormValue(dpTimeoutKey); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(dpTimeoutKey)
			return
		}
		params[dpTimeoutKey] = val
	}

	if value = r.FormValue(mpTimeoutKey); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(mpTimeoutKey)
			return
		}
		params[mpTimeoutKey] = val
	}

	if value = r.FormValue(decommissionLimit); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(decommissionLimit)
			return
		}
		params[decommissionLimit] = val
	}

	if value = r.FormValue(decommissionDiskLimit); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(decommissionDiskLimit)
			return
		}
		params[decommissionDiskLimit] = val
	}

	if value = r.FormValue(decommissionFirstHostDiskParallelLimit); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(decommissionFirstHostDiskParallelLimit)
			return
		}
		params[decommissionFirstHostDiskParallelLimit] = val
	}

	if value = r.FormValue(dataMediaTypeKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(dataMediaTypeKey)
			return
		}
		params[dataMediaTypeKey] = val
	}

	if value = r.FormValue(forbidWriteOpOfProtoVersion0); value != "" {
		noParams = false
		val := false
		val, err = strconv.ParseBool(value)
		if err != nil {
			err = unmatchedKey(forbidWriteOpOfProtoVersion0)
			return
		}
		params[forbidWriteOpOfProtoVersion0] = val
	}

	if value = r.FormValue(rackAwareLevelKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(rackAwareLevelKey)
			return
		}
		if !proto.RackAwareLevel(val).IsValid() {
			err = fmt.Errorf("rack aware level must be 0, 1 or 2")
			return
		}
		params[rackAwareLevelKey] = uint8(val)
	}

	if value = r.FormValue(learnerRecoverTimeoutSecondsKey); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(learnerRecoverTimeoutSecondsKey)
			return
		}
		if val <= 0 {
			err = fmt.Errorf("learnerRecoverTimeoutSeconds must be greater than 0")
			return
		}
		params[learnerRecoverTimeoutSecondsKey] = val
	}

	if value = r.FormValue(metaAutoAddReplicaLimitKey); value != "" {
		noParams = false
		val := uint32(0)
		tmp, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			err = unmatchedKey(metaAutoAddReplicaLimitKey)
			return params, err
		}
		val = uint32(tmp)
		params[metaAutoAddReplicaLimitKey] = val
	}

	if value = r.FormValue(metaManualDecommissionLimitKey); value != "" {
		noParams = false
		val := uint32(0)
		tmp, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			err = unmatchedKey(metaManualDecommissionLimitKey)
			return params, err
		}
		val = uint32(tmp)
		params[metaManualDecommissionLimitKey] = val
	}

	if value = r.FormValue(metaBalanceLimitKey); value != "" {
		noParams = false
		val := uint32(0)
		tmp, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			err = unmatchedKey(metaBalanceLimitKey)
			return params, err
		}
		val = uint32(tmp)
		params[metaBalanceLimitKey] = val
	}

	if value = r.FormValue(metaManualAddReplicaLimitKey); value != "" {
		noParams = false
		val := uint32(0)
		tmp, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			err = unmatchedKey(metaManualAddReplicaLimitKey)
			return params, err
		}
		val = uint32(tmp)
		params[metaManualAddReplicaLimitKey] = val
	}

	if value = r.FormValue(metaManualLearnerLimitKey); value != "" {
		noParams = false
		val := uint32(0)
		tmp, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			err = unmatchedKey(metaManualLearnerLimitKey)
			return params, err
		}
		val = uint32(tmp)
		params[metaManualLearnerLimitKey] = val
	}

	if value = r.FormValue(flashReadFlowLimit); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(flashReadFlowLimit)
			return
		}
		params[flashReadFlowLimit] = val
	}

	if value = r.FormValue(flashWriteFlowLimit); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(flashWriteFlowLimit)
			return
		}
		params[flashWriteFlowLimit] = val
	}

	if value = r.FormValue(flashKeyFlowLimit); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(flashKeyFlowLimit)
			return
		}
		params[flashKeyFlowLimit] = val
	}

	if value = r.FormValue(remoteClientFlowLimit); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			err = unmatchedKey(remoteClientFlowLimit)
			return
		}
		params[remoteClientFlowLimit] = val
	}

	if value = r.FormValue(cfgAutoFixTag); value != "" {
		noParams = false
		_, err = strconv.ParseBool(value)
		if err != nil {
			err = unmatchedKey(cfgAutoFixTag)
			return
		}
		params[cfgAutoFixTag] = value
	}

	if value = r.FormValue(cfgDefaultDpTag); value != "" {
		noParams = false
		params[cfgDefaultDpTag] = value
	}

	if value = r.FormValue(cfgDefaultMpTag); value != "" {
		noParams = false
		params[cfgDefaultMpTag] = value
	}

	if value = r.FormValue(poolIdKey); value != "" {
		noParams = false
		val := uint64(0)
		val, err = strconv.ParseUint(value, 10, 8)
		if err != nil {
			err = unmatchedKey(poolIdKey)
			return
		}
		params[poolIdKey] = uint8(val)
	}

	if value = r.FormValue(defaultMetaRegionKey); value != "" {
		noParams = false
		params[defaultMetaRegionKey] = value
	}

	if noParams {
		err = fmt.Errorf("no key assigned")
		return
	}
	return
}

func validateRequestToCreateMetaPartition(r *http.Request) (volName string, count int, region string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if countStr := r.FormValue(countKey); countStr == "" {
		err = keyNotFound(countKey)
		return
	} else if count, err = strconv.Atoi(countStr); err != nil || count == 0 {
		err = unmatchedKey(countKey)
		return
	}
	if count > maxMpCreationCount {
		err = fmt.Errorf("count[%d] exceeds maximum limit[%d]", count, maxMpCreationCount)
		return
	}
	if volName, err = extractName(r); err != nil {
		return
	}
	// Get optional region parameter, if not specified, will use volume's default region
	region = r.FormValue(regionKey)
	return
}

func parseRequestToUpdateMetaPartitionRegion(r *http.Request) (partitionID uint64, region string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	region = strings.TrimSpace(r.FormValue(regionKey))
	if region == "" {
		err = keyNotFound(regionKey)
		return
	}
	return
}

func parseAndExtractPartitionInfo(r *http.Request) (partitionID uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if partitionID, err = extractMetaPartitionID(r); err != nil {
		return
	}
	return
}

func extractMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(idKey); value == "" {
		err = keyNotFound(idKey)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func extractAuthKey(r *http.Request) (authKey string, err error) {
	if authKey = r.FormValue(volAuthKey); authKey == "" {
		err = keyNotFound(volAuthKey)
		return
	}
	return
}

func extractClientIDKey(r *http.Request) (clientIDKey string, err error) {
	if clientIDKey = r.FormValue(ClientIDKey); clientIDKey == "" {
		err = keyNotFound(ClientIDKey)
		return
	}
	return
}

func parseVolStatReq(r *http.Request) (name string, ver int, byMeta bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	name, err = extractName(r)
	if err != nil {
		return
	}

	ver, err = extractUint(r, clientVersion)
	if err != nil {
		return
	}
	byMeta, err = extractBoolWithDefault(r, CountByMeta, false)
	if err != nil {
		return
	}
	return
}

func parseQosInfo(r *http.Request) (info *proto.ClientReportLimitInfo, err error) {
	info = proto.NewClientReportLimitInfo()
	var body []byte
	if body, err = io.ReadAll(r.Body); err != nil {
		return
	}
	// log.LogInfof("action[parseQosInfo] body len:[%v],crc:[%v]", len(body), crc32.ChecksumIEEE(body))
	err = json.Unmarshal(body, info)
	return
}

func parseAndExtractName(r *http.Request) (name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractName(r)
}

func parseAndExtractDecommissionType(r *http.Request) (decommissionType int, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	return extractDecommissionType(r)
}

func extractName(r *http.Request) (name string, err error) {
	if name = r.FormValue(nameKey); name == "" {
		err = keyNotFound(nameKey)
		return
	}
	if !volNameRegexp.MatchString(name) {
		return "", proto.ErrVolNameRegExpNotMatch
	}

	return
}

func extractDecommissionType(r *http.Request) (decommissionType int, err error) {
	var val string
	if val = r.FormValue(decommissionTypeKey); val == "" {
		err = keyNotFound(decommissionTypeKey)
		return
	}
	var v int64
	if v, err = strconv.ParseInt(val, 10, 32); err != nil {
		return
	}
	decommissionType = int(v)
	return
}

func extractOwner(r *http.Request) (owner string, err error) {
	if owner = r.FormValue(volOwnerKey); owner == "" {
		err = keyNotFound(volOwnerKey)
		return
	}
	if !ownerRegexp.MatchString(owner) {
		return "", proto.ErrInvalidUserID
	}

	return
}

func parseAndCheckTicket(r *http.Request, key []byte, volName string) (jobj proto.APIAccessReq, ticket cryptoutil.Ticket, ts int64, err error) {
	var plaintext []byte

	if err = r.ParseForm(); err != nil {
		return
	}

	if plaintext, err = extractClientReqInfo(r); err != nil {
		return
	}

	if err = json.Unmarshal([]byte(plaintext), &jobj); err != nil {
		return
	}

	if err = proto.VerifyAPIAccessReqIDs(&jobj); err != nil {
		return
	}

	ticket, ts, err = extractTicketMess(&jobj, key, volName)

	return
}

func extractClientReqInfo(r *http.Request) (plaintext []byte, err error) {
	var message string
	if err = r.ParseForm(); err != nil {
		return
	}

	if message = r.FormValue(proto.ClientMessage); message == "" {
		err = keyNotFound(proto.ClientMessage)
		return
	}

	if plaintext, err = cryptoutil.Base64Decode(message); err != nil {
		return
	}

	return
}

func extractTicketMess(req *proto.APIAccessReq, key []byte, volName string) (ticket cryptoutil.Ticket, ts int64, err error) {
	if ticket, err = proto.ExtractTicket(req.Ticket, key); err != nil {
		err = fmt.Errorf("extractTicket failed: %s", err.Error())
		return
	}
	if time.Now().Unix() >= ticket.Exp {
		err = proto.ErrExpiredTicket
		return
	}
	if ts, err = proto.ParseVerifier(req.Verifier, ticket.SessionKey.Key); err != nil {
		err = fmt.Errorf("parseVerifier failed: %s", err.Error())
		return
	}
	if err = proto.CheckAPIAccessCaps(&ticket, proto.APIRsc, req.Type, proto.APIAccess); err != nil {
		err = fmt.Errorf("CheckAPIAccessCaps failed: %s", err.Error())
		return
	}
	if err = proto.CheckVOLAccessCaps(&ticket, volName, proto.VOLAccess, proto.MasterNode); err != nil {
		err = fmt.Errorf("CheckVOLAccessCaps failed: %s", err.Error())
		return
	}
	return
}

func checkTicket(encodedTicket string, key []byte, Type proto.MsgType) (ticket cryptoutil.Ticket, err error) {
	if ticket, err = proto.ExtractTicket(encodedTicket, key); err != nil {
		err = fmt.Errorf("extractTicket failed: %s", err.Error())
		return
	}
	if time.Now().Unix() >= ticket.Exp {
		err = proto.ErrExpiredTicket
		return
	}
	if err = proto.CheckAPIAccessCaps(&ticket, proto.APIRsc, Type, proto.APIAccess); err != nil {
		err = fmt.Errorf("CheckAPIAccessCaps failed: %s", err.Error())
		return
	}
	return
}

func newSuccessHTTPReply(data interface{}) *proto.HTTPReply {
	return &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: proto.ErrSuc.Error(), Data: data}
}

func newErrHTTPReply(err error) *proto.HTTPReply {
	if err == nil {
		return newSuccessHTTPReply("")
	}

	if errors.Is(err, proto.ErrFollowerReadLeaseTimeRange) {
		return &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()}
	}

	if errors.Is(err, errAutoMpMetaRepairNeedsLearnerDecommission) {
		return &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: errMsgAutoMpMetaRepairNeedsLearnerDecommission}
	}

	code, ok := proto.Err2CodeMap[err]
	if ok {
		return &proto.HTTPReply{Code: code, Msg: err.Error()}
	}
	return &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()}
}

func sendOkReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) (err error) {
	switch httpReply.Data.(type) {
	case *DataPartition:
		dp := httpReply.Data.(*DataPartition)
		dp.RLock()
		defer dp.RUnlock()
	case *MetaPartition:
		mp := httpReply.Data.(*MetaPartition)
		mp.RLock()
		defer mp.RUnlock()
	case *MetaNode:
		mn := httpReply.Data.(*MetaNode)
		mn.RLock()
		defer mn.RUnlock()
	case *DataNode:
		dn := httpReply.Data.(*DataNode)
		dn.RLock()
		defer dn.RUnlock()
	default:
		// do nothing
	}

	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply. URL[%v],remoteAddr[%v] err:[%v]", r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}

	if acceptEncoding := r.Header.Get(proto.HeaderAcceptEncoding); acceptEncoding != "" {
		if compressed, errx := compressor.New(acceptEncoding).Compress(reply); errx == nil {
			w.Header().Set(proto.HeaderContentEncoding, acceptEncoding)
			reply = compressed
		}
	}

	send(w, r, reply)
	return
}

func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
		return
	}
}

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response", r.URL, r.RemoteAddr)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply. URL[%v],remoteAddr[%v] err:[%v]", r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
	}
}

func parseRequestToUpdateDecommissionFirstHostParallelLimit(r *http.Request) (addr string, limit uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if addr = r.FormValue(addrKey); addr == "" {
		err = keyNotFound(addrKey)
		return
	}
	var value string
	if value = r.FormValue(decommissionFirstHostParallelLimit); value == "" {
		err = keyNotFound(decommissionFirstHostParallelLimit)
		return
	}

	limit, err = strconv.ParseUint(value, 10, 64)
	return
}

func parseRequestToUpdateDecommissionFirstHostDiskParallelLimit(r *http.Request) (limit uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	var value string
	if value = r.FormValue(decommissionFirstHostDiskParallelLimit); value == "" {
		err = keyNotFound(decommissionFirstHostDiskParallelLimit)
		return
	}

	limit, err = strconv.ParseUint(value, 10, 64)
	return
}

func parseRequestToUpdateDecommissionLimit(r *http.Request) (limit uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	var value string
	if value = r.FormValue(decommissionLimit); value == "" {
		err = keyNotFound(decommissionLimit)
		return
	}

	limit, err = strconv.ParseUint(value, 10, 32)
	return
}

func parseSetConfigParam(r *http.Request) (config map[string]string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	config = make(map[string]string)
	keyList := []string{
		cfgmetaPartitionInodeIdStep,
		cfgMetaNodeMemoryHighPer,
		cfgMetaNodeMemoryLowPer,
		cfgAutoMpMigrate,
		flashNodeHandleReadTimeout,
		flashNodeReadDataNodeTimeout,
		cfsMpMigrateThreads,
		flashHotKeyMissCount,
		preheatTotalTask,
		maxDisableFlashGroupPercent,
		flashReadFlowLimit,
		flashWriteFlowLimit,
		cfgDefaultVolStoreMode,
		cfgDefaultDpTag,
		cfgDefaultMpTag,
	}
	for _, key := range keyList {
		if value := r.FormValue(key); value != "" {
			config[key] = value
		}
	}

	if len(config) == 0 {
		err = keyNotFound("config")
	}
	return
}

func parseGetConfigParam(r *http.Request) (key string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if key = r.FormValue(configKey); key == "" {
		err = keyNotFound("config")
		return
	}
	log.LogInfo("parseGetConfigParam success.")
	return
}

func parseSetQuotaParam(r *http.Request, req *proto.SetMasterQuotaReuqest) (err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if req.VolName, err = extractName(r); err != nil {
		return
	}

	if req.MaxFiles, err = extractUint64WithDefault(r, MaxFilesKey, math.MaxUint64); err != nil {
		return
	}

	if req.MaxBytes, err = extractUint64WithDefault(r, MaxBytesKey, math.MaxUint64); err != nil {
		return
	}
	var body []byte
	if body, err = io.ReadAll(r.Body); err != nil {
		return
	}

	if err = json.Unmarshal(body, &req.PathInfos); err != nil {
		return
	}

	log.LogInfo("parserSetQuotaParam success.")
	return
}

func parseUpdateQuotaParam(r *http.Request, req *proto.UpdateMasterQuotaReuqest) (err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if req.VolName, err = extractName(r); err != nil {
		return
	}

	if req.QuotaId, err = extractQuotaId(r); err != nil {
		return
	}

	if req.MaxFiles, err = extractUint64WithDefault(r, MaxFilesKey, math.MaxUint64); err != nil {
		return
	}

	if req.MaxBytes, err = extractUint64WithDefault(r, MaxBytesKey, math.MaxUint64); err != nil {
		return
	}
	log.LogInfo("parserUpdateQuotaParam success.")
	return
}

func parseDeleteQuotaParam(r *http.Request) (volName string, quotaId uint32, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if volName, err = extractName(r); err != nil {
		return
	}

	if quotaId, err = extractQuotaId(r); err != nil {
		return
	}

	return
}

func parseGetQuotaParam(r *http.Request) (volName string, quotaId uint32, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if volName, err = extractName(r); err != nil {
		return
	}

	if quotaId, err = extractQuotaId(r); err != nil {
		return
	}
	return
}

func extractQuotaId(r *http.Request) (quotaId uint32, err error) {
	var value string
	if value = r.FormValue(quotaKey); value == "" {
		err = keyNotFound(quotaKey)
		return
	}
	tmp, err := strconv.ParseUint(value, 10, 32)
	quotaId = uint32(tmp)
	return
}

func parseRequestToSetTrashInterval(r *http.Request) (name, authKey string, interval int64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		return
	}
	if authKey, err = extractAuthKey(r); err != nil {
		return
	}
	if interval, err = extractInt64WithDefault(r, trashIntervalKey, 0); err != nil {
		return
	}

	if interval > maxTrashInterval {
		err = fmt.Errorf("trash interval can't be greater than %d, now %d", maxTrashInterval, interval)
		return
	}
	return
}

func parseRequestToUpdateDecommissionDiskLimit(r *http.Request) (limit uint32, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	var value string
	if value = r.FormValue(decommissionDiskLimit); value == "" {
		err = keyNotFound(decommissionDiskLimit)
		return
	}
	tmp, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return
	}
	limit = uint32(tmp)
	return
}

func parseS3QosReq(r *http.Request, req *proto.S3QosRequest) (err error) {
	var body []byte
	if body, err = io.ReadAll(r.Body); err != nil {
		return
	}

	if err = json.Unmarshal(body, &req); err != nil {
		return
	}

	log.LogInfo("parseS3QosReq success.")
	return
}

func parseRequestToSetDiskBrokenThreshold(r *http.Request) (ratio float64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if ratio, err = extractDiskBrokenThreshold(r); err != nil {
		return
	}
	return
}

func extractDiskBrokenThreshold(r *http.Request) (ratio float64, err error) {
	var value string
	if value = r.FormValue(markDiskBrokenThresholdKey); value == "" {
		err = keyNotFound(markDiskBrokenThresholdKey)
		return
	}
	return strconv.ParseFloat(value, 64)
}

func parseRequestToResetDpRestoreStatus(r *http.Request) (dpId uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	dpId, err = extractDataPartitionID(r)
	return
}

func extractMediaType(r *http.Request) (mediaType uint32, err error) {
	var value string
	if value = r.FormValue(mediaTypeKey); value == "" {
		mediaType = proto.MediaType_Unspecified
		return
	}

	parsedMediaType, err := strconv.ParseUint(value, 10, 32)
	mediaType = uint32(parsedMediaType)
	return
}

func parseRocksDbFieldToUpdateVol(r *http.Request, vol *Vol) (storeMode int, err error) {
	storeMode, err = extractUintWithDefault(r, StoreModeKey, int(vol.DefaultStoreMode))
	return
}

func extractStoreMode(r *http.Request) (storeMode int, err error) {
	storeMode, err = extractUint(r, StoreModeKey)
	return
}

func extractUint(r *http.Request, key string) (val int, err error) {
	str := r.FormValue(key)
	if str == "" {
		return 0, nil
	}

	if val, err = strconv.Atoi(str); err != nil || val < 0 {
		return 0, unmatchedKey(key)
	}

	return val, nil
}

func extractUint64(r *http.Request, key string) (val uint64, err error) {
	str := r.FormValue(key)
	if str == "" {
		return 0, nil
	}

	if val, err = strconv.ParseUint(str, 10, 64); err != nil {
		return 0, unmatchedKey(key)
	}

	return val, nil
}

func extractUint32(r *http.Request, key string) (val uint32, err error) {
	str := r.FormValue(key)
	if str == "" {
		return 0, nil
	}

	var valUint64 uint64
	if valUint64, err = strconv.ParseUint(str, 10, 32); err != nil {
		return 0, unmatchedKey(key)
	}

	val = uint32(valUint64)
	return val, nil
}

func extractPositiveUint64(r *http.Request, key string) (val uint64, err error) {
	str := r.FormValue(key)
	if str == "" {
		return 0, keyNotFound(key)
	}

	if val, err = strconv.ParseUint(str, 10, 64); err != nil || val <= 0 {
		return 0, unmatchedKey(key)
	}

	return val, nil
}

func extractStr(r *http.Request, key string) (val string) {
	return r.FormValue(key)
}

func extractUintWithDefault(r *http.Request, key string, def int) (val int, err error) {
	str := r.FormValue(key)
	if str == "" {
		return def, nil
	}

	if val, err = strconv.Atoi(str); err != nil || val < 0 {
		return 0, unmatchedKey(key)
	}

	return val, nil
}

func extractUint32WithDefault(r *http.Request, key string, def uint32) (val uint32, err error) {
	str := r.FormValue(key)
	if str == "" {
		return def, nil
	}

	var valUint64 uint64
	if valUint64, err = strconv.ParseUint(str, 10, 32); err != nil || valUint64 > math.MaxUint32 {
		return 0, unmatchedKey(key)
	}

	val = uint32(valUint64)
	return val, nil
}

func extractUint64WithDefault(r *http.Request, key string, def uint64) (val uint64, err error) {
	str := r.FormValue(key)
	if str == "" {
		return def, nil
	}

	if val, err = strconv.ParseUint(str, 10, 64); err != nil {
		return 0, unmatchedKey(key)
	}

	return val, nil
}

func extractUint8WithDefault(r *http.Request, key string, def uint8) (val uint8, err error) {
	str := r.FormValue(key)
	if str == "" {
		return def, nil
	}

	var tmpVal uint64
	if tmpVal, err = strconv.ParseUint(str, 10, 8); err != nil {
		return 0, unmatchedKey(key)
	}
	return uint8(tmpVal), nil
}

func extractInt64WithDefault(r *http.Request, key string, def int64) (val int64, err error) {
	str := r.FormValue(key)
	if str == "" {
		return def, nil
	}

	if val, err = strconv.ParseInt(str, 10, 64); err != nil || val < 0 {
		return 0, unmatchedKey(key)
	}

	return val, nil
}

func extractStrWithDefault(r *http.Request, key string, def string) (val string) {
	if val = r.FormValue(key); val == "" {
		return def
	}
	return val
}

func extractBoolWithDefault(r *http.Request, key string, def bool) (val bool, err error) {
	str := r.FormValue(key)
	if str == "" {
		return def, nil
	}

	if val, err = strconv.ParseBool(str); err != nil {
		return false, unmatchedKey(key)
	}

	return val, nil
}

func parseRequestForUpdateNode(r *http.Request) (nodeAddr string, id uint64, selectTag string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}

	value := r.FormValue(idKey)
	if value != "" {
		id, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return
		}
	}

	selectTag = r.FormValue(TagKey)

	return
}

// parseRequestToCreateStoragePool parses request parameters for creating storage pool
func parseRequestToCreateStoragePool(r *http.Request) (poolInfo *proto.StoragePoolInfo, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	poolInfo = &proto.StoragePoolInfo{}

	// Parse pool ID
	if idStr := r.FormValue(idKey); idStr != "" {
		var id uint64
		if id, err = strconv.ParseUint(idStr, 10, 8); err != nil {
			return nil, fmt.Errorf("invalid pool id: %v", err)
		}
		poolInfo.Id = uint8(id)
		// Validate pool ID range: 1-255
		if poolInfo.Id == 0 || poolInfo.Id > 255 {
			return nil, fmt.Errorf("pool id must be between 1 and 255, got %d", poolInfo.Id)
		}
	}

	// Parse pool name
	poolInfo.Name = r.FormValue(nameKey)
	err = validatePoolName(poolInfo.Name)
	if err != nil {
		return nil, err
	}

	// Parse storage class
	if scStr := r.FormValue(poolStorageClassKey); scStr != "" {
		var sc uint64
		if sc, err = strconv.ParseUint(scStr, 10, 8); err != nil {
			return nil, fmt.Errorf("invalid storage class: %v", err)
		}
		if sc == 0 {
			return nil, fmt.Errorf("storage class cannot be 0 (Unspecified), must be 1 (ReplicaSSD), 2 (ReplicaHDD), or 3 (BlobStore)")
		}
		if !proto.IsValidStorageClass(uint32(sc)) {
			return nil, fmt.Errorf("invalid storage class: %d, must be 1 (ReplicaSSD), 2 (ReplicaHDD), or 3 (BlobStore)", sc)
		}
		poolInfo.StorageClass = uint8(sc)
	}

	// Parse CId (EC cluster ID)
	if cidStr := r.FormValue(poolCIdKey); cidStr != "" {
		if poolInfo.CId, err = strconv.Atoi(cidStr); err != nil {
			return nil, fmt.Errorf("invalid cId: %v", err)
		}
	}

	// Parse ECAddr (EC cluster address)
	poolInfo.ECAddr = r.FormValue(poolECAddrKey)

	return
}

// parseRequestToUpdateStoragePool parses request parameters for updating storage pool
func parseRequestToUpdateStoragePool(r *http.Request) (poolId uint8, poolInfo *proto.StoragePoolInfo, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	// Parse pool ID (required)
	var id uint64
	if idStr := r.FormValue(idKey); idStr != "" {
		if id, err = strconv.ParseUint(idStr, 10, 8); err != nil {
			return 0, nil, fmt.Errorf("invalid pool id: %v", err)
		}
		poolId = uint8(id)
	} else {
		return 0, nil, fmt.Errorf("pool id is required")
	}

	poolInfo = &proto.StoragePoolInfo{
		Id: poolId,
	}

	updated := false
	// Parse pool name (optional)
	poolInfo.Name = r.FormValue(nameKey)
	// Validate pool name: only letters and numbers, max 32 characters
	if poolInfo.Name != "" {
		if err = validatePoolName(poolInfo.Name); err != nil {
			return 0, nil, err
		}
		updated = true
	}

	// Parse CId (EC cluster ID, optional)
	if cidStr := r.FormValue(poolCIdKey); cidStr != "" {
		if poolInfo.CId, err = strconv.Atoi(cidStr); err != nil {
			return 0, nil, fmt.Errorf("invalid cId: %v", err)
		}
		updated = true
	}

	// Parse ECAddr (EC cluster address, optional)
	poolInfo.ECAddr = r.FormValue(poolECAddrKey)
	if poolInfo.ECAddr != "" {
		updated = true
	}

	if !updated {
		return 0, nil, fmt.Errorf("at least one of name, cId, or ecAddr is required besides pool id")
	}

	return
}

// validateRegionName validates region name format and length (same rules as pool name).
func validateRegionName(region string) error {
	if region == "" {
		return nil // Empty region is allowed (callers may substitute default before add)
	}
	return validateResourceName(region, "region name")
}
