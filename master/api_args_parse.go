package master

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/cryptoutil"
	"github.com/cubefs/cubefs/util/log"
)

// Parse the request that adds/deletes a raft node.
func parseRequestForRaftNode(r *http.Request) (id uint64, host string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var idStr string
	if idStr = r.FormValue(idKey); idStr == "" {
		err = keyNotFound(idKey)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
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

func parseRequestForUpdateMetaNode(r *http.Request) (nodeAddr string, id uint64, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if id, err = extractNodeID(r); err != nil {
		return
	}
	return
}

func parseRequestForAddNode(r *http.Request) (nodeAddr, zoneName string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if nodeAddr, err = extractNodeAddr(r); err != nil {
		return
	}
	if zoneName = r.FormValue(zoneNameKey); zoneName == "" {
		zoneName = DefaultZoneName
	}
	return
}

func parseDecomNodeReq(r *http.Request) (nodeAddr string, limit int, err error) {
	nodeAddr, err = parseAndExtractNodeAddr(r)
	if err != nil {
		return
	}

	limit, err = parseUintParam(r, countKey)
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

func parseRequestToDecommissionNode(r *http.Request) (nodeAddr, diskPath string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	nodeAddr, err = extractNodeAddr(r)
	if err != nil {
		return
	}
	diskPath, err = extractDiskPath(r)
	return
}

func parseRequestToGetTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
	var body []byte
	if err = r.ParseForm(); err != nil {
		return
	}
	if body, err = ioutil.ReadAll(r.Body); err != nil {
		return
	}
	tr = &proto.AdminTask{}
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(body)))
	decoder.UseNumber()
	err = decoder.Decode(tr)
	return
}

func parseVolName(r *http.Request) (name string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	if name, err = extractName(r); err != nil {
		return
	}
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
	if p.name = r.FormValue(nameKey); p.name == "" {
		err = keyNotFound(nameKey)
		return
	}
	if !volNameRegexp.MatchString(p.name) {
		err = errors.New("name can only be number and letters")
		return
	}
	if p.authKey = r.FormValue(volAuthKey); !p.skipOwnerValidation && len(p.authKey) == 0 {
		err = keyNotFound(volAuthKey)
		return
	}
	return
}

func parseRequestToDeleteVol(r *http.Request) (name, authKey string, force bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}

	if name, err = extractName(r); err != nil {
		return
	}

	if authKey, err = extractAuthKey(r); err != nil {
		return
	}

	force, err = extractBoolWithDefault(r, forceDelVolKey, false)
	if err != nil {
		return
	}

	return

}

func extractUintWithDefault(r *http.Request, key string, def int) (val int, err error) {

	var str string
	if str = r.FormValue(key); str == "" {
		return def, nil
	}

	if val, err = strconv.Atoi(str); err != nil || val < 0 {
		return 0, fmt.Errorf("parse [%s] is not valid int [%d], err %v", key, val, err)
	}

	return val, nil
}

func extractUint64WithDefault(r *http.Request, key string, def uint64) (val uint64, err error) {

	var str string
	if str = r.FormValue(key); str == "" {
		return def, nil
	}

	if val, err = strconv.ParseUint(str, 10, 64); err != nil || val < 0 {
		return 0, fmt.Errorf("parse [%s] is not valid uint [%d], err %v", key, val, err)
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
	var str string
	if str = r.FormValue(key); str == "" {
		return def, nil
	}

	if val, err = strconv.ParseBool(str); err != nil {
		return false, fmt.Errorf("parse [%s] is not a bool val [%t]", key, val)
	}

	return val, nil
}

type updateVolReq struct {
	name           string
	authKey        string
	capacity       uint64
	followRead     bool
	authenticate   bool
	enablePosixAcl bool
	zoneName       string
	description    string
	dpSelectorName string
	dpSelectorParm string
	coldArgs       *coldVolArgs
}

func parseColdVolUpdateArgs(r *http.Request, vol *Vol) (args *coldVolArgs, err error) {
	args = &coldVolArgs{}

	if args.objBlockSize, err = extractUintWithDefault(r, ebsBlkSizeKey, vol.EbsBlkSize); err != nil {
		return
	}

	if args.cacheCap, err = extractUint64WithDefault(r, cacheCapacity, vol.CacheCapacity); err != nil {
		return
	}

	if args.cacheAction, err = extractUintWithDefault(r, cacheActionKey, vol.CacheAction); err != nil {
		return
	}

	if args.cacheThreshold, err = extractUintWithDefault(r, cacheThresholdKey, vol.CacheThreshold); err != nil {
		return
	}

	if args.cacheTtl, err = extractUintWithDefault(r, cacheTTLKey, vol.CacheTTL); err != nil {
		return
	}

	if args.cacheHighWater, err = extractUintWithDefault(r, cacheHighWaterKey, vol.CacheHighWater); err != nil {
		return
	}

	if args.cacheLowWater, err = extractUintWithDefault(r, cacheLowWaterKey, vol.CacheLowWater); err != nil {
		return
	}

	if args.cacheLRUInterval, err = extractUintWithDefault(r, cacheLRUIntervalKey, vol.CacheLRUInterval); err != nil {
		return
	}

	if args.cacheLRUInterval < 2 {
		return nil, fmt.Errorf("cacheLruInterval(%d) muster be bigger than 2 minute", args.cacheLRUInterval)
	}

	args.cacheRule = extractStrWithDefault(r, cacheRuleKey, vol.CacheRule)
	emptyCacheRule, err := extractBoolWithDefault(r, emptyCacheRuleKey, false)
	if err != nil {
		return
	}

	if emptyCacheRule {
		args.cacheRule = ""
	}

	// do some check
	if args.cacheLowWater >= args.cacheHighWater {
		return nil, fmt.Errorf("low water(%d) must be less than high water(%d)", args.cacheLowWater, args.cacheHighWater)
	}

	if args.cacheHighWater >= 90 || args.cacheLowWater >= 90 {
		return nil, fmt.Errorf("low(%d) or high water(%d) can't be large than 90, low than 0", args.cacheLowWater, args.cacheHighWater)
	}

	if args.cacheAction < proto.NoCache || args.cacheAction > proto.RWCache {
		return nil, fmt.Errorf("cache action is illegal (%d)", args.cacheAction)
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

	if req.capacity, err = extractUint64WithDefault(r, volCapacityKey, vol.Capacity); err != nil {
		return
	}

	if req.enablePosixAcl, err = extractBoolWithDefault(r, enablePosixAclKey, vol.enablePosixAcl); err != nil {
		return
	}

	if req.authenticate, err = extractBoolWithDefault(r, authenticateKey, vol.authenticate); err != nil {
		return
	}

	if req.followRead, err = extractBoolWithDefault(r, followerReadKey, vol.FollowerRead); err != nil {
		return
	}

	req.dpSelectorName = r.FormValue(dpSelectorNameKey)
	req.dpSelectorParm = r.FormValue(dpSelectorParmKey)

	if (req.dpSelectorName == "" && req.dpSelectorParm != "") || (req.dpSelectorName != "" && req.dpSelectorParm == "") {
		err = keyNotFound(dpSelectorNameKey + " or " + dpSelectorParmKey)
		return

	} else if req.dpSelectorParm == "" && req.dpSelectorName == "" {
		req.dpSelectorName = vol.dpSelectorName
		req.dpSelectorParm = vol.dpSelectorParm
	}
	var replicaNum int
	if replicaNumStr := r.FormValue(replicaNumKey); replicaNumStr != "" {
		if replicaNum, err = strconv.Atoi(replicaNumStr); err != nil {
			err = unmatchedKey(replicaNumKey)
			return
		}
	} else {
		replicaNum = int(vol.dpReplicaNum)
	}

	if replicaNum != 0 && replicaNum != int(vol.dpReplicaNum) {
		err = fmt.Errorf("replicaNum cann't be changed")
		return
	}

	if proto.IsCold(vol.VolType) {
		req.followRead = true
		req.coldArgs, err = parseColdVolUpdateArgs(r, vol)
		if err != nil {
			return
		}
	}

	return
}

func parseBoolFieldToUpdateVol(r *http.Request, vol *Vol) (followerRead, authenticate bool, err error) {
	if followerReadStr := r.FormValue(followerReadKey); followerReadStr != "" {
		if followerRead, err = strconv.ParseBool(followerReadStr); err != nil {
			err = unmatchedKey(followerReadKey)
			return
		}
	} else {
		followerRead = vol.FollowerRead
	}
	if authenticateStr := r.FormValue(authenticateKey); authenticateStr != "" {
		if authenticate, err = strconv.ParseBool(authenticateStr); err != nil {
			err = unmatchedKey(authenticateKey)
			return
		}
	} else {
		authenticate = vol.authenticate
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

	if capacity, err = extractUint(r, volCapacityKey); err != nil {
		return
	}

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
	objBlockSize     int
	cacheCap         uint64
	cacheAction      int
	cacheThreshold   int
	cacheTtl         int
	cacheHighWater   int
	cacheLowWater    int
	cacheLRUInterval int
	cacheRule        string
}

type createVolReq struct {
	name                                 string
	owner                                string
	size                                 int
	mpCount                              int
	dpReplicaNum                         int
	capacity                             int
	followerRead                         bool
	authenticate                         bool
	crossZone                            bool
	normalZonesFirst                     bool
	domainId                             uint64
	zoneName                             string
	description                          string
	volType                              int
	enablePosixAcl                       bool
	qosLimitArgs                         *qosArgs
	clientReqPeriod, clientHitTriggerCnt uint32
	// cold vol args
	coldArgs coldVolArgs
}

func checkCacheAction(action int) error {
	if action != proto.NoCache && action != proto.RCache && action != proto.RWCache {
		return fmt.Errorf("cache action is not legal, action [%d]", action)
	}

	return nil
}

func parseColdArgs(r *http.Request) (args coldVolArgs, err error) {

	args.cacheRule = extractStr(r, cacheRuleKey)

	if args.objBlockSize, err = extractUint(r, ebsBlkSizeKey); err != nil {
		return
	}

	if args.cacheCap, err = extractUint64(r, cacheCapacity); err != nil {
		return
	}

	if args.cacheAction, err = extractUint(r, cacheActionKey); err != nil {
		return
	}

	if args.cacheThreshold, err = extractUint(r, cacheThresholdKey); err != nil {
		return
	}

	if args.cacheTtl, err = extractUint(r, cacheTTLKey); err != nil {
		return
	}

	if args.cacheHighWater, err = extractUint(r, cacheHighWaterKey); err != nil {
		return
	}

	if args.cacheLowWater, err = extractUint(r, cacheLowWaterKey); err != nil {
		return
	}

	if args.cacheLRUInterval, err = extractUint(r, cacheLRUIntervalKey); err != nil {
		return
	}

	return
}

func parseRequestToCreateVol(r *http.Request, req *createVolReq) (err error) {

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

	if req.dpReplicaNum, err = extractUint(r, replicaNumKey); err != nil {
		return
	}

	if req.size, err = extractUintWithDefault(r, dataPartitionSizeKey, 120); err != nil {
		return
	}

	// default capacity 120
	if req.capacity, err = extractUint(r, volCapacityKey); err != nil {
		return
	}

	if req.volType, err = extractUint(r, volTypeKey); err != nil {
		return
	}

	followerRead, followerExist, err := extractFollowerRead(r)
	if err != nil {
		return
	}
	if followerExist && followerRead == false && proto.IsHot(req.volType) && req.dpReplicaNum == 1 {
		return fmt.Errorf("vol with one replia should enable followerRead")
	}
	req.followerRead = followerRead
	if proto.IsHot(req.volType) && req.dpReplicaNum == 1 {
		req.followerRead = true
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
	req.domainId, err = extractUint64WithDefault(r, domainIdKey, 0)
	if err != nil {
		return
	}

	req.enablePosixAcl, err = extractPosixAcl(r)

	return
}

func parseRequestToCreateDataPartition(r *http.Request) (count int, name string, err error) {
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
	if name, err = extractName(r); err != nil {
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

func extractDiskPath(r *http.Request) (diskPath string, err error) {
	if diskPath = r.FormValue(diskPathKey); diskPath == "" {
		err = keyNotFound(diskPathKey)
		return
	}
	return
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

func extractAuthenticate(r *http.Request) (authenticate bool, err error) {
	var value string
	if value = r.FormValue(authenticateKey); value == "" {
		authenticate = false
		return
	}
	if authenticate, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func extractCrossZone(r *http.Request) (crossZone bool, err error) {
	var value string
	if value = r.FormValue(crossZoneKey); value == "" {
		crossZone = false
		return
	}
	if crossZone, err = strconv.ParseBool(value); err != nil {
		return
	}
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
func parseAndExtractSetNodeSetInfoParams(r *http.Request) (params map[string]interface{}, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	params = make(map[string]interface{})
	if value = r.FormValue(countKey); value != "" {
		var count = uint64(0)
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
		var nodesetId = uint64(0)
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
		var batchCount = uint64(0)
		batchCount, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeDeleteBatchCountKey)
			return
		}
		params[nodeDeleteBatchCountKey] = batchCount
	}

	if value = r.FormValue(nodeMarkDeleteRateKey); value != "" {
		noParams = false
		var val = uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeMarkDeleteRateKey)
			return
		}
		params[nodeMarkDeleteRateKey] = val
	}

	if value = r.FormValue(nodeAutoRepairRateKey); value != "" {
		noParams = false
		var val = uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeAutoRepairRateKey)
			return
		}
		params[nodeAutoRepairRateKey] = val
	}

	if value = r.FormValue(nodeDeleteWorkerSleepMs); value != "" {
		noParams = false
		var val = uint64(0)
		val, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			err = unmatchedKey(nodeMarkDeleteRateKey)
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

	if noParams {
		err = keyNotFound(nodeDeleteBatchCountKey)
		return
	}
	return
}

func validateRequestToCreateMetaPartition(r *http.Request) (volName string, start uint64, err error) {
	if volName, err = extractName(r); err != nil {
		return
	}

	var value string
	if value = r.FormValue(startKey); value == "" {
		err = keyNotFound(startKey)
		return
	}

	start, err = strconv.ParseUint(value, 10, 64)
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

func parseVolStatReq(r *http.Request) (name string, ver int, err error) {
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

	return
}

func parseQosInfo(r *http.Request) (info *proto.ClientReportLimitInfo, err error) {
	info = proto.NewClientReportLimitInfo()
	var body []byte
	if body, err = ioutil.ReadAll(r.Body); err != nil {
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

func extractName(r *http.Request) (name string, err error) {
	if name = r.FormValue(nameKey); name == "" {
		err = keyNotFound(nameKey)
		return
	}
	if !volNameRegexp.MatchString(name) {
		return "", errors.New("name can only be number and letters")
	}

	return
}

func extractUint(r *http.Request, key string) (val int, err error) {
	var str string
	if str = r.FormValue(key); str == "" {
		return 0, nil
	}

	if val, err = strconv.Atoi(str); err != nil || val < 0 {
		return 0, fmt.Errorf("args [%s] is not legal, val %s", key, str)
	}

	return val, nil
}

func extractPositiveUint(r *http.Request, key string) (val int, err error) {
	var str string
	if str = r.FormValue(key); str == "" {
		return 0, fmt.Errorf("args [%s] is not legal", key)
	}

	if val, err = strconv.Atoi(str); err != nil || val <= 0 {
		return 0, fmt.Errorf("args [%s] is not legal, val %s", key, str)
	}

	return val, nil
}

func extractUint64(r *http.Request, key string) (val uint64, err error) {
	var str string
	if str = r.FormValue(key); str == "" {
		return 0, nil
	}

	if val, err = strconv.ParseUint(str, 10, 64); err != nil || val < 0 {
		return 0, fmt.Errorf("args [%s] is not legal, val %s", key, str)
	}

	return val, nil
}

func extractPositiveUint64(r *http.Request, key string) (val uint64, err error) {
	var str string
	if str = r.FormValue(key); str == "" {
		return 0, fmt.Errorf("args [%s] is not legal", key)
	}

	if val, err = strconv.ParseUint(str, 10, 64); err != nil || val <= 0 {
		return 0, fmt.Errorf("args [%s] is not legal, val %s", key, str)
	}

	return val, nil
}

func extractStr(r *http.Request, key string) (val string) {

	return r.FormValue(key)
}

func extractOwner(r *http.Request) (owner string, err error) {
	if owner = r.FormValue(volOwnerKey); owner == "" {
		err = keyNotFound(volOwnerKey)
		return
	}
	if !ownerRegexp.MatchString(owner) {
		return "", errors.New("owner can only be number and letters")
	}

	return
}

func parseAndCheckTicket(r *http.Request, key []byte, volName string) (jobj proto.APIAccessReq, ticket cryptoutil.Ticket, ts int64, err error) {
	var (
		plaintext []byte
	)

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
	var (
		message string
	)
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

func newSuccessHTTPReply(data interface{}) *proto.HTTPReply {
	return &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: proto.ErrSuc.Error(), Data: data}
}

func newErrHTTPReply(err error) *proto.HTTPReply {
	if err == nil {
		return newSuccessHTTPReply("")
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
	}

	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	send(w, r, reply)
	return
}

func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
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
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}

	return
}
