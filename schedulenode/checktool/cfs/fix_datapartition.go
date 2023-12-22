package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/checktool/ump"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	cfgFixBadPartition = "fixBadPartition"
	cfgUmpAPiToken     = "umpToken"
	endPoint           = "spark_master_warning"
	umpOpenAPiDomain   = "open.ump.jd.com"
	alarmRecordsMethod = "/alarm/records"
	cfsDomain          = "sparkchubaofs.jd.local"
)

var fixPartitionMap = make(map[uint64]time.Time, 0)

func (s *ChubaoFSMonitor) scheduleToFixBadDataPartition(cfg *config.Config) {
	if !cfg.GetBool(cfgFixBadPartition) {
		return
	}
	if cfg.GetString(cfgUmpAPiToken) == "" {
		log.LogErrorf("ump token not found in config")
		return
	}
	log.LogInfof("scheduleToFixBadDataPartition started")
	s.umpClient = ump.NewUmpClient(cfg.GetString(cfgUmpAPiToken), umpOpenAPiDomain)
	var fixTick = time.NewTicker(time.Second)
	defer fixTick.Stop()
	for {
		select {
		case <-fixTick.C:
			s.fixPartitions(parseBadReplicaNumPartition, fixBadReplicaNumPartition)
			s.fixPartitions(parseNotRecoverPartition, fix24HourNotRecoverPartition)
			fixTick.Reset(time.Minute)
		}
	}
}

func (s *ChubaoFSMonitor) fixPartitions(parseFunc func(string) []uint64, fixPartitionFunc func(partition uint64)) {
	var err error
	var badPartitions []uint64
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("action[fixPartitions] recover from panic:%v", r)
		}
		if err != nil {
			log.LogErrorf("action[fixPartitions] err:%v", err)
		}
	}()
	log.LogInfo("action[fixPartitions] start")
	badPartitions, err = s.parseBadPartitionIDsFromUmpRecord(parseFunc)
	if err != nil {
		return
	}
	if len(badPartitions) == 0 {
		return
	}
	log.LogWarnf("action[fixPartitions] domain[%v] found %v bad partitions:%v, start fix", cfsDomain, len(badPartitions), badPartitions)
	for _, partition := range badPartitions {
		if _, ok := fixPartitionMap[partition]; !ok {
			fixPartitionMap[partition] = time.Now()
		} else {
			if time.Since(fixPartitionMap[partition]) < time.Minute*10 {
				continue
			}
			fixPartitionMap[partition] = time.Now()
		}
		fixPartitionFunc(partition)
	}
	return
}

func (s *ChubaoFSMonitor) parseBadPartitionIDsFromUmpRecord(parseFunc func(string) []uint64) (ids []uint64, err error) {
	var alarmRecords *ump.AlarmRecordResponse
	ids = make([]uint64, 0)
	idsMap := make(map[uint64]bool, 0)
	alarmRecords, err = s.umpClient.GetAlarmRecords(alarmRecordsMethod, "chubaofs-node", "jdos", endPoint, time.Now().UnixMilli()-60*10*1000, time.Now().UnixMilli())
	if err != nil {
		return
	}
	for _, r := range alarmRecords.Records {
		pids := parseFunc(r.Content)
		for _, pid := range pids {
			if pid > 0 {
				idsMap[pid] = true
			}
		}
	}
	for id := range idsMap {
		ids = append(ids, id)
	}
	return ids, nil
}

func parseBadReplicaNumPartition(content string) (pids []uint64) {
	pids = make([]uint64, 0)
	if !strings.Contains(content, "FIX DataPartition replicaNum") {
		return
	}
	log.LogWarnf("action[parseBadReplicaNumPartition] content:%v", content)
	tmp := strings.Split(content, "partitionID:")[1]
	pidStr := strings.Split(tmp, " ")[0]
	if pidStr == "" {
		return
	}
	pid, err := strconv.ParseUint(pidStr, 10, 64)
	if err != nil {
		log.LogErrorf("parse partition id failed:%v", err)
		return
	}
	pids = append(pids, pid)
	return
}

// eg. action[checkDiskRecoveryProgress] clusterID[spark],has[1] has offlined more than 24 hours,still not recovered,ids[map[72392:1700900266]]
func parseNotRecoverPartition(content string) (pids []uint64) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("recover from panic:%v", r)
		}
	}()
	pids = make([]uint64, 0)
	log.LogWarnf("action[parseNotRecoverPartition] content:%v", content)
	if !strings.Contains(content, "has offlined more than 24 hours") && !strings.Contains(content, "has migrated more than 24 hours") {
		return
	}
	tmp := strings.Split(content, "map[")[1]
	pidsStr := strings.Split(tmp, " ")
	for _, pidStr := range pidsStr {
		pStr := strings.Split(pidStr, ":")[0]
		if pStr == "" {
			continue
		}
		pid, err := strconv.ParseUint(pStr, 10, 64)
		if err != nil {
			log.LogErrorf("action[parseNotRecoverPartition] parse partition id failed:%v", err)
			continue
		}
		pids = append(pids, pid)
	}
	return
}

func isNeedFix(client *master.MasterClient, partition uint64) (fix bool, dp *proto.DataPartitionInfo, err error) {
	dp, err = client.AdminAPI().GetDataPartition("", partition)
	if err != nil {
		return
	}
	if dp.ReplicaNum != 2 {
		return
	}
	if len(dp.Hosts) != 1 {
		return
	}
	leader := false
	for _, r := range dp.Replicas {
		if r.IsLeader {
			leader = true
		}
	}
	if !leader {
		err = fmt.Errorf("partition:%v no leader", partition)
		return
	}
	// len(hosts)==1, retry 20s later
	time.Sleep(time.Second * 20)
	dp, err = client.AdminAPI().GetDataPartition("", partition)
	if err != nil {
		return
	}
	if len(dp.Hosts) != 1 {
		return
	}
	fix = true
	return
}

func fixBadReplicaNumPartition(partition uint64) {
	var (
		dn         *proto.DataNodeInfo
		dp         *proto.DataPartitionInfo
		err        error
		needFix    bool
		extraHost  string
		remainHost string
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[fixBadReplicaNumPartition] err:%v", err)
		}
	}()
	client := master.NewMasterClient([]string{cfsDomain}, false)
	if needFix, dp, err = isNeedFix(client, partition); err != nil {
		log.LogErrorf("action[fixBadReplicaNumPartition] err:%v", err)
		return
	}
	if !needFix {
		return
	}
	for _, replica := range dp.Replicas {
		if replica.Addr == dp.Hosts[0] {
			continue
		}
		extraHost = replica.Addr
		break
	}
	remainHost = dp.Hosts[0]

	dn, err = client.NodeAPI().GetDataNode(extraHost)
	if err != nil {
		log.LogErrorf("action[fixBadReplicaNumPartition] err:%v", err)
		return
	}
	allNodeViews := make([]proto.NodeView, 0)
	topologyView, err := client.AdminAPI().GetTopology()
	if err != nil {
		return
	}
	for _, zone := range topologyView.Zones {
		if zone.Name != dn.ZoneName {
			continue
		}
		for _, ns := range zone.NodeSet {
			allNodeViews = append(allNodeViews, ns.DataNodes...)
		}
	}
	log.LogInfof("action[fixBadReplicaNumPartition] try to add learner to fix one replica partition:%v", partition)
	retry := 20
	for i := 0; i < retry; i++ {
		rand.Seed(time.Now().UnixNano())
		index := rand.Intn(len(allNodeViews) - 1)
		destNode := allNodeViews[index]
		if destNode.Addr == extraHost || destNode.Addr == remainHost {
			continue
		}
		var destNodeView *proto.DataNodeInfo
		destNodeView, err = client.NodeAPI().GetDataNode(destNode.Addr)
		if err != nil {
			log.LogErrorf("action[fixBadReplicaNumPartition] err:%v", err)
			continue
		}
		if destNodeView.UsageRatio > 0.8 {
			continue
		}
		if !destNodeView.IsActive {
			continue
		}
		err = client.AdminAPI().AddDataLearner(partition, destNode.Addr, true, 90)
		if err != nil {
			log.LogErrorf("action[fixBadReplicaNumPartition] partition:%v, err:%v", partition, err)
			break
		}
		exporter.WarningBySpecialUMPKey(UMPCFSSparkFixPartitionKey, fmt.Sprintf("Domain[%v] fix one replica partition:%v success, add learner:%v", cfsDomain, partition, destNode.Addr))
		break
	}
	return
}

func fix24HourNotRecoverPartition(partition uint64) {
	var (
		err        error
		dp         *proto.DataPartitionInfo
		minReplica *proto.DataReplica
		maxReplica *proto.DataReplica
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[fix24HourNotRecoverPartition] partition:%v err:%v", partition, err)
		}
	}()
	client := master.NewMasterClient([]string{cfsDomain}, false)
	dp, err = client.AdminAPI().GetDataPartition("", partition)
	if err != nil {
		return
	}
	if !dp.IsRecover {
		err = fmt.Errorf("partition has recovered")
		return
	}
	if len(dp.Replicas) == 0 {
		err = fmt.Errorf("replicas number is 0")
		return
	}
	if len(dp.MissingNodes) > 0 {
		err = fmt.Errorf("missing nodes:%v", dp.MissingNodes)
		return
	}
	minReplica = dp.Replicas[0]
	maxReplica = dp.Replicas[0]
	for _, r := range dp.Replicas {
		if r.Used == 0 {
			err = fmt.Errorf("used size is 0, maybe restarted not long ago")
			return
		}
		if r.IsRecover {
			err = fmt.Errorf("extent repair may be paused due to some reasons, please refer to more information on the hosts ")
			return
		}
		if r.Used < minReplica.Used {
			minReplica = r
		}
		if r.Used > maxReplica.Used {
			maxReplica = r
		}
	}

	var maxSum, minSum uint64
	maxSum, err = sumTinyAvailableSize(maxReplica.Addr, partition)
	if err != nil {
		return
	}
	minSum, err = sumTinyAvailableSize(minReplica.Addr, partition)
	if err != nil {
		return
	}
	var reason, execute string
	if maxSum < minSum || maxSum-minSum < unit.GB*9/10 {
		partitionPath := fmt.Sprintf("datapartition_%v_%v", partition, maxReplica.Total)
		err = stopReloadReplica(maxReplica.Addr, partition, partitionPath)
		if err != nil {
			log.LogErrorf("action[fix24HourNotRecoverPartition] stopReloadReplica max replica failed:%v", err)
		}
		time.Sleep(time.Second * 10)
		err = stopReloadReplica(minReplica.Addr, partition, partitionPath)
		if err != nil {
			log.LogErrorf("action[fix24HourNotRecoverPartition] stopReloadReplica min replica failed:%v", err)
		}
		reason = "size calculation is wrong"
		execute = "stop-reload data partition"
	} else {
		err = playBackTinyDeleteRecord(maxReplica.Addr, partition)
		reason = "tiny extent deletion is not synchronized"
		execute = "playback tiny extent delete record"
	}

	exporter.WarningBySpecialUMPKey(UMPCFSSparkFixPartitionKey, fmt.Sprintf("Domain[%v] 24 hours not recovered partition(%v), replica(%v), reason(%s), execute(%s)", cfsDomain, partition, maxReplica.Addr, reason, execute))
}

func sumTinyAvailableSize(addr string, partitionID uint64) (sum uint64, err error) {
	dHost := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], profPortMap[strings.Split(addr, ":")[1]])
	dataClient := http_client.NewDataClient(dHost, false)
	for i := uint64(1); i <= uint64(64); i++ {
		var extentHoleInfo *proto.DNTinyExtentInfo
		extentHoleInfo, err = dataClient.GetExtentHoles(partitionID, i)
		if err != nil {
			return
		}
		sum += extentHoleInfo.ExtentAvaliSize
	}
	return sum, nil
}

func stopReloadReplica(addr string, partitionID uint64, partitionPath string) (err error) {
	dHost := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], profPortMap[strings.Split(addr, ":")[1]])
	dataClient := http_client.NewDataClient(dHost, false)
	partition, err := dataClient.GetPartitionFromNode(partitionID)
	if err != nil {
		return
	}
	err = dataClient.StopPartition(partitionID)
	if err != nil {
		return err
	}
	time.Sleep(time.Second * 10)
	for i := 0; i < 3; i++ {
		if err = dataClient.ReLoadPartition(partitionPath, strings.Split(partition.Path, "/datapartition")[0]); err == nil {
			break
		}
	}
	return
}

func playBackTinyDeleteRecord(addr string, partitionID uint64) (err error) {
	dHost := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], profPortMap[strings.Split(addr, ":")[1]])
	dataClient := http_client.NewDataClient(dHost, false)
	err = dataClient.PlaybackPartitionTinyDelete(partitionID)
	return err
}
