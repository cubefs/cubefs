package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/robfig/cron"
	"github.com/tiglabs/raft"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	partitionTypeDP = "DP"
	partitionTypeMP = "MP"
)
const (
	defaultInodeCountWarnDiffCount = 1000
)

type PromoteConfig struct {
	AutoProm      bool  `json:"auto_prom"`
	PromThreshold uint8 `json:"prom_threshold"`
}

// Peer defines the peer of the node id and address.
type Peer struct {
	ID   uint64 `json:"id"`
	Addr string `json:"addr"`
}

// Learner defines the learner of the node id and address.
type Learner struct {
	ID       uint64         `json:"id"`
	Addr     string         `json:"addr"`
	PmConfig *PromoteConfig `json:"promote_config"`
}

type MNMetaPartitionInfo struct {
	LeaderAddr                 string       `json:"leaderAddr"`
	Peers                      []Peer       `json:"peers"`
	Learners                   []Learner    `json:"learners"`
	NodeId                     uint64       `json:"nodeId"`
	Cursor                     uint64       `json:"cursor"`
	RaftStatus                 *raft.Status `json:"raft_status"`
	RaftStatusOfReleaseCluster *raft.Status `json:"raftStatus"`
}

type PeerReplica struct {
	VolName       string
	ReplicationID uint64
	PeerErrorInfo string
	nodeAddr      string
	PeerAddr      string
}

type PeerReplicaService struct {
	channel chan PeerReplica
}

func (s *ChubaoFSMonitor) scheduleToCheckMpPeerCorrupt() {

	crontab := cron.New()
	crontab.AddFunc("00 09,17 * * *", s.checkMpPeerCorrupt)
	crontab.Start()
	go func() {
		select {
		case <-s.ctx.Done():
			crontab.Stop()
		}
	}()
}

func (s *ChubaoFSMonitor) checkMpPeerCorrupt() {
	mpCheckStartTime := time.Now()
	log.LogInfof("check all metaPartitions corrupt start")
	for _, host := range s.hosts {
		checkMpCorruptWg.Add(1)
		go checkHostMetaPartition(host)
	}
	checkMpCorruptWg.Wait()
	log.LogInfof("check all metaPartitions corrupt end, cost [%v]", time.Since(mpCheckStartTime))
	// print mp/dp raft pending applied check result
	for _, host := range s.hosts {
		printRaftPendingAndAppliedResult(host, partitionTypeMP)
	}
}

func getMetaPartitionsFromVolume(volName string, ch *ClusterHost) (mps []*MetaPartition, err error) {
	var (
		reqURL string
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[getMetaPartitionsFromVolume] host[%v],vol[%v],err[%v]", ch.host,
				volName, err)
		}
	}()
	reqURL = fmt.Sprintf("http://%v/client/metaPartitions?name=%v", ch.host, volName)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		log.LogErrorf("request volView from [%v] failed ", reqURL)
		return
	}
	if err = json.Unmarshal(data, &mps); err != nil {
		log.LogErrorf("unmarshal to mp data[%v],err[%v]", string(data), err)
		return
	}
	return
}

func checkMetaPartitionPeers(ch *ClusterHost, volName string, PartitionID uint64) {
	var (
		reqURL      string
		err         error
		badReplicas []PeerReplica
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[checkMetaPartitionPeers] host[%v], vol[%v], id[%v], err[%v]", ch.host, volName,
				PartitionID, err)
		}
	}()
	badReplicas = make([]PeerReplica, 0)
	if ch.host == strings.TrimSpace("id.chubaofs-seqwrite.jd.local") {
		reqURL = fmt.Sprintf("http://%v/client/metaPartition?name=%v&id=%v", ch.host, volName, PartitionID)
	} else {
		reqURL = fmt.Sprintf("http://%v/metaPartition/get?name=%v&id=%v", ch.host, volName, PartitionID)
	}
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	mp := &MetaPartition{}
	if err = json.Unmarshal(data, mp); err != nil {
		log.LogErrorf("unmarshal to mp data[%v],err[%v]", string(data), err)
		return
	}
	replicaRaftStatusMap := make(map[string]*raft.Status, len(mp.Replicas)) // 不同副本的raft状态
	hostStrings := getAddrFromMetaReplicas(mp.Replicas)
	// check peer for each mp.Replica
	for _, r := range mp.Replicas {
		var mnPartition *MNMetaPartitionInfo
		addr := strings.Split(r.Addr, ":")[0]
		for i := 0; i < 3; i++ {
			if mnPartition, err = getPartition(ch, addr, mp.PartitionID); err == nil {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if err != nil || mnPartition == nil {
			log.LogWarnf(fmt.Sprintf("get metaPartition [%v] in host: [%v] addr: [%v] failed, err: %v", mp.PartitionID, ch.host, addr, err))
			continue
		}
		if ch.isReleaseCluster {
			replicaRaftStatusMap[r.Addr] = mnPartition.RaftStatusOfReleaseCluster
		} else {
			replicaRaftStatusMap[r.Addr] = mnPartition.RaftStatus
		}
		peerStrings := convertPeersToArray(mnPartition.Peers)
		sort.Strings(peerStrings)
		sort.Strings(hostStrings)

		diffPeerToHost := diffSliceString(peerStrings, hostStrings)
		diffHostToPeer := diffSliceString(hostStrings, peerStrings)
		if len(peerStrings) == 4 && len(diffPeerToHost) == 1 && len(diffHostToPeer) == 0 {
			peerReplica := PeerReplica{
				VolName:       mp.VolName,
				ReplicationID: mp.PartitionID,
				PeerErrorInfo: "PEER4_HOST3",
				nodeAddr:      r.Addr,
				PeerAddr:      diffPeerToHost[0],
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if len(peerStrings) == 2 && len(diffHostToPeer) == 1 && len(diffPeerToHost) == 0 {
			peerReplica := PeerReplica{
				VolName:       mp.VolName,
				ReplicationID: mp.PartitionID,
				PeerErrorInfo: "PEER2_HOST3",
				nodeAddr:      r.Addr,
				PeerAddr:      diffHostToPeer[0],
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if len(peerStrings) == 3 && len(diffPeerToHost) == 1 && len(diffHostToPeer) == 0 {
			peerReplica := PeerReplica{
				VolName:       mp.VolName,
				ReplicationID: mp.PartitionID,
				PeerErrorInfo: "PEER3_HOST2",
				nodeAddr:      r.Addr,
				PeerAddr:      diffPeerToHost[0],
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if len(peerStrings) != len(hostStrings) || len(diffPeerToHost)+len(diffHostToPeer) > 0 {
			// ump alarm
			msg := fmt.Sprintf("[Domain: %v, vol: %v, partition: %v, host: %v] peerStrings: %v , hostStrings: %v ", ch.host, mp.VolName, mp.PartitionID, r.Addr, peerStrings, hostStrings)
			checktool.WarnBySpecialUmpKey(UMPKeyMetaPartitionPeerInconsistency, msg)
		}
	}
	if ch.isReleaseCluster {
		mp.Hosts = mp.PersistenceHosts
	}
	checkRaftReplicaStatus(ch, replicaRaftStatusMap, mp.PartitionID, mp.VolName, partitionTypeMP, mp.Hosts, mp.IsRecover, nil, mp)
	if len(badReplicas) == 0 {
		return
	}
	//check if this partition has a simple replica error, if simple, auto repair it
	isUniquePeerReplica := true
	if len(badReplicas) > 1 {
		for _, nextPeerReplica := range badReplicas[1:] {
			if badReplicas[0].PeerAddr != nextPeerReplica.PeerAddr || badReplicas[0].PeerErrorInfo != nextPeerReplica.PeerErrorInfo {
				isUniquePeerReplica = false
				break
			}
		}
	}
	if isUniquePeerReplica {
		repairMp(ch.isReleaseCluster, ch.host, badReplicas[0])
	} else {
		for _, item := range badReplicas {
			msg := fmt.Sprintf("[Domain: %v, VolName: %v, PartitionID: %v], nodeAddr: %v, PeerErrorInfo: %v, PeerAddr:%v ",
				ch.host, item.VolName, item.ReplicationID, item.nodeAddr, item.PeerErrorInfo, item.PeerAddr)
			checktool.WarnBySpecialUmpKey(UMPKeyMetaPartitionPeerInconsistency, msg)
		}
	}
}

func repairMp(release bool, host string, rep PeerReplica) {
	var outputStr string
	switch rep.PeerErrorInfo {
	case "PEER2_HOST3":
		if !release {
			if err := decommissionMp(host, rep.ReplicationID, rep.PeerAddr); err != nil {
				log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER2_HOST3] repair failed, err:%v", host, rep.ReplicationID, err)
			} else {
				outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER2_HOST3] has been automatically repaired, cmd[cfs-cli metapartition decommission %v %v]",
					host, rep.ReplicationID, rep.PeerAddr, rep.ReplicationID)
			}
		} else {
			outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER2_HOST3] cmd: cfs-cli metapartition decommission %v %v",
				host, rep.ReplicationID, rep.PeerAddr, rep.ReplicationID)
		}
	case "PEER4_HOST3":
		if !release {
			if err := addMpReplica(host, rep.ReplicationID, rep.PeerAddr); err != nil {
				log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER4_HOST3] repair-addMpReplica failed, err:%v", host, rep.ReplicationID, err)
				break
			}
			time.Sleep(time.Second * 3)
			if err := delMpReplica(host, rep.ReplicationID, rep.PeerAddr); err != nil {
				log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER4_HOST3] repair-delMpReplica failed, err:%v", host, rep.ReplicationID, err)
				break
			}
			outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER4_HOST3] has been automatically repaired, cmd[cfs-cli metapartition add-replica %v %v && sleep 3 && cfs-cli metapartition del-replica %v %v]",
				host, rep.ReplicationID, rep.PeerAddr, rep.ReplicationID, rep.PeerAddr, rep.ReplicationID)
		} else {
			outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER4_HOST3] cmd: cfs-cli metapartition add-replica %v %v && sleep 3 && cfs-cli metapartition del-replica %v %v",
				host, rep.ReplicationID, rep.PeerAddr, rep.ReplicationID, rep.PeerAddr, rep.ReplicationID)
		}
	case "HOST2_PEER3":
		outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: HOST2_PEER3] cmd: cfs-cli metapartition add-replica %v %v",
			host, rep.ReplicationID, rep.PeerAddr, rep.ReplicationID)
	default:
		log.LogErrorf("wrong error info, %v", rep.PeerErrorInfo)
		return
	}
	checktool.WarnBySpecialUmpKey(UMPKeyMetaPartitionPeerInconsistency, outputStr)
}

func decommissionMp(master string, partition uint64, addr string) (err error) {
	reqURL := fmt.Sprintf("http://%v/metaPartition/decommission?id=%v&addr=%v", master, partition, addr)
	_, err = doRequest(reqURL, false)
	return
}

func addMpReplica(master string, partition uint64, addr string) (err error) {
	reqURL := fmt.Sprintf("http://%v/metaReplica/add?id=%v&addr=%v", master, partition, addr)
	_, err = doRequest(reqURL, false)
	return
}

func delMpReplica(master string, partition uint64, addr string) (err error) {
	reqURL := fmt.Sprintf("http://%v/metaReplica/delete?id=%v&addr=%v", master, partition, addr)
	_, err = doRequest(reqURL, false)
	return
}
func getPartition(ch *ClusterHost, addr string, partitionID uint64) (mnPartition *MNMetaPartitionInfo, err error) {
	// set meta node port
	var port string
	if ch.host == "id.chubaofs.jd.local" || ch.host == "th.chubaofs.jd.local" {
		port = "17220"
	} else if ch.host == "cn.chubaofs.jd.local" || ch.host == "idbbak.chubaofs.jd.local" || ch.host ==
		"cn.chubaofs-seqwrite.jd.local" || ch.host == "cn.elasticdb.jd.local" || ch.host == "nl.chubaofs.jd.local" || ch.host == "nl.chubaofs.ochama.com" {
		port = "9092"
	} else {
		log.LogInfof("action[checkMetaNodeDiskStat] host[%v] can not match its MN port", ch.host)
		return
	}
	reqURL := fmt.Sprintf("http://%v:%v/getPartitionById?pid=%v", addr, port, partitionID)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		log.LogErrorf("request mp from [%v] failed ", reqURL)
		return
	}
	mp := &mnPartition
	if err = json.Unmarshal(data, mp); err != nil {
		log.LogErrorf("unmarshal to mp data[%v],err[%v]", string(data), err)
		return
	}
	return
}

func getAddrFromMetaReplicas(metaReplicas []*MetaReplica) (addrs []string) {
	addrs = make([]string, 0)
	for _, metaReplica := range metaReplicas {
		addrs = append(addrs, metaReplica.Addr)
	}
	return
}

func convertPeersToArray(peers []Peer) (addrs []string) {
	addrs = make([]string, 0)
	for _, peer := range peers {
		addrs = append(addrs, peer.Addr)
	}
	return
}

// diff = array1 - array2
func diffSliceString(array1 []string, array2 ...[]string) (data []string) {
	if len(array2) == 0 {
		return array1
	}

	i := 0
loop:
	for {
		if i == len(array1) {
			break
		}
		v := array1[i]
		for _, arr := range array2 {
			for _, val := range arr {
				if v == val {
					i++
					continue loop
				}
			}
		}
		data = append(data, v)
		i++
	}
	return
}

func checkHostMetaPartition(host *ClusterHost) {
	defer func() {
		checkMpCorruptWg.Done()
		if r := recover(); r != nil {
			msg := fmt.Sprintf("checkHostMetaPartition panic:%v", r)
			log.LogError(msg)
			fmt.Println(msg)
		}
	}()
	log.LogInfof("check [%v] metaPartition corrupt begin", host)
	startTime := time.Now()
	volStats, _, err := getAllVolStat(host)
	if err != nil {
		log.LogErrorf("[%v] get allVolStat occurred error, err:%v", host, err)
		return
	}
	for _, vss := range volStats {
		var wg sync.WaitGroup
		mps, _ := getMetaPartitionsFromVolume(vss.Name, host)
		if mps == nil || len(mps) == 0 {
			log.LogWarnf("action[checkHostMetaPartition] get metaPartitions of volume failed, vol:%v", vss.Name)
			continue
		}
		mpChan := make(chan *MetaPartition, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(w *sync.WaitGroup, ch chan *MetaPartition) {
				defer w.Done()
				for mp := range ch {
					checkMetaPartitionPeers(host, vss.Name, mp.PartitionID)
					time.Sleep(time.Millisecond * 16)
				}
			}(&wg, mpChan)
		}
		for _, mp := range mps {
			mpChan <- mp
		}
		close(mpChan)
		wg.Wait()
	}
	log.LogInfof("check [%v] metaPartition corrupt end, cost [%v]", host, time.Since(startTime))
}

func checkRaftReplicaStatus(host *ClusterHost, replicaRaftStatusMap map[string]*raft.Status, pID uint64, volName, partitionType string, hosts []string, isRecover bool, replicas []*DataReplica, mp *MetaPartition) {
	if len(replicaRaftStatusMap) == 0 {
		return
	}
	checkRaftReplicaStatusOfPendingReplica(host, replicaRaftStatusMap, pID, volName, partitionType, hosts)
	if isRecover {
		return
	}
	// appliedID差值告警处理
	checkRaftReplicaStatusOfRaftAppliedDiff(host, replicaRaftStatusMap, pID, volName, partitionType, hosts)
	checkRaftStoppedReplica(host, replicaRaftStatusMap, pID, volName, partitionType, replicas, mp)
	if partitionType == partitionTypeMP {
		checkRaftInodeCountOrDentryCountDiff(host, replicaRaftStatusMap, pID, volName, mp)
	}
}

//不是reccover 状态， applied相同的情况下，inodeCount/dentryCout > 1000 电话告警
func checkRaftInodeCountOrDentryCountDiff(host *ClusterHost, replicaRaftStatusMap map[string]*raft.Status, pID uint64, volName string, mp *MetaPartition) {
	if len(replicaRaftStatusMap) == 0 {
		return
	}
	if mp == nil || len(mp.Replicas) == 0 {
		return
	}

	applieds := make([]uint64, 0)
	for _, status := range replicaRaftStatusMap {
		if status == nil || status.Stopped {
			continue
		}
		applieds = append(applieds, status.Applied)
	}
	if len(applieds) < 2 {
		return
	}
	baseAppliedID := applieds[0]
	for i := 1; i < len(applieds); i++ {
		if applieds[i] != baseAppliedID {
			return
		}
	}

	maxInodeCount := uint64(0)
	maxDentryCount := uint64(0)
	var minInodeCount uint64 = math.MaxUint64
	var minDentryCount uint64 = math.MaxUint64
	for _, replica := range mp.Replicas {
		if replica == nil {
			continue
		}
		if replica.InodeCount > maxInodeCount {
			maxInodeCount = replica.InodeCount
		}
		if replica.InodeCount < minInodeCount {
			minInodeCount = replica.InodeCount
		}

		if replica.DentryCount > maxDentryCount {
			maxDentryCount = replica.DentryCount
		}
		if replica.DentryCount < minDentryCount {
			minDentryCount = replica.DentryCount
		}
	}
	subDentry := int(maxDentryCount - minDentryCount)
	subInode := int(maxInodeCount - minInodeCount)
	if subDentry > defaultInodeCountWarnDiffCount || subInode > defaultInodeCountWarnDiffCount {
		replicas := make([]MetaReplica, 0, len(mp.Replicas))
		for _, replica := range mp.Replicas {
			replicas = append(replicas, *replica)
		}
		msg := fmt.Sprintf("Mp Inode/Dentry CountDIff host:%v id:%v replicas:%v", host, pID, replicas)
		checktool.WarnBySpecialUmpKey(UMPCFSInodeCountDiffWarnKey, msg)
	}
}

func checkRaftStoppedReplica(host *ClusterHost, replicaRaftStatusMap map[string]*raft.Status, pID uint64, volName, partitionType string, replicas []*DataReplica, mp *MetaPartition) {
	// raft stop告警
	var port string
	switch partitionType {
	case partitionTypeDP:
		port = host.getDataNodePProfPort()
	case partitionTypeMP:
		port = host.getMetaNodePProfPort()
		//PhyPID PartitionID  两者不相等的不进行检测
		if mp != nil && mp.PhyPID != 0 && mp.PhyPID != mp.PartitionID {
			return
		}
	default:
		return
	}
	stoppedReplicas := make([]string, 0)
	for replicaAddr, status := range replicaRaftStatusMap {
		if status != nil && status.Stopped {
			// 如果副本是坏盘，不需要告警
			if partitionType == partitionTypeDP && isReplicaStatusUnavailable(replicaAddr, replicas) {
				continue
			}
			//检查节点升级的状态，只有节点升级完成时，才进行告警
			nodeStatus, err := checkNodeStartStatus(fmt.Sprintf("%v:%v", strings.Split(replicaAddr, ":")[0], port), 5)
			if err == nil && nodeStatus.StartComplete == true {
				stoppedReplicas = append(stoppedReplicas, replicaAddr)
			}
		}
	}
	if len(stoppedReplicas) != 0 {
		msg := fmt.Sprintf("RaftStopped host:%v vol:%v %vid:%v replicas:%v", host, volName, partitionType, pID, stoppedReplicas)
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	}
}

func isReplicaStatusUnavailable(replicaAddr string, replicas []*DataReplica) (ok bool) {
	for _, replica := range replicas {
		if replicaAddr == replica.Addr && replica.Status == Unavailable {
			ok = true
		}
	}
	return
}

// 1.遍历 检查 是否有pending的
// 2.获取最大、最小applied计算差值，如果距离上次大于24小时/不存在 存入，否则（存在且小于24H）告警
// 如果差别的副本变更了，只更新记录，不告警
func checkRaftReplicaStatusOfPendingReplica(host *ClusterHost, replicaRaftStatusMap map[string]*raft.Status, pID uint64, volName, partitionType string, hosts []string) {
	var (
		maxPendQueueCount       int
		pendQueueAlarmThreshold int
	)
	switch partitionType {
	case partitionTypeDP:
		maxPendQueueCount = host.DPMaxPendQueueCount
		pendQueueAlarmThreshold = host.DPPendQueueAlarmThreshold
	case partitionTypeMP:
		maxPendQueueCount = host.MPMaxPendQueueCount
		pendQueueAlarmThreshold = host.MPPendQueueAlarmThreshold
	default:
		return
	}
	pendingReplica := make(map[string]*PartitionPending, 0)
	for replicaAddr, status := range replicaRaftStatusMap {
		if status == nil || status.Stopped {
			continue
		}
		if status.PendQueue > maxPendQueueCount {
			pendingReplica[replicaAddr] = &PartitionPending{
				lastCheckTime: time.Now(),
				Applied:       status.Applied,
				PendQueue:     status.PendQueue,
				ContinueCount: 1,
			}
		}
	}
	// pending 异常处理
	key := fmt.Sprintf("%v:%v", partitionType, pID)
	host.badPartitionPendingMapMutex.Lock()
	defer host.badPartitionPendingMapMutex.Unlock()
	if len(pendingReplica) == 0 { //本次没有新的 清理
		delete(host.badPartitionPendingMap, key)
		return
	}
	pendingReplicaMap, ok := host.badPartitionPendingMap[key]
	if ok { //存在: 本次也检索到 且 小于24小时，则告警
		for replica, partitionPending := range pendingReplicaMap {
			newPendingInfo, newChecked := pendingReplica[replica]
			if !newChecked { //本次没有
				continue
			}
			if time.Since(partitionPending.lastCheckTime) > time.Hour*24 { // 距离上次超过24小时 只记录新的
				continue
			}
			//如果 pending的数量小于1000 且 和上次比applied在正常推进 则pending正常
			if newPendingInfo.PendQueue < 1000 && newPendingInfo.Applied-partitionPending.Applied > 0 {
				continue
			}
			partitionPending.ContinueCount++
			newPendingInfo.ContinueCount = partitionPending.ContinueCount
			if partitionPending.ContinueCount >= pendQueueAlarmThreshold {
				msg := fmt.Sprintf("RaftPendQueueErr host:%v vol:%v %vid:%v replica:%v count:%v", host, volName, partitionType, pID, replica, newPendingInfo.PendQueue)
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
			}
		}
	}
	host.badPartitionPendingMap[key] = pendingReplica
}

func checkRaftReplicaStatusOfRaftAppliedDiff(host *ClusterHost, replicaRaftStatusMap map[string]*raft.Status, pID uint64, volName, partitionType string, hosts []string) {
	if len(replicaRaftStatusMap) == 0 {
		return
	}
	var (
		maxAppliedIDReplica   string
		minAppliedIDReplica   string
		maxAppliedIDDiffCount int
	)
	switch partitionType {
	case partitionTypeDP:
		maxAppliedIDDiffCount = host.DPMaxAppliedIDDiffCount
	case partitionTypeMP:
		maxAppliedIDDiffCount = host.MPMaxAppliedIDDiffCount
	default:
		return
	}
	maxAppliedID := uint64(0)
	var minAppliedID uint64 = math.MaxUint64
	for replicaAddr, status := range replicaRaftStatusMap {
		if status == nil || status.Stopped {
			continue
		}
		if status.Applied > maxAppliedID {
			maxAppliedID = status.Applied
			maxAppliedIDReplica = replicaAddr
		}
		if status.Applied < minAppliedID {
			minAppliedID = status.Applied
			minAppliedIDReplica = replicaAddr
		}
	}

	sub := int(maxAppliedID - minAppliedID)
	key := fmt.Sprintf("%v:%v", partitionType, pID)
	host.badPartitionAppliedMapMutex.Lock()
	defer host.badPartitionAppliedMapMutex.Unlock()
	if maxAppliedID == 0 || minAppliedID == math.MaxUint64 || len(hosts) == 0 || maxAppliedID <= minAppliedID || sub <= maxAppliedIDDiffCount {
		delete(host.badPartitionAppliedMap, key)
		return
	}
	diffReplica := []string{maxAppliedIDReplica, minAppliedIDReplica}
	sort.Strings(diffReplica)
	msg := fmt.Sprintf("RaftAppliedDiff host:%v volName:%v %vid:%v DiffCount:%v maxAppliedID:%v replica:%v,minAppliedID:%v replica:%v",
		host, volName, partitionType, pID, sub, maxAppliedID, maxAppliedIDReplica, minAppliedID, minAppliedIDReplica)
	log.LogDebug(msg)
	partitionApplied, ok := host.badPartitionAppliedMap[key]
	if ok {
		// 上次存在 且不大于24H，则告警
		if reflect.DeepEqual(partitionApplied.lastDiffReplica, diffReplica) && time.Since(partitionApplied.lastCheckTime) < time.Hour*24 {
			checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
		}
	}
	pApplied := &PartitionApplied{
		lastCheckTime:   time.Now(),
		lastDiffReplica: diffReplica,
		Sub:             sub,
	}
	host.badPartitionAppliedMap[key] = pApplied
}

func printRaftPendingAndAppliedResult(host *ClusterHost, partitionType string) {
	appliedCount := 0
	pendingCount := 0
	host.badPartitionAppliedMapMutex.Lock()
	for info, applied := range host.badPartitionAppliedMap {
		if !strings.HasPrefix(info, partitionType) {
			continue
		}
		appliedCount++
		msg := fmt.Sprintf("RaftAppliedDiff Result host:%v %v DiffCount:%v diffReplica:%v lastCheckTime:%v",
			host, info, applied.Sub, applied.lastDiffReplica, applied.lastCheckTime)
		log.LogDebug(msg)
	}
	host.badPartitionAppliedMapMutex.Unlock()

	host.badPartitionPendingMapMutex.Lock()
	sb := strings.Builder{}
	for info, pendingReplica := range host.badPartitionPendingMap {
		if !strings.HasPrefix(info, partitionType) {
			continue
		}
		pendingCount++
		for replica, pending := range pendingReplica {
			sb.WriteString(fmt.Sprintf("replica:%v detail:%v,", replica, *pending))
		}
		msg := fmt.Sprintf("RaftPendQueueErr Result host:%v %v pendingReplica:%v", host, info, sb.String())
		log.LogDebug(msg)
		sb.Reset()
	}
	host.badPartitionPendingMapMutex.Unlock()
	log.LogDebugf("action[printRaftPendingAndAppliedResult] host[%v] partitionType:%v appliedCount:%v pendingCount:%v", host, partitionType, appliedCount, pendingCount)
}
