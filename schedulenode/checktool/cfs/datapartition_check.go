package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/robfig/cron"
	"github.com/tiglabs/raft"
	"sort"
	"strings"
	"sync"
	"time"
)

type DNDataPartitionInfo struct {
	VolName              string       `json:"volName"`
	ID                   uint64       `json:"id"`
	Size                 int          `json:"size"`
	Used                 int          `json:"used"`
	Status               int          `json:"status"`
	Path                 string       `json:"path"`
	FileCount            int          `json:"fileCount"`
	Replicas             []string     `json:"replicas"`
	TinyDeleteRecordSize int64        `json:"tinyDeleteRecordSize"`
	Peers                []Peer       `json:"peers"`
	Learners             []Learner    `json:"learners"`
	RaftStatus           *raft.Status `json:"raftStatus"`
}

// DataReplica represents the replica of a data partition
type DataReplica struct {
	Addr            string
	ReportTime      int64
	FileCount       uint32
	Status          int8
	HasLoadResponse bool   // if there is any response when loading
	Total           uint64 `json:"TotalSize"`
	Used            uint64 `json:"UsedSize"`
	IsLeader        bool
	NeedsToCompare  bool
	DiskPath        string
}

type DataPartitionInfo struct {
	PartitionID             uint64
	LastLoadedTime          int64
	ReplicaNum              uint8
	Status                  int8
	IsRecover               bool
	Replicas                []*DataReplica
	Hosts                   []string // host addresses
	Peers                   []Peer
	Learners                []Learner
	Zones                   []string
	MissingNodes            map[string]int64 // key: address of the missing node, value: when the node is missing
	VolName                 string
	VolID                   uint64
	OfflinePeerID           uint64
	FilesWithMissingReplica map[string]int64 // key: file name, value: last time when a missing replica is found
}

// DataPartitionResponse defines the response from a data node to the master that is related to a data partition.
type DataPartitionResponse struct {
	PartitionID uint64
	Status      int8
	ReplicaNum  uint8
	Hosts       []string
	LeaderAddr  string
	Epoch       uint64
	IsRecover   bool
}

// VolView defines the view of a volume
type VolView struct {
	Name           string
	Owner          string
	Status         uint8
	FollowerRead   bool
	MetaPartitions []*MetaPartitionView
	DataPartitions []*DataPartitionResponse
	CreateTime     int64
}

func (s *ChubaoFSMonitor) scheduleToCheckDpPeerCorrupt() {
	crontab := cron.New()
	crontab.AddFunc("30 10,18 * * *", s.checkDpPeerCorrupt)
	crontab.Start()
}

func (s *ChubaoFSMonitor) checkDpPeerCorrupt() {
	dpCheckStartTime := time.Now()
	log.LogInfof("check all dataPartitions corrupt start")
	for _, host := range s.hosts {
		checkDpCorruptWg.Add(1)
		go checkHostDataPartition(host)
	}
	checkDpCorruptWg.Wait()
	log.LogInfof("check dataPartition corrupt end, cost [%v]", time.Since(dpCheckStartTime))
	// print mp/dp raft pending applied check result
	for _, host := range s.hosts {
		printRaftPendingAndAppliedResult(host, partitionTypeDP)
	}
}

func checkDataPartitionPeers(ch *ClusterHost, volName string, PartitionID uint64) {
	var (
		reqURL      string
		err         error
		badReplicas []PeerReplica
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[checkDataPartitionPeers] host[%v], vol[%v], id[%v], err[%v]", ch.host, volName,
				PartitionID, err)
		}
	}()
	badReplicas = make([]PeerReplica, 0)
	reqURL = fmt.Sprintf("http://%v/dataPartition/get?id=%v&name=%v", ch.host, PartitionID, volName)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	dp := &DataPartitionInfo{}
	if err = json.Unmarshal(data, dp); err != nil {
		log.LogErrorf("unmarshal to dp data[%v],err[%v]", string(data), err)
		return
	}
	replicaRaftStatusMap := make(map[string]*raft.Status, len(dp.Replicas)) // 不同副本的raft状态
	for _, r := range dp.Replicas {
		var dnPartition *DNDataPartitionInfo
		addr := strings.Split(r.Addr, ":")[0]
		//check dataPartition by dataNode api
		for i := 0; i < 3; i++ {
			if dnPartition, err = dataNodeGetPartition(ch, addr, dp.PartitionID); err == nil {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if !dp.IsRecover && err != nil {
			msg := fmt.Sprintf("Domain [%v], data partition [%v] volName [%v] load failed in replica: "+
				"addr[%v], may be raft start failed or partition is stopped by fatal error", ch.host, dp.PartitionID, volName, addr)
			if strings.Contains(err.Error(), "partition not exist") {
				checktool.WarnBySpecialUmpKey(UMPKeyDataPartitionLoadFailed, msg)
			}
			continue
		}
		if dnPartition == nil {
			continue
		}
		if !ch.isReleaseCluster {
			replicaRaftStatusMap[r.Addr] = dnPartition.RaftStatus
		}
		peerStrings := convertPeersToArray(dnPartition.Peers)
		sort.Strings(peerStrings)
		sort.Strings(dnPartition.Replicas)

		diffPeerToHost := diffSliceString(peerStrings, dnPartition.Replicas)
		diffHostToPeer := diffSliceString(dnPartition.Replicas, peerStrings)
		if len(peerStrings) == 4 && len(diffPeerToHost) == 1 && len(diffHostToPeer) == 0 {
			peerReplica := PeerReplica{
				VolName:       volName,
				ReplicationID: dp.PartitionID,
				PeerErrorInfo: "PEER4_HOST3",
				nodeAddr:      r.Addr,
				PeerAddr:      diffPeerToHost[0],
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if len(peerStrings) == 2 && len(diffHostToPeer) == 1 && len(diffPeerToHost) == 0 {
			peerReplica := PeerReplica{
				VolName:       volName,
				ReplicationID: dp.PartitionID,
				PeerErrorInfo: "HOST3_PEER2",
				nodeAddr:      r.Addr,
				PeerAddr:      diffHostToPeer[0],
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if len(peerStrings) == 3 && len(diffPeerToHost) == 1 && len(diffHostToPeer) == 0 {
			peerReplica := PeerReplica{
				VolName:       volName,
				ReplicationID: dp.PartitionID,
				PeerErrorInfo: "PEER3_HOST2",
				nodeAddr:      r.Addr,
				PeerAddr:      diffPeerToHost[0],
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if len(peerStrings) != len(dnPartition.Replicas) || len(diffPeerToHost)+len(diffHostToPeer) > 0 {
			msg := fmt.Sprintf("[Domain: %v, vol: %v, partition: %v, host: %v] peerStrings: %v , hostStrings: %v ", ch.host, volName, dp.PartitionID, r.Addr, peerStrings, dnPartition.Replicas)
			checktool.WarnBySpecialUmpKey(UMPKeyDataPartitionPeerInconsistency, msg)
		}
	}
	if !ch.isReleaseCluster {
		checkRaftReplicaStatus(ch, replicaRaftStatusMap, dp.PartitionID, dp.VolName, partitionTypeDP, dp.Hosts, dp.IsRecover, dp.Replicas, nil)
	}
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
		repairDp(ch.isReleaseCluster, ch.host, badReplicas[0])
	} else {
		for _, item := range badReplicas {
			msg := fmt.Sprintf("[Domain: %v, VolName: %v, ReplicationID: %v], nodeAddr: %v, PeerErrorInfo: %v, PeerAddr:%v ", ch.host, item.VolName, item.ReplicationID, item.nodeAddr, item.PeerErrorInfo, item.PeerAddr)
			checktool.WarnBySpecialUmpKey(UMPKeyDataPartitionPeerInconsistency, msg)
		}
	}
}

func getDataPartitionsFromVolume(ch *ClusterHost, volName string, authKey string) (vv *VolView, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[getDataPartitionsFromVolume] host[%v],vol[%v],err[%v]", ch.host, volName, err)
		}
	}()
	vv = &VolView{
		MetaPartitions: make([]*MetaPartitionView, 0),
		DataPartitions: make([]*DataPartitionResponse, 0),
	}
	reqURL := fmt.Sprintf("http://%v/client/vol?name=%v&authKey=%v", ch.host, volName, authKey)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		log.LogErrorf("request volView from [%v] failed ", ch.host)
		return
	}
	if err = json.Unmarshal(data, &vv); err != nil {
		log.LogErrorf("unmarshal to dp data[%v],err[%v]", string(data), err)
		return
	}
	return
}

func dataNodeGetPartition(ch *ClusterHost, addr string, id uint64) (node *DNDataPartitionInfo, err error) {
	var (
		port   string
		reqURL string
	)
	if ch.host == "id.chubaofs.jd.local" || ch.host == "th.chubaofs.jd.local" {
		port = "17320"
	} else if ch.host == "cn.chubaofs.jd.local" || ch.host == "idbbak.chubaofs.jd.local" || ch.host ==
		"cn.chubaofs-seqwrite.jd.local" || ch.host == "nl.chubaofs.jd.local" || ch.host == "cn.elasticdb.jd.local" || ch.host == "nl.chubaofs.ochama.com" {
		port = "6001"
	} else {
		log.LogInfof("action[checkDetaNodeDiskStat] host[%v] can not match its DN port", ch.host)
		return
	}
	reqURL = fmt.Sprintf("http://%v:%v/partition?id=%v", addr, port, id)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		log.LogErrorf("request dp from host: [%v] addr: [%v] failed , err: %v", ch.host, reqURL, err)
		return
	}
	dp := &node
	if err = json.Unmarshal(data, dp); err != nil {
		log.LogErrorf("[%v] unmarshal to dp data[%v],err[%v]", addr, string(data), err)
		return
	}
	return
}

func repairDp(release bool, host string, replica PeerReplica) {
	var outputStr string
	switch replica.PeerErrorInfo {
	case "HOST3_PEER2":
		if !release {
			if err := decommissionDp(host, replica.ReplicationID, replica.PeerAddr); err != nil {
				log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: HOST3_PEER2] repair failed, err:%v", host, replica.ReplicationID, err)
			} else {
				outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: HOST3_PEER2] has been automatically repaired, cmd[cfs-cli datapartition decommission %v %v]", host, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID)
			}
		} else {
			outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: HOST3_PEER2] recommond cmd: cfs-cli datapartition decommission %v %v", host, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID)
		}
	case "PEER4_HOST3":
		if !release {
			if err := addDpReplica(host, replica.ReplicationID, replica.PeerAddr); err != nil {
				log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER4_HOST3] repair-addDpReplica failed, err:%v", host, replica.ReplicationID, err)
				break
			}
			time.Sleep(time.Second * 3)
			if err := delDpReplica(host, replica.ReplicationID, replica.PeerAddr); err != nil {
				log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER4_HOST3] repair-delDpReplica failed, err:%v", host, replica.ReplicationID, err)
				break
			}
			outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER4_HOST3] has been automatically repaired, cmd[cfs-cli datapartition add-replica %v %v && sleep 3 && cfs-cli datapartition del-replica %v %v]",
				host, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID)
		} else {
			outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: PEER4_HOST3] recommond cmd: cfs-cli datapartition add-replica %v %v && sleep 3 && cfs-cli datapartition del-replica %v %v",
				host, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID)
		}
	case "HOST2_PEER3":
		outputStr = fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: HOST2_PEER3] recommond cmd: cfs-cli datapartition add-replica %v %v",
			host, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID)
	default:
		log.LogErrorf("wrong error info, %v", replica.PeerErrorInfo)
		return
	}
	checktool.WarnBySpecialUmpKey(UMPKeyDataPartitionPeerInconsistency, outputStr)
}

func decommissionDp(master string, partition uint64, addr string) (err error) {
	reqURL := fmt.Sprintf("http://%v/dataPartition/decommission?id=%v&addr=%v", master, partition, addr)
	_, err = doRequest(reqURL, false)
	return
}

func addDpReplica(master string, partition uint64, addr string) (err error) {
	reqURL := fmt.Sprintf("http://%v/dataReplica/add?id=%v&addr=%v", master, partition, addr)
	_, err = doRequest(reqURL, false)
	return
}

func delDpReplica(master string, partition uint64, addr string) (err error) {
	reqURL := fmt.Sprintf("http://%v/dataReplica/delete?id=%v&addr=%v", master, partition, addr)
	_, err = doRequest(reqURL, false)
	return
}

func checkHostDataPartition(host *ClusterHost) {
	defer func() {
		checkDpCorruptWg.Done()
		if r := recover(); r != nil {
			msg := fmt.Sprintf("checkHostDataPartition panic:%v", r)
			log.LogError(msg)
			fmt.Println(msg)
		}
	}()
	log.LogInfof("check [%v] dataPartition corrupt begin", host)
	startTime := time.Now()
	volStats, _, err := getAllVolStat(host)
	if err != nil {
		log.LogErrorf("[%v] get allVolStat occurred error, err:%v", host, err)
		return
	}
	for _, vss := range volStats {
		var wg sync.WaitGroup
		var vol *SimpleVolView
		vol, err = getVolSimpleView(vss.Name, host)
		if err != nil || vol == nil {
			log.LogErrorf("get volume simple view failed: %v err:%v", vss.Name, err)
			continue
		}
		authKey := checktool.Md5(vol.Owner)
		var volView *VolView
		if volView, err = getDataPartitionsFromVolume(host, vss.Name, authKey); err != nil {
			log.LogErrorf("Found an invalid vol: %v err:%v", vol.Name, err)
			continue
		}
		if volView.DataPartitions == nil || len(volView.DataPartitions) == 0 {
			log.LogWarnf("action[checkHostDataPartition] get dataPartitions from volView failed, vol:%v", vol.Name)
			continue
		}
		dpChan := make(chan *DataPartitionResponse, 20)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(w *sync.WaitGroup, ch chan *DataPartitionResponse) {
				defer w.Done()
				for dp := range ch {
					checkDataPartitionPeers(host, vol.Name, dp.PartitionID)
					time.Sleep(time.Millisecond * 13)
				}
			}(&wg, dpChan)
		}
		for _, dp := range volView.DataPartitions {
			dpChan <- dp
		}
		close(dpChan)
		wg.Wait()
	}

	log.LogInfof("check [%v] dataPartition corrupt end, cost [%v]", host, time.Since(startTime))
}
