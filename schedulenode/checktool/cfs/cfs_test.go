package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/config"
	"github.com/tiglabs/raft"
	"sort"
	"sync"
	"testing"
	"time"
)

var cfsMonitor *ChubaoFSMonitor

func init() {
	cfsMonitor = NewChubaoFSMonitor()
}

func TestExtractChubaoFSInfo(t *testing.T) {
	err := cfsMonitor.extractChubaoFSInfo(checktool.ReDirPath("cfsmaster.json"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(cfsMonitor.chubaoFSMasterNodes)
	}
}

func TestCheckMasterNodesAlive(t *testing.T) {
	err := cfsMonitor.extractChubaoFSInfo(checktool.ReDirPath("cfsmaster.json"))
	if err != nil {
		fmt.Println(err)
	}
	cfsMonitor.checkMasterNodesAlive()
}

func TestExtractMinRWDPAndMPVols(t *testing.T) {
	err := cfsMonitor.extractMinRWDPAndMPVols(checktool.ReDirPath("min_rw_dp_mp.json"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cfsMonitor.MinRWDPAndMPVols)
}

func TestCheckSpecificVols(t *testing.T) {
	err := cfsMonitor.extractMinRWDPAndMPVols(checktool.ReDirPath("min_rw_dp_mp.json"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cfsMonitor.MinRWDPAndMPVols)
	cfsMonitor.checkSpecificVols()
}

func TestServerStart(t *testing.T) {
	c := make(chan int)
	cfg, _ := config.LoadConfigFile(checktool.ReDirPath("cfg.json"))
	cfsMonitor.parseConfig(cfg)
	cfsMonitor.scheduleToCheckMasterNodesAlive()
	<-c
}

func TestScheduleToCheckXBPTicket(t *testing.T) {
	cfsm := NewChubaoFSMonitor()
	cfsm.hosts = append(cfsm.hosts, &ClusterHost{host: "192.168.168.110:49413", isReleaseCluster: false})
	cfsm.scheduleInterval = 20
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go cfsm.scheduleToCheckDiskError()
	go cfsm.scheduleToCheckXBPTicket()
	cv, err := getCluster(cfsm.hosts[0])
	if err == nil {
		cv.checkDataNodeAlive(cfsm.hosts[0], cfsm)
	}
	wg.Wait()
}

func TestGetDataNode(t *testing.T) {
	host := &ClusterHost{
		host:             "cn.chubaofs.jd.local",
		isReleaseCluster: false,
	}
	node, err := getDataNode(host, "11.127.75.213:6000")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(node.Addr, len(node.PersistenceDataPartitions))
}

func TestGetCluster(t *testing.T) {
	host := &ClusterHost{
		host:             "cn.chubaofs.jd.local",
		isReleaseCluster: false,
	}
	cv, err := getCluster(host)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cv.BadPartitionIDs)
	badDPsCount, err := getBadPartitionIDsCount(host)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(badDPsCount)
}

func TestExtractWarnFaultToUsers(t *testing.T) {
	err := cfsMonitor.extractWarnFaultToUsers(checktool.ReDirPath("cfsWarnFaultToUsers.json"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckAndWarnFaultToUsers(t *testing.T) {
	err := cfsMonitor.extractWarnFaultToUsers(checktool.ReDirPath("cfsWarnFaultToUsers.json"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(cfsMonitor.MinRWDPAndMPVols)
	cfsMonitor.initWarnFaultToTargetUsers()
	cfsMonitor.checkAndWarnFaultToUsers()
}

func TestUpdateMaxPendQueueAndMaxAppliedIDDiffCountByConfig(t *testing.T) {
	cfg, _ := config.LoadConfigFile(checktool.ReDirPath("cfg.json"))
	testCfs := ChubaoFSMonitor{}
	testCfs.updateMaxPendQueueAndMaxAppliedIDDiffCountByConfig(cfg)
}

func TestCheckRaftReplicaStatusOfPendingReplica(t *testing.T) {
	clusterHost := newClusterHost("test")
	clusterHost.DPMaxPendQueueCount = 0
	clusterHost.DPPendQueueAlarmThreshold = 2

	var pID uint64 = 1
	addr1 := "192.168.1.1:6001"
	addr2 := "192.168.1.1:6005"
	replicaRaftStatusMap := make(map[string]*raft.Status)
	replicaRaftStatusMap["192.168.1.1:6000"] = &raft.Status{PendQueue: 0}
	replicaRaftStatusMap[addr1] = &raft.Status{PendQueue: 1}
	checkRaftReplicaStatusOfPendingReplica(clusterHost, replicaRaftStatusMap, pID, "testVol", partitionTypeDP, nil)
	replicaRaftStatusMap[addr2] = &raft.Status{PendQueue: 5}
	checkRaftReplicaStatusOfPendingReplica(clusterHost, replicaRaftStatusMap, pID, "testVol", partitionTypeDP, nil)
	key := fmt.Sprintf("%v:%v", partitionTypeDP, pID)
	dpMap, ok := clusterHost.badPartitionPendingMap[key]
	if ok {
		if pending, ok := dpMap[addr1]; !ok || pending.ContinueCount != 2 {
			t.Errorf("badPartitionPendingMap should contain key:%v", addr1)
		} else {
			pending.lastCheckTime = time.Now().Add(-time.Hour * 25)
		}
		if pending, ok := dpMap[addr2]; !ok || pending.ContinueCount != 1 {
			t.Errorf("badPartitionPendingMap should contain key:%v", addr2)
		}
	} else {
		t.Errorf("badPartitionPendingMap should contain key:%v", key)
	}
	checkRaftReplicaStatusOfPendingReplica(clusterHost, replicaRaftStatusMap, pID, "testVol", partitionTypeDP, nil)
	dpMap, ok = clusterHost.badPartitionPendingMap[key]
	if ok {
		if pending, ok := dpMap[addr1]; !ok || pending.ContinueCount != 1 {
			t.Errorf("badPartitionPendingMap should contain key:%v", addr1)
		}
		if pending, ok := dpMap[addr2]; !ok || pending.ContinueCount != 2 {
			t.Errorf("badPartitionPendingMap should contain key:%v", addr2)
		}
	} else {
		t.Errorf("badPartitionPendingMap should contain key:%v", key)
	}
	checkRaftReplicaStatusOfPendingReplica(clusterHost, make(map[string]*raft.Status), 1, "testVol", partitionTypeDP, nil)
	if len(clusterHost.badPartitionPendingMap) != 0 {
		t.Errorf("badPartitionPendingMap should be 0 but get :%v", len(clusterHost.badPartitionPendingMap))
	}
}

func TestCheckRaftStoppedReplica(t *testing.T) {
	addr1 := "192.168.1.1:6001"
	clusterHost := newClusterHost("test")
	replicaRaftStatusMap := make(map[string]*raft.Status)
	replicaRaftStatusMap["192.168.1.3:6000"] = &raft.Status{PendQueue: 0}
	replicaRaftStatusMap[addr1] = &raft.Status{PendQueue: 1, Stopped: true}
	replicas := []*DataReplica{{Addr: "192.168.1.3:6000", Status: 0}}

	checkRaftStoppedReplica(clusterHost, replicaRaftStatusMap, 12, "test", partitionTypeDP, replicas, nil)
	replicas = append(replicas, &DataReplica{Addr: addr1, Status: Unavailable})
	checkRaftStoppedReplica(clusterHost, replicaRaftStatusMap, 12, "test", partitionTypeDP, replicas, nil)
	checkRaftStoppedReplica(clusterHost, replicaRaftStatusMap, 12, "test", partitionTypeMP, nil, nil)
}

func TestCheckMetaNodeDiskStatByMDCInfoFromSre(t *testing.T) {
	cfsm := NewChubaoFSMonitor()
	cfg, _ := config.LoadConfigFile(checktool.ReDirPath("cfg.json"))
	cfsm.parseSreDBConfig(cfg)
	cfsm.metaNodeExportDiskUsedRatio = cfg.GetFloat(cfgKeyMetaNodeExportDiskUsedRatio)
	if cfsm.metaNodeExportDiskUsedRatio <= 0 {
		fmt.Printf("parse %v failed use default value\n", cfgKeyMetaNodeExportDiskUsedRatio)
	}
	if cfsm.metaNodeExportDiskUsedRatio < minMetaNodeExportDiskUsedRatio {
		cfsm.metaNodeExportDiskUsedRatio = minMetaNodeExportDiskUsedRatio
	}
	cfsm.hosts = []*ClusterHost{{host: "cn.elasticdb.jd.local", isReleaseCluster: false}}
	cv, err := getCluster(cfsm.hosts[0])
	if err == nil {
		cv.checkMetaNodeDiskStatByMDCInfoFromSre(cfsm.hosts[0], cfsm)
	}
}

func TestScheduleToCheckIDMetaNodeDiskStat(t *testing.T) {
	cfsm := NewChubaoFSMonitor()
	cfsm.scheduleToCheckIDMetaNodeDiskStat()
}

func TestGetVolList(t *testing.T) {
	volInfos, err := GetVolList("cn.chubaofs.jd.local", false)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(len(volInfos))
}

func TestGetVolStatFromVolList(t *testing.T) {
	volStats, err := GetVolStatFromVolList("cn.chubaofs.jd.local", false)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(len(volStats))
}

func TestCompareGetClusterAndGetVolStatFromVolList(t *testing.T) {
	host := &ClusterHost{
		host:             "cn.chubaofs.jd.local",
		isReleaseCluster: false,
	}
	cv, err := getCluster(host)
	if err != nil {
		t.Fatal(err)
	}
	for _, vol := range cv.VolStatInfo {
		vol.TotalGB = vol.TotalSize / GB
		vol.UsedGB = vol.UsedSize / GB
	}
	if len(cv.VolStat) != len(cv.VolStatInfo) {
		t.Fatal(fmt.Sprintf("vol count not equal,len(cv.VolStat):%v, len(cv.VolStatInfo):%v ", len(cv.VolStat), len(cv.VolStatInfo)))
		return
	}
	sort.Slice(cv.VolStat, func(i, j int) bool {
		return cv.VolStat[i].Name > cv.VolStat[j].Name
	})
	sort.Slice(cv.VolStatInfo, func(i, j int) bool {
		return cv.VolStatInfo[i].Name > cv.VolStatInfo[j].Name
	})
	for i := 0; i < len(cv.VolStatInfo); i++ {
		cvVol := cv.VolStat[i]
		volStat := cv.VolStatInfo[i]
		if cvVol.Name != volStat.Name {
			t.Fatal(fmt.Sprintf("vol name not equal :%v:%v", cvVol.Name, volStat.Name))
		}
		if cvVol.TotalSize != volStat.TotalSize || cvVol.TotalGB != volStat.TotalGB {
			t.Fatal(fmt.Sprintf("vol:%v TotalSize or TotalGB not equal,TotalSize(%v:%v),TotalGB(%v:%v)",
				cvVol.Name, cvVol.TotalSize, volStat.TotalSize, cvVol.TotalGB, volStat.TotalGB))
		}
		fmt.Printf("vol:%v,UsedGB(%v:%v),UsedSize(%v:%v),UsedRatio(%v:%v)\n",
			cvVol.Name, cvVol.UsedGB, volStat.UsedGB, cvVol.UsedSize, volStat.UsedSize, cvVol.UsedRatio, volStat.UsedRatio)
	}
}

func TestCheckMasterLbPodStatus(t *testing.T) {
	cfsMonitor.checkMasterLbPodStatus()
}

func TestCheckObjectNodeAlive(t *testing.T) {
	cfsMonitor.checkObjectNodeAlive()
}
