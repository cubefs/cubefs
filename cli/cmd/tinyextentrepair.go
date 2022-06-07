package cmd

import (

	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RepairStatus uint8

const (
	Origin RepairStatus = iota
	RepairSuccess
	RepairFail

	thresholdMb     = 1
	checkRepeatTime = 3
)

type DpExtInfo struct {
	Host       string
	ExtentSize uint64
	AvailSize  uint64
	HoleSize   uint64
	HoleNum    uint32
	BlockNum   uint64
}

type tinyExtentRepairServer struct {
	PartitionMap    map[uint64][]*ExtentRepairInfo `json:"partition_map"`
	lock            sync.Mutex
	holeNumFd       *os.File
	holeSizeFd      *os.File
	availSizeFd     *os.File
	hugeTinyFd      *os.File
	failedGetExtFd  *os.File
	successRepairFd *os.File
	failRepairFd    *os.File
	repairLimitCh   chan bool
	autoRepair      bool
	fdLock          sync.Mutex
}

type ExtentRepairInfo struct {
	HostStat map[string]RepairStatus `json:"host_stat"`
	ExtentID uint64                  `json:"extent_id"`
}

func newRepairServer(autoRepair bool) (rServer *tinyExtentRepairServer) {
	rServer = new(tinyExtentRepairServer)
	rServer.PartitionMap = make(map[uint64][]*ExtentRepairInfo, 0)
	rServer.repairLimitCh = make(chan bool, 10)
	rServer.autoRepair = autoRepair
	return
}

func (server *tinyExtentRepairServer) start() {
	server.holeNumFd, _ = os.OpenFile(fmt.Sprintf("tiny_extent_hole_diff_num_%v.csv", time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	server.holeSizeFd, _ = os.OpenFile(fmt.Sprintf("tiny_extent_hole_diff_size_%v.csv", time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	server.availSizeFd, _ = os.OpenFile(fmt.Sprintf("tiny_extent_avail_diff_size_%v.csv", time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	server.hugeTinyFd, _ = os.OpenFile(fmt.Sprintf("tiny_extent_huge_%v.csv", time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	server.failedGetExtFd, _ = os.OpenFile(fmt.Sprintf("tiny_extent_get_fail_%v.csv", time.Now().Format("2006010215")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	server.successRepairFd, _ = os.OpenFile("success_tiny_extent_repair.csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	server.failRepairFd, _ = os.OpenFile("fail_tiny_extent_repair.csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	if s, _ := server.holeNumFd.Stat(); s.Size() == 0 {
		server.holeNumFd.WriteString("Volume,Partition,Extent,DiffHolesNum,DiffExtentSize,MinHolesNum,MinHolesNumHost,MaxHolesNum,MaxHolesNumHost,BadHostsNum,BadHosts,Hosts\n")
	}
	if s, _ := server.holeSizeFd.Stat(); s.Size() == 0 {
		server.holeSizeFd.WriteString("Volume,Partition,Extent,DiffHolesSize,DiffExtentSize,MinHolesSize,MinHolesSizeHost,MaxHolesSize,MaxHolesSizeHost,BadHostsNum,BadHosts,Hosts\n")
	}
	if s, _ := server.availSizeFd.Stat(); s.Size() == 0 {
		server.availSizeFd.WriteString("Volume,Partition,Extent,DiffAvailSize,DiffExtentSize,MaxAvailSize,MaxAvailSizeHost,MinAvailSize,MinAvailSizeHost,BadHostsNum,BadHosts,Hosts\n")
	}
	if s, _ := server.hugeTinyFd.Stat(); s.Size() == 0 {
		server.hugeTinyFd.WriteString("Volume,Partition,MaxExtent,MaxExtentMaxReplica,MaxExtentMinReplica,MinExtent,MinExtentMaxReplicaMinExtentMinReplica,Hosts\n")
	}
}

func (server *tinyExtentRepairServer) stop() {
	server.holeNumFd.Close()
	server.holeSizeFd.Close()
	server.availSizeFd.Close()
	server.failedGetExtFd.Close()
	server.successRepairFd.Close()
	server.failRepairFd.Close()
}

func (server *tinyExtentRepairServer) writeFile(fd *os.File, s string) {
	if s == "" {
		return
	}
	server.fdLock.Lock()
	defer server.fdLock.Unlock()
	fd.WriteString(s)
}

func (server *tinyExtentRepairServer) putToPartitionMap(pid uint64, needRepairExtents []*ExtentRepairInfo) {
	server.lock.Lock()
	defer server.lock.Unlock()
	server.PartitionMap[pid] = needRepairExtents
}
func (server *tinyExtentRepairServer) doRepair(needRepairExtents []*ExtentRepairInfo, pid uint64) (err error) {
	server.repairLimitCh <- true
	defer func() {
		<-server.repairLimitCh
	}()
	if len(needRepairExtents) == 0 {
		log.LogWarnf("action[doRepair] partition:%v, no need repair extents", pid)
		return
	}

	var dp *proto.DNDataPartitionInfo
	var repairHost string
	//如果一个extent有多于1个副本需要修复，每次只修复一个host，等下次检查时在修复剩下的另一个host
	for host := range server.PartitionMap[pid][0].HostStat {
		repairHost = host
		break
	}
	sdnHost := fmt.Sprintf("%v:%v", strings.Split(repairHost, ":")[0], client.DataNodeProfPort)
	dnHelper := data.NewDataHttpClient(sdnHost, false)

	dp, err = dnHelper.GetPartitionFromNode(pid)
	if err != nil {
		log.LogErrorf("action[doRepair] partition:%v, host:%v, extent:%v, repair failed, err:%v", pid, repairHost, needRepairExtents[0].ExtentID, err)
		return
	}

	dpFromMaster, err := client.AdminAPI().GetDataPartition(dp.VolName, pid)
	if err != nil {
		log.LogErrorf("action[doRepair] partition:%v, host:%v, extent:%v, repair failed, err:%v", pid, repairHost, needRepairExtents[0].ExtentID, err)
		return
	}
	if dpFromMaster.IsRecover || len(dpFromMaster.MissingNodes) > 0 || dpFromMaster.Status == -1 || len(dpFromMaster.Hosts) != int(dpFromMaster.ReplicaNum) {
		err = fmt.Errorf("partition is in bad status, can not repair automatically, please check")
		log.LogErrorf("action[doRepair] volume:%v, partition:%v, host:%v, extent:%v, repair failed, err:%v", dpFromMaster.VolName, pid, repairHost, needRepairExtents[0].ExtentID, err)
		return
	}

	//repairExtent
	if len(needRepairExtents) == 1 {
		if err = dnHelper.RepairExtent(needRepairExtents[0].ExtentID, dp.Path, pid); err != nil {
			server.writeFile(server.failRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID))
			log.LogErrorf("action[doRepair] repairExtent, volume:%v, partition:%v, host:%v, path:%v, extent:%v, repair failed, err:%v", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID, err)
			return
		}
		server.writeFile(server.successRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID))
		log.LogInfof("action[doRepair] repairExtent, volume:%v, partition:%v, host:%v, path:%v, extent:%v, repair success", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID)
		server.successRepairFd.Sync()
		server.waitRepairFinish()
		return
	}

	//repairExtentBatch
	extents := make([]string, 0)
	for _, eif := range needRepairExtents {
		if _, ok := eif.HostStat[repairHost]; ok {
			extents = append(extents, strconv.Itoa(int(eif.ExtentID)))
		}
	}
	extentsRepairResult := make(map[uint64]string, 0)
	if extentsRepairResult, err = dnHelper.RepairExtentBatch(strings.Join(extents, "-"), dp.Path, pid); err != nil {
		for idx := range needRepairExtents {
			server.writeFile(server.failRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[idx].ExtentID))
		}
		log.LogErrorf("action[doRepair] repairExtentBatch, volume:%v, partition:%v, host:%v, path:%v, extent:%v, repair failed, err:%v", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID, err)
		return
	}
	successExtents := make([]uint64, 0)
	failedExtents := make([]uint64, 0)
	for _, eif := range needRepairExtents {
		if extentsRepairResult[eif.ExtentID] != "OK" {
			failedExtents = append(failedExtents, eif.ExtentID)
			server.writeFile(server.failRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, eif.ExtentID))
		} else {
			successExtents = append(successExtents, eif.ExtentID)
			server.writeFile(server.successRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, eif.ExtentID))
		}
	}
	log.LogInfof("action[doRepair] repairExtentBatch, volume:%v, partition:%v, host:%v, path:%v, success extents:%v, failed extents:%v", dpFromMaster.VolName, pid, repairHost, dp.Path, successExtents, failedExtents)
	server.successRepairFd.Sync()
	server.waitRepairFinish()
	return
}

//todo 进行修复校验
func (server *tinyExtentRepairServer) waitRepairFinish() {
	time.Sleep(time.Minute * 2)
}

func checkTinyExtent(volume string, partitionID uint64, partitionHosts []string, extent uint64) (resultNum, resultSize, resultAvailSize string, maxsize uint64, minSize uint64, eif *ExtentRepairInfo, err error) {
	var (
		maxHolesSize     uint64
		maxHolesSizeHost string
		maxHolesNum      int
		maxHolesNumHost  string
		minHolesSize     uint64
		minHolesSizeHost string
		minHolesNum      int
		minHolesNumHost  string
		maxAvailSize     uint64
		minAvailSize     uint64
		maxAvailSizeHost string
		minAvailSizeHost string
		maxExtentSize    uint64
		minExtentSize    uint64
	)
	minHolesNum = math.MaxUint32
	minHolesSize = math.MaxUint64
	minAvailSize = math.MaxUint64
	minExtentSize = math.MaxUint64
	dpExtInfos := make([]*DpExtInfo, 0)

	for _, h := range partitionHosts {
		sdnHost := fmt.Sprintf("%v:%v", strings.Split(h, ":")[0], client.DataNodeProfPort)
		dnHelper := data.NewDataHttpClient(sdnHost, false)
		var ehs *proto.DNTinyExtentInfo
		var ext *proto.ExtentInfoBlock
		ehs, err = dnHelper.GetExtentHoles(partitionID, extent)
		if err != nil {
			log.LogErrorf("action[checkTinyExtent] get extent holes failed, dp:%v, ext:%v, url:%v, err:%v", partitionID, extent, fmt.Sprintf("http://%v/tinyExtentHoleInfo?partitionID=%v&extentID=%v", sdnHost, partitionID, extent), err)
			return
		}
		ext, err = dnHelper.GetExtentInfo(partitionID, extent)
		if err != nil {
			log.LogErrorf("action[checkTinyExtent] get extent failed, dp:%v, ext:%v, url:%v, err:%v", partitionID, extent, fmt.Sprintf("http://%v/extent?partitionID=%v&extentID=%v", sdnHost, partitionID, extent), err)
			return
		}
		if len(ehs.Holes) > maxHolesNum {
			maxHolesNum = len(ehs.Holes)
			maxHolesNumHost = h
		}
		if len(ehs.Holes) < minHolesNum {
			minHolesNum = len(ehs.Holes)
			minHolesNumHost = h
		}
		var totalSize uint64
		for _, hole := range ehs.Holes {
			totalSize += hole.Size
		}
		if totalSize > maxHolesSize {
			maxHolesSize = totalSize
			maxHolesSizeHost = h
		}
		if totalSize < minHolesSize {
			minHolesSize = totalSize
			minHolesSizeHost = h
		}
		if ehs.ExtentAvaliSize < minAvailSize {
			minAvailSize = ehs.ExtentAvaliSize
			minAvailSizeHost = h
		}
		if ehs.ExtentAvaliSize > maxAvailSize {
			maxAvailSize = ehs.ExtentAvaliSize
			maxAvailSizeHost = h
		}
		if ext[proto.ExtentInfoSize] < minExtentSize {
			minExtentSize = ext[proto.ExtentInfoSize]
		}
		if ext[proto.ExtentInfoSize] > maxExtentSize {
			maxExtentSize = ext[proto.ExtentInfoSize]
		}
		dpExtInfos = append(dpExtInfos, &DpExtInfo{
			Host:       h,
			AvailSize:  ehs.ExtentAvaliSize,
			HoleSize:   totalSize,
			HoleNum:    uint32(len(ehs.Holes)),
			ExtentSize: ext[proto.ExtentInfoSize],
			BlockNum:   ehs.BlocksNum,
		})
	}
	maxsize = maxExtentSize
	minSize = minExtentSize
	if (maxHolesNum-minHolesNum) > 0 && maxExtentSize == minExtentSize {
		resultNum = fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n", volume, partitionID, extent, maxHolesNum-minHolesNum, maxExtentSize-minExtentSize, minHolesNum, minHolesNumHost,
			maxHolesNum, maxHolesNumHost, formatExtentInfo(dpExtInfos, func(ext *DpExtInfo) bool {
				if ext.HoleNum == uint32(minHolesNum) {
					return true
				}
				return false
			}), partitionHosts)
	}
	if (maxHolesSize-minHolesSize) > thresholdMb*util.MB && maxExtentSize == minExtentSize {
		resultSize = fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n", volume, partitionID, extent, maxHolesSize-minHolesSize, maxExtentSize-minExtentSize, minHolesSize, minHolesSizeHost,
			maxHolesSize, maxHolesSizeHost, formatExtentInfo(dpExtInfos, func(ext *DpExtInfo) bool {
				if ext.HoleSize == minHolesSize {
					return true
				}
				return false
			}), partitionHosts)
	}
	if (maxAvailSize-minAvailSize) > thresholdMb*util.MB && maxExtentSize == minExtentSize {
		resultAvailSize = fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n", volume, partitionID, extent, maxAvailSize-minAvailSize, maxExtentSize-minExtentSize, maxAvailSize, maxAvailSizeHost,
			minAvailSize, minAvailSizeHost, formatExtentInfo(dpExtInfos, func(ext *DpExtInfo) bool {
				if ext.AvailSize == maxAvailSize {
					return true
				}
				return false
			}), partitionHosts)
		eif = new(ExtentRepairInfo)
		eif.ExtentID = extent
		eif.HostStat = make(map[string]RepairStatus, 0)
		for _, e := range dpExtInfos {
			if e.AvailSize == maxAvailSize {
				continue
			}
			eif.HostStat[e.Host] = Origin
		}
		//当且仅当待修复副本占少数的时候需要修复，如果副本超过一半，则认为多数的是正确的结果，比如在删除失败的场景下，往往少数副本Size更大。
		if len(eif.HostStat) >= len(partitionHosts)/2+1 {
			eif = nil
		}
	}
	return
}

func formatExtentInfo(exts []*DpExtInfo, skipFunc func(ext *DpExtInfo) bool) string {
	result := make([]string, 0)
	for _, ext := range exts {
		if skipFunc(ext) {
			continue
		}
		result = append(result, fmt.Sprintf("%v_%v_%v_%v_%v_%v", ext.Host, ext.HoleSize, ext.HoleNum, ext.AvailSize, ext.BlockNum, ext.ExtentSize))
	}
	return strconv.Itoa(len(result)) + "," + strings.Join(result, ";")
}

func (server *tinyExtentRepairServer) checkAndRepairTinyExtents(volume string, partition *proto.DataPartitionResponse) (err error) {
	var resultsNum, resultsSize, resultsAvailSize, resultHugeExt, resultsFailExts string
	var needRepairExtents []*ExtentRepairInfo
	if partition.PartitionID < 1 || volume == "" || len(partition.Hosts) == 0 {
		return
	}
	needRepairExtents = make([]*ExtentRepairInfo, 0)
	var maxSizeTinyInfo = &struct {
		maxSize  uint64
		minSize  uint64
		extentID int
	}{}
	var minSizeTinyInfo = &struct {
		maxSize  uint64
		minSize  uint64
		extentID int
	}{
		maxSize: math.MaxUint64,
	}
	for i := 1; i < 65; i++ {
		var resultNum, resultSize, resultAvailSize string
		var minTinyReplicaSize uint64
		var maxTinyReplicaSize uint64
		var needRepairExtent *ExtentRepairInfo
		for j := 0; j < 3; j++ {
			resultNum, resultSize, resultAvailSize, maxTinyReplicaSize, minTinyReplicaSize, needRepairExtent, err = checkTinyExtent(volume, partition.PartitionID, partition.Hosts, uint64(i))
			if resultAvailSize == "" && err == nil {
				break
			}
			time.Sleep(10 * time.Second)
		}
		if err != nil {
			resultsFailExts += fmt.Sprintf("%v,%v,%v\n", volume, partition.PartitionID, i)
			continue
		}
		if needRepairExtent != nil {
			needRepairExtents = append(needRepairExtents, needRepairExtent)
		}
		if minSizeTinyInfo.maxSize > maxTinyReplicaSize {
			minSizeTinyInfo.maxSize = maxTinyReplicaSize
			minSizeTinyInfo.maxSize = maxTinyReplicaSize
			minSizeTinyInfo.minSize = minTinyReplicaSize
			minSizeTinyInfo.extentID = i
		}
		if maxSizeTinyInfo.maxSize < maxTinyReplicaSize {
			maxSizeTinyInfo.maxSize = maxTinyReplicaSize
			maxSizeTinyInfo.maxSize = maxTinyReplicaSize
			maxSizeTinyInfo.minSize = minTinyReplicaSize
			maxSizeTinyInfo.extentID = i
		}
		resultsNum += resultNum
		resultsSize += resultSize
		resultsAvailSize += resultAvailSize
	}
	if (minSizeTinyInfo.maxSize > 0 && (maxSizeTinyInfo.maxSize/minSizeTinyInfo.maxSize > 50) && (maxSizeTinyInfo.maxSize-minSizeTinyInfo.maxSize > 100*util.MB)) || maxSizeTinyInfo.maxSize > 10*util.TB {
		resultHugeExt = fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v\n", volume, partition.PartitionID, maxSizeTinyInfo.extentID, maxSizeTinyInfo.maxSize, maxSizeTinyInfo.minSize, minSizeTinyInfo.extentID, minSizeTinyInfo.maxSize, minSizeTinyInfo.minSize, partition.Hosts)
	}
	log.LogInfof("action[checkAndRepairTinyExtents] volume:%v, partition %v finish", volume, partition.PartitionID)

	server.writeFile(server.holeNumFd, resultsNum)
	server.writeFile(server.holeSizeFd, resultsSize)
	server.writeFile(server.availSizeFd, resultsAvailSize)
	server.writeFile(server.failedGetExtFd, resultHugeExt)
	server.writeFile(server.hugeTinyFd, resultHugeExt)
	if server.autoRepair && len(needRepairExtents) > 0 {
		server.putToPartitionMap(partition.PartitionID, needRepairExtents)
		server.doRepair(needRepairExtents, partition.PartitionID)
	}
	return
}
