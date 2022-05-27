package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"io"
	"math"
	"net/http"
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
	failedGetExtFd  *os.File
	successRepairFd *os.File
	failRepairFd    *os.File
	scanLimitCh     chan bool
	repairLimitCh   chan bool
	fdLock          sync.Mutex
}

type ExtentRepairInfo struct {
	HostStat map[string]RepairStatus `json:"host_stat"`
	ExtentID uint64                  `json:"extent_id"`
}

func (server *tinyExtentRepairServer) getRepairStatus(w http.ResponseWriter, _ *http.Request) {
	b, _ := json.Marshal(server.PartitionMap)
	w.Write(b)
}

func newRepairServer(limit uint64) (rServer *tinyExtentRepairServer) {
	rServer = new(tinyExtentRepairServer)
	rServer.PartitionMap = make(map[uint64][]*ExtentRepairInfo, 0)
	rServer.scanLimitCh = make(chan bool, limit)
	rServer.repairLimitCh = make(chan bool, 10)
	return
}
func (server *tinyExtentRepairServer) start() {
	fileNum, _ := os.OpenFile(fmt.Sprintf("tiny_extent_hole_diff_num%v.csv", time.Now().Unix()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	fileSize, _ := os.OpenFile(fmt.Sprintf("tiny_extent_hole_diff_size%v.csv", time.Now().Unix()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	fileAvailSize, _ := os.OpenFile(fmt.Sprintf("tiny_extent_avail_diff_size%v.csv", time.Now().Unix()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	fileGetFailExt, _ := os.OpenFile(fmt.Sprintf("tiny_extent_get_fail%v.csv", time.Now().Unix()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	successRepair, _ := os.OpenFile("success_tiny_extent_repair.csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	failRepair, _ := os.OpenFile("fail_tiny_extent_repair.csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	fileNum.WriteString("Volume,Partition,Extent,DiffHolesNum,DiffExtentSize,MinHolesNum,MinHolesNumHost,MaxHolesNum,MaxHolesNumHost,BadHostsNum,BadHosts,Hosts\n")
	fileSize.WriteString("Volume,Partition,Extent,DiffHolesSize,DiffExtentSize,MinHolesSize,MinHolesSizeHost,MaxHolesSize,MaxHolesSizeHost,BadHostsNum,BadHosts,Hosts\n")
	fileAvailSize.WriteString("Volume,Partition,Extent,DiffAvailSize,DiffExtentSize,MaxAvailSize,MaxAvailSizeHost,MinAvailSize,MinAvailSizeHost,BadHostsNum,BadHosts,Hosts\n")

	server.holeNumFd = fileNum
	server.holeSizeFd = fileSize
	server.availSizeFd = fileAvailSize
	server.failedGetExtFd = fileGetFailExt
	server.successRepairFd = successRepair
	server.failRepairFd = failRepair
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
	server.fdLock.Lock()
	defer server.fdLock.Unlock()
	fd.WriteString(s)
}

func (server *tinyExtentRepairServer) checkHolesAndRepair(autoRepair bool, vols []string, dps []uint64) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("panic err:%v", r)
		}
	}()
	server.start()
	defer server.stop()
	var existString = func(strs []string, target string) (exist bool) {
		for _, s := range strs {
			if s == target {
				exist = true
				return
			}
		}
		return
	}

	var existId = func(ids []uint64, target uint64) (exist bool) {
		for _, s := range ids {
			if s == target {
				exist = true
				return
			}
		}
		return
	}

	cv, err := client.AdminAPI().GetCluster()
	if err != nil {
		log.LogErrorf("err: %v", err)
		return
	}
	log.LogInfof("action[checkHolesAndRepair] cluster name: %v", cv.Name)
	for _, v := range cv.VolStatInfo {
		if len(vols) > 0 && !existString(vols, v.Name) {
			continue
		}
		volume, err1 := client.AdminAPI().GetVolumeSimpleInfo(v.Name)
		if err1 != nil {
			log.LogErrorf("admin get volume: %v, err: %v", v.Name, err1)
			continue
		}
		clv, err1 := client.ClientAPI().GetVolume(volume.Name, calcAuthKey(volume.Owner))
		if err1 != nil {
			log.LogErrorf("client get volume: %v, err: %v", v.Name, err1)
			continue
		}
		log.LogInfof("action[checkHolesAndRepair] scan volume:%v start", volume.Name)
		wg := sync.WaitGroup{}

		for _, dp := range clv.DataPartitions {
			if len(dps) > 0 && !existId(dps, dp.PartitionID) {
				continue
			}
			wg.Add(1)
			server.scanLimitCh <- true
			go func(partition *proto.DataPartitionResponse) {
				defer func() {
					wg.Done()
					<-server.scanLimitCh
				}()
				resultNum, resultSize, resultAvailSize, failExts, needRepairExtents := checkTinyExtents(volume.Name, partition.PartitionID, partition.Hosts)
				if resultNum != "" {
					server.writeFile(server.holeNumFd, resultNum)
				}
				if resultSize != "" {
					server.writeFile(server.holeSizeFd, resultSize)
				}
				if resultAvailSize != "" {
					server.writeFile(server.availSizeFd, resultAvailSize)
				}
				if failExts != "" {
					server.writeFile(server.failedGetExtFd, failExts)
				}
				if autoRepair && len(needRepairExtents) > 0 {
					for j := 0; j < checkRepeatTime; j++ {
						//等待一分钟，再检查一次，如果依然满足修复条件，则把它放入修复队列
						log.LogDebugf("action[checkTinyExtents] check partition:%v again before put it to repair slice after 1 minute", partition.PartitionID)
						time.Sleep(time.Minute)
						newNeedRepair := make([]*ExtentRepairInfo, 0)
						for _, needRepair := range needRepairExtents {
							_, _, _, _, eif, err2 := checkTinyExtent(volume.Name, partition.PartitionID, partition.Hosts, needRepair.ExtentID)
							if err2 != nil {
								log.LogErrorf("action[checkTinyExtents] checkTinyExtent, data partition:%v, extent:%v, err:%v", partition.PartitionID, needRepair.ExtentID, err2)
								continue
							}
							if eif != nil {
								newNeedRepair = append(newNeedRepair, eif)
								continue
							}
							log.LogWarnf("action[checkTinyExtents] data partition:%v, extent:%v, repaired by itself", partition.PartitionID, needRepair.ExtentID)
						}
						if len(newNeedRepair) == 0 {
							break
						}
						needRepairExtents = newNeedRepair
					}
					if len(needRepairExtents) > 0 {
						server.putToPartitionMap(partition.PartitionID, needRepairExtents)
						server.doRepair(needRepairExtents, partition.PartitionID)
					}
				}
			}(dp)
		}
		wg.Wait()
		log.LogInfof("action[checkHolesAndRepair] scan volume:%v, end", volume.Name)
		server.holeNumFd.Sync()
		server.holeSizeFd.Sync()
		server.availSizeFd.Sync()
		server.failedGetExtFd.Sync()
	}
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
			server.updateStat(pid, repairHost, 0, RepairFail)
			server.writeFile(server.failRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID))
			log.LogErrorf("action[doRepair] repairExtent, volume:%v, partition:%v, host:%v, path:%v, extent:%v, repair failed, err:%v", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID, err)
			return
		}
		server.writeFile(server.successRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID))
		server.updateStat(pid, repairHost, 0, RepairSuccess)
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
			server.updateStat(pid, repairHost, idx, RepairFail)
		}
		log.LogErrorf("action[doRepair] repairExtentBatch, volume:%v, partition:%v, host:%v, path:%v, extent:%v, repair failed, err:%v", dpFromMaster.VolName, pid, repairHost, dp.Path, needRepairExtents[0].ExtentID, err)
		return
	}
	successExtents := make([]uint64, 0)
	failedExtents := make([]uint64, 0)
	for idx, eif := range needRepairExtents {
		if extentsRepairResult[eif.ExtentID] != "OK" {
			failedExtents = append(failedExtents, eif.ExtentID)
			server.writeFile(server.failRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, eif.ExtentID))
			server.updateStat(pid, repairHost, idx, RepairFail)
		} else {
			successExtents = append(successExtents, eif.ExtentID)
			server.writeFile(server.successRepairFd, fmt.Sprintf("%v,%v,%v,%v,%v\n", dpFromMaster.VolName, pid, repairHost, dp.Path, eif.ExtentID))
			server.updateStat(pid, repairHost, idx, RepairSuccess)
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

//Deprecated 暂时没有用处
func (server *tinyExtentRepairServer) updateStat(pid uint64, host string, extentIdx int, newStat RepairStatus) {
	server.lock.Lock()
	defer server.lock.Unlock()
	if len(server.PartitionMap[pid]) < extentIdx+1 {
		return
	}
	server.PartitionMap[pid][extentIdx].HostStat[host] = newStat
}

func checkTinyExtent(volume string, partitionID uint64, partitionHosts []string, extent uint64) (resultNum, resultSize, resultAvailSize, failExtent string, eif *ExtentRepairInfo, err error) {
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
		ehs, err1 := dnHelper.GetExtentHoles(partitionID, extent)
		if err1 != nil {
			log.LogErrorf("action[checkTinyExtent] get extent holes failed, dp:%v, ext:%v, url:%v, err:%v", partitionID, extent, fmt.Sprintf("http://%v/tinyExtentHoleInfo?partitionID=%v&extentID=%v", sdnHost, partitionID, extent), err1)
			failExtent = fmt.Sprintf("%v,%v,%v,%v\n", volume, partitionID, extent, h)
			return
		}
		ext, err1 := dnHelper.GetExtentInfo(partitionID, extent)
		if err1 != nil {
			log.LogErrorf("action[checkTinyExtent] get extent failed, dp:%v, ext:%v, url:%v, err:%v", partitionID, extent, fmt.Sprintf("http://%v/extent?partitionID=%v&extentID=%v", sdnHost, partitionID, extent), err1)
			failExtent = fmt.Sprintf("%v,%v,%v,%v\n", volume, partitionID, extent, h)
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
	if (maxHolesNum - minHolesNum) > 1 {
		resultNum = fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n", volume, partitionID, extent, maxHolesNum-minHolesNum, maxExtentSize-minExtentSize, minHolesNum, minHolesNumHost,
			maxHolesNum, maxHolesNumHost, formatExtentInfo(dpExtInfos, func(ext *DpExtInfo) bool {
				if ext.HoleNum == uint32(minHolesNum) {
					return true
				}
				return false
			}), partitionHosts)
	}
	if (maxHolesSize - minHolesSize) > thresholdMb*util.MB {
		resultSize = fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n", volume, partitionID, extent, maxHolesSize-minHolesSize, maxExtentSize-minExtentSize, minHolesSize, minHolesSizeHost,
			maxHolesSize, maxHolesSizeHost, formatExtentInfo(dpExtInfos, func(ext *DpExtInfo) bool {
				if ext.HoleSize == minHolesSize {
					return true
				}
				return false
			}), partitionHosts)
	}
	if (maxAvailSize - minAvailSize) > thresholdMb*util.MB {
		resultAvailSize = fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n", volume, partitionID, extent, maxAvailSize-minAvailSize, maxExtentSize-minExtentSize, maxAvailSize, maxAvailSizeHost,
			minAvailSize, minAvailSizeHost, formatExtentInfo(dpExtInfos, func(ext *DpExtInfo) bool {
				if ext.AvailSize == maxAvailSize {
					return true
				}
				return false
			}), partitionHosts)
		//只修复Extent数据追平的数据,并且当且仅当待修复副本占少数的时候需要修复，如果副本超过一半，则认为多数的是正确的结果，比如在删除失败的场景下，往往少数副本Size更大。
		if maxExtentSize == minExtentSize && (maxAvailSize-minAvailSize) > thresholdMb*util.MB {
			eif = new(ExtentRepairInfo)
			eif.ExtentID = extent
			eif.HostStat = make(map[string]RepairStatus, 0)
			for _, e := range dpExtInfos {
				if e.AvailSize == maxAvailSize {
					continue
				}
				eif.HostStat[e.Host] = Origin
			}
			if len(eif.HostStat) >= len(partitionHosts)/2+1 {
				eif = nil
			}
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

func loadNeedRepairPartition() (ids []uint64) {
	ids = make([]uint64, 0)
	buf := make([]byte, 2048)
	var err error
	idsF, _ := os.OpenFile("ids", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer idsF.Close()
	o := bufio.NewReader(idsF)
	for {
		buf, _, err = o.ReadLine()
		if err == io.EOF {
			break
		}
		id, _ := strconv.ParseUint(string(buf), 10, 64)
		if id > 0 {
			ids = append(ids, id)
		}
	}
	return
}

func loadNeedRepairVolume() (vols []string) {
	volsF, _ := os.OpenFile("vols", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer volsF.Close()
	var err error
	r := bufio.NewReader(volsF)
	vols = make([]string, 0)
	buf := make([]byte, 2048)
	for {
		buf, _, err = r.ReadLine()
		if err == io.EOF {
			break
		}
		vols = append(vols, string(buf))
	}
	return
}

func checkTinyExtents(volume string, partitionID uint64, partitionHosts []string) (resultsNum, resultsSize, resultsAvailSize, resultsFailExts string, needRepairExtents []*ExtentRepairInfo) {
	if partitionID < 1 || volume == "" || len(partitionHosts) == 0 {
		return
	}
	needRepairExtents = make([]*ExtentRepairInfo, 0)
	for i := 1; i < 65; i++ {
		resultNum, resultSize, resultAvailSize, failExtent, eif, _ := checkTinyExtent(volume, partitionID, partitionHosts, uint64(i))
		if eif != nil {
			needRepairExtents = append(needRepairExtents, eif)
		}
		resultsNum += resultNum
		resultsSize += resultSize
		resultsAvailSize += resultAvailSize
		resultsFailExts += failExtent
	}
	log.LogInfof("action[checkTinyExtents] volume:%v, partition %v finish", volume, partitionID)
	return
}
