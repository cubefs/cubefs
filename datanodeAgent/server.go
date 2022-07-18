package datanodeAgent

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"hash/crc32"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	cfgProf             = "prof"
	defDataListen       = "6000"
	defDataProf         = "6001"
	cfgDataNodeCfgPath  = "dataCfgPath"
	cfgDataNodeDiskPath = "disks"
	cfgDisk             = "disk"
	cfgMod              = "mod"
	cfgHour             = "hour"
	cfgForce            = "force"
	paramPath           = "path"
	repairDirStr        = "repair_extents_backup"
)

type Extent struct {
	ExtentID   uint64
	Crc        uint32
	Size       int64
}

type DataPartition struct {
	PartitionID   uint64
	ExtentMap     map[uint64]*proto.ExtentInfoBlock
}

var regexpDataPartitionDir, _ = regexp.Compile("^datapartition_(\\d)+_(\\d)+$")
var masterClient = master.NewMasterClient(nil, false)

// APIResponse defines the structure of the response to an HTTP request
type APIResponse struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data,omitempty"`
}

// NewAPIResponse returns a new API response.
func NewAPIResponse(code int, msg string) *APIResponse {
	return &APIResponse{
		Code: code,
		Msg:  msg,
	}
}

// Marshal is a wrapper function of json.Marshal
func (api *APIResponse) Marshal() ([]byte, error) {
	return json.Marshal(api)
}

type DataNodeAgent struct {
	dataNodePprofPort  string
	dataNodeListenPort string
	agentProfPort      string
	localServerAddr    string
	disks              []string
	beforeRepairFunc   func(partitionID uint64) error
	repairFunc         func(repairTask *repairTask) error

	//afterRepairFunc ("/data1", "datapartition_2_128849018880")
	afterRepairFunc    func(diskPath, partitionPath string) error
	control            common.Control
}

func NewServer() *DataNodeAgent {
	return &DataNodeAgent{}
}

func (m *DataNodeAgent) Start(cfg *config.Config) (err error) {
	return m.control.Start(m, cfg, doStart)
}

// Shutdown stops the node.
func (m *DataNodeAgent) Shutdown() {
	m.control.Shutdown(m, doShutdown)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	m, ok := s.(*DataNodeAgent)
	if !ok {
		return errors.New("Invalid Node Type!")
	}


	if err = m.parseConfig(cfg); err != nil {
		return
	}
	m.registerAPIHandler()
	m.beforeRepairFunc = func(partitionID uint64) (err error) {
		return
	}
	m.afterRepairFunc = func(diskPath, partitionPath string) (err error) {
		return
	}
	return
}

func (m *DataNodeAgent) parseConfig(cfg *config.Config) (err error) {
	if cfg == nil {
		err = errors.New("invalid configuration")
		return
	}
	m.agentProfPort = cfg.GetString(cfgProf)

	cfgData, err := config.LoadConfigFile(cfg.GetString(cfgDataNodeCfgPath))
	if err != nil {
		return
	}
	for _, ip := range cfgData.GetSlice(proto.MasterAddr) {
		masterClient.AddNode(ip.(string))
	}
	m.dataNodePprofPort = cfgData.GetString("prof")
	if len(strings.TrimSpace(m.dataNodePprofPort)) == 0 {
		m.dataNodePprofPort = defDataProf
	}
	m.dataNodeListenPort = cfgData.GetString("listen")
	if len(strings.TrimSpace(m.dataNodeListenPort)) == 0 {
		m.dataNodeListenPort = defDataListen
	}
	m.LoadDisk(cfgData)

	var ci *proto.ClusterInfo
	var LocalIP string
	if ci, err = masterClient.AdminAPI().GetClusterInfo(); err != nil {
		log.LogErrorf("action[registerToMaster] cannot get ip from master(%v) err(%v).",
			masterClient.Leader(), err)
		return
	}
	if LocalIP == "" {
		LocalIP = ci.Ip
	}
	m.localServerAddr = fmt.Sprintf("%s:%v", LocalIP, m.dataNodeListenPort)
	return
}

func (m *DataNodeAgent) LoadDisk(cfg *config.Config) error {
	for _, d := range cfg.GetSlice(cfgDataNodeDiskPath) {
		log.LogDebugf("load disk raw config(%v).", d)
		arr := strings.Split(d.(string), ":")
		if len(arr) == 0 {
			return errors.New("Invalid disk configuration. Example: PATH[:RESERVE_SIZE]")
		}
		path := arr[0]
		fileInfo, err := os.Stat(path)
		if err != nil {
			log.LogErrorf("stat disk [%v] failed: %v", path, err)
			continue
		}
		if !fileInfo.IsDir() {
			return errors.New("Disk path is not dir")
		}
		m.disks = append(m.disks, path)
	}
	return nil
}

func (m *DataNodeAgent) RepairExtentCrcOnDisk(diskPath string, hour int) (failedTasks, successTasks []*repairTask, err error){
	failedTasks = make([]*repairTask, 0)
	successTasks = make([]*repairTask, 0)
	defer func() {
		if err != nil {
			log.LogErrorf("action[RepairExtentCrcOnDisk] from disk(%v) err(%v) ", diskPath, err.Error())
		}
	}()
	var dInfo *DataNodeInfo
	dInfo, err = m.getPersistPartitionsFromMaster()
	if err != nil {
		return
	}
	fList, err := ioutil.ReadDir(diskPath)
	if err != nil {
		return
	}
	if len(dInfo.PersistenceDataPartitions) == 0 {
		log.LogWarnf("action[RepairExtentCrcOnDisk]: length of PersistenceDataPartitions is 0, ExpiredPartition check " +
			"without effect")
		return
	}

	for _, fInfo := range fList {
		failedDpTasks, successDpTasks, err1 := m.repairPartition(diskPath, fInfo.Name(), dInfo.PersistenceDataPartitions, hour)
		if err1 == nil {
			failedTasks = append(failedTasks, failedDpTasks...)
			successTasks = append(successTasks, successDpTasks...)
		}
	}
	return
}

func (m *DataNodeAgent) repairPartition(diskPath, partitionDir string, persistPartitions []uint64, hour int) (failedTasks, successTasks []*repairTask, err error){
	var (
		partitionID       uint64
		localExtents      map[uint64]*proto.ExtentInfoBlock
		remoteExtentsMaps []map[uint64]*proto.ExtentInfoBlock
		repairTasks       []*repairTask
		partition         *proto.DataPartitionInfo
		quorum            int
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[repairPartition] err:%v", err)
		}
	}()
	failedTasks = make([]*repairTask, 0)
	successTasks = make([]*repairTask, 0)
	if !regexpDataPartitionDir.MatchString(partitionDir) {
		log.LogWarnf("action[repairPartition] partition path[%v] is not valid", partitionDir)
		return
	}
	if partitionID, _, err = unmarshalPartitionName(partitionDir); err != nil {
		err = fmt.Errorf("action[repairPartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
			partitionDir, diskPath, err.Error())
		return
	}
	if isExpiredPartition(partitionID, persistPartitions) {
		err = fmt.Errorf("action[repairPartition]: find expired partition[%s] "+
			"manually", partitionDir)
		return
	}
	partition, err = m.getPartitionFromMaster(partitionID)
	if err != nil {
		return
	}
	localExtents, err = getNormalExtents(diskPath, partitionDir, int64(60 * 60 * hour))
	if err != nil {
		return
	}
	remoteExtentsMaps, err = m.getRemoteExtentInfo(partition, partitionDir)
	if err != nil {
		return
	}

	quorum = int(partition.ReplicaNum) / 2 + 1
	validExtents := prepareValidExtents(quorum, localExtents, remoteExtentsMaps)
	repairTasks, err = m.prepareRepairExtentsTasks(localExtents, validExtents, diskPath + "/" + partitionDir, partitionID)
	if err != nil {
		return
	}
	if len(repairTasks) == 0 {
		return
	}
	err = m.beforeRepairFunc(partitionID)
	if err != nil {
		return
	}
	for _, task := range repairTasks {
		err = m.repairFunc(task)
		if err == nil {
			successTasks = append(successTasks, task)
		} else {
			task.Msg = err.Error()
			failedTasks = append(failedTasks, task)
		}
	}
	err = m.afterRepairFunc(diskPath, partitionDir)
	return
}

func (m *DataNodeAgent) getRemoteExtentInfo(partition *proto.DataPartitionInfo, partitionDir string) (remoteExtentsMaps []map[uint64]*proto.ExtentInfoBlock, err error){
	remoteExtentsMaps = make([]map[uint64]*proto.ExtentInfoBlock, 0)
	var lock sync.Mutex
	wg := sync.WaitGroup{}
	for _, h := range partition.Hosts {
		wg.Add(1)
		go func(host string) {
			var extentsMap map[uint64]*proto.ExtentInfoBlock
			defer wg.Done()
			ip := strings.Split(host, ":")
			if host == m.localServerAddr {
				return
			}
			dataAgentClient := data.NewDataHttpClient(ip[0] + ":" + m.agentProfPort, false)
			if extentsMap, err = dataAgentClient.FetchExtentsCrc(partitionDir); err != nil {
				return
			}
			lock.Lock()
			defer lock.Unlock()
			remoteExtentsMaps = append(remoteExtentsMaps, extentsMap)
		}(h)
	}
	wg.Wait()
	return
}
func prepareValidExtents(quorum int, localExtentsMap map[uint64]*proto.ExtentInfoBlock, remoteExtentsMaps []map[uint64]*proto.ExtentInfoBlock) (validExtents map[uint64]*proto.ExtentInfoBlock) {
	validExtents = make(map[uint64]*proto.ExtentInfoBlock, 0)
	for id := range localExtentsMap {
		crcMap := make(map[uint64]int, 0)
		for _, extMap := range remoteExtentsMaps {
			if _, ok := extMap[id]; !ok {
				continue
			}
			if extMap[id][proto.FileID] == 0 {
				continue
			}
			if _, ok := crcMap[extMap[id][proto.Crc]]; !ok {
				crcMap[extMap[id][proto.Crc]] = 1
			} else {
				crcMap[extMap[id][proto.Crc]] += 1
				if crcMap[extMap[id][proto.Crc]] >= quorum {
					validExtents[id] = extMap[id]
				}
			}
		}
	}
	return
}

type repairTask struct {
	ExtentDir   string
	PartitionID uint64
	LocalExtent *proto.ExtentInfoBlock
	ValidExtent *proto.ExtentInfoBlock
	Msg         string
}

func (m *DataNodeAgent) prepareRepairExtentsTasks(localExtentsMap map[uint64]*proto.ExtentInfoBlock, validExtents map[uint64]*proto.ExtentInfoBlock, extentDir string, partitionID uint64) (repairTasks []*repairTask, err error) {
	repairTasks = make([]*repairTask, 0)
	for _, ext := range localExtentsMap {
		if _, ok := validExtents[ext[proto.FileID]]; !ok {
			continue
		}
		if (ext[proto.Size] > 0 && validExtents[ext[proto.FileID]][proto.Size] > ext[proto.Size]) || (validExtents[ext[proto.FileID]][proto.Crc] != ext[proto.Crc] && validExtents[ext[proto.FileID]][proto.Size] == ext[proto.Size] ) {
			repairTasks = append(repairTasks, &repairTask{
				ExtentDir:   extentDir,
				PartitionID: partitionID,
				LocalExtent: ext,
				ValidExtent: validExtents[ext[proto.FileID]],
			})
		}
	}
	return
}

func getNormalExtents(diskPath, partitionDir string, duration int64) (extentsMap map[uint64]*proto.ExtentInfoBlock, err error) {
	timeNow := time.Now().Unix()
	extListFd, err := ioutil.ReadDir(diskPath + "/" + partitionDir)
	if err != nil {
		return
	}
	extentsMap = make(map[uint64]*proto.ExtentInfoBlock, 0)
	limitCh := make(chan bool, 10)
	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	for _, ef := range extListFd {
		var (
			eid uint64
			err1 error
		)
		if eid, err1 = strconv.ParseUint(ef.Name(), 10, 64); err1 != nil {
			continue
		}
		if eid < 65 {
			continue
		}
		limitCh <- true
		wg.Add(1)
		go func(extentPath string, extentID uint64, nowTime, duration int64) {
			defer func() {
				wg.Done()
				<- limitCh
			}()
			extentInfo, err2 := packExtentInfo(extentPath, extentID, nowTime, duration)
			if err2 != nil {
				return
			}
			lock.Lock()
			defer lock.Unlock()
			extentsMap[extentID] = extentInfo
		}(diskPath + "/" + partitionDir + "/" + ef.Name(), eid, timeNow, duration)
	}
	wg.Wait()
	log.LogInfof("action[getNormalExtents] partition path[%v] total extents[%v]", partitionDir, len(extentsMap))
	return
}

func packExtentInfo(extentPath string, extentID uint64, timeNow int64, duration int64) (extentInfo *proto.ExtentInfoBlock, err error){
	efd, err := os.OpenFile(extentPath, os.O_RDONLY, 0644)
	if err != nil {
		log.LogErrorf("open file failed, extent:%v, err:%v", extentID, err)
		return nil, err
	}
	stat, err := efd.Stat()
	if err != nil {
		return nil, err
	}
	if duration == 0 || stat.ModTime().Unix() >= timeNow - duration {
		crc, err1 := computeExtentCrc(efd, stat.Size())
		if err1 != nil {
			return nil, err1
		}
		extentInfo = &proto.ExtentInfoBlock{
			extentID,
			uint64(stat.Size()),
			uint64(crc),
			uint64(stat.ModTime().Unix()),
		}
		return extentInfo, nil
	}
	return nil, fmt.Errorf("skip extent")
}

func computeExtentCrc(fd *os.File, size int64) (crc uint32, err error) {
	var blockCnt int
	blockCnt = int(size / util.BlockSize)
	if size%util.BlockSize != 0 {
		blockCnt += 1
	}
	crcData := make([]byte, blockCnt*util.PerBlockCrcSize)
	for blockNo := 0; blockNo < blockCnt; blockNo++ {
		dataSize := math.Min(util.BlockSize, float64(size - int64(blockNo*util.BlockSize)))
		bdata := make([]byte, int(dataSize))
		offset := int64(blockNo * util.BlockSize)
		var readN int
		readN, err = fd.ReadAt(bdata[:int(dataSize)], offset)
		if readN == 0 && err != nil {
			break
		}
		blockCrc := crc32.ChecksumIEEE(bdata[:readN])
		binary.BigEndian.PutUint32(crcData[blockNo*util.PerBlockCrcSize:(blockNo+1)*util.PerBlockCrcSize], blockCrc)
	}
	crc = crc32.ChecksumIEEE(crcData)
	if err != nil {
		log.LogErrorf("computeExtentCrc failed, err:%v", err)
	}
	return
}

type DataNodeInfo struct {
	Addr                      string
	PersistenceDataPartitions []uint64
}

func (m *DataNodeAgent) getPersistPartitionsFromMaster() (dInfo *DataNodeInfo, err error) {
	var dataNode *proto.DataNodeInfo
	var convert = func(node *proto.DataNodeInfo) *DataNodeInfo {
		result := &DataNodeInfo{}
		result.Addr = node.Addr
		result.PersistenceDataPartitions = node.PersistenceDataPartitions
		return result
	}
	for i := 0; i < 3; i++ {
		dataNode, err = masterClient.NodeAPI().GetDataNode(m.localServerAddr)
		if err != nil {
			log.LogErrorf("action[getPersistPartitionsFromMaster]: getDataNode error %v", err)
			continue
		}
		break
	}
	if err != nil {
		return
	}
	dInfo = convert(dataNode)
	return
}


func (m *DataNodeAgent) getPartitionFromMaster(id uint64) (partition *proto.DataPartitionInfo, err error) {
	for i := 0; i < 3; i++ {
		partition, err = masterClient.AdminAPI().GetDataPartition("", id)
		if err != nil {
			log.LogErrorf("action[RestorePartition]: getDataNode error %v", err)
			continue
		}
		break
	}
	if err != nil {
		return
	}
	return
}

// isExpiredPartition return whether one partition is expired
// if one partition does not exist in master, we decided that it is one expired partition
func isExpiredPartition(id uint64, partitions []uint64) bool {
	if len(partitions) == 0 {
		return true
	}

	for _, existId := range partitions {
		if existId == id {
			return false
		}
	}
	return true
}
func unmarshalPartitionName(name string) (partitionID uint64, partitionSize int, err error) {
	arr := strings.Split(name, "_")
	if len(arr) != 3 {
		err = fmt.Errorf("error DataPartition name(%v)", name)
		return
	}
	if partitionID, err = strconv.ParseUint(arr[1], 10, 64); err != nil {
		return
	}
	if partitionSize, err = strconv.Atoi(arr[2]); err != nil {
		return
	}
	return
}

func doShutdown(s common.Server) {
	return
}

// Sync blocks the invoker's goroutine until the meta node shuts down.
func (m *DataNodeAgent) Sync() {
	m.control.Sync()
}

func (m *DataNodeAgent) registerAPIHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("DataNodeAgent")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc("/repairCrc", m.repairCrc)
	http.HandleFunc("/fetchExtentsCrc", m.fetchExtentsCrc)
	return
}

func (m *DataNodeAgent) moveExtentFileToBackup(pathStr string, partitionID, extentID uint64) (err error) {
	rootPath := pathStr[ : strings.LastIndex(pathStr, "/")]
	extentFilePath := path.Join(pathStr, strconv.Itoa(int(extentID)))
	now := time.Now()
	repairDirPath := path.Join(rootPath, repairDirStr, fmt.Sprintf("%d-%d-%d", now.Year(), now.Month(), now.Day()))
	os.MkdirAll(repairDirPath, 0655)
	fileInfo, err := os.Stat(repairDirPath)
	if err != nil || !fileInfo.IsDir() {
		err = fmt.Errorf("path[%s] is not exist or is not dir", repairDirPath)
		return
	}
	repairBackupFilePath := path.Join(repairDirPath, fmt.Sprintf("%d_%d_%d", partitionID, extentID, now.Unix()))
	log.LogWarnf("rename file[%s-->%s]", extentFilePath, repairBackupFilePath)
	if err = os.Rename(extentFilePath, repairBackupFilePath); err != nil {
		return
	}

	file, err := os.OpenFile(extentFilePath, os.O_CREATE | os.O_RDWR | os.O_EXCL, 0666)
	if  err != nil {
		return
	}
	file.Close()
	return nil
}

func (m *DataNodeAgent) repairCrc(w http.ResponseWriter, r *http.Request) {
	var (
		diskStr    string
		ttlHourStr string
		err        error
		modStr         string
		forceStr       string
		force          bool
	)
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Code = http.StatusInternalServerError
			resp.Msg = err.Error()
		}
		d, _ := resp.Marshal()
		if _, err = w.Write(d); err != nil {
			log.LogErrorf("[repairCrc] response %s", err)
		}
	}()
	diskStr = r.FormValue(cfgDisk)
	ttlHourStr = r.FormValue(cfgHour)
	if ttlHourStr == "" {
		err = fmt.Errorf("param hour is empty")
		return
	}
	hour, err := strconv.Atoi(ttlHourStr)
	if err != nil {
		err = fmt.Errorf("param hour(%v) invalid, err:%v", reflect.TypeOf(hour), err)
		return
	}
	forceStr = r.FormValue(cfgForce)
	if forceStr != "" {
		force, err = strconv.ParseBool(forceStr)
		if err != nil {
			err = fmt.Errorf("param force(%v) invalid, err:%v", reflect.TypeOf(force), err)
			return
		}
	}


	modStr = r.FormValue(cfgMod)
	switch modStr {
	case "scan":
		m.repairFunc = func(task *repairTask) error {
			log.LogWarnf("partition[%v] extent[%v] [%v_%v] crc invalid, the right quorum extent info[%v_%v]", task.PartitionID, task.LocalExtent[proto.FileID], task.LocalExtent[proto.Size], task.LocalExtent[proto.Crc], task.ValidExtent[proto.Size], task.ValidExtent[proto.Crc])
			return nil
		}
	case "repair":
		//判断当前系统中datanode进程是否存活
		dataNodeClient := data.NewDataHttpClient("127.0.0.1:" + m.dataNodePprofPort, false)
		_, err = dataNodeClient.GetPartitionsFromNode()
		if err == nil {
			if !force {
				err = fmt.Errorf("datanode is alive, can not repair; please repair until the datanode is currupt or set flag[force=true] to force repair, the partition will be stopped and reloaded during force repair")
				return
			}
			m.beforeRepairFunc = func(partitionID uint64) (err error) {
				dataClient := data.NewDataHttpClient("127.0.0.1:" + m.dataNodePprofPort, false)
				if err = dataClient.StopPartition(partitionID); err != nil {
					log.LogErrorf("stop partition[%v] failed, err:%v", partitionID, err)
				}
				return
			}
			m.afterRepairFunc = func(diskPath, partitionPath string) (err error) {
				dataClient := data.NewDataHttpClient("127.0.0.1:" + m.dataNodePprofPort, false)
				if err = dataClient.ReLoadPartition(partitionPath, diskPath); err != nil {
					log.LogErrorf("reload disk[%v] partition[%v] failed, err:%v", diskPath, partitionPath, err)
				}
				return
			}
		}
		m.repairFunc = func(task *repairTask) (err error) {
			err = m.moveExtentFileToBackup(task.ExtentDir, task.PartitionID, task.LocalExtent[proto.FileID])
			if err != nil {
				log.LogErrorf("partition[%v] extent[%v] [%v_%v] repair failed, the right extent info[%v_%v]", task.PartitionID, task.LocalExtent[proto.FileID], task.LocalExtent[proto.Size], task.LocalExtent[proto.Crc], task.ValidExtent[proto.Size], task.ValidExtent[proto.Crc])
				return
			}
			log.LogWarnf("partition[%v] extent[%v] [%v_%v] has been removed, the right extent info[%v_%v]", task.PartitionID, task.LocalExtent[proto.FileID], task.LocalExtent[proto.Size], task.LocalExtent[proto.Crc], task.ValidExtent[proto.Size], task.ValidExtent[proto.Crc])
			return
		}
	default:
		err = fmt.Errorf("invalid mod: %v", modStr)
		return
	}
	respData := &struct {
		FailedTasks []*repairTask
		SuccessTasks []*repairTask
	}{
		FailedTasks: make([]*repairTask, 0),
		SuccessTasks: make([]*repairTask, 0),
	}
	for _, disk := range m.disks {
		if diskStr != "" && disk != diskStr {
			continue
		}
		failTasks, successTasks, err1 := m.RepairExtentCrcOnDisk(disk, hour)
		if err1 == nil {
			respData.FailedTasks = append(respData.FailedTasks, failTasks...)
			respData.SuccessTasks = append(respData.SuccessTasks, successTasks...)
			break
		}
	}
	resp.Data = respData
	log.LogInfof("[repairCrc] success")
	return
}

func (m *DataNodeAgent) fetchExtentsCrc(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID      uint64
		partitionPathStr string
		err              error
		extentsMap       map[uint64]*proto.ExtentInfoBlock
	)
	resp := NewAPIResponse(http.StatusOK, http.StatusText(http.StatusOK))
	defer func() {
		if err != nil {
			resp.Code = http.StatusInternalServerError
			resp.Msg = err.Error()
		}
		d, _ := resp.Marshal()
		if _, err = w.Write(d); err != nil {
			log.LogErrorf("[fetchExtentsCrc] response %s", err)
		}
	}()
	partitionPathStr = r.FormValue(paramPath)
	partitionID, _, err = unmarshalPartitionName(partitionPathStr)
	if err != nil {
		return
	}
	for _, disk := range m.disks {
		if extentsMap, err = getNormalExtents(disk, partitionPathStr, 0); err == nil {
			break
		}
	}
	resp.Data = extentsMap
	log.LogInfof("[fetchExtentsCrc] partition:%v success", partitionID)
	return
}