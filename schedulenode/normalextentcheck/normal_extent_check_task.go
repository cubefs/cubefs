package normalextentcheck

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/bitset"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

type NormalExtentCheckTask struct {
	*proto.Task
	mpParaCnt          int
	metaDataSnapDir    string
	needSearchOwnerIno bool
	mistakeDelEK       map[uint64]*bitset.ByteSliceBitSet
	dataExtentsMap     map[uint64]*bitset.ByteSliceBitSet
	metaExtentsMap     map[uint64]*bitset.ByteSliceBitSet
	extentConflict     map[ExtentInfo][]uint64
	searchResult       []*proto.ExtentInfoWithInode
	searchFailed       []ExtentInfo
	masterClient       *master.MasterClient
	mailTo             []string
	alarmMembers       []string
	callMembers        []string
}

func NewNormalExtentCheckTask(task *proto.Task, mc *master.MasterClient, needSearchOwnerIno bool,
	metaDataSnapDir string, mailTo, alarmMembers, callMembers []string) *NormalExtentCheckTask {
	normalExtentCheckTask := new(NormalExtentCheckTask)
	normalExtentCheckTask.Task = task
	normalExtentCheckTask.masterClient = mc
	normalExtentCheckTask.mailTo = mailTo
	normalExtentCheckTask.alarmMembers = alarmMembers
	normalExtentCheckTask.callMembers = callMembers
	normalExtentCheckTask.needSearchOwnerIno = needSearchOwnerIno
	normalExtentCheckTask.metaDataSnapDir = metaDataSnapDir
	normalExtentCheckTask.extentConflict = make(map[ExtentInfo][]uint64)
	return normalExtentCheckTask
}

func (t *NormalExtentCheckTask) printfExtentsInfo(eks map[uint64]*bitset.ByteSliceBitSet) {
	for dpid, blockInfo := range eks {
		var extentsStr = make([]string, 0, blockInfo.Cap())
		maxNum := blockInfo.MaxNum()
		for index := 0; index <= maxNum; index++ {
			if blockInfo.Get(index) {
				extentsStr = append(extentsStr, fmt.Sprintf("%v", index))
			}
		}
		if len(extentsStr) == 0 {
			log.LogDebugf("[printfExtentsInfo] cluster[%s] volume[%s] data partition[%v] not exist extents deleted by mistake", t.Cluster, t.VolName, dpid)
			continue
		}
		log.LogDebugf("[printfExtentsInfo] cluster[%s] volume[%s] data partition[%v] mistake delete ekInfo [ekCount:%v eks:%s]", t.Cluster, t.VolName, dpid, len(extentsStr), strings.Join(extentsStr, ","))
	}
}

func (t *NormalExtentCheckTask) getDataPartitionView(dpID uint64, dataNodeAddr string) (dpView *DataPartitionView, err error) {
	var (
		resp *http.Response
		respData []byte
		client = &http.Client{}
	)
	client.Timeout = 60 * time.Second
	url := fmt.Sprintf("http://%s:%v/partition?id=%v", strings.Split(dataNodeAddr, ":")[0], t.masterClient.DataNodeProfPort, dpID)
	resp, err = client.Get(url)
	if err != nil {
		log.LogErrorf("get url(%s) failed, cluster:%s, error:%v", url, t.Cluster, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("invalid status code: %v %v", url, resp.StatusCode)
		return
	}

	respData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.NewErrorf("read all response body failed: %v", err)
		return
	}

	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}

	if err = json.Unmarshal(respData, &body); err != nil {
		log.LogErrorf("Unmarshal resp data failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
		return
	}

	if body.Code != 200 {
		err = fmt.Errorf("resp code not ok")
		log.LogErrorf("resp code not ok, cluster(%s), url(%s) code(%v)", t.Cluster, url, body.Code)
		return
	}

	dpView = new(DataPartitionView)
	if t.masterClient.IsDbBack {
		dbBackDataPartitionView := new(proto.DataPartitionViewDbBack)
		if err = json.Unmarshal(body.Data, dbBackDataPartitionView); err != nil {
			log.LogErrorf("Unmarshal data partition view failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
			return
		}
		dpView.VolName = dbBackDataPartitionView.VolName
		dpView.ID = uint64(dbBackDataPartitionView.ID)
		dpView.FileCount = dbBackDataPartitionView.FileCount
		for _, file := range dbBackDataPartitionView.Files {
			extentInfo := storage.ExtentInfoBlock{}
			extentInfo[proto.FileID] = file.FileId
			extentInfo[proto.Size] = file.Size
			extentInfo[proto.Crc] = uint64(file.Crc)
			extentInfo[proto.ModifyTime] = uint64(file.ModTime.Unix())
			dpView.Files = append(dpView.Files, extentInfo)
		}
		return
	}

	if err = json.Unmarshal(body.Data, dpView); err != nil {
		log.LogErrorf("Unmarshal data partition view failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
		return
	}

	return
}

func (t *NormalExtentCheckTask) checkDataNodeStatus(dataNodeAddr string) (startComplete bool) {
	client := &http.Client{}
	client.Timeout = time.Second * 60
	url := fmt.Sprintf("http://%s:%v/status", strings.Split(dataNodeAddr, ":")[0], t.masterClient.DataNodeProfPort)
	resp, err := client.Get(url)
	if err != nil {
		log.LogErrorf("get url(%s) failed, cluster:%s, error:%v", url, t.Cluster, err)
		return
	}
	defer resp.Body.Close()

	var respData []byte
	respData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.LogErrorf("read all response body failed: %v", err)
		return
	}

	body := &struct {
		StartComplete bool `json:"StartComplete"`
	}{}
	if err = json.Unmarshal(respData, &body); err != nil {
		log.LogErrorf("Unmarshal resp data failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
		return
	}
	startComplete = body.StartComplete
	return
}

func (t *NormalExtentCheckTask) getExtentsFromDataPartition(dpId uint64, dataNodeAddr string) (extentsID []uint64, err error) {
	var dpView *DataPartitionView
	for index := 0; index < 5; index++ {
		dpView, err = t.getDataPartitionView(dpId, dataNodeAddr)
		if err == nil {
			break
		}
		time.Sleep(time.Second*1)
	}
	if err != nil {
		log.LogErrorf("action[getExtentsByDataPartition] get cluster[%s] data partition[id: %v, addr: %s] view failed:%v",
			t.Cluster, dpId, dataNodeAddr, err)
		return
	}

	extentsID = make([]uint64, 0, len(dpView.Files))
	for _, file := range dpView.Files {
		if (!t.masterClient.IsDbBack && proto.IsTinyExtent(file[storage.FileID])) || (t.masterClient.IsDbBack && file[storage.FileID] >= 50000000) {
			continue
		}
		extentsID = append(extentsID, file[storage.FileID])
	}
	return
}

func parseExtentInfo(filename string, ExtentInfoCh chan *proto.ExtentInfoWithInode) (err error){
	var fp *os.File
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	fp, err = os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		err = fmt.Errorf("[parseExtentInfo] OpenFile: %s", err.Error())
		return
	}
	defer fp.Close()
	reader := bufio.NewReaderSize(fp, 4*1024*1024)
	inoBuf := make([]byte, 4)
	for {
		inoBuf = inoBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = fmt.Errorf("[parseExtentInfo] ReadHeader: %s", err.Error())
			return
		}
		length := binary.BigEndian.Uint32(inoBuf)

		// next read body
		if uint32(cap(inoBuf)) >= length {
			inoBuf = inoBuf[:length]
		} else {
			inoBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			err = fmt.Errorf("[parseExtentInfo] ReadBody: %s", err.Error())
			return
		}
		ino := metanode.NewInode(0, 0)
		if err = ino.UnmarshalV2(context.Background(), inoBuf); err != nil {
			err = fmt.Errorf("[parseExtentInfo] Unmarshal: %s", err.Error())
			return
		}
		if proto.IsDir(ino.Type) || ino.NLink == 0 {
			continue
		}
		ino.Extents.Range(func(ek proto.ExtentKey) bool {
			ExtentInfoCh <- &proto.ExtentInfoWithInode{
				Inode: ino.Inode,
				EK: ek,
			}
			return true
		})
	}
}

func (t *NormalExtentCheckTask) searchExtentsByMetaDataSnaps(mistakeDelEKs map[ExtentInfo]bool) {
	var partitionPathCh = make(chan string, 512)
	t.searchResult = make([]*proto.ExtentInfoWithInode, 0)
	pidDirs, err := os.ReadDir(t.metaDataSnapDir)
	if err != nil {
		log.LogErrorf("read dir[%s] failed:%s\n", t.metaDataSnapDir, err.Error())
		return
	}

	var resultWaitGroup sync.WaitGroup
	extCh := make(chan *proto.ExtentInfoWithInode, 1024)
	resultWaitGroup.Add(1)
	go func() {
		defer resultWaitGroup.Done()
		for {
			e := <- extCh
			if e == nil {
				break
			}
			if _, has := mistakeDelEKs[ExtentInfo{
				DataPartitionID: e.EK.PartitionId,
				ExtentID:        e.EK.ExtentId,
			}]; has {
				mistakeDelEKs[ExtentInfo{
					DataPartitionID: e.EK.PartitionId,
					ExtentID:        e.EK.ExtentId,
				}] = true
				t.searchResult = append(t.searchResult, e)
			}

		}
	}()

	go func() {
		defer close(partitionPathCh)
		for _, pidDir := range pidDirs {
			if !pidDir.IsDir() {
				continue
			}

			partitionPathCh <- pidDir.Name()
		}
	}()

	var wg sync.WaitGroup
	for index := 0 ; index < DefaultTaskConcurrency; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				partitionPath, ok := <- partitionPathCh
				if !ok {
					break
				}

				inodeFile := path.Join(path.Join(t.metaDataSnapDir, partitionPath, "inode"))

				if errorInfo := parseExtentInfo(inodeFile, extCh); errorInfo != nil {
					log.LogErrorf("action[searchExtentsBySnaps] get cluster[%s] volume [%s] extent id list " +
						"from mp[%s] failed: %v", t.Cluster, t.VolName, partitionPath, errorInfo)
					continue
				}
			}
		}()
	}
	wg.Wait()
	close(extCh)
	resultWaitGroup.Wait()
	return
}

func (t *NormalExtentCheckTask) searchExtentsByOnlineData(mistakeDelEKs map[ExtentInfo]bool) {
	var (
		mps             []*proto.MetaPartitionView
		err             error
		mpParaWG        sync.WaitGroup
		resultWaitGroup sync.WaitGroup
		extCh           = make(chan *proto.ExtentInfoWithInode, 1024)
		mpIDCh          = make(chan uint64, 512)
	)
	for index := 0; index < 5; index++ {
		mps, err = t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
		if err == nil {
			break
		}
		time.Sleep(time.Second * 1)
	}
	if err != nil {
		log.LogErrorf("action[searchExtents] get cluster[%s] volume[%s] meta partitions " +
			"failed: %v", t.Cluster, t.VolName, err)
		return
	}

	t.searchResult = make([]*proto.ExtentInfoWithInode, 0)
	resultWaitGroup.Add(1)
	go func() {
		defer resultWaitGroup.Done()
		for {
			e := <-extCh
			if e == nil {
				break
			}
			if _, has := mistakeDelEKs[ExtentInfo{
				DataPartitionID: e.EK.PartitionId,
				ExtentID:        e.EK.ExtentId,
			}]; has {
				mistakeDelEKs[ExtentInfo{
					DataPartitionID: e.EK.PartitionId,
					ExtentID:        e.EK.ExtentId,
				}] = true
				t.searchResult = append(t.searchResult, e)
			}
		}
	}()

	go func() {
		defer close(mpIDCh)
		for _, mp := range mps {
			mpIDCh <- mp.PartitionID
		}
	}()

	for index := 0 ; index < DefaultInodeConcurrency; index++ {
		mpParaWG.Add(1)
		go func() {
			defer mpParaWG.Done()
			for {
				mpID, ok := <- mpIDCh
				if !ok {
					break
				}

				_ = t.getExtentsFromMetaPartition(mpID, extCh)
			}
		}()
	}
	mpParaWG.Wait()
	close(extCh)
	resultWaitGroup.Wait()
	log.LogInfof("action[searchExtents] cluster[%s] volume[%s] extent search success count:%v", t.Cluster, t.VolName, len(t.searchResult))
	return
}

func (t *NormalExtentCheckTask)extentsSearch() {
	eksMap := make(map[ExtentInfo]bool, 0)
	for dpID, extentBitSet := range t.mistakeDelEK {
		maxNum := extentBitSet.MaxNum()
		for index := 0; index <= maxNum; index++ {
			if extentBitSet.Get(index) {
				eksMap[ExtentInfo{
					DataPartitionID: dpID,
					ExtentID:        uint64(index),
				}] = false
			}
		}
	}
	defer func() {
		t.searchFailed = make([]ExtentInfo, 0)
		for extentInfo, searchOK := range eksMap {
			if !searchOK {
				t.searchFailed = append(t.searchFailed, extentInfo)
			}
		}
	}()

	if t.metaDataSnapDir != "" {
		t.searchExtentsByMetaDataSnaps(eksMap)
		return
	}

	//search by online data
	volumeSimpleViewInfo, err := t.masterClient.AdminAPI().GetVolumeSimpleInfo(t.VolName)
	if err != nil {
		log.LogErrorf("[extentsSearch] get volume simple info failed:%v", err)
		return
	}

	if volumeSimpleViewInfo.InodeCount > DefaultInodeCntThresholdForSearch {
		//skip big volumes
		log.LogInfof("[extentsSearch] cluster(%s) volume(%s) inode count(%v) more than %v",
			t.Cluster, t.VolName, volumeSimpleViewInfo.InodeCount, DefaultInodeCntThresholdForSearch)
		return
	}

	t.searchExtentsByOnlineData(eksMap)

	return
}

func (t *NormalExtentCheckTask) getAllInodeExtents(mpId uint64, leaderAddr string, extentInfoCh chan *proto.ExtentInfoWithInode) (err error) {
	var (
		resp           *proto.MpAllInodesId
		start          = time.Date(2022, 9, 22, 0, 0, 0, 0, time.Local).Unix()
		end            = time.Now().Unix()
		metaHttpClient = meta.NewMetaHttpClient(fmt.Sprintf("%s:%v", strings.Split(leaderAddr, ":")[0],
			t.masterClient.MetaNodeProfPort), false, false)
		ekCountOverThresholdInodes = make([]*proto.InodeEKCountRecord, 0)
		muLock        sync.Mutex
	)

	defer func() {
		if len(ekCountOverThresholdInodes) == 0 {
			return
		}
		if err = mysql.RecordEKCountOverThresholdCheckResult(t.Cluster, t.VolName, mpId, ekCountOverThresholdInodes); err != nil {
			log.LogErrorf("action[getExtentsFromMetaPartition] record ek count over threshold inodes failed," +
				"cluster(%v) volName(%v) partitionID(%v) error(%v)", t.Cluster, t.VolName, mpId, err)
		}
	}()

	resp, err = metaHttpClient.ListAllInodesId(mpId, 0, start, end)
	if err != nil {
		log.LogErrorf("action[getExtentsFromMetaPartition] get cluster[%s] mp[%v] all inode info failed:%v",
			t.Cluster, mpId, err)
		return
	}

	inodeIDCh := make(chan uint64, 256)
	go func() {
		for _, inoID := range resp.Inodes {
			inodeIDCh <- inoID
		}
		close(inodeIDCh)
	}()

	errorCh := make(chan error, len(resp.Inodes))
	defer func() {
		close(errorCh)
	}()
	var wg sync.WaitGroup
	for i := 0; i < DefaultInodeConcurrency ; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				inodeID, ok := <- inodeIDCh
				if !ok {
					break
				}
				eksResp, errInfo := metaHttpClient.GetExtentKeyByInodeId(mpId, inodeID)
				if errInfo != nil {
					errorCh <- errInfo
					log.LogErrorf("action[getExtentsFromMetaPartition] get cluster[%s] mp[%v] extent " +
						"key by inode id[%v] failed: %v", t.Cluster, mpId, inodeID, errInfo)
					continue
				}
				if int64(len(eksResp.Extents)) > InodeExtentCountMaxCountThreshold {
					muLock.Lock()
					ekCountOverThresholdInodes = append(ekCountOverThresholdInodes, &proto.InodeEKCountRecord{
						InodeID:     inodeID,
						EKCount:     uint32(len(eksResp.Extents)),
					})
					muLock.Unlock()
				}
				for _, ek := range eksResp.Extents {
					extentInfoCh <- &proto.ExtentInfoWithInode{
						Inode: inodeID,
						EK: ek,
					}
				}
			}
		}()
	}
	wg.Wait()

	select {
	case <-errorCh:
		err = errors.NewErrorf("get extent key by inode id failed")
		return
	default:
	}
	return
}

func (t *NormalExtentCheckTask) getExtentsFromMetaPartition(mpID uint64, extCh chan *proto.ExtentInfoWithInode) (err error) {
	var metaPartitionInfo *proto.MetaPartitionInfo
	for i := 0; i < 5; i++ {
		metaPartitionInfo, err = t.masterClient.ClientAPI().GetMetaPartition(mpID, t.VolName)
		if err == nil {
			break
		}
		time.Sleep(time.Second * 1)
	}
	if err != nil {
		log.LogErrorf("action[getExtentsFromMetaPartition] get cluster[%s] volume[%s] mp[%v] info failed: %v",
			t.Cluster, t.VolName, mpID, err)
		return
	}

	if len(metaPartitionInfo.Replicas) == 0 {
		err = fmt.Errorf("cluster[%s] volume[%s] mp[%v] with zero replica", t.Cluster, t.VolName, mpID)
		log.LogErrorf("action[getExtentsFromMetaPartition] cluster[%s] volume[%s] mp[%v] replica count is 0",
			t.Cluster, t.VolName, mpID)
		return
	}

	addrs := make([]string, 0, len(metaPartitionInfo.Replicas))
	for _, replica := range metaPartitionInfo.Replicas {
		if replica.IsLeader {
			addrs = append(addrs, replica.Addr)
		}
	}

	if len(addrs) == 0 {
		//no leader
		maxInodeCount := uint64(0)
		leaderAddr := ""
		for _, replica := range metaPartitionInfo.Replicas {
			if replica.InodeCount >= maxInodeCount {
				maxInodeCount = replica.InodeCount
				leaderAddr = replica.Addr
			}
		}
		if leaderAddr != "" {
			addrs = append(addrs, leaderAddr)
		}
	}

	for _, replica := range metaPartitionInfo.Replicas {
		if replica.Addr != addrs[0] {
			addrs = append(addrs, replica.Addr)
		}
	}

	if len(addrs) == 0 {
		err = fmt.Errorf("cluster[%s] volume[%s] mp[%v] with zero replica", t.Cluster, t.VolName, mpID)
		log.LogErrorf("action[getExtentsFromMetaPartition] cluster[%s] volume[%s] mp[%v] replica count is 0",
			t.Cluster, t.VolName, mpID)
		return
	}

	for _, addr := range addrs {
		if addr == "" {
			continue
		}
		err = t.getAllInodeExtents(mpID, addr, extCh)
		if err == nil {
			break
		}
		log.LogErrorf("action[getExtentsFromMetaPartition] get cluster[%s] volume [%s] extent info from mp(%s:%v) failed:%v",
			t.Cluster, t.VolName, addr, mpID, err)
	}
	if err != nil {
		log.LogErrorf("action[getExtentsFromMetaPartition] get cluster[%s] volume [%s] extent info " +
			"from mp[%v] failed: %v", t.Cluster, t.VolName, mpID, err)
	}
	return
}

func (t *NormalExtentCheckTask) getExtentsByDPs() (err error) {
	var (
		dpsView        *proto.DataPartitionsView
		wg             sync.WaitGroup
		extentsMapLock sync.Mutex
		dpViewCh       = make(chan *proto.DataPartitionResponse, 10)
		errChan        chan error
	)
	for index := 1; index < 5; index++ {
		dpsView, err = t.masterClient.ClientAPI().GetDataPartitions(t.VolName)
		if err == nil {
			break
		}
		time.Sleep(time.Second * 1)
	}
	if err != nil {
		log.LogErrorf("action[getExtentsByDPs] get cluster[%s] volume[%s] data partition failed: %v",
			t.Cluster, t.VolName, err)
		return
	}

	errChan = make(chan error, len(dpsView.DataPartitions))
	t.dataExtentsMap = make(map[uint64]*bitset.ByteSliceBitSet, len(dpsView.DataPartitions))
	go func() {
		defer close(dpViewCh)
		for _, dp := range dpsView.DataPartitions {
			dpViewCh <- dp
		}
	}()

	for index := 0; index < 10; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				dp, ok := <- dpViewCh
				if !ok {
					break
				}

				if dp == nil {
					continue
				}

				if startComplete := t.checkDataNodeStatus(dp.Hosts[0]); !startComplete {
					err = fmt.Errorf("action[getExtentsByDPs] cluster[%s] volume[%s] dpHost[%s] start incomplete",
						t.Cluster, t.VolName, dp.Hosts[0])
					errChan <- err
					continue
				}

				var extentsID []uint64
				extentsID, err = t.getExtentsFromDataPartition(dp.PartitionID, dp.Hosts[0])
				if err != nil {
					log.LogErrorf("action[getExtentsByDPs] get cluster[%s] volume[%s] extent id list " +
						"from dp[%v] failed: %v",
						t.Cluster, t.VolName, dp.PartitionID, err)
					errChan <- err
					continue
				}
				if len(extentsID) == 0 {
					continue
				}
				sort.Slice(extentsID, func(i, j int) bool {
					return extentsID[i] < extentsID[j]
				})
				maxExtentsID := extentsID[len(extentsID)-1]
				bitSet := bitset.NewByteSliceBitSetWithCap(int(maxExtentsID))
				for _, extentID := range extentsID {
					bitSet.Set(int(extentID))
				}

				extentsMapLock.Lock()
				t.dataExtentsMap[dp.PartitionID] = bitSet
				extentsMapLock.Unlock()
			}
		}()
	}
	wg.Wait()
	select {
	case err = <- errChan:
		return
	default:
	}
	return
}

func (t *NormalExtentCheckTask) getExtentsByMPs() (err error) {
	t.metaExtentsMap = make(map[uint64]*bitset.ByteSliceBitSet, 0)

	var mps []*proto.MetaPartitionView
	for index := 0; index < 5; index++ {
		mps, err = t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
		if err == nil {
			break
		}
		time.Sleep(time.Second * 1)
	}
	if err != nil {
		log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume[%s] meta partitions " +
			"failed: %v", t.Cluster, t.VolName, err)
		return
	}

	errorCh := make(chan error, len(mps))
	defer func() {
		close(errorCh)
	}()

	var resultWaitGroup sync.WaitGroup
	var extentConflictMap = make(map[ExtentInfo]map[uint64]byte)
	extCh := make(chan *proto.ExtentInfoWithInode, 1024)
	resultWaitGroup.Add(1)
	go func() {
		defer resultWaitGroup.Done()
		for {
			e := <-extCh
			if e == nil {
				break
			}
			if (!t.masterClient.IsDbBack && proto.IsTinyExtent(e.EK.ExtentId)) || (t.masterClient.IsDbBack && e.EK.ExtentId >= 50000000) {
				continue
			}

			if t.Cluster == "mysql" {
				if _, ok := extentConflictMap[ExtentInfo{DataPartitionID: e.EK.PartitionId, ExtentID: e.EK.ExtentId}]; !ok {
					extentConflictMap[ExtentInfo{DataPartitionID: e.EK.PartitionId, ExtentID: e.EK.ExtentId}] = make(map[uint64]byte)
				}
				extentConflictMap[ExtentInfo{DataPartitionID: e.EK.PartitionId, ExtentID: e.EK.ExtentId}][e.Inode] = 0
			}

			if _, ok := t.metaExtentsMap[e.EK.PartitionId]; !ok {
				t.metaExtentsMap[e.EK.PartitionId] = bitset.NewByteSliceBitSetWithCap(storage.MaxExtentCount*3)
			}
			t.metaExtentsMap[e.EK.PartitionId].Set(int(e.EK.ExtentId))
		}
	}()

	mpIDCh := make(chan uint64, 512)
	go func() {
		defer close(mpIDCh)
		for _, mp := range mps {
			mpIDCh <- mp.PartitionID
		}
	}()

	//todo: para goroutine count

	var wg sync.WaitGroup
	for index := 0 ; index < 10; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mpID, ok := <- mpIDCh
				if !ok {
					break
				}

				if errGet := t.getExtentsFromMetaPartition(mpID, extCh); errGet != nil {
					errorCh <- errGet
				}
			}
		}()
	}
	wg.Wait()
	close(extCh)
	resultWaitGroup.Wait()

	select {
	case err = <- errorCh:
		//meta info must be complete
		log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume [%s] extent id list from meta partition failed:%v",
			t.Cluster, t.VolName, err)
		return
	default:
	}

	if t.Cluster == "mysql" {
		for extentInfo, inodeMap := range extentConflictMap {
			if len(inodeMap) != 1 {
				inodes := make([]uint64, 0, len(inodeMap))
				for inode := range inodeMap {
					inodes = append(inodes, inode)
				}
				t.extentConflict[extentInfo] = inodes
			}
		}
	}

	log.LogDebugf("action[getExtentsByMPs] cluster[%s] volume [%s] conflict extents count:%v", t.Cluster, t.VolName, len(t.extentConflict))
	return
}

func (t *NormalExtentCheckTask) checkMistakeDeleteExtents() {
	t.mistakeDelEK = make(map[uint64]*bitset.ByteSliceBitSet, len(t.metaExtentsMap))
	for dpid, metaEKsBitSet := range t.metaExtentsMap {
		if _, ok := t.dataExtentsMap[dpid]; !ok {
			t.mistakeDelEK[dpid] = metaEKsBitSet
			continue
		}
		r := metaEKsBitSet.Xor(t.dataExtentsMap[dpid]).And(metaEKsBitSet)
		if r.IsNil() {
			continue
		}
		t.mistakeDelEK[dpid] = r
	}
	return
}

func (t *NormalExtentCheckTask) reCheckMistakeDeleteExtents() {
	mistakeDelEK := make(map[uint64]*bitset.ByteSliceBitSet, 0)
	for dpID, extentsID := range t.mistakeDelEK {
		if _, ok := t.metaExtentsMap[dpID]; !ok {
			log.LogInfof("reCheckEKsDeletedByMistake dpID:%v, eks:%v not exist in meta data", dpID, extentsID)
			continue
		}
		r := extentsID.And(t.metaExtentsMap[dpID])
		if r.IsNil() {
			continue
		}
		mistakeDelEK[dpID] = r
	}
	t.mistakeDelEK = mistakeDelEK
	return
}

func (t *NormalExtentCheckTask) doCheckExtentsDeletedByMistake() (err error) {
	if err = t.getExtentsByMPs(); err != nil {
		log.LogErrorf("action[checkExtentsDeletedByMistake] get cluster[%s] volume[%s] extents from mp failed:%v",
			t.Cluster, t.VolName, err)
		err = fmt.Errorf("volume: %s , meta failed: %s", t.VolName, err.Error())
		return
	}
	log.LogDebugf("action[checkExtentsDeletedByMistake] cluster[%s] volume[%s] meta extents info:", t.Cluster, t.VolName)
	t.printfExtentsInfo(t.metaExtentsMap)

	if err = t.getExtentsByDPs(); err != nil {
		log.LogErrorf("action[checkExtentsDeletedByMistake] get cluster[%s] volume[%s] extents from dp failed:%v",
			t.Cluster, t.VolName, err)
		err = fmt.Errorf("volume: %s , data failed: %s", t.VolName, err.Error())
		return
	}
	log.LogDebugf("action[checkExtentsDeletedByMistake] cluster[%s] volume[%s] data extents info:", t.Cluster, t.VolName)
	t.printfExtentsInfo(t.dataExtentsMap)

	t.checkMistakeDeleteExtents()

	log.LogDebugf("action[checkExtentsDeletedByMistake] cluster[%s] volume[%s] first check result:", t.Cluster, t.VolName)
	t.printfExtentsInfo(t.mistakeDelEK)

	//re get meta data
	if err = t.getExtentsByMPs(); err != nil {
		log.LogErrorf("action[checkExtentsDeletedByMistake] get cluster[%s] volume[%s] extents from mp failed:%v",
			t.Cluster, t.VolName, err)
		err = fmt.Errorf("volume: %s , meta failed: %s", t.VolName, err.Error())
		return
	}

	//recheck
	t.reCheckMistakeDeleteExtents()
	if len(t.mistakeDelEK) == 0 {
		log.LogInfof("action[checkExtentsDeletedByMistake] cluster[%s] volume[%s] not exist mistake delete eks", t.Cluster, t.VolName)
		return
	}
	log.LogDebugf("action[checkExtentsDeletedByMistake] cluster[%s] volume[%s] mistake delete eks:", t.Cluster, t.VolName)
	t.printfExtentsInfo(t.mistakeDelEK)
	return
}

func (t *NormalExtentCheckTask) RunOnce() (err error) {
	defer func() {
		if err != nil && (strings.Contains(err.Error(), "start incomplete") || strings.Contains(err.Error(), "vol not exists")) {
			log.LogInfof("NormalExtentCheckTask RunOnce %s %s check error: %v", err.Error())
			err = nil
		}
	}()
	log.LogInfof("NormalExtentCheckTask RunOnce %s %s start check", t.Cluster, t.VolName)
	if err = t.doCheckExtentsDeletedByMistake(); err != nil {
		log.LogErrorf("NormalExtentCheckTask RunOnce, %s %s mistake delete extent check failed: %v", t.Cluster,
			t.VolName, err)
		return
	}

	if len(t.mistakeDelEK) != 0 && t.needSearchOwnerIno {
		t.extentsSearch()
	}
	log.LogInfof("NormalExtentCheckTask RunOnce %s %s check finished", t.Cluster, t.VolName)
	return
}