package blck

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/bitset"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"io/fs"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var gConnPool = connpool.NewConnectPool()

type BlockCheckTask struct {
	*proto.Task
	mpParaCnt           int
	safeCleanInterval   int64
	garbageSize         uint64
	exportDir           string
	needClean           bool
	needDumpResult      bool
	garbageBlocks       map[uint64]*bitset.ByteSliceBitSet
	dataExtentsMap      map[uint64]*bitset.ByteSliceBitSet
	metaExtentsMap      map[uint64]*bitset.ByteSliceBitSet
	masterClient        *master.MasterClient
	metaExportDir       string
	checkByMetaDumpFile bool
}

func NewBlockCheckTask(task *proto.Task, mc *master.MasterClient, needClean, needDumpResult bool, safeCleanIntervalSecond int64, exportDir string) *BlockCheckTask {
	blckTask := new(BlockCheckTask)
	blckTask.Task = task
	blckTask.masterClient = mc
	blckTask.needClean = needClean
	blckTask.needDumpResult = needDumpResult
	blckTask.exportDir = exportDir
	blckTask.safeCleanInterval = safeCleanIntervalSecond
	return blckTask
}

func (t *BlockCheckTask) calcGarbageBlockSize() (err error) {
	var (
		dpInfo    *proto.DataPartitionInfo
		dpView    *DataPartitionView
		wg        sync.WaitGroup
		dpIDCh    = make(chan uint64, 128)
	)

	go func() {
		defer close(dpIDCh)
		for dpID := range t.garbageBlocks {
			dpIDCh <- dpID
		}
	}()

	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				dpID, ok := <- dpIDCh
				if !ok {
					return
				}
				extsBitSet, has := t.garbageBlocks[dpID]
				if !has {
					continue
				}
				garbageSize := uint64(0)
				dpInfo, err = t.masterClient.AdminAPI().GetDataPartition(t.VolName, dpID)
				if err != nil || len(dpInfo.Hosts) == 0 {
					log.LogErrorf("[calcGarbageBlockSize] get cluster[%s] volume[%s] dp[%v] host failed:%v",
						t.Cluster, t.VolName, dpID, err)
					continue
				}
				dpView, err = t.getDataPartitionView(dpID, dpInfo.Hosts[0])
				if err != nil {
					log.LogErrorf("[calcGarbageBlockSize] get cluster[%s] volume[%s] dp[%v] extent info failed:%v",
						t.Cluster, t.VolName, dpID, err)
					continue
				}

				if extsBitSet == nil || extsBitSet.MaxNum() == 0 {
					continue
				}

				for _, file := range dpView.Files {
					if extsBitSet.Get(int(file[storage.FileID])) {
						garbageSize += file[storage.Size]
					}
				}
				log.LogInfof("calcGarbageBlockSize %s %s dataPartition(%v) Garbage Block Size: %v", t.Cluster,
					t.VolName, dpID, garbageSize)
				atomic.AddUint64(&t.garbageSize, garbageSize)
			}
		}()
	}
	wg.Wait()
	log.LogInfof("[calcGarbageBlockSize] %s %s Garbage Block Size: %v", t.Cluster, t.VolName, t.garbageSize)
	return
}

func (t *BlockCheckTask) getDataPartitionView(dpID uint64, dataNodeAddr string) (dpView *DataPartitionView, err error) {
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

func (t *BlockCheckTask) getExtentsByDPs(dps []*proto.DataPartitionResponse) (err error) {
	var (
		wg             sync.WaitGroup
		extentsMapLock sync.Mutex
		dpViewCh       = make(chan *proto.DataPartitionResponse, 10)
	)

	t.dataExtentsMap = make(map[uint64]*bitset.ByteSliceBitSet, len(dps))
	go func() {
		defer close(dpViewCh)
		for _, dp := range dps {
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

				var extentsID []uint64
				extentsID, err = t.getExtentsByDataPartition(dp.PartitionID, dp.Hosts[0])
				if err != nil {
					log.LogErrorf("action[getExtentsByDPs] get cluster[%s] volume[%s] extent id list " +
						"from dp[%v] failed: %v",
						t.Cluster, t.VolName, dp.PartitionID, err)
					continue
				}
				if len(extentsID) == 0 {
					continue
				}
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
	return
}

func (t *BlockCheckTask) getExtentsByDataPartition(dpId uint64, dataNodeAddr string) (extentsID []uint64, err error) {
	var dpView *DataPartitionView
	dpView, err = t.getDataPartitionView(dpId, dataNodeAddr)
	if err != nil {
		log.LogErrorf("action[getExtentsByDataPartition] get cluster[%s] data partition[id: %v, addr: %s] view failed:%v",
			t.Cluster, dpId, dataNodeAddr, err)
		return
	}

	year, month, day := time.Now().Date()
	today := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
	extentsID = make([]uint64, 0, len(dpView.Files))
	for _, ex := range dpView.Files {
		if (!t.masterClient.IsDbBack && proto.IsTinyExtent(ex[storage.FileID])) || (t.masterClient.IsDbBack && ex[storage.FileID] >= 50000000) {
			continue
		}

		if today.Unix() - int64(ex[storage.ModifyTime]) >= t.safeCleanInterval {
			extentsID = append(extentsID, ex[storage.FileID])
		}
	}
	return
}

func (t *BlockCheckTask) getExtentsByMPs() (err error) {
	if t.metaExportDir != "" && t.checkByMetaDumpFile {
		if err = t.parseVolumeMetaExtentsInfo(); err != nil {
			log.LogErrorf("getExtentsByMPs parse meta extents failed: %v", err)
		}
		return
	}

	t.metaExtentsMap = make(map[uint64]*bitset.ByteSliceBitSet, 0)

	var mps []*proto.MetaPartitionView
	mps, err = t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
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
	extCh := make(chan *ExtentInfo, 1024)
	resultWaitGroup.Add(1)
	go func() {
		defer resultWaitGroup.Done()
		for {
			e := <-extCh
			if e == nil {
				break
			}

			if (!t.masterClient.IsDbBack && proto.IsTinyExtent(e.ExtentID)) || (t.masterClient.IsDbBack && e.ExtentID >= 50000000) {
				continue
			}

			if _, ok := t.metaExtentsMap[e.DataPartitionID]; !ok {
				t.metaExtentsMap[e.DataPartitionID] = bitset.NewByteSliceBitSetWithCap(storage.MaxExtentCount*3)
			}
			t.metaExtentsMap[e.DataPartitionID].Set(int(e.ExtentID))
		}
	}()

	mpIDCh := make(chan uint64, 512)
	go func() {
		defer close(mpIDCh)
		for _, mp := range mps {
			mpIDCh <- mp.PartitionID
		}
	}()

	var wg sync.WaitGroup
	mpConcurrency := parallelMpCnt.Load()
	for index := int32(0) ; index < mpConcurrency; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mpID, ok := <- mpIDCh
				if !ok {
					break
				}

				log.LogInfof("action[getExtentsByMPs] start get %s %s metaPartition(%v) inode", t.Cluster, t.VolName, mpID)
				metaPartitionInfo, errGetMPInfo := t.masterClient.ClientAPI().GetMetaPartition(mpID, t.VolName)
				if errGetMPInfo != nil {
					errorCh <- errGetMPInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume[%s] mp[%v] info failed: %v",
						t.Cluster, t.VolName, mpID, errGetMPInfo)
					continue
				}
				var (
					maxInodeCount uint64 = 0
					leaderAddr = ""
				)

				for _, replica := range metaPartitionInfo.Replicas {
					if replica.InodeCount >= maxInodeCount {
						maxInodeCount = replica.InodeCount
						leaderAddr = replica.Addr
					}
				}

				if errorInfo := t.getExtentsFromMetaPartition(mpID, leaderAddr, extCh); errorInfo != nil {
					errorCh <- errorInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume [%s] extent id list " +
						"from mp[%v] failed: %v", t.Cluster, t.VolName, mpID, errorInfo)
					continue
				}
			}
		}()
	}
	wg.Wait()
	close(extCh)
	resultWaitGroup.Wait()

	select {
	case e := <- errorCh:
		//meta info must be complete
		log.LogErrorf("get extent id list from meta partition failed:%v", e)
		err = errors.NewErrorf("get extent id list from meta partition failed:%v", e)
		return
	default:
	}
	log.LogInfof("meta extent map count:%v", len(t.metaExtentsMap))
	return
}

func (t *BlockCheckTask) getExtentsFromMetaPartition(mpId uint64, leaderAddr string, ExtentInfoCh chan *ExtentInfo) (err error) {
	var resp *proto.MpAllInodesId
	var retryCnt = 5
	var metaHttpClient *meta.MetaHttpClient
	if t.masterClient.IsDbBack {
		metaHttpClient = meta.NewDBBackMetaHttpClient(fmt.Sprintf("%s:%v", strings.Split(leaderAddr, ":")[0], t.masterClient.MetaNodeProfPort), false)
	} else {
		metaHttpClient = meta.NewMetaHttpClient(fmt.Sprintf("%s:%v", strings.Split(leaderAddr, ":")[0], t.masterClient.MetaNodeProfPort), false)
	}
	for retryCnt > 0 {
		err = nil
		if t.masterClient.IsDbBack {
			resp, err = metaHttpClient.ListAllInodesId(mpId, 0, 0, 0)
		} else {
			resp, err = metaHttpClient.ListAllInodesIDAndDelInodesID(mpId, 0, 0, 0)
		}
		if err == nil {
			break
		}
		time.Sleep(time.Second*5)
		retryCnt--
	}
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
		for _, delInodeID := range resp.DelInodes {
			inodeIDCh <- delInodeID
		}
		close(inodeIDCh)
	}()

	errorCh := make(chan error, resp.Count)
	defer func() {
		close(errorCh)
	}()
	var wg sync.WaitGroup
	inodeConcurrency := parallelInodeCnt.Load()
	for i := int32(0); i < inodeConcurrency ; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				inodeID, ok := <- inodeIDCh
				if !ok {
					break
				}
				var eksResp *proto.GetExtentsResponse
				var errInfo error
				var retryCount = 5
				for retryCount > 0 {
					errInfo = nil
					if t.masterClient.IsDbBack {
						eksResp, errInfo = metaHttpClient.GetExtentKeyByInodeId(mpId, inodeID)
					} else {
						eksResp, errInfo = metaHttpClient.GetExtentKeyByDelInodeId(mpId, inodeID)
					}
					if errInfo == nil {
						break
					}
					time.Sleep(time.Second*5)
					retryCount--
				}
				if errInfo != nil {
					errorCh <- errInfo
					log.LogErrorf("action[getExtentsFromMetaPartition] get cluster[%s] mp[%v] extent " +
						"key by inode id[%v] failed: %v", t.Cluster, mpId, inodeID, errInfo)
					continue
				}
				//log.LogDebugf("inode id:%v, eks cnt:%v", inodeID, len(eksResp.Extents))
				for _, ek := range eksResp.Extents {
					if (t.masterClient.IsDbBack && ek.ExtentId >= 50000000) || (!t.masterClient.IsDbBack && proto.IsTinyExtent(ek.ExtentId)) {
						continue
					}
					ExtentInfoCh <- &ExtentInfo{
						DataPartitionID: ek.PartitionId,
						ExtentID:        ek.ExtentId,
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

func batchDeleteExtent(host string, dp *metanode.DataPartition, eks []*proto.ExtentKey) (err error) {
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(host)
	defer func() {
		if err != nil {
			gConnPool.PutConnect(conn, true)
		} else {
			gConnPool.PutConnect(conn, false)
		}
	}()

	if err != nil {
		err = errors.NewErrorf("get conn from pool failed:%v, data node host:%s, data partition id:%v", err, host, dp.PartitionID)
		return
	}
	packet := metanode.NewPacketToBatchDeleteExtent(context.Background(), dp, eks)
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime*10); err != nil {
		err = errors.NewErrorf("write to dataNode %s, %s", packet.GetUniqueLogId(), err.Error())
		return
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		err = errors.NewErrorf("read response from dataNode %s, %s", packet.GetUniqueLogId(), err.Error())
		return
	}
	if packet.ResultCode != proto.OpOk {
		err = errors.NewErrorf("batch delete extent %s response: %s", packet.GetUniqueLogId(), packet.GetResultMsg())
	}
	return
}

func (t *BlockCheckTask) checkGarbageBlocks() {
	t.garbageBlocks = make(map[uint64]*bitset.ByteSliceBitSet, len(t.dataExtentsMap))
	for dpid, extsBitSet := range t.dataExtentsMap {
		if _, ok := t.metaExtentsMap[dpid]; !ok {
			//log.LogInfof("dp id:%v all garbage block, continue", dpid)
			t.garbageBlocks[dpid] = extsBitSet
			continue
		}
		//log.LogInfof("dp cap:%v, mp cap:%v", extsBitSet.Cap(), t.metaExtentsMap[dpid].Cap())
		t.garbageBlocks[dpid] = extsBitSet.Xor(t.metaExtentsMap[dpid]).And(extsBitSet)
	}
	return
}

func (t *BlockCheckTask) reCheckGarbageBlocks() {
	garbageBlocksReCheckResult := make(map[uint64]*bitset.ByteSliceBitSet, len(t.garbageBlocks))
	for dpid, extsBitSet := range t.garbageBlocks {
		if _, ok := t.metaExtentsMap[dpid]; !ok {
			//log.LogInfof("dp id:%v all garbage block, continue", dpid)
			garbageBlocksReCheckResult[dpid] = extsBitSet
			continue
		}
		//log.LogInfof("dp cap:%v, mp cap:%v", extsBitSet.Cap(), t.metaExtentsMap[dpid].Cap())
		garbageBlocksReCheckResult[dpid] = extsBitSet.Xor(t.metaExtentsMap[dpid]).And(extsBitSet)
	}
	t.garbageBlocks = garbageBlocksReCheckResult
	return
}

func (t *BlockCheckTask) doCheckGarbageInBatches() (err error) {
	log.LogInfof("doCheckGarbageInBatches %s %s start check garbage in batches", t.Cluster, t.VolName)
	garbageBlock := make(map[uint64]*bitset.ByteSliceBitSet, 0)
	var dpsView *proto.DataPartitionsView
	dpsView, err = t. masterClient.ClientAPI().GetDataPartitions(t.VolName)
	if err != nil {
		log.LogErrorf("action[doCheckGarbageInBatches] get cluster[%s] volume[%s] data partition failed: %v",
			t.Cluster, t.VolName, err)
		return
	}

	batchCount := 8192
	start, end := 0, 0
	for end < len(dpsView.DataPartitions) {
		end = start + batchCount
		if end > len(dpsView.DataPartitions) {
			end = len(dpsView.DataPartitions)
		}
		log.LogInfof("doCheckGarbageInBatches %s %s start check dp section[%v:%v)", t.Cluster, t.VolName, start, end)
		if err = t.getExtentsByDPs(dpsView.DataPartitions[start:end]); err != nil {
			log.LogErrorf("[doCheckGarbageInBatches] get cluster[%s] volume[%s] extents from dp failed:%v",
				t.Cluster, t.VolName, err)
			return
		}

		if err = t.getExtentsByMPs(); err != nil {
			log.LogErrorf("[doCheckGarbageInBatches] get cluster[%s] volume[%s] extents from mp failed:%v",
				t.Cluster, t.VolName, err)
			return
		}
		t.checkGarbageBlocks()

		//re get meta data
		if err = t.getExtentsByMPs(); err != nil {
			log.LogErrorf("[doCheckGarbageInBatches] get cluster[%s] volume[%s] extents from mp failed:%v",
				t.Cluster, t.VolName, err)
			return
		}

		t.reCheckGarbageBlocks()

		if len(t.garbageBlocks) == 0 {
			continue
		}

		if t.needDumpResult && t.exportDir != "" {
			t.dumpGarbageBlock()
		}

		for dpID, garbageBlockBitMap := range t.garbageBlocks {
			garbageBlock[dpID] = garbageBlockBitMap
			log.LogDebugf("[doCheckGarbageInBatches] dataPartitionID(%v) exist garbage block", dpID)
		}

		start = end
	}

	t.garbageBlocks = garbageBlock
	return
}

func (t *BlockCheckTask) doCheckGarbage() (err error) {
	var dpsView *proto.DataPartitionsView
	dpsView, err = t. masterClient.ClientAPI().GetDataPartitions(t.VolName)
	if err != nil {
		log.LogErrorf("action[doCheckGarbageInBatches] get cluster[%s] volume[%s] data partition failed: %v",
			t.Cluster, t.VolName, err)
		return
	}

	if err = t.getExtentsByDPs(dpsView.DataPartitions); err != nil {
		log.LogErrorf("[doCheckGarbage] get cluster[%s] volume[%s] extents from dp failed:%v",
			t.Cluster, t.VolName, err)
		return
	}

	if err = t.getExtentsByMPs(); err != nil {
		log.LogErrorf("[doCheckGarbage] get cluster[%s] volume[%s] extents from mp failed:%v",
			t.Cluster, t.VolName, err)
		return
	}
	t.checkGarbageBlocks()

	//re get meta data
	if err = t.getExtentsByMPs(); err != nil {
		log.LogErrorf("[doCheckGarbage] get cluster[%s] volume[%s] extents from mp failed:%v",
			t.Cluster, t.VolName, err)
		return
	}

	t.reCheckGarbageBlocks()
	return
}

func (t *BlockCheckTask) doCleanGarbage() (err error) {
	var failedCnt int
	for dpID, blockInfo := range t.garbageBlocks {
		maxNum := blockInfo.MaxNum()
		extentsID := make([]uint64, 0, maxNum)
		for index := 0; index <= maxNum; index++ {
			if blockInfo.Get(index) {
				extentsID = append(extentsID, uint64(index))
			}
		}

		var dpInfo *proto.DataPartitionInfo
		if dpInfo, err = t.masterClient.AdminAPI().GetDataPartition(t.VolName, dpID); err != nil || len(dpInfo.Hosts) == 0 {
			log.LogErrorf("[doCleanGarbage] get cluster[%s] volume[%s] dp[%v] host failed:%v",
				t.Cluster, t.VolName, dpID, err)
			failedCnt++
			continue
		}

		dp := &metanode.DataPartition{
			PartitionID: dpID,
			Hosts:       dpInfo.Hosts,
		}

		var eks []*proto.ExtentKey
		for _, extentID := range extentsID {
			ek := &proto.ExtentKey{
				PartitionId: dpID,
				ExtentId:    extentID,
			}
			eks = append(eks, ek)
		}
		if len(eks) == 0 {
			continue
		}

		startIndex := 0
		for startIndex < len(eks) {
			endIndex := startIndex + 1024
			if endIndex > len(eks) {
				endIndex = len(eks)
			}
			log.LogInfof("[doCleanGarbage] start batch delete extent, host addr[%s], partition id[%v], " +
				"extents is %v", dpInfo.Hosts[0], dpID, extentsID[startIndex:endIndex])
			if err = batchDeleteExtent(dpInfo.Hosts[0], dp, eks[startIndex: endIndex]); err != nil {
				log.LogErrorf("batch delete extent failed:%v", err)
				failedCnt++
				break
			}
			startIndex = endIndex
		}
		log.LogInfof("[doCleanGarbage] batch delete extent finish, partition id:%v", dpID)
	}
	log.LogInfof("[doCleanGarbage] totalCount:%v, failedCount:%v", len(t.garbageBlocks), failedCnt)
	return
}

func (t *BlockCheckTask) dumpGarbageBlock() {
	if t.exportDir == "" {
		return
	}
	for dpid, blockInfo := range t.garbageBlocks {
		var extentsStr = make([]string, 0, blockInfo.Cap())
		for index := 0; index <= blockInfo.MaxNum(); index++ {
			if blockInfo.Get(index) {
				extentsStr = append(extentsStr, fmt.Sprintf("%v", index))
			}
		}
		if len(extentsStr) == 0 {
			log.LogInfof("[dumpGarbageBlock] %s %s data partition %v not exist garbage blocks", t.Cluster,
				t.VolName, dpid)
			continue
		}
		filePath := path.Join(t.exportDir, fmt.Sprintf("%d", dpid))
		log.LogInfof("[dumpGarbageBlock] %s %s data partition %v garbage blocks count:%v, file path:%s", t.Cluster, t.VolName, dpid, len(extentsStr), filePath)
		err := ioutil.WriteFile(filePath, []byte(strings.Join(extentsStr, "\n")), 0666)
		if err != nil {
			log.LogErrorf("[dumpGarbageBlock] write to file(%s) failed:%v", filePath, err)
		}
	}
	return
}

func (t *BlockCheckTask) dumpMetExtentInfo() {
	if t.metaExportDir == "" {
		return
	}
	for dpid, blockInfo := range t.metaExtentsMap {
		var extentsStr = make([]string, 0, blockInfo.Cap())
		for index := 0; index <= blockInfo.MaxNum(); index++ {
			if blockInfo.Get(index) {
				extentsStr = append(extentsStr, fmt.Sprintf("%v", index))
			}
		}
		if len(extentsStr) == 0 {
			continue
		}
		filePath := path.Join(t.metaExportDir, fmt.Sprintf("%d", dpid))
		err := ioutil.WriteFile(filePath, []byte(strings.Join(extentsStr, "\n")), 0666)
		if err != nil {
			log.LogErrorf("[dumpGarbageBlock] write to file(%s) failed:%v", filePath, err)
		}
	}
	return
}

func (t *BlockCheckTask) init() (err error) {
	if t.exportDir == "" {
		return
	}

	if err = os.RemoveAll(t.exportDir); err != nil {
		return
	}

	if err = os.MkdirAll(t.exportDir, 0666); err != nil {
		return
	}

	return
}

func (t *BlockCheckTask) RunOnce() {
	log.LogInfof("BlockCheckTask RunOnce %s %s start check", t.Cluster, t.VolName)
	if err := t.init(); err != nil {
		log.LogErrorf("BlockCheckTask RunOnce, %s %s init bock check task failed: %v", t.Cluster, t.VolName, err)
		return
	}

	volView, err := t.masterClient.AdminAPI().GetVolumeSimpleInfo(t.VolName)
	if err != nil {
		log.LogErrorf("BlockCheckTask RunOnce, %s %s getVolumeSimpleInfo failed: %v", t.Cluster, t.VolName, err)
		return
	}

	if volView.RwDpCnt > 10000 {
		err = t.doCheckGarbageInBatches()
		if err != nil {
			log.LogErrorf("BlockCheckTask RunOnce, %s %s check garbage block failed: %v", t.Cluster, t.VolName, err)
			return
		}
	} else {
		err = t.doCheckGarbage()
		if err != nil {
			log.LogErrorf("BlockCheckTask RunOnce, %s %s check garbage block failed: %v", t.Cluster, t.VolName, err)
			return
		}
		if t.needDumpResult && t.exportDir != "" {
			t.dumpGarbageBlock()
		}
	}

	if err = t.calcGarbageBlockSize(); err != nil {
		log.LogErrorf("BlockCheckTask RunOnce, %s %s calculate garbage block size failed: %v", t.Cluster, t.VolName, err)
	}

	if !t.needClean {
		return
	}

	if err = t.doCleanGarbage(); err != nil {
		log.LogErrorf("BlockCheckTask RunOnce, %s %s clean garbage block failed: %v", t.Cluster, t.VolName, err)
		return
	}
	log.LogInfof("BlockCheckTask RunOnce %s %s check finish", t.Cluster, t.VolName)
}

func (t *BlockCheckTask) parseVolumeGarbageBlocksInfo() (err error) {
	var (
		fileInfoList []fs.FileInfo
		wg           sync.WaitGroup
		lock         sync.Mutex
		fileNameCh   = make(chan string, 128)
	)

	fileInfoList, err = ioutil.ReadDir(t.exportDir)
	if err != nil {
		return
	}

	t.garbageBlocks = make(map[uint64]*bitset.ByteSliceBitSet, len(fileInfoList))

	go func() {
		defer close(fileNameCh)
		for _, fileInfo := range fileInfoList {
			if fileInfo.IsDir() {
				continue
			}
			fileNameCh <- fileInfo.Name()
		}
	}()

	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				fileName, ok := <- fileNameCh
				if !ok {
					return
				}

				var (
					dpID uint64
					bitSet *bitset.ByteSliceBitSet
				)
				dpID, bitSet, err = t.parseDataPartitionGarbageBlocks(fileName, false)
				if err != nil {
					log.LogErrorf("parse file[%s] failed:%v",  path.Join(t.exportDir, fileName), err)
					continue
				}
				lock.Lock()
				t.garbageBlocks[dpID] = bitSet
				lock.Unlock()
			}
		}()
	}
	wg.Wait()
	return
}

func (t *BlockCheckTask) parseVolumeMetaExtentsInfo() (err error) {
	var (
		fileInfoList []fs.FileInfo
		wg           sync.WaitGroup
		lock         sync.Mutex
		fileNameCh   = make(chan string, 128)
	)

	if t.metaExportDir == "" {
		err = fmt.Errorf("meta export dir is null")
		return
	}

	log.LogInfof("start parse %s %s meta extent info", t.Cluster, t.VolName)

	fileInfoList, err = ioutil.ReadDir(t.metaExportDir)
	if err != nil {
		return
	}

	t.metaExtentsMap = make(map[uint64]*bitset.ByteSliceBitSet, len(fileInfoList))

	go func() {
		defer close(fileNameCh)
		for _, fileInfo := range fileInfoList {
			if fileInfo.IsDir() {
				continue
			}
			fileNameCh <- fileInfo.Name()
		}
	}()

	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				fileName, ok := <- fileNameCh
				if !ok {
					return
				}

				var (
					dpID uint64
					bitSet *bitset.ByteSliceBitSet
				)
				dpID, bitSet, err = t.parseDataPartitionGarbageBlocks(fileName, true)
				if err != nil {
					log.LogErrorf("parse file[%s] failed:%v",  path.Join(t.exportDir, fileName), err)
					continue
				}
				lock.Lock()
				t.metaExtentsMap[dpID] = bitSet
				lock.Unlock()
			}
		}()
	}
	wg.Wait()
	return
}

func (t *BlockCheckTask) parseDataPartitionGarbageBlocks(fileName string, parseMetaDir bool) (dpID uint64, bitSet *bitset.ByteSliceBitSet, err error) {
	dpID, err = strconv.ParseUint(fileName, 10, 64)
	if err != nil {
		return
	}

	var filePath string
	if parseMetaDir{
		filePath = path.Join(t.metaExportDir, fileName)
	} else {
		filePath = path.Join(t.exportDir, fileName)
	}

	fp, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		return
	}

	bitSet = bitset.NewByteSliceBitSetWithCap(storage.MaxExtentCount*3)
	extentsIDStr := strings.Split(string(data), "\n")
	var extentID uint64
	for _, extentIDStr := range extentsIDStr {
		extentID, err = strconv.ParseUint(extentIDStr, 10, 64)
		if err != nil {
			continue
		}
		bitSet.Set(int(extentID))
	}
	return
}

func (t *BlockCheckTask) formatGarbageBlockInfoEmailContent() (emailContent string) {
	emailContent += fmt.Sprintf("%s %s 废块检查结果如下:</br>", t.Cluster, t.VolName)
	emailContent += fmt.Sprintf("废块总大小:%v </br>", t.garbageSize)
	for dpid, blockInfo := range t.garbageBlocks {
		var extentsStr = make([]string, 0, blockInfo.Cap())
		for index := 0; index <= blockInfo.MaxNum(); index++ {
			if blockInfo.Get(index) {
				extentsStr = append(extentsStr, fmt.Sprintf("%v", index))
			}
		}
		if len(extentsStr) == 0 {
			log.LogInfof("[formatGarbageBlockInfoEmailContent] %s %s data partition %v not exist garbage blocks", t.Cluster,
				t.VolName, dpid)
			continue
		}
		emailContent += fmt.Sprintf("DataPartitionID: %v, ExtentID: %s </br>", dpid, extentsStr)
	}
	return
}