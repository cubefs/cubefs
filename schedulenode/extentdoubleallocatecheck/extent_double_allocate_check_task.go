package extentdoubleallocatecheck

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/bitset"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

var gConnPool = connpool.NewConnectPool()

type ExtentDoubleAllocateCheckTask struct {
	*proto.Task
	mpParaCnt                 int
	doubleAllocateExtentsMap  map[uint64]*bitset.ByteSliceBitSet
	doubleAllocateExtentsInfo map[string][]uint64
	metaExtentsMap            map[uint64]*bitset.ByteSliceBitSet
	masterClient              *master.MasterClient
	exportDir                 string
	inodeExtentInfoDir        string
	checkByMetaDumpFile       bool
}

func NewExtentDoubleAllocateCheckTask(task *proto.Task, mc *master.MasterClient, exportDir string) *ExtentDoubleAllocateCheckTask {
	t := new(ExtentDoubleAllocateCheckTask)
	t.Task = task
	t.masterClient = mc
	t.exportDir = exportDir
	return t
}

func walkRawFile(fp *os.File, f func(data []byte) error) (err error) {
	var buf []byte

	reader := bufio.NewReader(fp)
	for {
		var (
			line     []byte
			isPrefix bool
		)

		line, isPrefix, err = reader.ReadLine()

		buf = append(buf, line...)
		if isPrefix {
			continue
		}

		if err != nil {
			if err == io.EOF {
				if len(buf) == 0 {
					err = nil
				} else {
					err = f(buf)
				}
			}
			break
		}

		if err = f(buf); err != nil {
			break
		}
		buf = buf[:0]
	}

	return
}

func (t *ExtentDoubleAllocateCheckTask) parseExtents(extentInfoCh chan *ExtentInfo) (err error) {
	var childFilesInfo []fs.FileInfo
	childFilesInfo, err = ioutil.ReadDir(t.inodeExtentInfoDir)
	var errCh = make(chan error, len(childFilesInfo))
	var wg sync.WaitGroup
	for _, childFile := range childFilesInfo {
		if childFile.IsDir() {
			continue
		}
		wg.Add(1)
		go func(fileName string) {
			defer wg.Done()
			//open file and read line
			fp, tmpErr := os.Open(path.Join(t.inodeExtentInfoDir, fileName))
			if tmpErr != nil {
				errCh <- tmpErr
				return
			}
			defer fp.Close()
			tmpErr = walkRawFile(fp, func(data []byte) error {
				inodeExtents := &struct{
					InodeID uint64            `json:"ino"`
					Extents []proto.ExtentKey `json:"eks"`
				}{}
				if err = json.Unmarshal(data, inodeExtents); err != nil {
					return err
				}
				for _, extent := range inodeExtents.Extents {
					if (t.masterClient.IsDbBack && extent.ExtentId >= 50000000) || (!t.masterClient.IsDbBack && proto.IsTinyExtent(extent.ExtentId)) {
						continue
					}
					extentInfoCh <- &ExtentInfo{
						InodeID:         inodeExtents.InodeID,
						DataPartitionID: extent.PartitionId,
						ExtentID:        extent.ExtentId,
					}
				}
				return nil
			})
			if tmpErr != nil {
				errCh <- tmpErr
			}
		}(childFile.Name())
	}
	wg.Wait()
	select{
	case err = <- errCh:
		log.LogErrorf("parse extent info file error: %v", err)
	default:
	}
	return
}

func (t *ExtentDoubleAllocateCheckTask) getExtentsFromMetaPartition(mpId uint64, leaderAddr string, ExtentInfoCh chan *ExtentInfo) (err error) {
	var resp *proto.MpAllInodesId
	var retryCnt = 5
	var filePath = path.Join(t.inodeExtentInfoDir, fmt.Sprintf("%v", mpId))
	var fp *os.File
	fp, err = os.Create(filePath)
	if err != nil {
		log.LogErrorf("action[getExtentsFromMetaPartition] create file %s failed: %v", filePath, err)
		return
	}
	defer fp.Close()

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
	var mu sync.Mutex
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
				if len(eksResp.Extents) == 0 {
					continue
				}

				for _, ek := range eksResp.Extents {
					if (t.masterClient.IsDbBack && ek.ExtentId >= 50000000) || (!t.masterClient.IsDbBack && proto.IsTinyExtent(ek.ExtentId)) {
						continue
					}
					ExtentInfoCh <- &ExtentInfo{
						DataPartitionID: ek.PartitionId,
						ExtentID:        ek.ExtentId,
					}
				}

				//dump to mp file
				mu.Lock()
				var inodeExtentStruct = struct {
					InodeID uint64            `json:"ino"`
					Extents []proto.ExtentKey `json:"eks"`
				}{
					InodeID: inodeID,
					Extents: eksResp.Extents,
				}
				var writeData []byte
				writeData, err = json.Marshal(inodeExtentStruct)
				if err != nil {
					continue
				}
				_, err = fp.Write(writeData)
				if err != nil {
					continue
				}
				_, err = fp.Write([]byte{'\n'})
				if err != nil {
					continue
				}
				mu.Unlock()
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

func (t *ExtentDoubleAllocateCheckTask) RunOnce() {
	log.LogInfof("ExtentDoubleAllocateCheckTask RunOnce %s %s start check", t.Cluster, t.VolName)
	t.inodeExtentInfoDir = path.Join(t.exportDir, "inode_extents_info")
	err := os.MkdirAll(t.inodeExtentInfoDir, 0666)
	if err != nil {
		log.LogErrorf("ExtentDoubleAllocateCheckTask RunOnce, mkdir path %s failed: %v", t.inodeExtentInfoDir, err)
		return
	}
	defer func() {
		os.RemoveAll(t.inodeExtentInfoDir)
	}()
	err = t.checkDoubleAllocateExtents()
	if err != nil {
		log.LogErrorf("ExtentDoubleAllocateCheckTask RunOnce, %s %s check double allocate extents failed: %v", t.Cluster, t.VolName, err)
		return
	}
	if len(t.doubleAllocateExtentsMap) == 0 {
		log.LogInfof("ExtentDoubleAllocateCheckTask Runonce %s %s check finish, not exist double allocate extents", t.Cluster, t.VolName)
		return
	}
	t.doubleAllocateExtentsInfo = make(map[string][]uint64, 0)
	for dpID, blockInfo := range t.doubleAllocateExtentsMap {
		var extentIDs = make([]uint64, 0, blockInfo.Cap())
		for index := 0; index <= blockInfo.MaxNum(); index++ {
			if blockInfo.Get(index) {
				extentIDs = append(extentIDs, uint64(index))
				t.doubleAllocateExtentsInfo[fmt.Sprintf("%v_%v", dpID, index)] = make([]uint64, 0)
			}
		}
		if len(extentIDs) == 0 {
			continue
		}
		log.LogCriticalf("ExtentDoubleAllocateCheckTask Runonce cluster(%s) vol(%s) dataPartition(%v) doubleAllocateExtents(%v)",
			t.Cluster, t.VolName, dpID, extentIDs)
	}
	//extent search
	extentInfoCh := make(chan *ExtentInfo, 512)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			extentInfo, ok := <- extentInfoCh
			if !ok || extentInfo == nil {
				break
			}

			if _, has := t.doubleAllocateExtentsInfo[fmt.Sprintf("%v_%v", extentInfo.DataPartitionID, extentInfo.ExtentID)]; has {
				t.doubleAllocateExtentsInfo[fmt.Sprintf("%v_%v", extentInfo.DataPartitionID, extentInfo.ExtentID)] = append(
					t.doubleAllocateExtentsInfo[fmt.Sprintf("%v_%v", extentInfo.DataPartitionID, extentInfo.ExtentID)], extentInfo.InodeID)
			}
		}
	}()

	if err = t.parseExtents(extentInfoCh); err != nil {
		log.LogErrorf("ExtentDoubleAllocateCheckTask RunOnce cluster(%s) vol(%s) parse inode extents failed: %v",
			t.Cluster, t.VolName, err)
	}
	close(extentInfoCh)
	wg.Wait()
	log.LogInfof("ExtentDoubleAllocateCheckTask RunOnce %s %s check finish", t.Cluster, t.VolName)
}

func (t *ExtentDoubleAllocateCheckTask) checkDoubleAllocateExtents() (err error) {
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
			if t.metaExtentsMap[e.DataPartitionID].Get(int(e.ExtentID)) {
				if _, ok := t.doubleAllocateExtentsMap[e.DataPartitionID]; !ok {
					t.doubleAllocateExtentsMap[e.DataPartitionID] = bitset.NewByteSliceBitSetWithCap(storage.MaxExtentCount*3)
				}
				t.doubleAllocateExtentsMap[e.DataPartitionID].Set(int(e.ExtentID))
			} else {
				t.metaExtentsMap[e.DataPartitionID].Set(int(e.ExtentID))
			}
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

func (t *ExtentDoubleAllocateCheckTask) formatDoubleAllocateExtents() (emailContent string) {
	emailContent += fmt.Sprintf("%s %s Extent重复分配检查结果如下:</br>", t.Cluster, t.VolName)
	for dpid, blockInfo := range t.doubleAllocateExtentsMap {
		var extentsStr = make([]string, 0, blockInfo.Cap())
		for index := 0; index <= blockInfo.MaxNum(); index++ {
			if blockInfo.Get(index) {
				extentsStr = append(extentsStr, fmt.Sprintf("%v", index))
			}
		}
		if len(extentsStr) == 0 {
			log.LogInfof("[formatDoubleAllocateExtents] %s %s data partition %v not exist garbage blocks", t.Cluster,
				t.VolName, dpid)
			continue
		}
		emailContent += fmt.Sprintf("DataPartitionID: %v, ExtentID: %s </br>", dpid, extentsStr)
	}

	for key, inodes := range t.doubleAllocateExtentsInfo {
		arr := strings.Split(key, "_")
		if len(arr) != 2 {
			log.LogErrorf("ExtentDoubleAllocateCheckTask Runonce cluster(%s) vol(%s) extentInfo(%s) inodes(%v)",
				t.Cluster, t.VolName, key, inodes)
			continue
		}
		inodesStr := make([]string, 0, len(inodes))
		for _, inode := range inodes {
			inodesStr = append(inodesStr, fmt.Sprintf("%v", inode))
		}
		emailContent += fmt.Sprintf("DataPartitionID: %v, ExtentID: %s, OwnerInodes: %s </br>", arr[0], arr[1], strings.Join(inodesStr, ","))
		log.LogCriticalf("ExtentDoubleAllocateCheckTask Runonce cluster(%s) vol(%s) dataPartition(%v) extentID(%v) ownerInodes(%v)",
			t.Cluster, t.VolName, arr[0], arr[1], strings.Join(inodesStr, ","))
	}

	return
}