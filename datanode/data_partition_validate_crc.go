package datanode

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/sdk/data"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

type DataPartitionValidateCRCTask struct {
	TaskType   uint8
	addr       string
	extents    map[uint64]storage.ExtentInfoBlock
	LeaderAddr string
	Source     string
}

func NewDataPartitionValidateCRCTask(extentFiles []storage.ExtentInfoBlock, source, leaderAddr string) (task *DataPartitionValidateCRCTask) {
	task = &DataPartitionValidateCRCTask{
		extents:    make(map[uint64]storage.ExtentInfoBlock, len(extentFiles)),
		LeaderAddr: leaderAddr,
		Source:     source,
	}
	for _, extentFile := range extentFiles {
		task.extents[extentFile[storage.FileID]] = extentFile
	}
	return
}

func (dp *DataPartition) runValidateCRC(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("DataPartition(%v) runValidateCRC panic(%v)", dp.partitionID, r)
			log.LogWarnf(msg)
		}
	}()
	if dp.partitionStatus == proto.Unavailable {
		return
	}
	if !dp.isLeader {
		return
	}

	start := time.Now().UnixNano()
	log.LogInfof("action[runValidateCRC] partition(%v) start.", dp.partitionID)
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		log.LogErrorf("action[runValidateCRC] partition(%v) replicas is nil.", dp.partitionID)
		return
	}

	validateCRCTasks := make([]*DataPartitionValidateCRCTask, len(replicas))
	err := dp.buildDataPartitionValidateCRCTask(ctx, validateCRCTasks, replicas)
	if err != nil {
		log.LogErrorf("action[runValidateCRC] partition(%v) err(%v).", dp.partitionID, err)
		if isGetConnectError(err) || isConnectionRefusedFailure(err) || isIOTimeoutFailure(err) || isPartitionRecoverFailure(err) {
			return
		}
		dpCrcInfo := proto.DataPartitionExtentCrcInfo{
			PartitionID:               dp.partitionID,
			IsBuildValidateCRCTaskErr: true,
			ErrMsg:                    err.Error(),
		}
		if err = MasterClient.NodeAPI().DataNodeValidateCRCReport(&dpCrcInfo); err != nil {
			log.LogErrorf("report DataPartition Validate CRC result failed,PartitionID(%v) err:%v", dp.partitionID, err)
			return
		}
		return
	}

	dp.validateCRC(validateCRCTasks)
	end := time.Now().UnixNano()
	log.LogWarnf("action[runValidateCRC] partition(%v) finish cost[%vms].", dp.partitionID, (end-start)/int64(time.Millisecond))
}

func isGetConnectError(err error) bool {
	if strings.Contains(err.Error(), errorGetConnectMsg) {
		return true
	}
	return false
}

func isConnectionRefusedFailure(err error) bool {
	if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorConnRefusedMsg)) {
		return true
	}
	return false
}

func isIOTimeoutFailure(err error) bool {
	if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorIOTimeoutMsg)) {
		return true
	}
	return false
}
func isPartitionRecoverFailure(err error) bool {
	if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorPartitionRecoverMsg)) {
		return true
	}
	return false
}

func (dp *DataPartition) buildDataPartitionValidateCRCTask(ctx context.Context, validateCRCTasks []*DataPartitionValidateCRCTask, replicas []string) (err error) {
	if isRecover, err1 := dp.isRecover(); err1 != nil || isRecover {
		err = fmt.Errorf(errorPartitionRecoverMsg)
		return
	}
	// get the local extent info
	extents, err := dp.getLocalExtentInfoForValidateCRC()
	if err != nil {
		return err
	}
	leaderAddr := replicas[0]
	// new validate crc task for the leader
	validateCRCTasks[0] = NewDataPartitionValidateCRCTask(extents, leaderAddr, leaderAddr)
	validateCRCTasks[0].addr = leaderAddr

	// new validate crc task for the followers
	for index := 1; index < len(replicas); index++ {
		var followerExtents []storage.ExtentInfoBlock
		followerAddr := replicas[index]
		if followerExtents, err = dp.getRemoteExtentInfoForValidateCRCWithRetry(ctx, followerAddr); err != nil {
			return
		}
		validateCRCTasks[index] = NewDataPartitionValidateCRCTask(followerExtents, followerAddr, leaderAddr)
		validateCRCTasks[index].addr = followerAddr
	}
	return
}

func (dp *DataPartition) isRecover() (isRecover bool, err error) {
	var replyNum uint8
	isRecover, replyNum = dp.getRemoteReplicaRecoverStatus(context.Background())
	if int(replyNum) < len(dp.config.Hosts) / 2 {
		err = fmt.Errorf("reply from remote replica is no enough")
		return
	}
	return isRecover, nil
}

// Get all replica recover status
func (dp *DataPartition) getRemoteReplicaRecoverStatus(ctx context.Context) (recovering bool, replyNum uint8) {
	hosts := dp.getReplicaClone()
	if len(hosts) == 0 {
		log.LogErrorf("action[getRemoteReplicaRecoverStatus] partition(%v) replicas is nil.", dp.partitionID)
		return
	}
	errSlice := make(map[string]error)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)
	for _, host := range hosts {
		if dp.IsLocalAddress(host) {
			continue
		}
		wg.Add(1)
		go func(curAddr string) {
			var isRecover bool
			var err error
			isRecover, err = dp.isRemotePartitionRecover(curAddr)

			ok := false
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				errSlice[curAddr] = err
			} else {
				ok = true
				if isRecover {
					recovering = true
				}
			}
			log.LogDebugf("action[getRemoteReplicaRecoverStatus]: get commit id[%v] ok[%v] from host[%v], pid[%v]", isRecover, ok, curAddr, dp.partitionID)
			wg.Done()
		}(host)
	}
	wg.Wait()
	replyNum = uint8(len(hosts) - 1 - len(errSlice))
	log.LogDebugf("action[getRemoteReplicaRecoverStatus]: get commit id from hosts[%v], pid[%v]", hosts, dp.partitionID)
	return
}

// Get target members recover status
func (dp *DataPartition) isRemotePartitionRecover(target string) (isRecover bool, err error) {
	if dp.disk == nil || dp.disk.space == nil || dp.disk.space.dataNode == nil {
		err = fmt.Errorf("action[getRemotePartitionSimpleInfo] data node[%v] not ready", target)
		return
	}
	profPort := dp.disk.space.dataNode.httpPort
	httpAddr := fmt.Sprintf("%v:%v", strings.Split(target, ":")[0], profPort)
	dataClient := data.NewDataHttpClient(httpAddr, false)
	var dpInfo *proto.DNDataPartitionInfo
	for i := 0; i < 3; i++ {
		dpInfo, err = dataClient.GetPartitionSimple(dp.partitionID)
		if err == nil {
			break
		}
	}
	if err != nil {
		err = fmt.Errorf("action[getRemotePartitionSimpleInfo] datanode[%v] get partition failed, err:%v", target, err)
		return
	}
	isRecover = dpInfo.IsRecover
	log.LogDebugf("[getRemotePartitionSimpleInfo] partition(%v) recover(%v)", dp.partitionID, dpInfo.IsRecover)
	return
}
func (dp *DataPartition) getLocalExtentInfoForValidateCRC() (extents []storage.ExtentInfoBlock, err error) {
	if !dp.ExtentStore().IsFinishLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	extents, err = dp.extentStore.GetAllWatermarks(proto.AllExtentType, storage.ExtentFilterForValidateCRC())
	if err != nil {
		err = fmt.Errorf("getLocalExtentInfoForValidateCRC DataPartition(%v) err:%v", dp.partitionID, err)
		return
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfoForValidateCRCWithRetry(ctx context.Context, target string) (extentFiles []storage.ExtentInfoBlock, err error) {
	for i := 0; i < GetRemoteExtentInfoForValidateCRCRetryTimes; i++ {
		extentFiles, err = dp.getRemoteExtentInfoForValidateCRC(ctx, target)
		if err == nil {
			return
		}
		log.LogWarnf("getRemoteExtentInfoForValidateCRCWithRetry PartitionID(%v) on(%v) err(%v)", dp.partitionID, target, err)
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfoForValidateCRC(ctx context.Context, target string) (extentFiles []storage.ExtentInfoBlock, err error) {
	var packet = proto.NewPacketToGetAllExtentInfo(ctx, dp.partitionID)
	var conn *net.TCPConn
	if conn, err = gConnPool.GetConnect(target); err != nil {
		err = errors.Trace(err, errorGetConnectMsg)
		return
	}
	defer func() {
		gConnPool.PutConnectWithErr(conn, err)
	}()
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.Trace(err, "write packet to connection failed")
		return
	}
	var reply = new(repl.Packet)
	reply.SetCtx(ctx)
	if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
		err = errors.Trace(err, "read reply from connection failed")
		return
	}
	if reply.ResultCode != proto.OpOk {
		err = errors.NewErrorf("reply result code: %v", reply.GetOpMsg())
		return
	}
	if reply.Size%20 != 0 {
		// 合法的data长度与20对齐，每20个字节存储一个Extent信息，[0:8)为FileID，[8:16)为Size，[16:20)为Crc
		err = errors.NewErrorf("illegal result data length: %v", len(reply.Data))
		return
	}
	extentFiles = make([]storage.ExtentInfoBlock, 0, len(reply.Data)/20)
	for index := 0; index < int(reply.Size)/20; index++ {
		var offset = index * 20
		var extentID = binary.BigEndian.Uint64(reply.Data[offset:])
		var size = binary.BigEndian.Uint64(reply.Data[offset+8:])
		var crc = binary.BigEndian.Uint32(reply.Data[offset+16:])
		eiBlock := storage.ExtentInfoBlock{
			storage.FileID: extentID,
			storage.Size:   size,
			storage.Crc:    uint64(crc),
		}
		extentFiles = append(extentFiles, eiBlock)
	}
	return
}

func (dp *DataPartition) validateCRC(validateCRCTasks []*DataPartitionValidateCRCTask) {
	if len(validateCRCTasks) <= 1 {
		return
	}
	var (
		extentInfo          storage.ExtentInfoBlock
		ok                  bool
		extentReplicaInfos  []storage.ExtentInfoBlock
		extentReplicaSource map[int]string
		extentCrcInfo       *proto.ExtentCrcInfo
		crcNotEqual         bool
		extentCrcResults    []*proto.ExtentCrcInfo
	)
	for extentID, localExtentInfo := range validateCRCTasks[0].extents {
		extentReplicaInfos = make([]storage.ExtentInfoBlock, 0, len(validateCRCTasks))
		extentReplicaSource = make(map[int]string, len(validateCRCTasks))
		extentReplicaInfos = append(extentReplicaInfos, localExtentInfo)
		extentReplicaSource[0] = validateCRCTasks[0].Source
		for i := 1; i < len(validateCRCTasks); i++ {
			extentInfo, ok = validateCRCTasks[i].extents[extentID]
			if !ok {
				continue
			}
			extentReplicaInfos = append(extentReplicaInfos, extentInfo)
			extentReplicaSource[len(extentReplicaInfos)-1] = validateCRCTasks[i].Source
		}
		if proto.IsTinyExtent(extentID) {
			extentCrcInfo, crcNotEqual = dp.checkTinyExtentFile(extentReplicaInfos, extentReplicaSource)
		} else {
			extentCrcInfo, crcNotEqual = dp.checkNormalExtentFile(extentReplicaInfos, extentReplicaSource)
		}
		if crcNotEqual {
			extentCrcResults = append(extentCrcResults, extentCrcInfo)
		}
	}

	if len(extentCrcResults) != 0 {
		dpCrcInfo := proto.DataPartitionExtentCrcInfo{
			PartitionID:    dp.partitionID,
			ExtentCrcInfos: extentCrcResults,
		}
		if err := MasterClient.NodeAPI().DataNodeValidateCRCReport(&dpCrcInfo); err != nil {
			log.LogErrorf("report DataPartition Validate CRC result failed,PartitionID(%v) err:%v", dp.partitionID, err)
			return
		}
	}
	return
}

func (dp *DataPartition) checkTinyExtentFile(extentInfos []storage.ExtentInfoBlock, extentReplicaSource map[int]string) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	if !needCrcRepair(extentInfos) {
		return
	}
	if !hasSameSize(extentInfos) {
		sb := new(strings.Builder)
		sb.WriteString(fmt.Sprintf("checkTinyExtentFileErr size not match, dpID[%v] FileID[%v] ", dp.partitionID, extentInfos[0][storage.FileID]))
		for i, extentInfo := range extentInfos {
			sb.WriteString(fmt.Sprintf("fm[%v]:size[%v] ", extentReplicaSource[i], extentInfo[storage.Size]))
		}
		log.LogWarn(sb.String())
		return
	}
	extentCrcInfo, crcNotEqual = getExtentCrcInfo(extentInfos, extentReplicaSource)
	return
}

func (dp *DataPartition) checkNormalExtentFile(extentInfos []storage.ExtentInfoBlock, extentReplicaSource map[int]string) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	if !needCrcRepair(extentInfos) {
		return
	}
	extentCrcInfo, crcNotEqual = getExtentCrcInfo(extentInfos, extentReplicaSource)
	return
}

func needCrcRepair(extentInfos []storage.ExtentInfoBlock) (needCheckCrc bool) {
	if len(extentInfos) <= 1 {
		return
	}
	baseCrc := extentInfos[0][storage.Crc]
	for _, extentInfo := range extentInfos {
		if extentInfo[storage.Crc] == 0 || extentInfo[storage.Crc] == EmptyCrcValue {
			return
		}
		if extentInfo[storage.Crc] != baseCrc {
			needCheckCrc = true
			return
		}
	}
	return
}

func hasSameSize(extentInfos []storage.ExtentInfoBlock) (same bool) {
	same = true
	if len(extentInfos) <= 1 {
		return
	}
	baseSize := extentInfos[0][storage.Size]
	for _, extentInfo := range extentInfos {
		if extentInfo[storage.Size] != baseSize {
			same = false
			return
		}
	}
	return
}

func getExtentCrcInfo(extentInfos []storage.ExtentInfoBlock, extentReplicaSource map[int]string) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	crcLocAddrMap := make(map[uint64][]string)
	for i, extentInfo := range extentInfos {
		crcLocAddrMap[extentInfo[storage.Crc]] = append(crcLocAddrMap[extentInfo[storage.Crc]], extentReplicaSource[i])
	}
	if len(crcLocAddrMap) <= 1 {
		return
	}
	crcNotEqual = true
	extentCrcInfo = &proto.ExtentCrcInfo{
		FileID:        extentInfos[0][storage.FileID],
		ExtentNum:     len(extentInfos),
		CrcLocAddrMap: crcLocAddrMap,
	}
	return
}
