package datanode

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"net"
	"strings"
	"time"
)

type DataPartitionValidateCRCTask struct {
	TaskType   uint8
	addr       string
	extents    map[uint64]*storage.ExtentInfo
	LeaderAddr string
}

func NewDataPartitionValidateCRCTask(extentFiles []*storage.ExtentInfo, source, leaderAddr string) (task *DataPartitionValidateCRCTask) {
	task = &DataPartitionValidateCRCTask{
		extents:    make(map[uint64]*storage.ExtentInfo, len(extentFiles)),
		LeaderAddr: leaderAddr,
	}
	for _, extentFile := range extentFiles {
		extentFile.Source = source
		task.extents[extentFile.FileID] = extentFile
	}
	return
}

func (dp *DataPartition) runValidateCRC(ctx context.Context) {
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
		return
	}

	dp.validateCRC(validateCRCTasks)
	end := time.Now().UnixNano()
	log.LogWarnf("action[runValidateCRC] partition(%v) finish cost[%vms].", dp.partitionID, (end-start)/int64(time.Millisecond))
}

func (dp *DataPartition) buildDataPartitionValidateCRCTask(ctx context.Context, validateCRCTasks []*DataPartitionValidateCRCTask, replicas []string) (err error) {
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
		var followerExtents []*storage.ExtentInfo
		followerAddr := replicas[index]
		if followerExtents, err = dp.getRemoteExtentInfoForValidateCRC(ctx, followerAddr); err != nil {
			log.LogErrorf("buildDataPartitionValidateCRCTask PartitionID(%v) on(%v) err(%v)", dp.partitionID, followerAddr, err)
			continue
		}
		validateCRCTasks[index] = NewDataPartitionValidateCRCTask(followerExtents, followerAddr, leaderAddr)
		validateCRCTasks[index].addr = followerAddr
	}
	return
}

func (dp *DataPartition) getLocalExtentInfoForValidateCRC() (extents []*storage.ExtentInfo, err error) {
	if !dp.ExtentStore().IsFininshLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	extents, _, err = dp.extentStore.GetAllWatermarks(storage.ExtentFilterForValidateCRC())
	if err != nil {
		err = fmt.Errorf("getLocalExtentInfoForValidateCRC DataPartition(%v) err:%v", dp.partitionID, err)
		return
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfoForValidateCRC(ctx context.Context, target string) (extentFiles []*storage.ExtentInfo, err error) {
	var packet = proto.NewPacketToGetAllExtentInfo(ctx, dp.partitionID)
	var conn *net.TCPConn
	if conn, err = gConnPool.GetConnect(target); err != nil {
		err = errors.Trace(err, "get connection failed")
		return
	}
	defer func() {
		gConnPool.PutConnectWithErr(conn, err)
	}()
	if err = packet.WriteToConn(conn); err != nil {
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
	extentFiles = make([]*storage.ExtentInfo, 0, len(reply.Data)/20)
	for index := 0; index < int(reply.Size)/20; index++ {
		var offset = index * 20
		var extentID = binary.BigEndian.Uint64(reply.Data[offset:])
		var size = binary.BigEndian.Uint64(reply.Data[offset+8:])
		var crc = binary.BigEndian.Uint32(reply.Data[offset+16:])
		extentFiles = append(extentFiles, &storage.ExtentInfo{FileID: extentID, Size: size, Crc: crc})
	}
	return
}

func (dp *DataPartition) validateCRC(validateCRCTasks []*DataPartitionValidateCRCTask) {
	if len(validateCRCTasks) <= 1 {
		return
	}
	var (
		extentInfo         *storage.ExtentInfo
		ok                 bool
		extentReplicaInfos []*storage.ExtentInfo
		extentCrcInfo      *proto.ExtentCrcInfo
		crcNotEqual        bool
		extentCrcResults   []*proto.ExtentCrcInfo
	)
	for extentID, localExtentInfo := range validateCRCTasks[0].extents {
		if localExtentInfo == nil {
			continue
		}
		if localExtentInfo.IsDeleted {
			continue
		}
		extentReplicaInfos = make([]*storage.ExtentInfo, 0, 3)
		extentReplicaInfos = append(extentReplicaInfos, localExtentInfo)
		for i := 1; i < len(validateCRCTasks); i++ {
			extentInfo, ok = validateCRCTasks[i].extents[extentID]
			if !ok || extentInfo == nil {
				continue
			}
			extentReplicaInfos = append(extentReplicaInfos, extentInfo)
		}
		if storage.IsTinyExtent(extentID) {
			extentCrcInfo, crcNotEqual = dp.checkTinyExtentFile(extentReplicaInfos)
		} else {
			extentCrcInfo, crcNotEqual = dp.checkNormalExtentFile(extentReplicaInfos)
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

func (dp *DataPartition) checkTinyExtentFile(extentInfos []*storage.ExtentInfo) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	if !needCrcRepair(extentInfos) {
		return
	}
	if !hasSameSize(extentInfos) {
		sb := new(strings.Builder)
		sb.WriteString(fmt.Sprintf("checkTinyExtentFileErr size not match, dpID[%v] FileID[%v] ", dp.partitionID, extentInfos[0].FileID))
		for _, extentInfo := range extentInfos {
			sb.WriteString(fmt.Sprintf("fm[%v]:size[%v] ", extentInfo.Source, extentInfo.Size))
		}
		log.LogWarn(sb.String())
		return
	}
	extentCrcInfo, crcNotEqual = getExtentCrcInfo(extentInfos)
	return
}

func (dp *DataPartition) checkNormalExtentFile(extentInfos []*storage.ExtentInfo) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	if !needCrcRepair(extentInfos) {
		return
	}
	extentCrcInfo, crcNotEqual = getExtentCrcInfo(extentInfos)
	return
}

func needCrcRepair(extentInfos []*storage.ExtentInfo) (needCheckCrc bool) {
	if len(extentInfos) <= 1 {
		return
	}
	baseCrc := extentInfos[0].Crc
	for _, extentInfo := range extentInfos {
		if extentInfo.Crc == 0 || extentInfo.Crc == EmptyCrcValue {
			return
		}
		if extentInfo.Crc != baseCrc {
			needCheckCrc = true
			return
		}
	}
	return
}

func hasSameSize(extentInfos []*storage.ExtentInfo) (same bool) {
	same = true
	if len(extentInfos) <= 1 {
		return
	}
	baseSize := extentInfos[0].Size
	for _, extentInfo := range extentInfos {
		if extentInfo.Size != baseSize {
			same = false
			return
		}
	}
	return
}

func getExtentCrcInfo(extentInfos []*storage.ExtentInfo) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	crcLocAddrMap := make(map[uint32][]string)
	for _, extentInfo := range extentInfos {
		crcLocAddrMap[extentInfo.Crc] = append(crcLocAddrMap[extentInfo.Crc], extentInfo.Source)
	}
	if len(crcLocAddrMap) <= 1 {
		return
	}
	crcNotEqual = true
	extentCrcInfo = &proto.ExtentCrcInfo{
		FileID:        extentInfos[0].FileID,
		ExtentNum:     len(extentInfos),
		CrcLocAddrMap: crcLocAddrMap,
	}
	return
}
