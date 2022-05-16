package ecnode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/ecstorage"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/ec"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/shopspring/decimal"
	"hash/crc32"
	"sync"
	"sync/atomic"
)

type ecStripe struct {
	e               *EcNode
	ep              *EcPartition
	partitionId     uint64
	extentID        uint64
	dataSize        uint64
	paritySize      uint64
	localServerAddr string
	hosts           []string
	coder           *ec.EcHandler
}

type extentData struct {
	OriginExtentSize uint64 `json:"originExtentSize"`
	TinyDelNodeIndex uint32 `json:"tinyDelNodeIndex"`
	RepairFlag       int    `json:"repairFlag"`
	Data             []byte `json:"data"`
}

func NewEcStripe(ep *EcPartition, stripeUnitSize uint64, extentID uint64) (*ecStripe, error) {
	nodeNum := len(ep.Hosts)
	hosts := proto.GetEcHostsByExtentId(uint64(nodeNum), extentID, ep.Hosts)
	coder, err := ec.NewEcCoder(int(stripeUnitSize), int(ep.EcDataNum), int(ep.EcParityNum))
	if err != nil {
		log.LogErrorf("NewEcCoder fail. err:%v", err)
		return nil, errors.NewErrorf("NewEcCoder fail. err:%v", err)
	}

	return &ecStripe{
		e:               ep.ecNode,
		ep:              ep,
		partitionId:     ep.PartitionID,
		extentID:        extentID,
		dataSize:        stripeUnitSize * uint64(ep.EcDataNum),
		paritySize:      stripeUnitSize * uint64(ep.EcParityNum),
		localServerAddr: ep.ecNode.localServerAddr,
		hosts:           hosts,
		coder:           coder,
	}, nil
}

func (ee *ecStripe) repairReadStripeUnitData(readNodeAddr string, offset, curReadSize uint64, readFlag int, repairInfo *repairExtentInfo) (data []byte, crc uint32, err error) {
	if curReadSize == util.ReadBlockSize {
		data, _ = proto.Buffers.Get(util.ReadBlockSize)
	} else {
		data = make([]byte, curReadSize)
	}
	if readFlag == proto.DeleteRepair && readNodeAddr == ee.ep.Hosts[repairInfo.tinyDelNodeIndex] { //tiny delete update parity node,data is 0
		log.LogDebugf("readStripeData offset(%v) readSize(%v) delNodeAddr(%v)",
			offset, curReadSize, ee.ep.Hosts[repairInfo.tinyDelNodeIndex])
		return
	}

	// read from node
	request := repl.NewExtentStripeRead(ee.partitionId, ee.extentID, offset, curReadSize)
	if storage.IsTinyExtent(ee.extentID) {
		request.Opcode = proto.OpEcTinyRepairRead
	}

	err = DoRequest(request, readNodeAddr, proto.ReadDeadlineTime, ee.e)
	if err != nil {
		log.LogErrorf("read from remote[%v] fail. packet:%v offset[%v] size[%v] crc[%v] readFlag[%v] err:%v",
			readNodeAddr, request, offset, request.Size, request.CRC, readFlag, err)
		data = nil
		return
	}
	if request.ResultCode != proto.OpOk {
		log.LogErrorf("read from remote[%v] fail. packet:%v offset[%v] size[%v] crc[%v] err:%v ResultCode[%v]",
			readNodeAddr, request, offset, request.Size, request.CRC, err, request.ResultCode)
		err = errors.New("ExtentStripeRead err, request.ResultCode != proto.OpOk")
		data = nil
		return
	}
	log.LogDebugf("read from remote[%v]. packet:%v offset[%v] size[%v] crc[%v]",
		readNodeAddr, request, offset, curReadSize, request.CRC)
	data = request.Data
	crc = request.CRC
	return
}

func (ee *ecStripe) readStripeUnitData(readNodeAddr string, offset, curReadSize uint64, readFlag int) (data []byte, crc uint32, err error) {
	if curReadSize == util.ReadBlockSize {
		data, _ = proto.Buffers.Get(util.ReadBlockSize)
	} else {
		data = make([]byte, curReadSize)
	}

	// read from node
	request := repl.NewExtentStripeRead(ee.partitionId, ee.extentID, offset, curReadSize)
	err = DoRequest(request, readNodeAddr, proto.ReadDeadlineTime, ee.e)
	if err != nil {
		log.LogErrorf("read from remote[%v] fail. packet:%v offset[%v] size[%v] crc[%v] readFlag[%v] err:%v",
			readNodeAddr, request, offset, request.Size, request.CRC, readFlag, err)
		data = nil
		return
	}
	if request.ResultCode != proto.OpOk {
		log.LogErrorf("read from remote[%v] fail. packet:%v offset[%v] size[%v] crc[%v] err:%v ResultCode[%v]",
			readNodeAddr, request, offset, request.Size, request.CRC, err, request.ResultCode)
		err = errors.New("ExtentStripeRead err, request.ResultCode != proto.OpOk")
		data = nil
		return
	}
	log.LogDebugf("read from remote[%v]. packet:%v offset[%v] size[%v] crc[%v]",
		readNodeAddr, request, offset, curReadSize, request.CRC)
	data = request.Data
	crc = request.CRC
	return
}

func (ee *ecStripe) calcNode(extentOffset uint64, stripeUnitSize uint64) string {
	stripeUnitSizeDecimal := decimal.NewFromInt(int64(stripeUnitSize))
	extentOffsetDecimal := decimal.NewFromInt(int64(extentOffset))
	div := extentOffsetDecimal.Div(stripeUnitSizeDecimal).Floor()
	index := div.Mod(decimal.NewFromInt(int64(ee.ep.EcDataNum))).IntPart()
	return ee.hosts[index]
}

func (ee *ecStripe) calcStripeUnitFileOffset(extentOffset uint64, stripeUnitSize uint64) uint64 {
	stripeUnitSizeDecimal := decimal.NewFromInt(int64(stripeUnitSize))
	extentOffsetDecimal := decimal.NewFromInt(int64(extentOffset))
	div := extentOffsetDecimal.Div(stripeUnitSizeDecimal).Floor()
	stripeN := div.Div(decimal.NewFromInt(int64(ee.ep.EcDataNum))).Floor().IntPart()
	offset := extentOffsetDecimal.Mod(stripeUnitSizeDecimal).IntPart()
	return uint64(stripeN)*stripeUnitSize + uint64(offset)
}

func (ee *ecStripe) calcCanReadSize(extentOffset uint64, canReadSize uint64, stripeUnitSize uint64) uint64 {
	canReadSizeInStripeUnit := stripeUnitSize - extentOffset%stripeUnitSize
	if canReadSize < canReadSizeInStripeUnit {
		return canReadSize
	} else {
		return canReadSizeInStripeUnit
	}
}

func (ee *ecStripe) calcCanDelSize(extentOffset uint64, canDelSize uint64, stripeUnitSize uint64) uint64 {
	canDelSizeInStripeUnit := stripeUnitSize - extentOffset%stripeUnitSize
	if canDelSize < canDelSizeInStripeUnit {
		return canDelSize
	} else {
		return canDelSizeInStripeUnit
	}
}

func (ee *ecStripe) verifyAndReconstructData(data [][]byte, size uint64) (err error) {
	verify, _ := ee.coder.Verify(data)
	if verify {
		return
	}

	// reconstruct data only support the situation that the EcExtent file does not exist.
	// If the EcExtent file exist, but it have silent data corruption or bit rot problem, it cannot be repaired.
	// Because we use the reedsolomon library that is not support this action
	err = ee.reconstructData(data, size)
	if err != nil {
		return errors.NewErrorf("reconstruct data fail, PartitionID(%v) ExtentID(%v) node(%v), err:%v",
			ee.partitionId, ee.extentID, ee.hosts, err)
	}
	return
}

func (ee *ecStripe) reconstructStripeData(stripeUnitFileOffset, needReadSize uint64, repairInfo *repairExtentInfo, readFlag int) (data [][]byte, err error) {
	data, err = ee.repairReadStripeData(stripeUnitFileOffset, needReadSize, repairInfo, readFlag)
	if err != nil {
		log.LogErrorf("reconstructStripeData[%v] extentId[%v]", err, ee.extentID)
		return
	}
	err = ee.verifyAndReconstructData(data, needReadSize)
	return
}

func (ee *ecStripe) repairStripeData(stripeUnitFileOffset, needReadSize uint64, repairInfo *repairExtentInfo, readFlag int) ([][]byte, error) {
	data, err := ee.reconstructStripeData(stripeUnitFileOffset, needReadSize, repairInfo, readFlag)
	if err != nil {
		return nil, err
	}

	log.LogDebugf("reconstruct data success, PartitionID(%v) ExtentID(%v) node(%v) offset(%v)",
		ee.partitionId, ee.extentID, ee.hosts, stripeUnitFileOffset)
	if readFlag != proto.DegradeRead { //degradeRead don't need repair data
		err = ee.writeBackReconstructData(data, stripeUnitFileOffset, repairInfo, readFlag)
	}
	return data, err
}

func isRepairHost(nodeAddr string, repairHosts []*repairHostInfo) (needRepair bool) {
	needRepair = false
	for _, repairHost := range repairHosts {
		if nodeAddr == repairHost.nodeAddr {
			needRepair = true
			break
		}
	}
	return
}

func (ee *ecStripe) readStripeData(stripeUnitFileOffset, needReadSize uint64, readFlag int) (data [][]byte, err error) {
	var (
		successDataNum int32
		originExtentSize uint64
		stripeUnitFileSize uint64
	    repairInfo repairExtentInfo
	)
	ecPartition := ee.ep
	ecNodeNum := len(ecPartition.Hosts)
	data = make([][]byte, ecNodeNum)

	value, ok := ecPartition.originExtentSizeMap.Load(ee.extentID)
	if !ok {
		err = ecstorage.ExtentNotFoundError
		originExtentSize = value.(uint64)
		return
	}
	stripeUnitFileSize, err = ee.calcStripeUnitFileSize(originExtentSize, ee.hosts[ecNodeNum-1])
	if err != nil {
		return
	}
	ecPartition.fillRepairExtentInfo(stripeUnitFileSize, stripeUnitFileOffset, needReadSize, originExtentSize, ee.hosts[ecNodeNum-1], &repairInfo)
	wg := sync.WaitGroup{}
	for index := 0; index < len(ee.hosts); index++ {
		wg.Add(1)
		go func(innerData [][]byte, index int, innerNodeAddr string) {
			defer wg.Done()
			innerData[index], _, err = ee.repairReadStripeUnitData(innerNodeAddr, stripeUnitFileOffset, needReadSize, readFlag, &repairInfo)
			if err == nil {
				atomic.AddInt32(&successDataNum, 1)
			}
		}(data, index, ee.hosts[index])
	}
	wg.Wait()
	if successDataNum < int32(ecPartition.EcDataNum) {
		err = errors.NewErrorf("no enough data to reconstruct successDataNum[%v]", successDataNum)
	}
	return
}

func (ee *ecStripe) repairReadStripeData(stripeUnitFileOffset, needReadSize uint64, repairInfo *repairExtentInfo, readFlag int) (data [][]byte, err error) {
	var (
		readDataNum    int32
		successDataNum int32
	)
	ecPartition := ee.ep
	ecNodeNum := len(ecPartition.Hosts)
	data = make([][]byte, ecNodeNum)
	wg := sync.WaitGroup{}
	for index := 0; index < len(ee.hosts); index++ {
		if isRepairHost(ee.hosts[index], repairInfo.needRepairHosts) { //don't need read repairHost data
			continue
		}
		wg.Add(1)
		readDataNum++
		go func(innerData [][]byte, index int, innerNodeAddr string) {
			defer wg.Done()
			innerData[index], _, err = ee.repairReadStripeUnitData(innerNodeAddr, stripeUnitFileOffset, needReadSize, readFlag, repairInfo)
			if err == nil {
				atomic.AddInt32(&successDataNum, 1)
			}
		}(data, index, ee.hosts[index])
		if readDataNum >= int32(ecPartition.EcDataNum) { //can repair data, don't need read next
			wg.Wait()
			readDataNum = atomic.LoadInt32(&successDataNum)
			errHosts := ecPartition.EcDataNum - uint32(readDataNum) + uint32(len(repairInfo.needRepairHosts))
			if errHosts > ecPartition.EcParityNum {
				err = errors.NewErrorf("no enough data to reconstruct ecDataNum[%v] errHost[%v] successNum[%v]",
					ecPartition.EcDataNum, errHosts, readDataNum)
				return
			}
			if readDataNum < int32(ecPartition.EcDataNum) {
				continue
			}
			break
		}
	}
	wg.Wait()
	if readDataNum < int32(ecPartition.EcDataNum) {
		err = errors.NewErrorf("no enough data to reconstruct[%v]", readDataNum)
	}
	return
}

func (ee *ecStripe) reconstructData(pBytes [][]byte, stripeUnitSize uint64) error {
	log.LogDebugf("reconstructData, partitionID(%v) extentID(%v)", ee.partitionId, ee.extentID)
	ep := ee.ep
	coder, err := ec.NewEcCoder(int(stripeUnitSize), int(ep.EcDataNum), int(ep.EcParityNum))
	if err != nil {
		return errors.New(fmt.Sprintf("NewEcCoder error:%s", err))
	}

	return coder.Reconstruct(pBytes)
}

func getRepairHostInfo(repairInfo *repairExtentInfo, nodeAddr string) *repairHostInfo {
	for _, hostInfo := range repairInfo.needRepairHosts {
		if hostInfo.nodeAddr == nodeAddr {
			return hostInfo
		}
	}
	return nil
}

// only write back the data that the EcExtent file does not exist.
// Because we use the reedsolomon library that only support handle this situation.
func (ee *ecStripe) writeBackReconstructData(data [][]byte, stripeUnitFileOffset uint64, repairInfo *repairExtentInfo, readFlag int) (err error) {
	for i := 0; i < len(ee.hosts); i++ {
		nodeAddr := ee.hosts[i]
		if !isRepairHost(nodeAddr, repairInfo.needRepairHosts) {
			continue
		}
		hostInfo := getRepairHostInfo(repairInfo, nodeAddr)
		if hostInfo == nil {
			return errors.NewErrorf("nodeAddr[%v] don't need repair", nodeAddr)
		}

		repairSize := uint64(len(data[i]))
		newOffset := stripeUnitFileOffset
		log.LogDebugf("stripeUnitFileOffset(%v) repairSize(%v) offset(%v) stripeUnitFileSize(%v) extentId(%v)",
			stripeUnitFileOffset, repairSize, hostInfo.offset, hostInfo.stripeUnitFileSize, ee.extentID)
		if stripeUnitFileOffset+repairSize <= hostInfo.offset { //node don't need repair
			return
		}

		if stripeUnitFileOffset < hostInfo.offset {
			newOffset = hostInfo.offset
			repairSize = repairSize - (newOffset - stripeUnitFileOffset)
		}

		if newOffset+repairSize > hostInfo.stripeUnitFileSize {
			repairSize = hostInfo.stripeUnitFileSize - newOffset
		}

		dataStartIndex := newOffset - stripeUnitFileOffset
		dataEndIndex := dataStartIndex + repairSize

		newData := &extentData{
			OriginExtentSize: repairInfo.originExtentSize,
			TinyDelNodeIndex: repairInfo.tinyDelNodeIndex,
			RepairFlag:       readFlag,
			Data:             data[i][dataStartIndex:dataEndIndex],
		}
		var ctx context.Context
		request := repl.NewPacket(ctx)
		request.ExtentID = ee.extentID
		request.PartitionID = ee.partitionId
		request.ExtentOffset = int64(newOffset)
		request.Data, err = json.Marshal(newData)
		request.Size = uint32(len(request.Data))
		if err != nil {
			log.LogErrorf("extentData marshal error[%v]", err)
			return
		}
		request.CRC = crc32.ChecksumIEEE(request.Data)
		request.Opcode = proto.OpNotifyReplicasToRepair
		request.ReqID = proto.GenerateRequestID()
		err = DoRequest(request, nodeAddr, proto.ReadDeadlineTime, ee.e)
		// if err not nil, do nothing, only record log
		if err != nil || request.ResultCode != proto.OpOk {
			err = errors.NewErrorf("writeBack reconstruct data fail. partition(%v) extent(%v) node(%v) offset(%v) size(%v), resultCode(%v) err:%v",
				ee.partitionId, ee.extentID, nodeAddr, stripeUnitFileOffset, len(data[i]), request.ResultCode, err)
			return
		}
		log.LogDebugf("writeBack reconstruct data. partition(%v) extent(%v) node(%v) offset(%v) size(%v)",
			ee.partitionId, ee.extentID, nodeAddr, stripeUnitFileOffset, len(data[i]))
	}
	return
}

func (ee *ecStripe) calcStripeUnitFileSize(extentSize uint64, nodeAddr string) (stripeUnitFileSize uint64, err error) {
	stripeUnitSize := ee.dataSize / uint64(ee.ep.EcDataNum)
	stripeN := extentSize / ee.dataSize
	remainSize := extentSize % stripeUnitSize
	idx := -1
	for i, host := range ee.hosts {
		if host == nodeAddr {
			idx = i
			break
		}
	}

	if idx == -1 {
		log.LogErrorf("calcRealstripeUnitSize in hosts(%v) not fount host(%v)", ee.hosts, ee.localServerAddr)
		err = errors.New("calcRealstripeUnitSize not fount host")
		return
	}

	if idx >= int(ee.ep.EcDataNum) {
		stripeRemainSize := extentSize % ee.dataSize
		if stripeRemainSize == 0 {
			stripeUnitFileSize = stripeN * stripeUnitSize
			return
		}

		if stripeRemainSize < stripeUnitSize && stripeN > 0 {
			stripeUnitFileSize = stripeN*stripeUnitSize + stripeRemainSize
		} else {
			stripeUnitFileSize = stripeN*stripeUnitSize + stripeUnitSize
		}
		return
	}

	stripeUnitN := (extentSize - stripeN*ee.dataSize) / stripeUnitSize
	if uint64(idx) > stripeUnitN {
		stripeUnitFileSize = stripeN * stripeUnitSize
	} else if uint64(idx) == stripeUnitN {
		stripeUnitFileSize = stripeN*stripeUnitSize + remainSize
	} else {
		stripeUnitFileSize = stripeN*stripeUnitSize + stripeUnitSize
	}

	log.LogDebugf("calcStripeUnitFileSize extentSize(%v) stripeUnitSize(%v) hosts(%v) hostidx(%v) stripeUnitFileSize(%v)",
		extentSize, stripeUnitSize, ee.hosts, idx, stripeUnitFileSize)
	return
}
