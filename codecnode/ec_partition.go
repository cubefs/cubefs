// Copyright 2021 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package codecnode

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	dataSdk "github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/connpool"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/unit"
)

const (
	MaxRetryLimit          = 5
	RetryInterval          = time.Second * 5
	ReportToMasterInterval = 64
)

var (
	gConnPool  = connpool.NewConnectPool()
	SdkMap     = make(map[string]*dataSdk.Wrapper)
	SdkMapLock sync.Mutex
)

type EcPartition struct {
	proto.IssueMigrationTaskRequest
	originExtentInfo []proto.ExtentInfo
	dataWrapper      *dataSdk.Wrapper
}

type readFunc func(ctx context.Context, req *dataSdk.ExtentRequest) (readBytes int, err error)

func getDataSdkWrapper(volume string, masters []string) (w *dataSdk.Wrapper, err error) {
	SdkMapLock.Lock()
	defer SdkMapLock.Unlock()
	if w, exist := SdkMap[volume]; exist {
		return w, nil
	}
	limit := MaxRetryLimit
retry:
	w, err = dataSdk.NewDataPartitionWrapper(volume, masters)
	if err != nil {
		if limit <= 0 {
			return nil, errors.Trace(err, "Init data wrapper failed!")
		} else {
			limit--
			time.Sleep(RetryInterval)
			goto retry
		}
	}
	SdkMap[volume] = w
	log.LogDebugf("Volume(%v) NewDataPartitionWrapper", volume)
	return
}

func NewEcPartition(req *proto.IssueMigrationTaskRequest, masters []string) (ecp *EcPartition, err error) {
	dw, err := getDataSdkWrapper(req.VolName, masters)
	if err != nil {
		return
	}
	return &EcPartition{
		IssueMigrationTaskRequest: *req,
		dataWrapper:               dw,
	}, nil
}

func (ecp *EcPartition) GetOriginExtentInfo() (err error) {
	var (
		dnPartition *proto.DNDataPartitionInfo
		num         int
	)
	extentInfo := make([]proto.ExtentInfo, 0)
	extentInfoMap := make(map[uint64]proto.ExtentInfo)
	log.LogDebugf("GetOriginExtentInfo PartitionId(%v) ProfHosts(%v)", ecp.PartitionId, ecp.ProfHosts)
	for index, host := range ecp.ProfHosts {
		arr := strings.Split(host, ":")
		if num, err = fmt.Sscanf(arr[1], "%d", &MasterClient.DataNodeProfPort); num != 1 || err != nil {
			log.LogWarnf("Get DataNodeProfPort[%v] err[%v]", host, err)
			if index == 0 { //leader host bad, can't migrateEc
				break
			}
			continue
		}
		if dnPartition, err = MasterClient.NodeAPI().DataNodeGetPartition(arr[0], ecp.PartitionId); err != nil {
			log.LogWarnf("DataNode[%v] getPartition[%v] err[%v]", host, ecp.PartitionId, err)
			if index == 0 { //leader host bad, can't migrateEc
				break
			}
			continue
		}

		for _, ei := range dnPartition.Files {
			leader, ok := extentInfoMap[ei[storage.FileID]]
			if ok {
				if ei[storage.Size] > leader.Size {
					err = fmt.Errorf("extentId(%v) leader size(%v) < replicaSize(%v), can't migrateEc",
						leader.FileID, leader.Size, ei[storage.Size])
					return
				}
				continue
			}
			info := proto.ExtentInfo{
				FileID:     ei[storage.FileID],
				Size:       ei[storage.Size],
				Crc:        uint32(ei[storage.Crc]),
				ModifyTime: int64(ei[storage.ModifyTime]),
			}
			extentInfoMap[ei[storage.FileID]] = info
			extentInfo = append(extentInfo, info)
		}
	}

	sort.Slice(extentInfo, func(i, j int) bool {
		return extentInfo[i].FileID < extentInfo[j].FileID
	})
	ecp.originExtentInfo = extentInfo
	return
}

func (ecp *EcPartition) GetTinyExtentHolesAndAvaliSize(extentId uint64) (holes []*proto.TinyExtentHole, extentAvaliSize uint64, err error) {
	max := &proto.DNTinyExtentInfo{}
	for _, host := range ecp.ProfHosts {
		arr := strings.Split(host, ":")
		if n, err := fmt.Sscanf(arr[1], "%d", &MasterClient.DataNodeProfPort); n != 1 || err != nil {
			log.LogWarnf("Get DataNodeProfPort[%v] err[%v]", host, err)
			continue
		}
		tinyExtentInfo, err := MasterClient.NodeAPI().DataNodeGetTinyExtentHolesAndAvali(arr[0], ecp.PartitionId, extentId)
		if err != nil {
			log.LogWarnf("Get DataNodeProfPort[%v] err[%v]", host, err)
			continue
		}
		if len(tinyExtentInfo.Holes) > len(max.Holes) {
			max.Holes = tinyExtentInfo.Holes
		}
		if tinyExtentInfo.ExtentAvaliSize > max.ExtentAvaliSize {
			max.ExtentAvaliSize = tinyExtentInfo.ExtentAvaliSize
		}
	}

	holes = max.Holes
	extentAvaliSize = max.ExtentAvaliSize
	log.LogDebugf("GetTinyExtentHolesAndAvaliSize partitionId(%v) extentId(%v) holes(%v) extentAvaliSize(%v)",
		ecp.PartitionId, extentId, holes, extentAvaliSize)
	return
}

func (ecp *EcPartition) PersistHolesInfoToEcnode(ctx context.Context, extentId uint64, holes []*proto.TinyExtentHole) (err error) {
	var (
		wg       sync.WaitGroup
		count    int32
		sendData []byte
	)

	if len(holes) <= 0 {
		return
	}

	if sendData, err = json.Marshal(holes); err != nil {
		return
	}
	log.LogDebugf("PersistHolesInfoToEcnode volume(%v) partitionid(%v) extentid(%v) sendData(%v)",
		ecp.VolName, ecp.PartitionId, extentId, string(sendData))

	for _, h := range ecp.Hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			var conn *net.TCPConn

			if conn, err = gConnPool.GetConnect(host); err != nil {
				log.LogErrorf("PersistHolesInfoToEcnode GetConnect volume(%v) partitionid(%v) extentid(%v) err(%v)",
					ecp.VolName, ecp.PartitionId, extentId, err)
				return
			}
			defer gConnPool.PutConnect(conn, true)

			p := proto.NewPacketReqID(ctx)
			p.Opcode = proto.OpPersistTinyExtentDelete
			p.PartitionID = ecp.PartitionId
			p.ExtentID = extentId
			p.Data = sendData
			p.Size = uint32(len(sendData))
			if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
				log.LogErrorf("PersistHolesInfoToEcnode WriteToConn volume(%v) partitionid(%v) extentid(%v) err(%v)",
					ecp.VolName, ecp.PartitionId, extentId, err)
				return
			}

			if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
				log.LogErrorf("PersistHolesInfoToEcnode ReadFromConn volume(%v) partitionid(%v) extentid(%v) err(%v)",
					ecp.VolName, ecp.PartitionId, extentId, err)
				return
			}

			if p.ResultCode != proto.OpOk {
				log.LogErrorf("PersistHolesInfoToEcnode ResultCode(%x) err(%v) host(%v)", p.ResultCode, err, host)
				return
			}

			atomic.AddInt32(&count, 1)
		}(h)
	}
	wg.Wait()

	if count != int32(len(ecp.Hosts)) {
		err = errors.New("failed to PersistHolesInfoToEcnode")
	}
	return
}

func (ecp *EcPartition) CreateExtentForWrite(ctx context.Context, extentId uint64, extentSize uint64) (err error) {
	var (
		wg    sync.WaitGroup
		count int32
	)

	log.LogDebugf("CreateExtentsForWrite volume(%v) partitionid(%v) extentid(%v) extentsize(%v)",
		ecp.VolName, ecp.PartitionId, extentId, extentSize)
	for _, h := range ecp.Hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			var conn *net.TCPConn

			if conn, err = gConnPool.GetConnect(host); err != nil {
				log.LogErrorf("CreateExtentsForWrite GetConnect volume(%v) partitionid(%v) extentid(%v) err(%v)",
					ecp.VolName, ecp.PartitionId, extentId, err)
				return
			}
			defer gConnPool.PutConnect(conn, true)

			p := proto.NewPacketReqID(ctx)
			p.Opcode = proto.OpCreateExtent
			p.ExtentOffset = int64(extentSize)
			p.PartitionID = ecp.PartitionId
			p.ExtentID = extentId

			if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
				log.LogErrorf("CreateExtentsForWrite WriteToConn volume(%v) partitionid(%v) extentid(%v) err(%v)",
					ecp.VolName, ecp.PartitionId, extentId, err)
				return
			}

			if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
				log.LogErrorf("CreateExtentsForWrite ReadFromConn volume(%v) partitionid(%v) extentid(%v) err(%v)",
					ecp.VolName, ecp.PartitionId, extentId, err)
				return
			}

			if p.ResultCode != proto.OpOk {
				log.LogErrorf("CreateExtentsForWrite ResultCode(%x) partitionid(%v) extentid(%v) err(%v) host(%v)", p.ResultCode, ecp.PartitionId, extentId, err, host)
				return
			}

			atomic.AddInt32(&count, 1)
		}(h)
	}
	wg.Wait()

	if count != int32(len(ecp.Hosts)) {
		err = errors.NewErrorf("failed to create extent partitionid(%v) extentid(%v)", ecp.PartitionId, extentId)
	}

	return
}

func (ecp *EcPartition) Read(ctx context.Context, extentId uint64, data []byte, offset uint64, size int) (readSize int, err error) {
	var partition *dataSdk.DataPartition
	if partition, err = ecp.dataWrapper.GetDataPartition(ecp.PartitionId); err != nil {
		return
	}

	ek := &proto.ExtentKey{
		PartitionId:  ecp.PartitionId,
		ExtentId:     extentId,
		ExtentOffset: offset,
	}

	req := &dataSdk.ExtentRequest{
		Size:      size,
		Data:      data,
		ExtentKey: ek,
	}

	var reader *dataSdk.ExtentReader
	var readFunc readFunc
	reader = dataSdk.NewExtentReader(0, ek, partition, true, false)
	if proto.IsTinyExtent(extentId) {
		readFunc = reader.EcTinyExtentRead
	} else {
		readFunc = reader.Read
	}
	if readSize, err = readFunc(ctx, req); err != nil || readSize < size {
		err = errors.NewErrorf("failed to read data[%v] size[%v]", err, readSize)
	}

	return
}

func (ecp *EcPartition) Write(ctx context.Context, data [][]byte, extentId uint64, offset uint64) (err error) {
	if len(data) != int(ecp.EcDataNum+ecp.EcParityNum) {
		err = errors.New("unmatched number of shards")
		return
	}

	nodeNum := ecp.EcDataNum + ecp.EcParityNum
	hosts := proto.GetEcHostsByExtentId(uint64(nodeNum), extentId, ecp.Hosts)

	var (
		wg     sync.WaitGroup
		count  int32
		errMsg error
	)
	for i, h := range hosts {
		wg.Add(1)
		go func(block []byte, host string) {
			defer wg.Done()
			size := len(block)
			if size == 0 {
				atomic.AddInt32(&count, 1)
				return
			}

			curOffset := 0
			for size > 0 {
				if size <= 0 {
					break
				}
				curSize := unit.Min(size, unit.EcBlockSize)
				curData := block[curOffset : curOffset+curSize]
				if err = sendToEcNode(ctx, host, curData, ecp.PartitionId, extentId, offset+uint64(curOffset)); err != nil {
					log.LogErrorf("sendToEcNode host(%v) PartitionId(%v) extentId(%v) offset(%v) size(%v) err(%v)",
						host, ecp.PartitionId, extentId, offset+uint64(curOffset), curSize, err)
					errMsg = err
					return
				}
				curOffset += curSize
				size -= curSize
			}
			atomic.AddInt32(&count, 1)
		}(data[i], h)
	}
	wg.Wait()

	if count != int32(len(ecp.Hosts)) {
		err = errMsg
	}
	return
}

func sendToEcNode(ctx context.Context, host string, block []byte, partitionId, extentId, offset uint64) (err error) {
	var conn *net.TCPConn
	if conn, err = gConnPool.GetConnect(host); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)

	p := proto.NewPacketReqID(ctx)
	p.Opcode = proto.OpEcWrite
	p.Data = block
	p.ExtentOffset = int64(offset)
	p.Size = uint32(len(block))
	p.PartitionID = partitionId
	p.ExtentID = extentId
	p.CRC = crc32.ChecksumIEEE(p.Data)

	log.LogDebugf("WriteToEcNode(%v): ReqID(%x), Offset(%v), Size(%v), PartitionID(%v), ExtentID(%v), CRC(%x)",
		host, p.ReqID, p.ExtentOffset, p.Size, p.PartitionID, p.ExtentID, p.CRC)

	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}

	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("ReadFromEcNode(%v): ResultCode(%x) ReqID(%x), Offset(%v), Size(%v), PartitionID(%v), ExtentID(%v), CRC(%x), err(%v)",
			host, p.ResultCode, p.ReqID, p.ExtentOffset, p.Size, p.PartitionID, p.ExtentID, p.CRC, err)
		return
	}

	return
}
func (ecp *EcPartition) CalMisAlignMentShards(shards [][]byte, readSize uint64, stripeUnitSize uint64) (err error) {
	if len(shards) != int(ecp.EcDataNum+ecp.EcParityNum) {
		err = errors.New("unmatched number of shards")
		return
	}

	index := readSize / stripeUnitSize
	shards[index] = shards[index][:readSize%stripeUnitSize]
	for i := index + 1; i < uint64(ecp.EcDataNum); i++ {
		shards[i] = nil
	}

	return
}
