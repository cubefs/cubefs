// Copyright 2018 The ChuBao Authors.
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

package stream

import (
	"fmt"
	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/sdk/data/wrapper"
	"github.com/chubaoio/cbfs/util"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/chubaoio/cbfs/util/pool"
	"github.com/juju/errors"
	"hash/crc32"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

const (
	ForceCloseConnect = true
	NoCloseConnect    = false
)

var (
	ReadConnectPool = pool.NewConnPool()
)

type ExtentReader struct {
	inode            uint64
	startInodeOffset uint64
	endInodeOffset   uint64
	dp               *wrapper.DataPartition
	key              proto.ExtentKey
	readerIndex      uint32
}

func NewExtentReader(inode uint64, inInodeOffset int, key proto.ExtentKey) (reader *ExtentReader, err error) {
	reader = new(ExtentReader)
	reader.dp, err = gDataWrapper.GetDataPartition(key.PartitionId)
	if err != nil {
		return
	}
	reader.inode = inode
	reader.key = key
	reader.startInodeOffset = uint64(inInodeOffset)
	reader.endInodeOffset = reader.startInodeOffset + uint64(key.Size)
	rand.Seed(time.Now().UnixNano())
	hasFindLocalReplica := false
	for index, host := range reader.dp.Hosts {
		if strings.Split(host, ":")[0] == wrapper.LocalIP {
			reader.readerIndex = uint32(index)
			hasFindLocalReplica = true
			break
		}
	}
	if !hasFindLocalReplica {
		reader.readerIndex = uint32(rand.Intn(int(reader.dp.ReplicaNum)))
	}
	return reader, nil
}

func (reader *ExtentReader) read(data []byte, offset, size, kerneloffset, kernelsize int) (err error) {
	if size <= 0 {
		return
	}
	err = reader.readDataFromDataPartition(offset, size, data, kerneloffset, kernelsize)

	return
}

func (reader *ExtentReader) readDataFromDataPartition(offset, size int, data []byte, kerneloffset, kernelsize int) (err error) {
	var host string
	if _, host, err = reader.streamReadDataFromHost(offset, size, data, kerneloffset, kernelsize); err != nil {
		if reader.isUseCloseConnectErr(err) {
			reader.forceDestoryAllConnect(host)
		}
		log.LogWarnf(err.Error())
		goto forLoop
	}
	return
forLoop:
	mesg := ""
	for i := 0; i < len(reader.dp.Hosts); i++ {
		_, host, err = reader.streamReadDataFromHost(offset, size, data, kerneloffset, kernelsize)
		if err == nil {
			return
		} else if reader.isUseCloseConnectErr(err) {
			reader.forceDestoryAllConnect(host)
			i--
		}
		log.LogWarn(err.Error())
		mesg += fmt.Sprintf(" (index(%v) err(%v))", i, err.Error())
	}
	log.LogWarn(mesg)
	err = fmt.Errorf(mesg)

	return
}

func (reader *ExtentReader) isUseCloseConnectErr(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}

func (reader *ExtentReader) forceDestoryAllConnect(host string) {
	ReadConnectPool.ReleaseAllConnect(host)
}

func (reader *ExtentReader) streamReadDataFromHost(offset, expectReadSize int, data []byte, kerneloffset,
	kernelsize int) (actualReadSize int, host string, err error) {
	request := NewStreamReadPacket(&reader.key, offset, expectReadSize)
	var connect *net.TCPConn
	index := atomic.LoadUint32(&reader.readerIndex)
	if index >= uint32(reader.dp.ReplicaNum) {
		index = 0
		atomic.StoreUint32(&reader.readerIndex, 0)
	}
	host = reader.dp.Hosts[index]
	connect, err = ReadConnectPool.Get(host)
	if err != nil {
		atomic.AddUint32(&reader.readerIndex, 1)
		return 0, host, errors.Annotatef(err, reader.toString()+
			"streamReadDataFromHost dp(%v) cannot get  connect from host(%v) request(%v) ",
			reader.key.PartitionId, host, request.GetUniqueLogId())

	}
	defer func() {
		if err != nil {
			ReadConnectPool.Put(connect, ForceCloseConnect)
			if reader.isUseCloseConnectErr(err) {
				return
			}
			atomic.AddUint32(&reader.readerIndex, 1)
		} else {
			ReadConnectPool.Put(connect, NoCloseConnect)
		}
	}()

	if err = request.WriteToConn(connect); err != nil {
		err = errors.Annotatef(err, reader.toString()+"streamReadDataFromHost host(%v) error request(%v)",
			host, request.GetUniqueLogId())
		return 0, host, err
	}

	for {
		if actualReadSize >= expectReadSize {
			break
		}
		reply := NewReply(request.ReqID, reader.dp.PartitionID, request.FileID)
		canRead := util.Min(util.ReadBlockSize, expectReadSize-actualReadSize)
		reply.Data = data[actualReadSize : canRead+actualReadSize]
		err = reply.ReadFromConnStream(connect, proto.ReadDeadlineTime)
		if err != nil {
			err = errors.Annotatef(err, reader.toString()+"streamReadDataFromHost host(%v)  error reqeust(%v)",
				host, request.GetUniqueLogId())
			return 0, host, err
		}
		err = reader.checkStreamReply(request, reply, kerneloffset, kernelsize)
		if err != nil {
			return 0, host, err
		}
		actualReadSize += int(reply.Size)
		if actualReadSize >= expectReadSize {
			break
		}
	}

	return actualReadSize, host, nil
}

func (reader *ExtentReader) checkStreamReply(request *Packet, reply *Packet, kerneloffset, kernelsize int) (err error) {
	if reply.ResultCode != proto.OpOk {
		return errors.Annotatef(fmt.Errorf("reply status code(%v) is not ok,request (%v) "+
			"but reply (%v) ", reply.ResultCode, request.GetUniqueLogId(), reply.GetUniqueLogId()),
			fmt.Sprintf("reader(%v)", reader.toString()))
	}
	if !request.IsEqualStreamReadReply(reply) {
		return errors.Annotatef(fmt.Errorf("request not equare reply , request (%v) "+
			"and reply (%v) ", request.GetUniqueLogId(), reply.GetUniqueLogId()),
			fmt.Sprintf("reader(%v)", reader.toString()))
	}
	expectCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
	if reply.Crc != expectCrc {
		return errors.Annotatef(fmt.Errorf("crc not match on  request (%v) "+
			"and reply (%v) expectCrc(%v) but reciveCrc(%v) ", request.GetUniqueLogId(), reply.GetUniqueLogId(), expectCrc, reply.Crc),
			fmt.Sprintf("reader(%v)", reader.toString()))
	}
	return nil
}

func (reader *ExtentReader) updateKey(key proto.ExtentKey) (update bool) {
	if !(key.PartitionId == reader.key.PartitionId && key.ExtentId == reader.key.ExtentId) {
		return
	}
	if key.Size <= reader.key.Size {
		return
	}
	reader.key = key
	end := atomic.LoadUint64(&reader.startInodeOffset) + uint64(key.Size)
	atomic.StoreUint64(&reader.endInodeOffset, end)

	return true
}

func (reader *ExtentReader) toString() (m string) {
	return fmt.Sprintf("inode (%v) extentKey(%v) start(%v) end(%v)", reader.inode,
		reader.key.Marshal(), reader.startInodeOffset, reader.endInodeOffset)
}
