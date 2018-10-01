// Copyright 2018 The Containerfs Authors.
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

package blob

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/pool"
	"hash/crc32"
	"net"
	"strings"
	"syscall"
)

const (
	MaxRetryCnt = 100
)

type BlobClient struct {
	cluster string
	volname string
	conns   *pool.ConnectPool
	wraper  *wrapper.Wrapper
}

func NewBlobClient(volname, masters string) (*BlobClient, error) {
	client := new(BlobClient)
	client.volname = volname
	var err error
	client.conns = pool.NewConnPool()
	client.wraper, err = wrapper.NewDataPartitionWrapper(volname, masters)
	if err != nil {
		return nil, err
	}
	client.cluster = client.wraper.GetClusterName()
	return client, nil
}

func (client *BlobClient) checkWriteResponse(request, reply *proto.Packet) (err error) {
	if reply.ResultCode != proto.OpOk {
		return fmt.Errorf("WriteRequest(%v) reply(%v) replyOpCode(%v) Err msg(%v)",
			request.GetUniqueLogId(), reply.GetUniqueLogId(), reply.Opcode, string(reply.Data[:reply.Size]))
	}
	if request.ReqID != reply.ReqID {
		return fmt.Errorf("WriteRequest(%v) reply(%v) REQID not equare", request.GetUniqueLogId(), reply.GetUniqueLogId())
	}
	if request.PartitionID != reply.PartitionID {
		return fmt.Errorf("WriteRequest(%v) reply(%v) PartitionID not equare", request.GetUniqueLogId(), reply.GetUniqueLogId())
	}
	if request.Crc != reply.Crc {
		return fmt.Errorf("WriteRequest(%v) reply(%v) CRC not equare,request(%v) reply(%v)", request.GetUniqueLogId(),
			reply.GetUniqueLogId(), request.Crc, reply.Crc)
	}

	return
}

func (client *BlobClient) checkReadResponse(request, reply *proto.Packet, expectCrc uint32) (err error) {
	if reply.ResultCode != proto.OpOk {
		return fmt.Errorf("ReadRequest(%v) reply(%v) replyOp Err msg(%v)",
			request.GetUniqueLogId(), reply.GetUniqueLogId(), string(reply.Data[:reply.Size]))
	}
	if request.ReqID != reply.ReqID {
		return fmt.Errorf("ReadRequest(%v) reply(%v) REQID not equare", request.GetUniqueLogId(), reply.GetUniqueLogId())
	}
	if request.PartitionID != reply.PartitionID {
		return fmt.Errorf("ReadRequest(%v) reply(%v) PartitionID not equare", request.GetUniqueLogId(), reply.GetUniqueLogId())
	}
	replyBodyCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size])
	if replyBodyCrc != reply.Crc {
		return fmt.Errorf("ReadRequest(%v) reply(%v) CRC not equare,replyBodyCrc(%v)"+
			" replyHeaderCrc(%v) userExpectCrc(%v)", request.GetUniqueLogId(),
			reply.GetUniqueLogId(), replyBodyCrc, reply.Crc, expectCrc)
	}
	if expectCrc != replyBodyCrc {
		return fmt.Errorf("ReadRequest(%v) reply(%v) CRC not equare,request(%v) userExpectCrc(%v) reply(%v)", request.GetUniqueLogId(),
			reply.GetUniqueLogId(), request.Crc, expectCrc, replyBodyCrc)
	}
	return
}

func (client *BlobClient) Write(data []byte) (key string, err error) {
	var (
		dp *wrapper.DataPartition
	)
	exclude := make([]uint32, 0)
	writeData := make([]byte, len(data))
	writeSize := len(data)
	copy(writeData, data)
	for i := 0; i < MaxRetryCnt; i++ {
		dp, err = client.wraper.GetWriteDataPartition(exclude)
		if err != nil {
			log.LogErrorf("Write: No write data partition")
			return "", syscall.ENOMEM
		}
		var (
			conn *net.TCPConn
		)
		request := NewBlobWritePacket(dp, writeData)
		if conn, err = client.conns.Get(dp.Hosts[0]); err != nil {
			log.LogWarnf("WriteRequest(%v) Get connect from host(%v) err(%v)", request.GetUniqueLogId(), dp.Hosts[0], err.Error())
			exclude = append(exclude, dp.PartitionID)
			continue
		}
		if err = request.WriteToConn(conn); err != nil {
			client.conns.CheckErrorForPutConnect(conn, dp.Hosts[0], err)
			log.LogWarnf("WriteRequest(%v) Write to  host(%v) err(%v)", request.GetUniqueLogId(), dp.Hosts[0], err.Error())
			exclude = append(exclude, dp.PartitionID)
			continue
		}
		reply := new(proto.Packet)
		if err = reply.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			client.conns.Put(conn, true)
			log.LogWarnf("WriteRequest(%v) Write host(%v) err(%v)", request.GetUniqueLogId(), dp.Hosts[0], err.Error())
			exclude = append(exclude, dp.PartitionID)
			continue
		}
		if err = client.checkWriteResponse(request, reply); err != nil {
			client.conns.Put(conn, true)
			log.LogWarnf("WriteRequest CheckWriteResponse error(%v)", err.Error())
			exclude = append(exclude, dp.PartitionID)
			continue
		}
		partitionID, fileID, objID, crc := ParsePacket(reply)
		client.conns.Put(conn, false)
		key = GenKey(client.cluster, client.volname, partitionID, fileID, objID, uint32(writeSize), crc)
		return key, nil
	}

	return "", syscall.EIO
}

func (client *BlobClient) readDataFromHost(request *proto.Packet, target string, expectCrc uint32) (data []byte, err error) {

	var (
		conn *net.TCPConn
	)
	if conn, err = client.conns.Get(target); err != nil {
		err = errors.Annotatef(err, "ReadRequest(%v) Get connect from host(%v)-", request.GetUniqueLogId(), target)
		return
	}
	if err = request.WriteToConn(conn); err != nil {
		client.conns.CheckErrorForPutConnect(conn, target, err)
		err = errors.Annotatef(err, "ReadRequest(%v) Write To host(%v)-", request.GetUniqueLogId(), target)
		return
	}
	reply := new(proto.Packet)
	if err = reply.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		client.conns.Put(conn, true)
		err = errors.Annotatef(err, "ReadRequest(%v) ReadFrom host(%v)-", request.GetUniqueLogId(), target)
		return
	}
	if err = client.checkReadResponse(request, reply, expectCrc); err != nil {
		client.conns.Put(conn, true)
		err = errors.Annotatef(err, "ReadRequest CheckReadResponse from (%v)", target)
		return
	}
	client.conns.Put(conn, false)
	return reply.Data, nil

}

func (client *BlobClient) Read(key string) (data []byte, err error) {
	cluster, volname, partitionID, fileID, objID, size, crc, err := ParseKey(key)
	if err != nil || strings.Compare(cluster, client.cluster) != 0 || strings.Compare(volname, client.volname) != 0 {
		log.LogErrorf("Read: err(%v)", err)
		return nil, syscall.EINVAL
	}

	dp, err := client.wraper.GetDataPartition(partitionID)
	if dp == nil {
		log.LogErrorf("Read: No partition, key(%v) err(%v)", key, err)
		return
	}

	request := NewBlobReadPacket(partitionID, fileID, objID, size)
	mesg := ""
	for i := 0; i < len(dp.Hosts); i++ {
		data, err = client.readDataFromHost(request, dp.Hosts[i], crc)
		if err == nil {
			return
		}
		log.LogWarn(err.Error())
		mesg += fmt.Sprintf(" (index(%v) err(%v))", i, err.Error())
	}
	log.LogWarn(mesg)
	err = fmt.Errorf(mesg)

	return nil, err
}

func (client *BlobClient) Delete(key string) (err error) {
	cluster, volname, partitionID, fileID, objID, _, _, err := ParseKey(key)
	if err != nil || strings.Compare(cluster, client.cluster) != 0 || strings.Compare(volname, client.volname) != 0 {
		log.LogErrorf("Delete: err(%v)", err)
		return syscall.EINVAL
	}

	dp, err := client.wraper.GetDataPartition(partitionID)
	if dp == nil {
		log.LogErrorf("Delete: No partition, key(%v) err(%v)", key, err)
		return
	}
	request := NewBlobDeletePacket(dp, fileID, objID)
	var (
		conn *net.TCPConn
	)
	if conn, err = client.conns.Get(dp.Hosts[0]); err != nil {
		err = errors.Annotatef(err, "DeleteRequest(%v) Get connect from host(%v)-", key, dp.Hosts[0])
		client.conns.Put(conn, true)
		return
	}
	if err = request.WriteToConn(conn); err != nil {
		client.conns.CheckErrorForPutConnect(conn, dp.Hosts[0], err)
		err = errors.Annotatef(err, "DeleteRequest(%v) Write To host(%v)-", key, dp.Hosts[0])
		return
	}
	reply := new(proto.Packet)
	if err = reply.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		client.conns.Put(conn, true)
		err = errors.Annotatef(err, "DeleteRequest(%v) readResponse from host(%v)-", key, dp.Hosts[0])
		return
	}
	if reply.ResultCode != proto.OpOk {
		client.conns.Put(conn, true)
		return fmt.Errorf("DeleteRequest(%v) reply(%v) replyOp Err msg(%v)",
			request.GetUniqueLogId(), reply.GetUniqueLogId(), string(reply.Data[:reply.Size]))
	}
	client.conns.Put(conn, false)

	return nil
}
