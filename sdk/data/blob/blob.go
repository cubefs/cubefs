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
	"strings"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/pool"
)

const (
	WriteRetryLimit   = 3
	ReadRetryLimit    = 10
	SendRetryLimit    = 10
	SendRetryInterval = 100 * time.Millisecond
	MaxRetryCnt       = 100
)

type BlobClient struct {
	cluster string
	volname string
	conns   *pool.ConnPool
	wraper  *wrapper.Wrapper
}

func NewBlobClient(volname, masters string) (*BlobClient, error) {
	client := new(BlobClient)
	client.volname = volname
	var err error
	client.wraper, err = wrapper.NewDataPartitionWrapper(volname, masters)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (client *BlobClient) Write(data []byte) (key string, err error) {
	var (
		req, resp *proto.Packet
		dp        *wrapper.DataPartition
	)
	exclude := make([]uint32, 0)
	for i := 0; i < MaxRetryCnt; i++ {
		dp, err = client.wraper.GetWriteDataPartition(exclude)
		if err != nil {
			log.LogErrorf("Write: No write data partition")
			return "", syscall.ENOMEM
		}

		req = NewWritePacket(dp, data)
		resp, err = client.sendToDataPartition(dp, req)
		if err != nil || resp.ResultCode != proto.OpOk {
			log.LogErrorf("Write: dp(%v) err(%v) result(%v)", dp, err, resp.GetResultMesg())
			continue
		}

		partitionID, fileID, objID, size := ParsePacket(resp)
		if partitionID != dp.PartitionID {
			log.LogWarnf("Write: req partition id(%v) resp partition id(%v)", dp.PartitionID, partitionID)
			continue
		}
		key = GenKey(client.cluster, client.volname, partitionID, fileID, objID, size)
		return key, nil
	}

	return "", syscall.EIO
}

func (client *BlobClient) Read(key string) (data []byte, err error) {
	cluster, volname, partitionID, fileID, objID, size, err := ParseKey(key)
	if err != nil || strings.Compare(cluster, client.cluster) != 0 || strings.Compare(volname, client.volname) != 0 {
		log.LogErrorf("Read: err(%v)", err)
		return nil, syscall.EINVAL
	}

	dp, err := client.wraper.GetDataPartition(partitionID)
	if dp == nil {
		log.LogErrorf("Read: No partition, key(%v) err(%v)", key, err)
	}

	req := NewReadPacket(partitionID, fileID, objID, size)
	resp, err := client.sendToDataPartition(dp, req)
	if err != nil || resp.ResultCode != proto.OpOk {
		log.LogErrorf("Read: key(%v) err(%v) result(%v)", key, err, resp.GetResultMesg())
		return nil, syscall.EIO
	}

	return resp.Data, nil
}

func (client *BlobClient) Delete(key string) (err error) {
	//TODO: Parse key
	//TODO: Create delete packet
	//TODO: Send request to data node
	//TODO: Read response
	return nil
}
