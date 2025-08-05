// Copyright 2022 The CubeFS Authors.
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

package bcache

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const (
	_ int = iota
	statusOK
	statusNoent
	statusError
)

type BcacheClient struct {
	connPool *ConnPool
}

var (
	once   sync.Once
	client *BcacheClient
)

func NewBcacheClient() *BcacheClient {
	once.Do(func() {
		expireTime := int64(time.Second * ConnectExpireTime)
		cp := NewConnPool(UnixSocketPath, 20, 200, expireTime)
		client = &BcacheClient{connPool: cp}
	})
	return client
}

func (c *BcacheClient) Get(vol, key string, buf []byte, offset uint64, size uint32) (int, error) {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("bcache-get", err, bgTime, 1)
	}()
	req := &GetCacheRequest{
		CacheKey: key,
		Offset:   offset,
		Size:     size,
	}
	packet := NewBlockCachePacket()
	packet.Opcode = OpBlockCacheGet
	data, err := req.Marshal()
	if err != nil {
		log.LogDebugf("get block cache: req(%v) err(%v)", req.CacheKey, err)
		return 0, err
	}
	defer func() {
		bytespool.Free(data)
	}()
	packet.Data = data
	packet.Size = uint32(len(packet.Data))
	stat.EndStat("bcache-get-marshal", err, bgTime, 1)
	conn, err := c.connPool.Get()
	if err != nil {
		log.LogDebugf("get block cache: get Conn failed, req(%v) err(%v)", req.CacheKey, err)
		return 0, err
	}
	defer func() {
		c.connPool.Put(conn)
	}()
	stat.EndStat("bcache-get-conn", err, bgTime, 1)
	getCachePathMetric := exporter.NewTPCnt("bcache-get-cachepath")
	err = packet.WriteToConn(*conn)
	if err != nil {
		log.LogDebugf("Failed to write to conn, req(%v) err(%v)", req.CacheKey, err)
		getCachePathMetric.SetWithLabels(err, map[string]string{exporter.Vol: vol})
		return 0, errors.NewErrorf("Failed to write to conn, req(%v) err(%v)", req.CacheKey, err)
	}
	stat.EndStat("bcache-get-writeconn", err, bgTime, 1)
	err = packet.ReadFromConn(*conn, 1)
	if err != nil {
		log.LogDebugf("Failed to read from conn, req(%v), err(%v)", req.CacheKey, err)
		getCachePathMetric.SetWithLabels(err, map[string]string{exporter.Vol: vol})
		return 0, errors.NewErrorf("Failed to read from conn, req(%v), err(%v)", req.CacheKey, err)
	}
	stat.EndStat("bcache-get-readconn", err, bgTime, 1)

	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogDebugf("get block cache: req(%v) err(%v) result(%v)", req.CacheKey, err, packet.GetResultMsg())
		getCachePathMetric.SetWithLabels(err, map[string]string{exporter.Vol: vol})
		return 0, err
	}

	resp := new(GetCachePathResponse)
	err = resp.UnmarshalValue(packet.Data)
	if err != nil {
		log.LogDebugf("get block cache: req(%v) err(%v) PacketData(%v)", req.CacheKey, err, string(packet.Data))
		getCachePathMetric.SetWithLabels(err, map[string]string{exporter.Vol: vol})
		return 0, err
	}
	cachePath := resp.CachePath
	stat.EndStat("bcache-get-meta", err, bgTime, 1)
	getCachePathMetric.SetWithLabels(nil, map[string]string{exporter.Vol: vol})
	readBgTime := stat.BeginStat()
	subs := strings.Split(cachePath, "/")
	if subs[len(subs)-1] != key {
		log.LogDebugf("cacheKey(%v) cache path(%v) is not legal",
			key, cachePath)
		return 0, errors.NewErrorf("cacheKey(%v) cache path is not legal: %v", key, cachePath)
	}
	readCacheMetric := exporter.NewTPCnt("bcache-read-cachefile")
	defer readCacheMetric.SetWithLabels(err, map[string]string{exporter.Vol: vol})
	f, err := os.Open(cachePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := f.ReadAt(buf, int64(offset))
	if n != int(size) {
		log.LogDebugf("get block cache: BCache client GET() error,exception size(%v),but readSize(%v)", size, n)
		return 0, errors.NewErrorf("BcacheClient GET() error, exception size(%v), but readSize(%v)", size, n)
	}
	if err != nil {
		log.LogDebugf("get block cache: BCache client read %v err %v", cachePath, err.Error())
		return 0, errors.NewErrorf("get block cache: BCache client read %v err %v", cachePath, err.Error())
	}
	encryptXOR(buf[:n])
	stat.EndStat("bcache-get-read", err, readBgTime, 1)
	return n, nil
}

func (c *BcacheClient) Put(vol, key string, buf []byte) error {
	var err error
	bgTime := stat.BeginStat()
	cacheFileMetric := exporter.NewTPCnt("bcache-put-cachefile")
	defer func() {
		stat.EndStat("bcache-put", err, bgTime, 1)
		cacheFileMetric.SetWithLabels(err, map[string]string{exporter.Vol: vol})
	}()

	req := &PutCacheRequest{
		CacheKey: key,
		Data:     buf,
		VolName:  vol,
	}
	packet := NewBlockCachePacket()
	packet.Opcode = OpBlockCachePut
	data, err := req.Marshal()
	defer func() {
		bytespool.Free(data)
	}()
	if err != nil {
		log.LogDebugf("put block cache: req(%v) err(%v)", req.CacheKey, err)
		return err
	}
	packet.Data = data
	packet.Size = uint32(len(packet.Data))
	conn, err := c.connPool.Get()
	if err != nil {
		log.LogDebugf("put block cache: get Conn failed, req(%v) err(%v)", req.CacheKey, err)
		return err
	}
	defer func() {
		c.connPool.Put(conn)
	}()

	err = packet.WriteToConn(*conn)
	if err != nil {
		log.LogDebugf("Failed to write to conn, req(%v) err(%v)", req.CacheKey, err)
		return errors.NewErrorf("Failed to write to conn, req(%v) err(%v)", req.CacheKey, err)
	}

	err = packet.ReadFromConn(*conn, proto.NoReadDeadlineTime)
	if err != nil {
		log.LogDebugf("Failed to read from conn, req(%v), err(%v)", req.CacheKey, err)
		return errors.NewErrorf("Failed to read from conn, req(%v), err(%v)", req.CacheKey, err)
	}
	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogDebugf("put block cache: req(%v) err(%v) result(%v)", req.CacheKey, err, packet.GetResultMsg())
		return err
	}

	return err
}

func (c *BcacheClient) Evict(key string) error {
	req := &DelCacheRequest{CacheKey: key}
	packet := NewBlockCachePacket()
	packet.Opcode = OpBlockCacheDel
	data, err := req.Marshal()
	if err != nil {
		log.LogDebugf("del block cache: req(%v) err(%v)", req.CacheKey, err)
		return err
	}
	defer func() {
		bytespool.Free(data)
	}()
	packet.Data = data
	packet.Size = uint32(len(packet.Data))
	conn, err := c.connPool.Get()
	if err != nil {
		log.LogDebugf("del block cache: get Conn failed, req(%v) err(%v)", req.CacheKey, err)
		return err
	}
	defer func() {
		c.connPool.Put(conn)
	}()

	err = packet.WriteToConn(*conn)
	if err != nil {
		return err
	}

	err = packet.ReadFromConn(*conn, proto.NoReadDeadlineTime)
	if err != nil {
		return err
	}
	status := parseStatus(packet.ResultCode)
	if status != statusOK {
		err = errors.New(packet.GetResultMsg())
		log.LogErrorf("del block cache: req(%v) err(%v) result(%v)", req.CacheKey, err, packet.GetResultMsg())
		return err
	}
	log.LogDebugf("del block cache success: req(%v)", req.CacheKey)
	return nil
}

func parseStatus(result uint8) (status int) {
	switch result {
	case proto.OpOk:
		status = statusOK
	case proto.OpNotExistErr:
		status = statusNoent
	default:
		status = statusError
	}
	return
}
