// Copyright 2023 The CubeFS Authors.
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
	"context"
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

type PrepareRemoteCacheRequest struct {
	ctx   context.Context
	inode uint64
	ek    *proto.ExtentKey
}

func (pr *PrepareRemoteCacheRequest) String() string {
	if pr == nil {
		return ""
	}
	return fmt.Sprintf("PrepareRemoteCacheRequest{ino: %v, ek: %v}", pr.inode, pr.ek)
}

func (s *Streamer) enableRemoteCache() bool {
	if !s.client.IsCacheBoostEnabled() || s.client.RemoteCache == nil {
		return false
	}
	return s.bloomStatus
}

func (s *Streamer) enableCacheAutoPrepare() bool {
	if !s.client.EnableCacheAutoPrepare {
		return false
	}
	return s.enableRemoteCache()
}

func (s *Streamer) sendToPrepareRomoteCacheChan(req *PrepareRemoteCacheRequest) {
	select {
	case s.client.prepareRemoteCacheCh <- req:
	default:
		log.LogWarnf("sendToPrepareRomoteCacheChan: chan is full, discard req(%v)", req)
	}
}

func (s *Streamer) prepareRemoteCache(ctx context.Context, ek *proto.ExtentKey) {
	cReadRequests, err := s.prepareCacheRequests(ek.FileOffset, uint64(ek.Size), nil)
	if err != nil {
		log.LogWarnf("Streamer prepareRemoteCache: prepareCacheRequests failed. start(%v), size(%v), err(%v)", ek.FileOffset, ek.Size, err)
		return
	}

	for _, req := range cReadRequests {
		fg := s.getFlashGroup(req.CacheRequest.FixedFileOffset)
		if fg == nil {
			err = fmt.Errorf("cannot find any flashGroups")
			log.LogWarnf("Streamer prepareRemoteCache failed: %v", err)
			break
		}
		prepareReq := &proto.CachePrepareRequest{
			CacheRequest: req.CacheRequest,
			FlashNodes:   fg.Hosts,
		}

		if err = s.client.RemoteCache.Prepare(ctx, fg, s.inode, prepareReq); err != nil {
			log.LogWarnf("Streamer prepareRemoteCache: flashGroup prepare failed. fg(%v) req(%v) err(%v)", fg, prepareReq, err)
		}
	}

	log.LogDebugf("prepareRemoteCache: inode(%d), err(%v)", s.inode, err)
}

func (s *Streamer) readFromRemoteCache(ctx context.Context, offset, size uint64, cReadRequests []*CacheReadRequest) (total int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("remote-cache-read", err, bgTime, 1)
	}()

	var read int
	for _, req := range cReadRequests {
		if len(req.CacheRequest.Sources) == 0 {
			total += int(req.Size_)
			continue
		}
		fg := s.getFlashGroup(req.CacheRequest.FixedFileOffset)
		if fg == nil {
			err = fmt.Errorf("readFromRemoteCache failed: Cannot find any flashGroups")
			return
		}

		if read, err = s.client.RemoteCache.Read(ctx, fg, s.inode, req); err != nil {
			log.LogWarnf("readFromRemoteCache: flashGroup read failed. offset(%v) size(%v) fg(%v) req(%v) err(%v)", offset, size, fg, req, err)
			return
		} else {
			total += read
		}
	}
	log.LogDebugf("readFromRemoteCache: inode(%d), cacheReadRequests(%v) offset(%v) size(%v) total(%v)", s.inode, cReadRequests, offset, size, total)
	return total, nil
}

func (s *Streamer) getFlashGroup(fixedFileOffset uint64) *FlashGroup {
	slot := proto.ComputeCacheBlockSlot(s.client.dataWrapper.VolName, s.inode, fixedFileOffset)
	return s.client.RemoteCache.GetFlashGroupBySlot(slot)
}

func (s *Streamer) getDataSource(start, size, fixedFileOffset uint64, isRead bool) ([]*proto.DataSource, error) {
	sources := make([]*proto.DataSource, 0)
	var revisedRequests []*ExtentRequest
	var data []byte
	eReqs := s.extents.PrepareReadRequests(int(fixedFileOffset), proto.CACHE_BLOCK_SIZE, nil)
	for _, req := range eReqs {
		if req.ExtentKey == nil {
			continue
		}
		if req.ExtentKey.PartitionId == 0 || req.ExtentKey.ExtentId == 0 {
			s.writeLock.Lock()
			if err := s.IssueFlushRequest(); err != nil {
				s.writeLock.Unlock()
				return nil, err
			}
			revisedRequests = s.extents.PrepareReadRequests(int(fixedFileOffset), proto.CACHE_BLOCK_SIZE, data)
			s.writeLock.Unlock()
			break
		}
	}
	if revisedRequests != nil {
		eReqs = revisedRequests
	}

	for _, eReq := range eReqs {
		if eReq.ExtentKey == nil {
			continue
		}

		dp, err := s.client.dataWrapper.GetDataPartition(eReq.ExtentKey.PartitionId)
		if err != nil {
			log.LogWarnf("Streamer getDataSource: GetDataPartition failed. PartitionId(%v), err(%v)", eReq.ExtentKey.PartitionId, err)
			return nil, err
		}

		sortedHosts := dp.SortHostsByPingElapsed()

		source := &proto.DataSource{
			FileOffset:   uint64(eReq.FileOffset),
			Size_:        uint64(eReq.Size),
			PartitionID:  eReq.ExtentKey.PartitionId,
			ExtentID:     eReq.ExtentKey.ExtentId,
			ExtentOffset: uint64(eReq.FileOffset) - eReq.ExtentKey.FileOffset + eReq.ExtentKey.ExtentOffset,
			Hosts:        sortedHosts,
		}
		sources = append(sources, source)
	}
	return sources, nil
}

func (s *Streamer) prepareCacheRequests(offset, size uint64, data []byte) ([]*CacheReadRequest, error) {
	var (
		cReadRequests []*CacheReadRequest
		cRequests     = make([]*proto.CacheRequest, 0)
		isRead        = data != nil
	)
	for fixedOff := offset / proto.CACHE_BLOCK_SIZE * proto.CACHE_BLOCK_SIZE; fixedOff < offset+size; fixedOff += proto.CACHE_BLOCK_SIZE {
		sources, err := s.getDataSource(offset, size, fixedOff, isRead)
		if err != nil {
			log.LogWarnf("Streamer prepareCacheRequests: getDataSource failed. fixedOff(%v) err(%v)", fixedOff, err)
			return nil, err
		}
		cReq := &proto.CacheRequest{
			Volume:          s.client.dataWrapper.VolName,
			Inode:           s.inode,
			FixedFileOffset: fixedOff,
			TTL:             s.client.CacheTTL,
			Sources:         sources,
			Version:         proto.ComputeSourcesVersion(sources),
		}
		cRequests = append(cRequests, cReq)
	}
	if isRead {
		cReadRequests = getCacheReadRequests(offset, size, data, cRequests)
	} else {
		cReadRequests = make([]*CacheReadRequest, 0, len(cRequests))
		for _, cReq := range cRequests {
			if len(cReq.Sources) == 0 {
				continue
			}
			cReadRequest := new(CacheReadRequest)
			cReadRequest.CacheRequest = cReq
			cReadRequests = append(cReadRequests, cReadRequest)
		}
	}
	return cReadRequests, nil
}

func getCacheReadRequests(offset uint64, size uint64, data []byte, cRequests []*proto.CacheRequest) (cReadRequests []*CacheReadRequest) {
	cReadRequests = make([]*CacheReadRequest, 0, len(cRequests))
	startFixedOff := offset / proto.CACHE_BLOCK_SIZE * proto.CACHE_BLOCK_SIZE
	endFixedOff := (offset + size - 1) / proto.CACHE_BLOCK_SIZE * proto.CACHE_BLOCK_SIZE

	for _, cReq := range cRequests {
		cReadReq := new(CacheReadRequest)
		cReadReq.CacheRequest = cReq
		if cReq.FixedFileOffset == startFixedOff {
			cReadReq.Offset = offset - startFixedOff
		} else {
			cReadReq.Offset = 0
		}

		if cReq.FixedFileOffset == endFixedOff {
			cReadReq.Size_ = offset + size - cReq.FixedFileOffset - cReadReq.Offset
		} else {
			cReadReq.Size_ = proto.CACHE_BLOCK_SIZE - cReadReq.Offset
		}

		dataStart := cReadReq.Offset + cReq.FixedFileOffset - offset
		cReadReq.Data = data[dataStart : dataStart+cReadReq.Size_]
		cReadRequests = append(cReadRequests, cReadReq)
	}
	return cReadRequests
}
