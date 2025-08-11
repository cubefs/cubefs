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
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

const SIZE_GB = 1024 * 1024 * 1024

type PrepareRemoteCacheRequest struct {
	ctx    context.Context
	inode  uint64
	ek     *proto.ExtentKey
	warmUp bool
	gen    uint64
}

func NewPrepareRemoteCacheRequest(inode uint64, ek proto.ExtentKey, warmUp bool, gen uint64) *PrepareRemoteCacheRequest {
	return &PrepareRemoteCacheRequest{
		ctx:    context.Background(),
		inode:  inode,
		ek:     &ek,
		warmUp: warmUp,
		gen:    gen,
	}
}

func (pr *PrepareRemoteCacheRequest) String() string {
	if pr == nil {
		return ""
	}
	return fmt.Sprintf("PrepareRemoteCacheRequest{ino: %v, ek: %v}", pr.inode, pr.ek)
}

func (s *Streamer) enableRemoteCache() bool {
	fileSize, _ := s.extents.Size()
	enableRemoteCache := s.client.IsRemoteCacheEnabled() && int64(fileSize) <= s.client.RemoteCache.remoteCacheMaxFileSizeGB*SIZE_GB
	bloomStatus := s.client.shouldRemoteCache(s.fullPath)
	log.LogDebugf("Streamer inode %v fullPath %v parent %v fileSize %v enableRemoteCache %v bloomStatus %v",
		s.inode, s.fullPath, s.parentInode, fileSize, enableRemoteCache, bloomStatus)
	return enableRemoteCache && bloomStatus
}

func (s *Streamer) enableRemoteCacheAutoPrepare() bool {
	return s.enableRemoteCache() && s.client.RemoteCache.AutoPrepare
}

func (s *Streamer) sendToPrepareRomoteCacheChan(req *PrepareRemoteCacheRequest) {
	select {
	case s.client.RemoteCache.PrepareCh <- req:
	default:
		log.LogWarnf("sendToPrepareRomoteCacheChan: chan is full, discard req(%v)", req)
	}
}

func (s *Streamer) prepareRemoteCache(ctx context.Context, ek *proto.ExtentKey, gen uint64) {
	cReadRequests, err := s.prepareCacheRequests(ek.FileOffset, uint64(ek.Size), nil, gen)
	if err != nil {
		log.LogWarnf("Streamer prepareRemoteCache: prepareCacheRequests failed. start(%v), size(%v), err(%v)", ek.FileOffset, ek.Size, err)
		return
	}
	for _, req := range cReadRequests {
		slot, fg, ownerSlot := s.getFlashGroup(req.CacheRequest.FixedFileOffset)
		if fg == nil {
			err = fmt.Errorf("cannot find any flashGroups")
			log.LogWarnf("Streamer prepareRemoteCache failed: %v", err)
			break
		}
		req.CacheRequest.Slot = uint64(slot)<<32 | uint64(ownerSlot)
		prepareReq := &proto.CachePrepareRequest{
			CacheRequest: req.CacheRequest,
			FlashNodes:   fg.Hosts,
		}
		if err = s.client.RemoteCache.Prepare(ctx, fg, s.inode, prepareReq); err != nil {
			log.LogWarnf("Streamer prepareRemoteCache: flashGroup prepare failed. fg(%v) req(%v) err(%v)", fg, prepareReq, err)
		}
	}
	if log.EnableDebug() {
		log.LogDebugf("prepareRemoteCache: inode(%d),ek(%v) err(%v)", s.inode, ek, err)
	}
}

func (s *Streamer) readFromRemoteCache(ctx context.Context, offset, size uint64, cReadRequests []*CacheReadRequest) (total int, err error) {
	metric := exporter.NewTPCnt("readFromRemoteCache")
	metricBytes := exporter.NewCounter("readFromRemoteCacheBytes")
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: s.client.volumeName})
		metricBytes.AddWithLabels(int64(total), map[string]string{exporter.Vol: s.client.volumeName})
	}()

	var read int
	for _, req := range cReadRequests {
		if len(req.CacheRequest.Sources) == 0 {
			total += int(req.Size_)
			continue
		}
		slot, fg, ownerSlot := s.getFlashGroup(req.CacheRequest.FixedFileOffset)
		if fg == nil {
			err = fmt.Errorf("readFromRemoteCache failed: cannot find any flashGroups")
			return
		}
		req.CacheRequest.Slot = uint64(slot)<<32 | uint64(ownerSlot)
		if read, err = s.client.RemoteCache.Read(ctx, fg, s.inode, req); err != nil {
			if !proto.IsFlashNodeLimitError(err) {
				log.LogWarnf("readFromRemoteCache: flashGroup read failed. offset(%v) size(%v) fg(%v) req(%v) err(%v)", offset, size, fg, req, err)
			}
			return
		} else {
			log.LogDebugf("readFromRemoteCache: inode(%d) cacheReadRequest version %v, source %v",
				s.inode, req.CacheRequest.Version, req.CacheRequest.Sources)
			total += read
		}
	}
	log.LogDebugf("readFromRemoteCache: inode(%d), cacheReadRequests(%v) offset(%v) size(%v) total(%v)", s.inode, cReadRequests, offset, size, total)
	return total, nil
}

func (s *Streamer) getFlashGroup(fixedFileOffset uint64) (uint32, *FlashGroup, uint32) {
	slot := proto.ComputeCacheBlockSlot(s.client.dataWrapper.VolName, s.inode, fixedFileOffset)
	fg, ownerSlot := s.client.RemoteCache.GetFlashGroupBySlot(slot)
	return slot, fg, ownerSlot
}

func (s *Streamer) getDataSource(start, size, fixedFileOffset uint64, isRead bool) ([]*proto.DataSource, error) {
	sources := make([]*proto.DataSource, 0)
	log.LogDebugf("getDataSource. start %v size %v fixedFileOffset %v isRead %v", start, size, fixedFileOffset, isRead)
	eReqs := s.extents.PrepareReadRequests(int(fixedFileOffset), proto.CACHE_BLOCK_SIZE, nil)
	for _, eReq := range eReqs {
		log.LogDebugf("getDataSource. eReq %v", eReq)
		if eReq.ExtentKey == nil {
			continue
		}
		if eReq.ExtentKey.PartitionId == 0 {
			err := fmt.Errorf("temporary ek(%v), isRead[%v] start(%v) size(%v) eReq(%v) fixedOff(%v)", eReq.ExtentKey, isRead, start, size, eReq, fixedFileOffset)
			log.LogWarnf("getDataSource failed: err(%v)", err)
			return nil, err
		}

		dp, ok := s.client.dataWrapper.TryGetPartition(eReq.ExtentKey.PartitionId)
		if !ok {
			log.LogWarnf("getDataSource: partitionId(%v) not exist", eReq.ExtentKey.PartitionId)
			return nil, errors.NewErrorf("getDataSource: partitionId(%v) not exist", eReq.ExtentKey.PartitionId)
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
		log.LogDebugf("getDataSource: append  source inode %v PartitionID %v ExtentID %v FileOffset %v "+
			"ExtentOffset %v fixedFileOffset %v size %v",
			s.inode, source.PartitionID, source.ExtentID, source.FileOffset, source.ExtentOffset, fixedFileOffset,
			source.Size_)
	}
	return sources, nil
}

func (s *Streamer) prepareCacheRequests(offset, size uint64, data []byte, gen uint64) ([]*CacheReadRequest, error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("prepareCacheRequests", nil, bgTime, 1)
	}()

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
			TTL:             s.client.RemoteCache.TTL,
			Sources:         sources,
			Version:         proto.ComputeSourcesVersion(sources, gen),
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
	log.LogDebugf("prepareCacheRequests: inode %v extent[offset=%v,size=%v] cReadRequests %v ", s.inode, offset, size, cReadRequests)
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
