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

package diskmgr

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	defaultRetryTimes = 3
)

var defaultAllocTolerateBuff int64 = 50

// idcStorage represent an idc allocator
type idcStorage struct {
	idc string
	// freeChunk should always read and write by atomic
	freeChunk int64
	diffRack  bool
	diffHost  bool

	rackStorages     map[string]*rackStorage
	blobNodeStorages []*blobNodeStorage
}

// rackStorage represent an rack storage info
type rackStorage struct {
	rack string
	// freeChunk should always read and write by atomic
	freeChunk        int64
	blobNodeStorages []*blobNodeStorage
}

// blobNodeStorage represent an data node storage info
type blobNodeStorage struct {
	host string
	// freeChunk should always read and write by atomic
	freeChunk int64
	free      int64
	disks     []*diskItem
}

// allocDisk will choose disk by disk free chunk count weight
func (d *blobNodeStorage) allocDisk(ctx context.Context, excludes map[proto.DiskID]*diskItem) (chosenDisk *diskItem) {
	span := trace.SpanFromContextSafe(ctx)
	totalFreeChunk := atomic.LoadInt64(&d.freeChunk)
	if totalFreeChunk <= 0 {
		return nil
	}
	total := len(d.disks)
	randTotal := total
	disks := make([]*diskItem, 0, total)
	disks = append(disks, d.disks...)

	for i := 0; i < total; i++ {
		chosenDisk = func() *diskItem {
			randNum := rand.Intn(randTotal)
			defer func() {
				disks[randTotal-1], disks[randNum] = disks[randNum], disks[randTotal-1]
				randTotal--
			}()
			disk := disks[randNum]
			disk.lock.RLock()
			defer disk.lock.RUnlock()
			freeChunk := disk.info.FreeChunkCnt
			if freeChunk <= 0 {
				return nil
			}
			// ignore not writable disk
			if !disk.isWritable() {
				span.Debugf("disk %d is not writable, is it expire: %s", disk.diskID, disk.isExpire())
				return nil
			}

			if _, ok := excludes[disk.diskID]; !ok {
				span.Debugf("chosen disk: %#v", disk.info)
				return disk
			}
			return nil
		}()
		if chosenDisk != nil {
			return
		}
	}
	return chosenDisk
}

func (s *idcStorage) alloc(ctx context.Context, count int, excludes map[proto.DiskID]*diskItem) ([]proto.DiskID, error) {
	span := trace.SpanFromContextSafe(ctx)
	var chosenRacks map[string]int
	var chosenDataStorages map[*blobNodeStorage]int
	var chosenDisks map[proto.DiskID]*diskItem
	ret := make([]proto.DiskID, 0)

	totalFreeChunk := atomic.LoadInt64(&s.freeChunk)
	span.Debugf("%s idc total free chunk: %d", s.idc, totalFreeChunk)
	if totalFreeChunk < int64(count) {
		return nil, ErrNoEnoughSpace
	}

	if s.diffRack && s.diffHost {
		chosenRacks, chosenDataStorages, chosenDisks = s.allocFromRack(ctx, count, excludes)
	} else {
		chosenDataStorages, chosenDisks = s.allocFromBlobNodeStorages(ctx, count, totalFreeChunk-defaultAllocTolerateBuff, s.blobNodeStorages, excludes)
	}

	if len(chosenDisks) < count {
		span.Warnf("alloc failed, chosenRacks: %v, chosenBlobNodeStorages: %+v, chosenDisks: %v", chosenRacks, chosenDataStorages, chosenDisks)
		return nil, ErrNoEnoughSpace
	}

	atomic.AddInt64(&s.freeChunk, int64(-count))
	for rack, num := range chosenRacks {
		atomic.AddInt64(&s.rackStorages[rack].freeChunk, int64(-num))
	}
	for stg, num := range chosenDataStorages {
		atomic.AddInt64(&stg.freeChunk, int64(-num))
	}
	for id := range chosenDisks {
		chosenDisks[id].lock.Lock()
		chosenDisks[id].info.FreeChunkCnt -= 1
		chosenDisks[id].lock.Unlock()
		ret = append(ret, id)
	}

	return ret, nil
}

// 1. alloc rack with free chunk weight
// 2. alloc from rack's data node storage
// 3. if can't meet the alloc count request, then retry with enable same rack
func (s *idcStorage) allocFromRack(ctx context.Context, count int, excludes map[proto.DiskID]*diskItem) (chosenRacksRet map[string]int, chosenDataStorages map[*blobNodeStorage]int, chosenDisks map[proto.DiskID]*diskItem) {
	span := trace.SpanFromContextSafe(ctx)
	rackNum := len(s.rackStorages)
	chosenRacksRet = make(map[string]int, count)
	chosenRacks := make([]string, 0, rackNum/2)
	chosenRacksNum := make(map[string]int, rackNum/2)
	chosenDataStorages = make(map[*blobNodeStorage]int)
	chosenDisks = make(map[proto.DiskID]*diskItem)
	totalFreeChunk := atomic.LoadInt64(&s.freeChunk) - defaultAllocTolerateBuff
	_totalFreeChunk := totalFreeChunk
	_count := count

	rackStorages := make([]*rackStorage, 0, len(s.rackStorages))
	for _, rackStg := range s.rackStorages {
		rackStorages = append(rackStorages, rackStg)
	}

	duplicatedCount := 0
	randNum := int64(0)
	idx := 0

RETRY:
	if _totalFreeChunk > 0 {
		randNum = rand.Int63n(_totalFreeChunk)
	} else {
		randNum = 0
	}
	for i := idx; i < rackNum; i++ {
		rackStorage := rackStorages[i]
		rack := rackStorage.rack
		freeChunk := atomic.LoadInt64(&rackStorage.freeChunk)
		if freeChunk > 0 && freeChunk >= randNum && chosenRacksNum[rack] <= duplicatedCount &&
			(s.diffHost && len(rackStorage.blobNodeStorages) > chosenRacksNum[rack]) {
			allocNum := 1
			if _, ok := chosenRacksNum[rack]; ok {
				// retry with same rack, add all rest num into chosenRacksNum
				allocNum = len(rackStorage.blobNodeStorages) - chosenRacksNum[rack]
				chosenRacksNum[rack] += allocNum
			} else {
				chosenRacks = append(chosenRacks, rack)
				chosenRacksNum[rack] = allocNum
			}
			rackStorages[idx], rackStorages[i] = rackStorages[i], rackStorages[idx]
			idx += 1
			if duplicatedCount <= 0 {
				_totalFreeChunk -= freeChunk
			}
			_count -= allocNum
			goto RETRY
		}
		randNum -= freeChunk
	}
	// in the end, we still can't find enough rack. then we should try duplicated rack.
	if duplicatedCount <= 0 {
		span.Info("can't find enough rack, try duplicated rack")
		idx = 0
		duplicatedCount = 1 << 32
		goto RETRY
	}

	if _count > 0 {
		span.Warnf("still can't find enough rack, chosen racks: %v, chosen racks num: %v", chosenRacks, chosenRacksNum)
		return nil, nil, nil
	}
	span.Infof("chosen racks: %v, chosen racks num: %v", chosenRacks, chosenRacksNum)

	// shuffle chosen racks, [0-count) will range by rack free chunk weight
	// [count, total) will be shuffle by random, ensure allocation more evenly
	total := len(chosenRacks)
	if total > count {
		for i := count; i < total; i++ {
			rand.Shuffle(total-count, func(i, j int) {
				chosenRacks[i+count], chosenRacks[j+count] = chosenRacks[j+count], chosenRacks[i+count]
			})
		}
	}

	// alloc chunk from rack's blobNodeStorages
	_count = count
	for _, rack := range chosenRacks {
		num := chosenRacksNum[rack]
		if num > _count {
			num = _count
		}
		dataStorages, disks := s.allocFromBlobNodeStorages(ctx, num, atomic.LoadInt64(&s.rackStorages[rack].freeChunk), s.rackStorages[rack].blobNodeStorages, excludes)
		for id := range disks {
			chosenDisks[id] = disks[id]
			chosenRacksRet[rack]++
			_count--
		}
		for stg := range dataStorages {
			chosenDataStorages[stg] += dataStorages[stg]
		}
		// got enough disk, then return
		if _count == 0 {
			return
		}
	}
	return
}

// 1. copy rack's blobNodeStorage pointer array
// 2. alloc from blobNodeStorage array
// 3. the alloc result length may not equal to count if there is no enough space or something else
func (s *idcStorage) allocFromBlobNodeStorages(ctx context.Context, count int, totalFreeChunk int64, srcBlobNodeStorages []*blobNodeStorage, excludes map[proto.DiskID]*diskItem) (chosenDataStorages map[*blobNodeStorage]int, chosenDisks map[proto.DiskID]*diskItem) {
	span := trace.SpanFromContextSafe(ctx)
	excludeHosts := make(map[string]bool)
	chosenDisks = make(map[proto.DiskID]*diskItem)
	chosenDataStorages = make(map[*blobNodeStorage]int)
	randNum := int64(0)

	for _, diskInfo := range excludes {
		diskInfo.lock.RLock()
		if excludeHosts == nil {
			excludeHosts = map[string]bool{diskInfo.info.Host: true}
		} else {
			excludeHosts[diskInfo.info.Host] = true
		}
		diskInfo.lock.RUnlock()
	}

	blobNodeStorages := make([]*blobNodeStorage, 0, len(s.blobNodeStorages))
	blobNodeStorageNum := 0
	// build available blobNodeStorages, filter exclude host or disk
	for i := range srcBlobNodeStorages {
		// not allow same host, then filter exclude host
		if s.diffHost && excludeHosts[srcBlobNodeStorages[i].host] {
			freeChunk := atomic.LoadInt64(&srcBlobNodeStorages[i].freeChunk)
			totalFreeChunk -= freeChunk
			continue
		}
		blobNodeStorages = append(blobNodeStorages, srcBlobNodeStorages[i])
		// allow same host, then exclude target disk. it's quite slowly but alright in test env which enable same host alloc
		if !s.diffHost && len(excludes) > 0 {
			freeChunk := atomic.LoadInt64(&srcBlobNodeStorages[i].freeChunk)
			newDisks := make([]*diskItem, 0, len(srcBlobNodeStorages[i].disks))
			for _, disk := range srcBlobNodeStorages[i].disks {
				if _, ok := excludes[disk.diskID]; ok {
					disk.lock.RLock()
					totalFreeChunk -= disk.info.FreeChunkCnt
					freeChunk -= disk.info.FreeChunkCnt
					disk.lock.RUnlock()
					continue
				}
				newDisks = append(newDisks, disk)
			}
			blobNodeStorages[blobNodeStorageNum] = &blobNodeStorage{
				host:      srcBlobNodeStorages[i].host,
				freeChunk: freeChunk,
				disks:     newDisks,
			}
		}
		blobNodeStorageNum += 1
	}
	span.Debugf("total blobNodeStorages num: %d, excludes host: %v, excludes disk: %v", blobNodeStorageNum, excludeHosts, excludes)
	// no available data node after exclude, then return
	if blobNodeStorageNum == 0 {
		return
	}
	// no available chunk after exclude, then return
	if totalFreeChunk <= 0 {
		return
	}

	chosenIdx := 0
	retryTimes := 0
	maxRetryTimes := defaultRetryTimes
	// maxRetry times will equal to count when blobNodeStorageNum less than target count
	if blobNodeStorageNum < count {
		maxRetryTimes = count
	}
	_totalFreeChunk := totalFreeChunk

RETRY:
	for count > 0 {
		// generate randNum every chosen
		if _totalFreeChunk > 0 {
			randNum = rand.Int63n(_totalFreeChunk)
		} else {
			randNum = 0
		}
		for i := chosenIdx; i < blobNodeStorageNum; i++ {
			freeChunk := atomic.LoadInt64(&blobNodeStorages[i].freeChunk)
			span.Debugf("total free chunk: %d, blobNode(%s) free chunk: %d, randNum: %d", _totalFreeChunk, blobNodeStorages[i].host, freeChunk, randNum)
			if freeChunk >= randNum {
				if selectedDisk := blobNodeStorages[i].allocDisk(ctx, chosenDisks); selectedDisk != nil {
					chosenDisks[selectedDisk.diskID] = selectedDisk
					chosenDataStorages[blobNodeStorages[i]] += 1
					blobNodeStorages[chosenIdx], blobNodeStorages[i] = blobNodeStorages[i], blobNodeStorages[chosenIdx]
					_totalFreeChunk -= freeChunk
					count -= 1
					chosenIdx += 1
					goto RETRY
				}
			}
			randNum -= freeChunk
		}
		// go to the end of all data nodes, then check if retry when diffHost is false
		if !s.diffHost && retryTimes < maxRetryTimes {
			span.Infof("%s retry choose with same host", s.idc)
			retryTimes += 1
			// reset chosenIdx and _totalFreeChunk when retry same host
			chosenIdx = 0
			_totalFreeChunk = totalFreeChunk
			goto RETRY
		}
		return
	}
	return
}
