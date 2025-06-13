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

package cluster

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	defaultRetryTimes = 3
)

var defaultAllocTolerateBuff int64 = 50

type clusterInfoGetter interface {
	getNode(nodeID proto.NodeID) (node *nodeItem, exist bool)
	getDisk(diskID proto.DiskID) (disk *diskItem, exist bool)
}

type topoInfoGetter interface {
	getNodeNum(diskType proto.DiskType, id proto.NodeSetID) int
}

type (
	nodeSetAllocatorMap map[proto.NodeSetID]*nodeSetAllocator
	diskSetAllocatorMap map[proto.DiskSetID]*diskSetAllocator
)

type allocatorConfig struct {
	nodeSets map[proto.DiskType]nodeSetAllocatorMap
	diskSets map[proto.DiskType]diskSetAllocatorMap
	dg       clusterInfoGetter
	tg       topoInfoGetter
	diffRack bool
	diffHost bool
}

func newAllocator(cfg allocatorConfig) *allocator {
	return &allocator{
		nodeSets: cfg.nodeSets,
		diskSets: cfg.diskSets,
		cfg:      cfg,
	}
}

type allocator struct {
	nodeSets map[proto.DiskType]nodeSetAllocatorMap
	diskSets map[proto.DiskType]diskSetAllocatorMap
	cfg      allocatorConfig
}

type allocRet struct {
	Idc   string
	Disks []proto.DiskID
}

// Alloc alloc disk id
// todo: add retry when diskset alloc failed or idc alloc failed
func (a *allocator) Alloc(ctx context.Context, diskType proto.DiskType, mode codemode.CodeMode, excludes []proto.DiskSetID) ([]allocRet, error) {
	span := trace.SpanFromContextSafe(ctx)
	var (
		err        error
		ret        = make([]allocRet, 0)
		idcIndexes = mode.T().GetECLayoutByAZ()
		allocCount = mode.GetShardNum()
	)

	// alloc nodeset
	nodeSetAllocator, err := a.allocNodeSet(ctx, diskType, mode)
	if err != nil {
		span.Errorf("alloc nodeset failed, err: %s", err.Error())
		return nil, err
	}
	// alloc diskset
	diskSetAllocator, err := nodeSetAllocator.allocDiskSet(ctx, allocCount, excludes)
	if err != nil {
		span.Errorf("alloc diskset failed, err: %s", err.Error())
		return nil, err
	}

	idcAllocators := diskSetAllocator.alloc(ctx, len(idcIndexes[0]))
	if len(idcAllocators) < len(idcIndexes) {
		span.Errorf("need %d idcAllocators, but got %d", len(idcIndexes), len(idcAllocators))
		return nil, ErrNoEnoughSpace
	}

	for i := range idcIndexes {
		count := len(idcIndexes[i])
		_disks, _err := idcAllocators[i].alloc(ctx, count, nil)
		if _err != nil {
			span.Errorf("alloc from idc allocator failed, err:%s", _err.Error())
			return nil, _err
		}

		ret = append(ret, allocRet{
			Idc:   idcAllocators[i].idc,
			Disks: _disks,
		})
	}
	// update diskset and nodeset free item
	atomic.AddInt64(&diskSetAllocator.weight, -int64(allocCount))
	atomic.AddInt64(&nodeSetAllocator.weight, -int64(allocCount))

	return ret, nil
}

type reAllocPolicy struct {
	diskType  proto.DiskType
	diskSetID proto.DiskSetID
	idc       string
	count     int
	excludes  []proto.DiskID
}

func (a *allocator) ReAlloc(ctx context.Context, policy reAllocPolicy) ([]proto.DiskID, error) {
	if policy.diskSetID == nullDiskSetID {
		policy.diskSetID = ecDiskSetID
	}
	stg := a.diskSets[policy.diskType][policy.diskSetID].idcAllocators[policy.idc]

	_excludes := make(map[proto.DiskID]*diskItem)
	if len(policy.excludes) > 0 {
		for _, diskID := range policy.excludes {
			_excludes[diskID], _ = a.cfg.dg.getDisk(diskID)
		}
	}

	return stg.alloc(ctx, policy.count, _excludes)
}

func (a *allocator) allocNodeSet(ctx context.Context, diskType proto.DiskType, mode codemode.CodeMode) (*nodeSetAllocator, error) {
	span := trace.SpanFromContextSafe(ctx)

	count := mode.GetShardNum()
	nodeSetAllocators, ok := a.nodeSets[diskType]
	if !ok {
		span.Errorf("can not find nodeset of diskType: %s", diskType.String())
		return nil, ErrNoEnoughSpace
	}

	// EC mode
	if !mode.T().IsReplicateMode() {
		nodeSetAllocator, ok := nodeSetAllocators[ecNodeSetID]
		if !ok || nodeSetAllocator.weight < int64(count) {
			span.Errorf("can not find nodeset of EC mode, diskType: %s", diskType.String())
			return nil, ErrNoEnoughSpace
		}
		return nodeSetAllocator, nil
	}

	// choose nodeset by free item count weight
	total := len(nodeSetAllocators)
	totalWeight := int64(0)
	allocatableNodeSets := make([]*nodeSetAllocator, 0, total)
	for _, n := range nodeSetAllocators {
		if n.nodeSetID == ecNodeSetID {
			continue
		}
		// filter nodeset which is not match the alloc node count
		c := a.cfg.tg.getNodeNum(diskType, n.nodeSetID)
		if a.cfg.diffHost && c < count {
			span.Debugf("filter nodeset id:%d, need %d node but got:%d, diskType:%d", n.nodeSetID, count, c, diskType)
			continue
		}
		allocatableNodeSets = append(allocatableNodeSets, n)
		totalWeight += atomic.LoadInt64(&n.weight)
	}
	if totalWeight <= 0 {
		span.Errorf("totalWeight <= 0, no nodeset can be allocate")
		return nil, ErrNoEnoughSpace
	}

	randNum := rand.Int63n(totalWeight)
	for i := 0; i < total; i++ {
		ns := allocatableNodeSets[i]
		free := atomic.LoadInt64(&ns.weight)
		if free > randNum && free > int64(count) {
			return ns, nil
		}
		randNum -= free
	}
	span.Errorf("allocate nodeSet failed, codeMode: %s, allocate num: %d", mode.String(), count)
	return nil, ErrNoEnoughSpace
}

func newNodeSetAllocator(id proto.NodeSetID) *nodeSetAllocator {
	return &nodeSetAllocator{
		nodeSetID: id,
		diskSets:  make(map[proto.DiskSetID]*diskSetAllocator),
	}
}

type nodeSetAllocator struct {
	nodeSetID proto.NodeSetID
	weight    int64
	diskSets  map[proto.DiskSetID]*diskSetAllocator
}

func (n *nodeSetAllocator) addDiskSet(diskSet *diskSetAllocator) {
	n.diskSets[diskSet.diskSetID] = diskSet
	n.weight += diskSet.weight
}

func (n *nodeSetAllocator) allocDiskSet(ctx context.Context, count int, excludes []proto.DiskSetID) (*diskSetAllocator, error) {
	span := trace.SpanFromContextSafe(ctx)

	_excludes := make(map[proto.DiskSetID]struct{})
	if len(excludes) != 0 {
		for _, diskSetID := range excludes {
			_excludes[diskSetID] = struct{}{}
		}
	}
	randNum := rand.Int63n(atomic.LoadInt64(&n.weight))
	for diskSetID, diskSet := range n.diskSets {
		if _, ok := _excludes[diskSetID]; ok {
			span.Warnf("diskSet:%d is excluded", diskSetID)
			continue
		}
		free := atomic.LoadInt64(&diskSet.weight)
		if free >= randNum && free >= int64(count) {
			return diskSet, nil
		}
		randNum -= free
	}
	span.Errorf("allocate diskSet from nodeSet:%d failed, allocate num: %d", n.nodeSetID, count)
	return nil, ErrNoEnoughSpace
}

func newDiskSetAllocator(id proto.DiskSetID, weight int64, idcAllocators map[string]*idcAllocator) *diskSetAllocator {
	return &diskSetAllocator{
		diskSetID:     id,
		weight:        weight,
		idcAllocators: idcAllocators,
	}
}

type diskSetAllocator struct {
	diskSetID     proto.DiskSetID
	weight        int64
	idcAllocators map[string]*idcAllocator
}

func (d *diskSetAllocator) alloc(ctx context.Context, count int) (ret []*idcAllocator) {
	span := trace.SpanFromContextSafe(ctx)
	for _, idcAllocator := range d.idcAllocators {
		nodeNum := len(idcAllocator.nodeStorages)
		if idcAllocator.diffHost && nodeNum < count {
			span.Errorf("allocate diff host idcAllocator from diskSet: %d failed, allocate num: %d, node num: %d", d.diskSetID, count, nodeNum)
			continue
		}
		if free := atomic.LoadInt64(&idcAllocator.weight); free < int64(count) {
			span.Errorf("allocate idcAllocator from diskSet: %d failed, allocate num: %d, idc free: %d", d.diskSetID, count, free)
			continue
		}
		ret = append(ret, idcAllocator)
	}
	return
}

// idcAllocator represent an idc allocator
type idcAllocator struct {
	idc string
	// weight should always read and write by atomic
	weight   int64
	diffRack bool
	diffHost bool

	rackStorages map[string]*rackAllocator
	nodeStorages []*nodeAllocator
}

// rackAllocator represent an rack storage info
type rackAllocator struct {
	rack string
	// weight should always read and write by atomic
	weight       int64
	nodeStorages []*nodeAllocator
}

// nodeAllocator represent an data node storage info
type nodeAllocator struct {
	host string
	// weight should always read and write by atomic
	weight int64
	free   int64
	disks  []*diskItem
}

// allocDisk will choose disk by disk free item count weight
func (d *nodeAllocator) allocDisk(ctx context.Context, excludes map[proto.DiskID]*diskItem) (chosenDisk *diskItem) {
	span := trace.SpanFromContextSafe(ctx)
	totalWeight := atomic.LoadInt64(&d.weight)
	if totalWeight <= 0 {
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
			err := disk.withRLocked(func() error {
				weight := disk.weight()
				if weight <= 0 {
					return ErrNoEnoughSpace
				}
				// ignore not writable disk
				if !disk.isWritable() {
					span.Debugf("disk %d is not writable, is it expired: %v", disk.diskID, disk.isExpire())
					return ErrNoEnoughSpace
				}
				return nil
			})
			if err != nil {
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

func (s *idcAllocator) alloc(ctx context.Context, count int, excludes map[proto.DiskID]*diskItem) ([]proto.DiskID, error) {
	span := trace.SpanFromContextSafe(ctx)
	var chosenRacks map[string]int
	var chosenDataStorages map[*nodeAllocator]int
	var chosenDisks map[proto.DiskID]*diskItem
	ret := make([]proto.DiskID, 0)

	totalWeight := atomic.LoadInt64(&s.weight)
	span.Debugf("%s idc total free item: %d", s.idc, totalWeight)
	if totalWeight < int64(count) {
		return nil, ErrNoEnoughSpace
	}

	if s.diffRack && s.diffHost {
		chosenRacks, chosenDataStorages, chosenDisks = s.allocFromRack(ctx, count, excludes)
	} else {
		chosenDataStorages, chosenDisks = s.allocFromNodeStorages(ctx, count, totalWeight-defaultAllocTolerateBuff, s.nodeStorages, excludes)
	}

	if len(chosenDisks) < count {
		span.Warnf("alloc failed, chosenRacks: %v, chosenNodeStorages: %+v, chosenDisks: %v", chosenRacks, chosenDataStorages, chosenDisks)
		return nil, ErrNoEnoughSpace
	}

	atomic.AddInt64(&s.weight, int64(-count))
	for rack, num := range chosenRacks {
		atomic.AddInt64(&s.rackStorages[rack].weight, int64(-num))
	}
	for stg, num := range chosenDataStorages {
		atomic.AddInt64(&stg.weight, int64(-num))
	}
	for id, disk := range chosenDisks {
		disk.withLocked(func() error {
			disk.decrWeight(1)
			return nil
		})
		ret = append(ret, id)
	}

	return ret, nil
}

// 1. alloc rack with free item weight
// 2. alloc from rack's data node storage
// 3. if can't meet the alloc count request, then retry with enable same rack
func (s *idcAllocator) allocFromRack(ctx context.Context, count int, excludes map[proto.DiskID]*diskItem) (chosenRacksRet map[string]int, chosenDataStorages map[*nodeAllocator]int, chosenDisks map[proto.DiskID]*diskItem) {
	span := trace.SpanFromContextSafe(ctx)
	rackNum := len(s.rackStorages)
	chosenRacksRet = make(map[string]int, count)
	chosenRacks := make([]string, 0, rackNum/2)
	chosenRacksNum := make(map[string]int, rackNum/2)
	chosenDataStorages = make(map[*nodeAllocator]int)
	chosenDisks = make(map[proto.DiskID]*diskItem)
	totalWeight := atomic.LoadInt64(&s.weight) - defaultAllocTolerateBuff
	_totalWeight := totalWeight
	_count := count

	rackStorages := make([]*rackAllocator, 0, len(s.rackStorages))
	for _, rackStg := range s.rackStorages {
		rackStorages = append(rackStorages, rackStg)
	}

	duplicatedCount := 0
	randNum := int64(0)
	idx := 0

RETRY:
	if _totalWeight > 0 {
		randNum = rand.Int63n(_totalWeight)
	} else {
		randNum = 0
	}
	for i := idx; i < rackNum; i++ {
		rackStorage := rackStorages[i]
		rack := rackStorage.rack
		weight := atomic.LoadInt64(&rackStorage.weight)
		if weight > 0 && weight >= randNum && chosenRacksNum[rack] <= duplicatedCount &&
			(s.diffHost && len(rackStorage.nodeStorages) > chosenRacksNum[rack]) {
			allocNum := 1
			if _, ok := chosenRacksNum[rack]; ok {
				// retry with same rack, add all rest num into chosenRacksNum
				allocNum = len(rackStorage.nodeStorages) - chosenRacksNum[rack]
				chosenRacksNum[rack] += allocNum
			} else {
				chosenRacks = append(chosenRacks, rack)
				chosenRacksNum[rack] = allocNum
			}
			rackStorages[idx], rackStorages[i] = rackStorages[i], rackStorages[idx]
			idx += 1
			if duplicatedCount <= 0 {
				_totalWeight -= weight
			}
			_count -= allocNum
			goto RETRY
		}
		randNum -= weight
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

	// shuffle chosen racks, [0-count) will range by rack free item weight
	// [count, total) will be shuffle by random, ensure allocation more evenly
	total := len(chosenRacks)
	if total > count {
		for i := count; i < total; i++ {
			rand.Shuffle(total-count, func(i, j int) {
				chosenRacks[i+count], chosenRacks[j+count] = chosenRacks[j+count], chosenRacks[i+count]
			})
		}
	}

	// alloc item from rack's nodeStorages
	_count = count
	for _, rack := range chosenRacks {
		num := chosenRacksNum[rack]
		if num > _count {
			num = _count
		}
		dataStorages, disks := s.allocFromNodeStorages(ctx, num, atomic.LoadInt64(&s.rackStorages[rack].weight), s.rackStorages[rack].nodeStorages, excludes)
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

// 1. copy rack's nodeAllocator pointer array
// 2. alloc from nodeAllocator array
// 3. the alloc result length may not equal to count if there is no enough space or something else
func (s *idcAllocator) allocFromNodeStorages(ctx context.Context, count int, totalWeight int64, srcNodeStorages []*nodeAllocator, excludes map[proto.DiskID]*diskItem) (chosenDataStorages map[*nodeAllocator]int, chosenDisks map[proto.DiskID]*diskItem) {
	span := trace.SpanFromContextSafe(ctx)
	excludeHosts := make(map[string]bool)
	chosenDisks = make(map[proto.DiskID]*diskItem)
	chosenDataStorages = make(map[*nodeAllocator]int)
	randNum := int64(0)

	for _, diskInfo := range excludes {
		diskInfo.lock.RLock()
		excludeHosts[diskInfo.info.Host] = true
		diskInfo.lock.RUnlock()
	}

	nodeStorages := make([]*nodeAllocator, 0, len(s.nodeStorages))
	nodeStorageNum := 0
	// build available nodeStorages, filter exclude host or disk
	for i := range srcNodeStorages {
		// not allow same host, then filter exclude host
		if s.diffHost && excludeHosts[srcNodeStorages[i].host] {
			weight := atomic.LoadInt64(&srcNodeStorages[i].weight)
			totalWeight -= weight
			continue
		}
		nodeStorages = append(nodeStorages, srcNodeStorages[i])
		// allow same host, then exclude target disk. it's quite slowly but alright in test env which enable same host alloc
		if !s.diffHost && len(excludes) > 0 {
			weight := atomic.LoadInt64(&srcNodeStorages[i].weight)
			newDisks := make([]*diskItem, 0, len(srcNodeStorages[i].disks))
			for _, disk := range srcNodeStorages[i].disks {
				if _, ok := excludes[disk.diskID]; ok {
					diskWeight := disk.weight()
					totalWeight -= diskWeight
					weight -= diskWeight
					continue
				}
				newDisks = append(newDisks, disk)
			}
			nodeStorages[nodeStorageNum] = &nodeAllocator{
				host:   srcNodeStorages[i].host,
				weight: weight,
				disks:  newDisks,
			}
		}
		nodeStorageNum += 1
	}
	span.Debugf("total nodeStorages num: %d, excludes host: %v, excludes disk: %v", nodeStorageNum, excludeHosts, excludes)
	// no available data node after exclude, then return
	if nodeStorageNum == 0 {
		return
	}
	// no available item after exclude, then return
	if totalWeight <= 0 {
		return
	}

	chosenIdx := 0
	retryTimes := 0
	maxRetryTimes := defaultRetryTimes
	// maxRetry times will equal to count when nodeStorageNum less than target count
	if nodeStorageNum < count {
		maxRetryTimes = count
	}
	_totalWeight := totalWeight

RETRY:
	for count > 0 {
		// generate randNum every chosen
		if _totalWeight > 0 {
			randNum = rand.Int63n(_totalWeight)
		} else {
			randNum = 0
		}
		for i := chosenIdx; i < nodeStorageNum; i++ {
			weight := atomic.LoadInt64(&nodeStorages[i].weight)
			span.Debugf("total free item: %d, node(%s) free item: %d, randNum: %d", _totalWeight, nodeStorages[i].host, weight, randNum)
			if weight >= randNum {
				if selectedDisk := nodeStorages[i].allocDisk(ctx, chosenDisks); selectedDisk != nil {
					chosenDisks[selectedDisk.diskID] = selectedDisk
					chosenDataStorages[nodeStorages[i]] += 1
					nodeStorages[chosenIdx], nodeStorages[i] = nodeStorages[i], nodeStorages[chosenIdx]
					_totalWeight -= weight
					count -= 1
					chosenIdx += 1
					goto RETRY
				}
			}
			randNum -= weight
		}
		// go to the end of all data nodes, then check if retry when diffHost is false
		if !s.diffHost && retryTimes < maxRetryTimes {
			span.Infof("%s retry choose with same host", s.idc)
			retryTimes += 1
			// reset chosenIdx and _totalWeight when retry same host
			chosenIdx = 0
			_totalWeight = totalWeight
			goto RETRY
		}
		return
	}
	return
}
