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

package loadutil

import (
	"errors"
	"fmt"
	"time"

	"github.com/shirou/gopsutil/disk"
)

func getMatchCount(lhs string, rhs string) int {
	count := len(lhs)
	if count > len(rhs) {
		count = len(rhs)
	}
	for i := 0; i < count; i++ {
		if lhs[i] != rhs[i] {
			return i + 1
		}
	}
	return count
}

func GetMatchParation(path string) (*disk.PartitionStat, error) {
	partitons, err := disk.Partitions(true)
	if err != nil {
		return nil, err
	}
	maxMatch := 0
	matchParation := disk.PartitionStat{}
	for _, partition := range partitons {
		match := getMatchCount(path, partition.Mountpoint)
		if match == len(partition.Mountpoint) && match > maxMatch {
			matchParation = partition
		}
	}
	return &matchParation, nil
}

var (
	ErrInvalidDiskPartition = errors.New("invalid disk partiton")
	ErrFailedToGetIoCounter = errors.New("failed to get io counter")
)

func getDeviceNameFromPartition(partition *disk.PartitionStat) (string, error) {
	var name string
	if n, err := fmt.Sscanf(partition.Device, "/dev/%s", &name); n != 1 || err != nil {
		return "", ErrInvalidDiskPartition
	}
	return name, nil
}

func GetIoCounter(partition *disk.PartitionStat) (*disk.IOCountersStat, error) {
	name, err := getDeviceNameFromPartition(partition)
	if err != nil {
		return nil, err
	}
	counters, err := disk.IOCounters(name)
	if err != nil {
		return nil, err
	}
	counter, exist := counters[name]
	if !exist {
		return nil, ErrFailedToGetIoCounter
	}
	return &counter, nil
}

type DiskIoSampleItem struct {
	time      time.Time
	ioCounter *disk.IOCountersStat
}

func getDiskIoSampleItem(partition *disk.PartitionStat) (*DiskIoSampleItem, error) {
	ioCounter, err := GetIoCounter(partition)
	if err != nil {
		return nil, err
	}
	time := time.Now()
	return &DiskIoSampleItem{
		time:      time,
		ioCounter: ioCounter,
	}, nil
}

func getReadFlow(first *DiskIoSampleItem, second *DiskIoSampleItem) uint64 {
	t := second.time.Sub(first.time)
	ms := uint64(t.Milliseconds())
	bytes := second.ioCounter.ReadBytes - first.ioCounter.ReadBytes
	return bytes * 1000 / ms
}

func getWriteFlow(first *DiskIoSampleItem, second *DiskIoSampleItem) uint64 {
	t := second.time.Sub(first.time)
	ms := uint64(t.Milliseconds())
	bytes := second.ioCounter.WriteBytes - first.ioCounter.WriteBytes
	return bytes * 1000 / ms
}

func getIoCount(first *DiskIoSampleItem, second *DiskIoSampleItem) uint64 {
	count := second.ioCounter.ReadCount - first.ioCounter.ReadCount + second.ioCounter.WriteCount - first.ioCounter.WriteCount
	return count
}

func getTotalReadWaitTime(first *DiskIoSampleItem, second *DiskIoSampleItem) uint64 {
	count := second.ioCounter.ReadTime - first.ioCounter.ReadTime
	return count
}

func getTotalWriteWaitTime(first *DiskIoSampleItem, second *DiskIoSampleItem) uint64 {
	count := second.ioCounter.WriteTime - first.ioCounter.WriteTime
	return count
}

func getIoTotalWaitTime(first *DiskIoSampleItem, second *DiskIoSampleItem) uint64 {
	count := second.ioCounter.IoTime - first.ioCounter.IoTime
	return count
}

func getIoTotalWeightedWaitTime(first *DiskIoSampleItem, second *DiskIoSampleItem) uint64 {
	count := second.ioCounter.WeightedIO - first.ioCounter.WeightedIO
	return count
}

type DiskIoSample struct {
	partition  *disk.PartitionStat
	firstItem  *DiskIoSampleItem
	secondItem *DiskIoSampleItem
}

func (sample *DiskIoSample) GetReadCount() uint64 {
	return sample.secondItem.ioCounter.ReadCount - sample.firstItem.ioCounter.ReadCount
}

func (sample *DiskIoSample) GetReadFlow() uint64 {
	return getReadFlow(sample.firstItem, sample.secondItem)
}

func (sample *DiskIoSample) GetReadBytes() uint64 {
	return sample.secondItem.ioCounter.ReadBytes - sample.firstItem.ioCounter.ReadBytes
}

func (sample *DiskIoSample) GetReadTotalWaitTime() time.Duration {
	return time.Duration(getTotalReadWaitTime(sample.firstItem, sample.secondItem)) * time.Millisecond
}

func (sample *DiskIoSample) GetReadAvgWaitTime() time.Duration {
	if sample.GetReadCount() == 0 {
		return 0
	}
	return sample.GetReadTotalWaitTime() / time.Duration(sample.GetReadCount())
}

func (sample *DiskIoSample) GetMergedReadCount() uint64 {
	return sample.secondItem.ioCounter.MergedReadCount - sample.firstItem.ioCounter.MergedReadCount
}

func (sample *DiskIoSample) GetWriteCount() uint64 {
	return sample.secondItem.ioCounter.WriteCount - sample.firstItem.ioCounter.WriteCount
}

func (sample *DiskIoSample) GetWriteFlow() uint64 {
	return getWriteFlow(sample.firstItem, sample.secondItem)
}

func (sample *DiskIoSample) GetWriteBytes() uint64 {
	return sample.secondItem.ioCounter.WriteBytes - sample.firstItem.ioCounter.WriteBytes
}

func (sample *DiskIoSample) GetWriteTotalWaitTime() time.Duration {
	return time.Duration(getTotalWriteWaitTime(sample.firstItem, sample.secondItem)) * time.Millisecond
}

func (sample *DiskIoSample) GetWriteAvgWaitTime() time.Duration {
	if sample.GetWriteCount() == 0 {
		return 0
	}
	return sample.GetWriteTotalWaitTime() / time.Duration(sample.GetWriteCount())
}

func (sample *DiskIoSample) GetMergedWriteCount() uint64 {
	return sample.secondItem.ioCounter.MergedWriteCount - sample.firstItem.ioCounter.MergedWriteCount
}

func (sample *DiskIoSample) GetIoCount() uint64 {
	return getIoCount(sample.firstItem, sample.secondItem)
}

func (sample *DiskIoSample) GetIoTotalWaitTime() time.Duration {
	return time.Duration(getIoTotalWaitTime(sample.firstItem, sample.secondItem)) * time.Millisecond
}

func (sample *DiskIoSample) GetIoAvgWaitTime() time.Duration {
	if sample.GetIoCount() == 0 {
		return 0
	}
	return sample.GetIoTotalWaitTime() / time.Duration(sample.GetIoCount())
}

func (sample *DiskIoSample) GetWeightedTotalWaitTime() time.Duration {
	return time.Duration(getIoTotalWeightedWaitTime(sample.firstItem, sample.secondItem)) * time.Millisecond
}

func (sample *DiskIoSample) GetWeightedAvgWaitTime() time.Duration {
	if sample.GetIoCount() == 0 {
		return 0
	}
	return sample.GetWeightedTotalWaitTime() / time.Duration(sample.GetIoCount())
}

func (sample *DiskIoSample) GetIopsInProgress() uint64 {
	return sample.secondItem.ioCounter.IopsInProgress
}

func (sample *DiskIoSample) GetIoUtilPercent() float64 {
	return float64(sample.GetIoTotalWaitTime()) / float64(sample.GetSampleDuration()) * 100
}

func (sample *DiskIoSample) GetSampleDuration() time.Duration {
	return sample.secondItem.time.Sub(sample.firstItem.time)
}

func (sample *DiskIoSample) GetPartition() *disk.PartitionStat {
	return sample.partition
}

func GetDiskIoSample(partition *disk.PartitionStat, duration time.Duration) (DiskIoSample, error) {
	var sample DiskIoSample
	first, err := getDiskIoSampleItem(partition)
	if err != nil {
		return sample, err
	}
	time.Sleep(duration)
	second, err := getDiskIoSampleItem(partition)
	if err != nil {
		return sample, err
	}
	sample.partition = partition
	sample.firstItem = first
	sample.secondItem = second
	return sample, nil
}

func GetDisksIoSample(partitions []*disk.PartitionStat, duration time.Duration) (map[string]DiskIoSample, error) {
	count := len(partitions)
	samples := make(map[string]DiskIoSample)
	if count != 0 {
		firstItems := make([]*DiskIoSampleItem, 0, count)
		for i := 0; i < count; i++ {
			first, err := getDiskIoSampleItem(partitions[i])
			if err != nil {
				return nil, err
			}
			firstItems = append(firstItems, first)
		}
		time.Sleep(duration)
		for i := 0; i < count; i++ {
			var sample DiskIoSample
			first := firstItems[i]
			second, err := getDiskIoSampleItem(partitions[i])
			if err != nil {
				return nil, err
			}
			sample.partition = partitions[i]
			sample.firstItem = first
			sample.secondItem = second
			samples[partitions[i].Device] = sample
		}
	}
	return samples, nil
}
