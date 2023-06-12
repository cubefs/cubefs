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

package loadutil_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/loadutil"
	"github.com/shirou/gopsutil/disk"
)

func TestGetPartition(t *testing.T) {
	partition, err := loadutil.GetMatchParation("/")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(partition.String())
}

func TestGetIoCounter(t *testing.T) {
	partition, err := loadutil.GetMatchParation("/")
	if err != nil {
		t.Error(err)
		return
	}
	if !strings.HasPrefix(partition.Device, "/dev/") {
		t.Log("Unsupport device")
		return
	}
	counter, err := loadutil.GetIoCounter(partition)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	data, _ := json.MarshalIndent(counter, "", "  ")
	t.Log(string(data))
}

func TestSample(t *testing.T) {
	partition, err := loadutil.GetMatchParation("/")
	if err != nil {
		t.Errorf("failed to get disk partition %v", err.Error())
	}
	if !strings.HasPrefix(partition.Device, "/dev/") {
		t.Log("Unsupport device")
		return
	}
	sample, err := loadutil.GetDiskIoSample(partition, time.Second)
	if err != nil {
		t.Errorf("failed to get disk sample %v", err.Error())
	}
	t.Logf("ReadCount:\t%v\n", sample.GetReadCount())
	t.Logf("ReadFlow:\t%v\n", sample.GetReadFlow())
	t.Logf("ReadTotalWaitTime:\t%v\n", sample.GetReadTotalWaitTime())
	t.Logf("ReadAvgWaitTime:\t%v\n", sample.GetReadAvgWaitTime())
	t.Logf("MergedReadCount:\t%v\n", sample.GetMergedReadCount())

	t.Logf("WriteCount:\t%v\n", sample.GetWriteCount())
	t.Logf("WriteFlow:\t%v\n", sample.GetWriteFlow())
	t.Logf("WriteTotalWaitTime:\t%v\n", sample.GetWriteTotalWaitTime())
	t.Logf("WriteAvgWaitTime:\t%v\n", sample.GetWriteAvgWaitTime())
	t.Logf("MergedWriteCount:\t%v\n", sample.GetMergedWriteCount())

	t.Logf("IoCount:\t%v\n", sample.GetIoCount())
	t.Logf("IoTotalWait:\t%v\n", sample.GetIoTotalWaitTime())
	t.Logf("IoAvgWaitTime:\t%v\n", sample.GetIoAvgWaitTime())
	t.Logf("WeightedIoAvgWaitTime:\t%v\n", sample.GetWeightedAvgWaitTime())
	t.Logf("IoInProgress:\t%v\n", sample.GetIopsInProgress())
	t.Logf("IoUtilPercent:\t%v%%\n", sample.GetIoUtilPercent())
	t.Logf("Sample Duration:\t%v\n", sample.GetSampleDuration())
}

func TestMultiSample(t *testing.T) {
	partition, err := loadutil.GetMatchParation("/")
	if err != nil {
		t.Errorf("failed to get disk partition %v", err.Error())
	}
	if !strings.HasPrefix(partition.Device, "/dev/") {
		t.Log("Unsupport device")
		return
	}
	partitions := make([]*disk.PartitionStat, 1)
	partitions[0] = partition
	samples, err := loadutil.GetDisksIoSample(partitions, time.Second)
	if err != nil {
		t.Errorf("failed to get disk sample %v", err.Error())
	}
	sample := samples[partition.Device]
	t.Logf("ReadCount:\t%v\n", sample.GetReadCount())
	t.Logf("ReadFlow:\t%v\n", sample.GetReadFlow())
	t.Logf("ReadTotalWaitTime:\t%v\n", sample.GetReadTotalWaitTime())
	t.Logf("ReadAvgWaitTime:\t%v\n", sample.GetReadAvgWaitTime())
	t.Logf("MergedReadCount:\t%v\n", sample.GetMergedReadCount())

	t.Logf("WriteCount:\t%v\n", sample.GetWriteCount())
	t.Logf("WriteFlow:\t%v\n", sample.GetWriteFlow())
	t.Logf("WriteTotalWaitTime:\t%v\n", sample.GetWriteTotalWaitTime())
	t.Logf("WriteAvgWaitTime:\t%v\n", sample.GetWriteAvgWaitTime())
	t.Logf("MergedWriteCount:\t%v\n", sample.GetMergedWriteCount())

	t.Logf("IoCount:\t%v\n", sample.GetIoCount())
	t.Logf("IoTotalWait:\t%v\n", sample.GetIoTotalWaitTime())
	t.Logf("IoAvgWaitTime:\t%v\n", sample.GetIoAvgWaitTime())
	t.Logf("WeightedIoAvgWaitTime:\t%v\n", sample.GetWeightedAvgWaitTime())
	t.Logf("IoInProgress:\t%v\n", sample.GetIopsInProgress())
	t.Logf("IoUtilPercent:\t%v%%\n", sample.GetIoUtilPercent())
	t.Logf("Sample Duration:\t%v\n", sample.GetSampleDuration())
}
