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

package flow

import (
	"github.com/cubefs/cubefs/blobstore/common/iostat"
)

type DiskViewer struct {
	viewers []iostat.IOViewer
}

func NewDiskViewer(ioFlow *IOFlowStat) iostat.IOViewer {
	dv := &DiskViewer{}
	for k := range ioFlow {
		if ioFlow[k] == nil {
			continue
		}
		iov := iostat.NewIOViewer(ioFlow[k].Stat, 0)
		dv.viewers = append(dv.viewers, iov)
	}
	return dv
}

func (d *DiskViewer) ReadStat() *iostat.StatData {
	sd := &iostat.StatData{}
	for _, v := range d.viewers {
		sd.Iops += v.ReadStat().Iops
		sd.Bps += v.ReadStat().Bps
	}
	return sd
}

func (d *DiskViewer) WriteStat() *iostat.StatData {
	sd := &iostat.StatData{}
	for _, v := range d.viewers {
		sd.Iops += v.WriteStat().Iops
		sd.Bps += v.WriteStat().Bps
	}
	return sd
}

func (d *DiskViewer) Update() {
}

func (d *DiskViewer) Close() {
}
