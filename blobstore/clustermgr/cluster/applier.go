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
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	OperTypeAddDisk = iota + 1
	OperTypeSetDiskStatus
	OperTypeDroppingDisk
	OperTypeDroppedDisk
	OperTypeHeartbeatDiskInfo
	OperTypeSwitchReadonly
	OperTypeAdminUpdateDisk
	OperTypeAddNode
	OperTypeDroppingNode
	OperTypeDroppedNode
)

const synchronizedDiskID = 1

func (d *manager) SetModuleName(module string) {
	d.module = module
}

// Flush will flush disks heartbeat info into rocksdb
func (d *manager) Flush(ctx context.Context) error {
	if time.Since(d.lastFlushTime) < time.Duration(d.cfg.FlushIntervalS)*time.Second {
		return nil
	}
	d.lastFlushTime = time.Now()

	d.metaLock.RLock()
	// fast copy all diskItem pointer
	disks := make([]*diskItem, 0, len(d.allDisks))
	for _, disk := range d.allDisks {
		disks = append(disks, disk)
	}
	d.metaLock.RUnlock()

	for _, disk := range disks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		disk.lock.RLock()
		if !disk.dropping && disk.info.Status == proto.DiskStatusNormal && time.Since(disk.expireTime) <= 0 {
			err := d.persistentHandler.updateDiskNoLocked(disk)
			if err != nil {
				disk.lock.RUnlock()
				return err
			}
		}
		disk.lock.RUnlock()
	}

	return nil
}

func (d *manager) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
	// Do nothing.
}

func (d *manager) getTaskIdx(diskID proto.DiskID) int {
	return int(uint32(diskID) % d.cfg.ApplyConcurrency)
}
