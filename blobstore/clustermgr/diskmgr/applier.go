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
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	OperTypeAddDisk = iota + 1
	OperTypeSetDiskStatus
	OperTypeDroppingDisk
	OperTypeDroppedDisk
	OperTypeHeartbeatDiskInfo
	OperTypeSwitchReadonly
	OperTypeAdminUpdateDisk
)

func (d *DiskMgr) LoadData(ctx context.Context) error {
	diskDBs, err := d.diskTbl.GetAllDisks()
	if err != nil {
		return errors.Info(err, "get all disks failed").Detail(err)
	}
	droppingDiskDBs, err := d.droppedDiskTbl.GetAllDroppingDisk()
	if err != nil {
		return errors.Info(err, "get dropping disks failed").Detail(err)
	}
	droppingDisks := make(map[proto.DiskID]bool)
	for _, diskID := range droppingDiskDBs {
		droppingDisks[diskID] = true
	}

	allDisks := make(map[proto.DiskID]*diskItem)
	for _, disk := range diskDBs {
		info := diskInfoRecordToDiskInfo(disk)
		di := &diskItem{
			diskID: info.DiskID,
			info:   info,
			// bug fix: do not initial disk expire time, or may cause volume health change when start volume manager
			// lastExpireTime: time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second),
			// expireTime:     time.Now().Add(time.Duration(d.HeartbeatExpireIntervalS) * time.Second),
		}
		if droppingDisks[di.diskID] {
			di.dropping = true
		}
		allDisks[info.DiskID] = di
		if di.needFilter() {
			d.hostPathFilter.Store(di.genFilterKey(), 1)
		}
	}
	d.allDisks = allDisks
	return nil
}

func (d *DiskMgr) GetModuleName() string {
	d.metaLock.RLock()
	defer d.metaLock.RUnlock()
	return d.module
}

func (d *DiskMgr) SetModuleName(module string) {
	d.metaLock.RLock()
	if d.module != "" {
		d.metaLock.RUnlock()
		return
	}
	d.metaLock.RUnlock()

	d.metaLock.Lock()
	defer d.metaLock.Unlock()
	if d.module != "" {
		return
	}
	d.module = module
}

func (d *DiskMgr) Apply(ctx context.Context, operTypes []int32, datas [][]byte, contexts []base.ProposeContext) error {
	span := trace.SpanFromContextSafe(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(operTypes))
	errs := make([]error, len(operTypes))

	for i, t := range operTypes {
		idx := i
		_, taskCtx := trace.StartSpanFromContextWithTraceID(ctx, "", contexts[idx].ReqID)

		switch t {
		case OperTypeAddDisk:
			diskInfo := &blobnode.DiskInfo{}
			err := json.Unmarshal(datas[idx], diskInfo)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			d.taskPool.Run(d.getTaskIdx(diskInfo.DiskID), func() {
				// don't return error if disk already exist
				err = d.addDisk(taskCtx, *diskInfo)
				if err != nil && err != ErrDiskExist {
					errs[idx] = err
				}
				wg.Done()
			})
		case OperTypeSetDiskStatus:
			setStatusArgs := &clustermgr.DiskSetArgs{}
			err := json.Unmarshal(datas[idx], setStatusArgs)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			d.taskPool.Run(d.getTaskIdx(setStatusArgs.DiskID), func() {
				errs[idx] = d.SetStatus(taskCtx, setStatusArgs.DiskID, setStatusArgs.Status, true)
				wg.Done()
			})
		case OperTypeDroppingDisk:
			args := &clustermgr.DiskInfoArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			d.taskPool.Run(d.getTaskIdx(args.DiskID), func() {
				errs[idx] = d.droppingDisk(taskCtx, args.DiskID)
				wg.Done()
			})
		case OperTypeDroppedDisk:
			args := &clustermgr.DiskInfoArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			d.taskPool.Run(d.getTaskIdx(args.DiskID), func() {
				errs[idx] = d.droppedDisk(taskCtx, args.DiskID)
				wg.Done()
			})
		case OperTypeHeartbeatDiskInfo:
			args := &clustermgr.DisksHeartbeatArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// disk heartbeat has no necessary to run in single goroutine, so we just put it on random goroutine
			d.taskPool.Run(rand.Intn(int(d.ApplyConcurrency)), func() {
				errs[idx] = d.heartBeatDiskInfo(taskCtx, args.Disks)
				wg.Done()
			})
		case OperTypeSwitchReadonly:
			args := &clustermgr.DiskAccessArgs{}
			err := json.Unmarshal(datas[i], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			d.taskPool.Run(d.getTaskIdx(args.DiskID), func() {
				errs[idx] = d.SwitchReadonly(args.DiskID, args.Readonly)
				wg.Done()
			})
		case OperTypeAdminUpdateDisk:
			args := &blobnode.DiskInfo{}
			err := json.Unmarshal(datas[i], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			d.taskPool.Run(d.getTaskIdx(args.DiskID), func() {
				errs[idx] = d.adminUpdateDisk(ctx, args)
				wg.Done()
			})
		}
	}
	wg.Wait()
	failedCount := 0
	for i := range errs {
		if errs[i] != nil {
			failedCount += 1
			span.Error(fmt.Sprintf("operation type: %d, apply failed => ", operTypes[i]), errors.Detail(errs[i]))
		}
	}
	if failedCount > 0 {
		return errors.New(fmt.Sprintf("batch apply failed, failed count: %d", failedCount))
	}

	return nil
}

// Flush will flush disks heartbeat info into rocksdb
func (d *DiskMgr) Flush(ctx context.Context) error {
	if time.Since(d.lastFlushTime) < time.Duration(d.FlushIntervalS)*time.Second {
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
			err := d.diskTbl.UpdateDisk(disk.diskID, diskInfoToDiskInfoRecord(disk.info))
			if err != nil {
				disk.lock.RUnlock()
				return err
			}
		}
		disk.lock.RUnlock()
	}

	return nil
}

// DiskMgr do nothing when leader change
func (d *DiskMgr) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
}

func (d *DiskMgr) getTaskIdx(diskID proto.DiskID) int {
	return int(uint32(diskID) % d.ApplyConcurrency)
}
