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

package volumemgr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	OperTypeCreateVolume int32 = iota + 1
	OperTypeAllocVolume
	OperTypeRetainVolume
	OperTypeChangeVolumeStatus
	OperTypeUpdateVolumeUnit
	OperTypeChunkReport
	OperTypeChunkSetCompact
	OperTypeVolumeUnitSetWritable
	OperTypeAllocVolumeUnit
	OperTypeDeleteTask
	OperTypeExpireVolume
	OperTypeAdminUpdateVolume
	OperTypeAdminUpdateVolumeUnit
	OperTypeInitCreateVolume
	OperTypeIncreaseVolumeUnitsEpoch
)

type CreateVolumeCtx struct {
	VuInfos []*clustermgr.VolumeUnitInfo `json:"vu_infos"`
	VolInfo clustermgr.VolumeInfoBase    `json:"vol_info"`
	Vid     proto.Vid                    `json:"vid"`
}

func (c *CreateVolumeCtx) Encode() (data []byte, err error) {
	data, err = json.Marshal(c)
	return
}

func (c *CreateVolumeCtx) Decode(raw []byte) error {
	return json.Unmarshal(raw, c)
}

func (c *CreateVolumeCtx) ToVolume(ctx context.Context) (*volume, error) {
	vUnits := make([]*volumeUnit, len(c.VuInfos))
	for i, vuInfo := range c.VuInfos {
		vUnits[i] = &volumeUnit{
			vuidPrefix: vuInfo.Vuid.VuidPrefix(),
			epoch:      vuInfo.Vuid.Epoch(),
			nextEpoch:  vuInfo.Vuid.Epoch(),
			vuInfo:     vuInfo,
		}
	}
	vol := &volume{
		vid:         c.Vid,
		vUnits:      vUnits,
		volInfoBase: c.VolInfo,
	}
	return vol, nil
}

type AllocVolumeCtx struct {
	Vids               []proto.Vid `json:"vids"`
	Host               string      `json:"host"`
	ExpireTime         int64       `json:"expire_time"`
	PendingAllocVolKey interface{} `json:"pending_alloc_vol_key"`
}

type ChangeVolStatusCtx struct {
	Vid      proto.Vid           `json:"vid"`
	TaskID   string              `json:"task_id"`
	TaskType base.VolumeTaskType `json:"type"`
}

type allocVolumeUnitCtx struct {
	Vuid           proto.Vuid  `json:"vuid"`
	NextEpoch      uint32      `json:"next_epoch"`
	PendingVuidKey interface{} `json:"pending_vuid_key"`
}

type DeleteTaskCtx struct {
	Vid      proto.Vid           `json:"vid"`
	TaskType base.VolumeTaskType `json:"type"`
	TaskId   string              `json:"task_id"`
}

func (v *VolumeMgr) LoadData(ctx context.Context) error {
	if err := v.loadVolume(ctx); err != nil {
		return errors.Info(err, "load volume failed").Detail(err)
	}
	if err := v.reloadTasks(); err != nil {
		return errors.Info(err, "reload task failed").Detail(err)
	}
	return nil
}

func (v *VolumeMgr) GetModuleName() string {
	return v.module
}

func (v *VolumeMgr) SetModuleName(module string) {
	v.module = module
}

func (v *VolumeMgr) Apply(ctx context.Context, operTypes []int32, datas [][]byte, contexts []base.ProposeContext) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(operTypes))
	errs := make([]error, len(operTypes))

	for i, t := range operTypes {
		idx := i
		taskSpan, taskCtx := trace.StartSpanFromContextWithTraceID(ctx, "", contexts[idx].ReqID)
		switch t {
		case OperTypeAllocVolume:
			// alloc volume will distribute into single volume alloc request,
			// and every single volume alloc request will be run on the same task idx goroutine to avoid concurrent problem
			var (
				args             = &AllocVolumeCtx{}
				allocVolumeInfos []clustermgr.AllocVolumeInfo
			)
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}

			v.applyTaskPool.Run(1, func() {
				defer wg.Done()
				for _, vid := range args.Vids {
					ret, err := v.applyAllocVolume(taskCtx, vid, args.Host, args.ExpireTime)
					if err != nil {
						errs[idx] = errors.Info(err, "apply alloc volume failed, args: ", args).Detail(err)
						return
					}
					if ret.Vid > proto.InvalidVid {
						allocVolumeInfos = append(allocVolumeInfos, ret)
					}
				}
				// return allocated volumes
				if _, ok := v.pendingEntries.Load(args.PendingAllocVolKey); ok {
					v.pendingEntries.Store(args.PendingAllocVolKey, &clustermgr.AllocatedVolumeInfos{AllocVolumeInfos: allocVolumeInfos})
				}
			})

		case OperTypeCreateVolume:
			args := &CreateVolumeCtx{}
			err := args.Decode(datas[idx])
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			volume, err := args.ToVolume(ctx)
			if err != nil {
				errs[idx] = errors.Info(err, "transform create volume context into volume failed, create volume context: ", args).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(v.getTaskIdx(volume.vid), func() {
				if err = v.applyCreateVolume(taskCtx, volume); err != nil {
					errs[idx] = errors.Info(err, "apply create volume failed, volume: ", volume).Detail(err)
				}
				wg.Done()
			})

		case OperTypeChangeVolumeStatus:
			args := &ChangeVolStatusCtx{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(v.getTaskIdx(args.Vid), func() {
				if err = v.applyVolumeTask(taskCtx, args.Vid, args.TaskID, args.TaskType); err != nil {
					errs[idx] = errors.Info(err, "apply change volume status failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeRetainVolume:
			args := &clustermgr.RetainVolumes{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// retain volume run on random task pool goroutine, it's safe when apply concurrently
			v.applyTaskPool.Run(1, func() {
				if err = v.applyRetainVolume(taskCtx, args.RetainVolTokens); err != nil {
					errs[idx] = errors.Info(err, "apply retain volume failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeUpdateVolumeUnit:
			args := &clustermgr.UpdateVolumeArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(v.getTaskIdx(args.NewVuid.Vid()), func() {
				if err = v.applyUpdateVolumeUnit(taskCtx, args.NewVuid, args.NewDiskID); err != nil {
					errs[idx] = errors.Info(err, "apply update volume unit failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeChunkReport:
			start := time.Now()
			args := &clustermgr.ReportChunkArgs{}
			err := args.Decode(bytes.NewReader(datas[idx]))
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			unmarshalCost := time.Since(start) / time.Microsecond
			start = time.Now()
			// chunk report only modify volume unit and volume free/total/used size, it's safe when apply concurrently
			v.applyTaskPool.Run(rand.Intn(int(v.ApplyConcurrency)), func() {
				if err = v.applyChunkReport(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply chunk report failed, args: ", args).Detail(err)
				}
				wg.Done()
				taskSpan.Debugf("receive apply msg, unmarshal cost: %dus, apply chunk report cost: %dus",
					unmarshalCost, time.Since(start)/time.Microsecond)
			})

		case OperTypeChunkSetCompact:
			args := &clustermgr.SetCompactChunkArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(v.getTaskIdx(args.Vuid.Vid()), func() {
				if err = v.applyChunkSetCompact(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply chunk set compact failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeVolumeUnitSetWritable:
			args := make([]proto.VuidPrefix, 0)
			err := json.Unmarshal(datas[idx], &args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			// disk writable change only modify volume's health, it's safe when apply concurrently
			v.applyTaskPool.Run(1, func() {
				if err = v.applyDiskWritableChange(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply disk writable change failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeAllocVolumeUnit:
			args := &allocVolumeUnitCtx{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(v.getTaskIdx(args.Vuid.Vid()), func() {
				if err = v.applyAllocVolumeUnit(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply all volume unit failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeDeleteTask:
			var args DeleteTaskCtx
			err := json.Unmarshal(datas[idx], &args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(v.getTaskIdx(args.Vid), func() {
				if err = v.applyRemoveVolumeTask(taskCtx, args.Vid, args.TaskId, args.TaskType); err != nil {
					errs[idx] = errors.Info(err, "apply remove volume task failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeExpireVolume:
			args := make([]proto.Vid, 0)
			err := json.Unmarshal(datas[idx], &args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(1, func() {
				if err = v.applyExpireVolume(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply expire volume failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeAdminUpdateVolume:
			args := &clustermgr.VolumeInfoBase{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(1, func() {
				if err = v.applyAdminUpdateVolume(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply admin update volume failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeAdminUpdateVolumeUnit:
			args := &clustermgr.AdminUpdateUnitArgs{}
			err := json.Unmarshal(datas[idx], args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(1, func() {
				if err = v.applyAdminUpdateVolumeUnit(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply admin update volume unit failed, args: ", args).Detail(err)
				}
				wg.Done()
			})

		case OperTypeInitCreateVolume:
			args := &CreateVolumeCtx{}
			err := args.Decode(datas[idx])
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			volume, err := args.ToVolume(ctx)
			if err != nil {
				errs[idx] = errors.Info(err, "transform create volume context into volume failed, create volume context: ", args).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(v.getTaskIdx(volume.vid), func() {
				if err = v.applyInitCreateVolume(taskCtx, volume); err != nil {
					errs[idx] = errors.Info(err, "apply initial create volume failed, volume: ", volume).Detail(err)
				}
				wg.Done()
			})

		case OperTypeIncreaseVolumeUnitsEpoch:
			args := make([]*volumedb.VolumeUnitRecord, 0)
			err := json.Unmarshal(datas[idx], &args)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			v.applyTaskPool.Run(v.getTaskIdx(args[0].VuidPrefix.Vid()), func() {
				if err = v.applyIncreaseVolumeUnitsEpoch(taskCtx, args); err != nil {
					errs[idx] = errors.Info(err, "apply increase volume units epoch failed").Detail(err)
				}
				wg.Done()
			})

		default:
			errs[idx] = errors.New("unsupported operation")
			wg.Done()
		}
	}

	wg.Wait()
	failedCount := 0
	for i := range errs {
		if errs[i] != nil {
			failedCount += 1
			span.Errorf("operation type: %d, apply failed => ", operTypes[i], errors.Detail(errs[i]))
		}
	}
	if failedCount > 0 {
		return errors.New(fmt.Sprintf("batch apply failed, failed count: %d", failedCount))
	}

	return nil
}

// Flush will flush memory data into persistent storage
func (v *VolumeMgr) Flush(ctx context.Context) error {
	if time.Since(v.lastFlushTime) < time.Duration(v.FlushIntervalS)*time.Second {
		return nil
	}
	v.lastFlushTime = time.Now()

	var (
		retErr error
		span   = trace.SpanFromContextSafe(ctx)
	)

	dirty := v.dirty.Load().(*shardedVolumes)
	v.dirty.Store(newShardedVolumes(v.VolumeSliceMapNum))

	dirty.rangeVol(func(vol *volume) (err error) {
		select {
		case <-ctx.Done():
			return errors.New("cancel ctx")
		default:
		}

		vol.lock.RLock()
		err = v.volumeTbl.PutVolumeAndVolumeUnit([]*volumedb.VolumeRecord{vol.ToRecord()}, [][]*volumedb.VolumeUnitRecord{volumeUnitsToVolumeUnitRecords(vol.vUnits)})
		vol.lock.RUnlock()
		retErr = err
		return
	})

	if retErr != nil {
		span.Error("volumeMgr flush put volume units failed, err: ", retErr)
		return retErr
	}
	return nil
}

// Switch manager work when leader change
func (v *VolumeMgr) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
}

func (v *VolumeMgr) getTaskIdx(vid proto.Vid) int {
	return 1
	// return int(uint32(vid) % v.ApplyConcurrency)
}
