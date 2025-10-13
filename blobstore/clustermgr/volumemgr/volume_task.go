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
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/volumedb"
	apierrs "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

var errNotLeader = errors.New("this node is not leader")

func (m *VolumeMgr) reloadTasks() error {
	err := m.volumeTbl.ListTaskRecords(func(rec *volumedb.VolumeTaskRecord) bool {
		m.taskMgr.AddTask(newVolTask(rec.Vid, rec.TaskType, rec.TaskId, m.setVolumeStatus))
		m.lastTaskIdMap.Store(rec.Vid, rec.TaskId)
		return true
	})
	return err
}

func (m *VolumeMgr) setVolumeStatus(task *volTask) error {
	var (
		retErr  error
		wg      sync.WaitGroup
		once    sync.Once
		diskIds []proto.DiskID
		vid     proto.Vid
		vuids   []proto.Vuid
	)
	if !m.raftServer.IsLeader() {
		return errNotLeader
	}
	vol := m.all.getVol(task.vid)
	vol.lock.RLock()
	for _, v := range vol.vUnits {
		diskIds = append(diskIds, v.vuInfo.DiskID)
		vuids = append(vuids, proto.EncodeVuid(v.vuidPrefix, v.epoch))
	}
	vid = vol.vid
	vol.lock.RUnlock()

	if task.context == nil {
		task.context = make([]byte, len(diskIds))
	}
	span, ctx := trace.StartSpanFromContext(context.Background(), "")
	span.Infof("execute task %s", task.String())
	for i := 0; i < len(diskIds); i++ {
		if task.context[i] != 0 {
			continue
		}
		diskInfo, err := m.diskMgr.GetDiskInfo(ctx, diskIds[i])
		if err != nil {
			span.Errorf("failed to get disk info [task=%s vid=%d index=%d vuid=%d diskId=%d err=%v]",
				task.String(), vid, i, vuids[i], diskIds[i], err)
			return err
		}
		if diskInfo.Status == proto.DiskStatusBroken {
			span.Infof("ignore broken disk: info [task=%s vid=%d index=%d vuid=%d diskId=%d err=%v]",
				task.String(), vid, i, vuids[i], diskIds[i], err)
			continue
		}
		wg.Add(1)
		// send msg to blobnode
		go func(i int, host string, diskID proto.DiskID) {
			var (
				e   error
				msg string
			)
			defer wg.Done()
			arg := blobnode.ChangeChunkStatusArgs{
				DiskID: diskID,
				Vuid:   vuids[i],
			}
			switch task.taskType {
			case base.VolumeTaskTypeLock:
				msg = "readonly"
				e = m.blobNodeClient.SetChunkReadonly(ctx, host, &arg)
			case base.VolumeTaskTypeUnlock:
				msg = "readwrite"
				e = m.blobNodeClient.SetChunkReadwrite(ctx, host, &arg)
				errCode := rpc.DetectStatusCode(e)
				if errCode == apierrs.CodeDiskNotFound || errCode == apierrs.CodeDiskBroken {
					isDroppingDisk, err := m.diskMgr.IsDroppingDisk(ctx, diskID)
					if err != nil {
						span.Errorf("check disk[%d] is dropping failed, [task=%s blobnode=%s vuid=%d] error: %s", diskID, task, host, vuids[i], err)
					} else if isDroppingDisk {
						span.Warnf("skip dropping disk[%d] when meet blobnode return disk not found error", diskID)
						e = nil
					}
				}
			case base.VolumeTaskTypeUnlockForce:
				msg = "readwrite"
				e = m.blobNodeClient.SetChunkReadwrite(ctx, host, &arg)
				if e != nil {
					span.Errorf("set chunk %s [task=%s blobnode=%s vuid=%d] error: %v", msg, task.String(), host, vuids[i], e)
					e = nil
				}
			default:
				log.Panicf("Unknown taskType(%d)", task.taskType)
			}
			if e == nil {
				span.Infof("set chunk %s [task=%s blobnode=%s vuid=%d] success", msg, task.String(), host, vuids[i])
				task.context[i] = 1
			} else {
				span.Errorf("set chunk %s [task=%s blobnode=%s vuid=%d] error: %v", msg, task.String(), host, vuids[i], e)
				once.Do(func() {
					retErr = e
				})
			}
		}(i, diskInfo.Host, diskInfo.DiskID)
	}
	wg.Wait()
	if retErr != nil {
		return retErr
	}
	// delete task
	if retErr = m.deleteTask(ctx, task); retErr != nil {
		span.Errorf("delete task %s error: %v", task.String(), retErr)
		return retErr
	}
	return nil
}

func (m *VolumeMgr) applyVolumeTask(ctx context.Context, args *ChangeVolStatusCtx) error {
	// get volume from cache
	span := trace.SpanFromContextSafe(ctx)
	vol := m.all.getVol(args.Vid)
	task := newVolTask(args.Vid, args.TaskType, args.TaskID, m.setVolumeStatus)
	span.Infof("create task %s", task.String())
	// set volume status=lock if t is base.VolumeTaskTypeLock
	var (
		err        error
		addNewTask bool
		taskRecord = &volumedb.VolumeTaskRecord{
			Vid:      args.Vid,
			TaskType: task.taskType,
			TaskId:   task.taskId,
		}
	)
	// double check volume epoch when apply (api concurrent request or raft log replay)
	err = vol.withRLocked(func() error {
		if vol.getEpoch() != args.Epoch {
			return apierrs.ErrVolumeEpochNotMatch
		}
		return nil
	})
	if err != nil {
		// return err by pendingEntries in commit case
		if _, ok := m.pendingEntries.Load(args.PendingErrKey); ok {
			m.pendingEntries.Store(args.PendingErrKey, err)
		}
		return nil
	}

	switch args.TaskType {
	case base.VolumeTaskTypeLock:
		err = vol.withLocked(func() error {
			if !vol.canLock() {
				span.Warnf("volume can't lock, status=%d", vol.getStatus())
				return nil
			}

			addNewTask = true
			// set volume status into lock, it'll call change volume status function
			vol.setStatus(ctx, proto.VolumeStatusLock)
			vol.increaseEpoch()
			rec := vol.ToRecord()
			// store task to db
			return m.volumeTbl.PutVolumeAndTask(rec, taskRecord)
		})
	case base.VolumeTaskTypeUnlock:
		err = vol.withLocked(func() error {
			if !vol.canUnlock() {
				span.Warnf("volume can't unlock, status=%d", vol.getStatus())
				return nil
			}

			addNewTask = true
			vol.setStatus(ctx, proto.VolumeStatusUnlocking)
			vol.increaseEpoch()
			rec := vol.ToRecord()
			// store task to db
			return m.volumeTbl.PutVolumeAndTask(rec, taskRecord)
		})
	case base.VolumeTaskTypeUnlockForce:
		err = vol.withLocked(func() error {
			if !vol.canUnlockForce() {
				span.Warnf("volume can't unlock force, status=%d", vol.getStatus())
				return nil
			}

			oldTask := m.taskMgr.GetTask(args.Vid)
			if oldTask != nil {
				if oldTask.taskType == args.TaskType {
					span.Warnf("volume already in unlocking force")
					return nil
				}
				// other task type exist, remove old task firstly
				if err = m.volumeTbl.DeleteTaskRecord(args.Vid); err != nil {
					span.Errorf("remove old task type task %s error: %v", task.String(), err)
					return err
				}
			}

			addNewTask = true
			vol.setStatus(ctx, proto.VolumeStatusUnlocking)
			vol.increaseEpoch()
			rec := vol.ToRecord()
			// store task to db
			return m.volumeTbl.PutVolumeAndTask(rec, taskRecord)
		})
	default:
		span.Panicf("Unknown task type(%d)", args.TaskType)
	}
	if err != nil {
		span.Errorf("persist task %s error: %v", task.String(), err)
		return err
	}
	if !addNewTask {
		return nil
	}

	// add task into taskManager
	m.lastTaskIdMap.Store(args.Vid, task.taskId)
	m.taskMgr.AddTask(task)
	return nil
}

func (m *VolumeMgr) applyRemoveVolumeTask(ctx context.Context, vid proto.Vid, taskId string, taskType base.VolumeTaskType) error {
	span := trace.SpanFromContextSafe(ctx)
	vol := m.all.getVol(vid)
	if vol == nil {
		return ErrVolumeNotExist
	}
	value, ok := m.lastTaskIdMap.Load(vid)
	if !ok {
		span.Infof("task[vid=%d taskId=%s type=%s] not found in last task map, this task maybe is deleted", vid, taskId, taskType.String())
		return nil
	}
	if value.(string) != taskId {
		span.Infof("task[vid=%d taskId=%s type=%s] in last task map, but the last taskId is %s, this task maybe update", vid, taskId, taskType.String(), value.(string))
		return nil
	}
	m.lastTaskIdMap.Delete(vid)
	m.taskMgr.DeleteTask(vid, taskId) // follower should delete this task from task manager
	if taskType == base.VolumeTaskTypeUnlock || taskType == base.VolumeTaskTypeUnlockForce {
		vol.lock.Lock()
		// set volume status into idle, it'll call change volume status function
		span.Debugf("vid: %d, status is: %s", vol.vid, vol.getStatus().String())
		vol.setStatus(ctx, proto.VolumeStatusIdle)
		rec := vol.ToRecord()
		if err := m.volumeTbl.PutVolumeRecord(rec); err != nil {
			span.Errorf("delete task[vid=%d taskId=%s type=%s] error, update volume error: %v", vid, taskId, taskType.String(), err)
			vol.lock.Unlock()
			return err
		}
		vol.lock.Unlock()
	}
	if err := m.volumeTbl.DeleteTaskRecord(vid); err != nil {
		span.Errorf("delete task[vid=%d taskId=%s type=%s] error, delete task record error: %v", vid, taskId, taskType.String(), err)
		return err
	}
	span.Infof("delete task[vid=%d taskId=%s type=%s] success", vid, taskId, taskType.String())
	return nil
}

func (m *VolumeMgr) deleteTask(ctx context.Context, task *volTask) error {
	span := trace.SpanFromContextSafe(ctx)
	data, err := json.Marshal(DeleteTaskCtx{
		Vid:      task.vid,
		TaskType: task.taskType,
		TaskId:   task.taskId,
	})
	if err != nil {
		return err
	}
	pr := base.EncodeProposeInfo(m.GetModuleName(), OperTypeDeleteTask, data, base.ProposeContext{ReqID: span.TraceID()})
	return m.raftServer.Propose(ctx, pr)
}

func (m *VolumeMgr) taskLoop() {
	m.taskMgr.run()
}
