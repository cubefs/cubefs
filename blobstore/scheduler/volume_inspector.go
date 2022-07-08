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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/counter"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/client"
	"github.com/cubefs/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

// IVolumeInspector define the interface of volume inspect manager
type IVolumeInspector interface {
	AcquireInspect(ctx context.Context) (*proto.InspectTask, error)
	CompleteInspect(ctx context.Context, ret *proto.InspectRet)
	GetTaskStats() (finished, timeout [counter.SLOT]int)
	Enabled() bool
	Run()
	closer.Closer
}

const (
	defaultPrepareFailSleepS = 10
	zeroVid                  = proto.Vid(0)
	defaultDuplicateCnt      = 10000000
)

// manager of volumes inspect
// batch execution in steps(Non-persistent)
// step1.gen inspect task
// step2.worker execute inspect task
// step3.collect inspect missed shards info and notice mq proxy
var (
	errTaskHasAcquired  = errors.New("task has been acquired")
	errForbiddenAcquire = errors.New("forbidden acquire task")
)

// IVolsGetter define the interface of clustermgr used by inspect
type IVolsGetter interface {
	ListVolume(ctx context.Context, vid proto.Vid, count int) ([]*client.VolumeInfoSimple, proto.Vid, error)
	GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *client.VolumeInfoSimple, err error)
}

// IRepairShardSender define the shard repair interface used by inspect
type IRepairShardSender interface {
	SendShardRepairMsg(ctx context.Context, vid proto.Vid, bid proto.BlobID, badIdx []uint8) error
}

type inspectTaskInfo struct {
	t           *proto.InspectTask
	ret         *proto.InspectRet
	acquireTime *time.Time
}

func (t *inspectTaskInfo) tryAcquire() error {
	if t.acquired() {
		return errTaskHasAcquired
	}
	now := time.Now()
	t.acquireTime = &now
	return nil
}

func (t *inspectTaskInfo) complete(ret *proto.InspectRet) {
	t.ret = ret
}

func (t *inspectTaskInfo) running(timeoutMs time.Duration) bool {
	if t.completed() || t.timeout(timeoutMs) {
		return false
	}
	return true
}

func (t *inspectTaskInfo) timeout(timeoutMs time.Duration) bool {
	if t.acquired() && !t.completed() {
		deadline := t.acquireTime.Add(timeoutMs * time.Millisecond)
		return time.Now().After(deadline)
	}
	return false
}

func (t *inspectTaskInfo) hasMissedShard() bool {
	return t.completed() && len(t.ret.MissedShards) != 0
}

func (t *inspectTaskInfo) acquired() bool {
	return t.acquireTime != nil
}

func (t *inspectTaskInfo) completed() bool {
	return t.ret != nil
}

type badShardDeduplicator struct {
	l              sync.Mutex
	badShards      map[string]struct{}
	shardsCntLimit int
}

func newBadShardDeduplicator(shardsCntLimit int) *badShardDeduplicator {
	return &badShardDeduplicator{
		badShards:      make(map[string]struct{}),
		shardsCntLimit: shardsCntLimit,
	}
}

func (d *badShardDeduplicator) add(vid proto.Vid, bid proto.BlobID, badIdxs []uint8) {
	d.l.Lock()
	defer d.l.Unlock()

	if len(d.badShards) >= d.shardsCntLimit {
		d.badShards = make(map[string]struct{})
	}
	key := d.key(vid, bid, badIdxs)
	d.badShards[key] = struct{}{}
}

func (d *badShardDeduplicator) reduplicate(vid proto.Vid, bid proto.BlobID, badIdxs []uint8) bool {
	d.l.Lock()
	defer d.l.Unlock()

	key := d.key(vid, bid, badIdxs)
	if _, ok := d.badShards[key]; ok {
		return true
	}
	return false
}

func (d *badShardDeduplicator) key(vid proto.Vid, bid proto.BlobID, badIdxs []uint8) string {
	sortBads(badIdxs)
	return fmt.Sprintf("%d-%d-%v", vid, bid, badIdxs)
}

// VolumeInspectMgrCfg inspect task manager config
type VolumeInspectMgrCfg struct {
	InspectIntervalS int `json:"inspect_interval_s"`
	InspectBatch     int `json:"inspect_batch"`

	// iops of list volume info
	ListVolStep       int `json:"list_vol_step"`
	ListVolIntervalMs int `json:"list_vol_interval_ms"`

	// timeout of inspect
	TimeoutMs int `json:"timeout_ms"`
}

// VolumeInspectMgr inspect task manager
type VolumeInspectMgr struct {
	closer.Closer
	tasks  map[string]*inspectTaskInfo
	tasksL sync.Mutex

	acquireEnable  bool
	acquireEnableL sync.Mutex

	// start vid in current batch
	startVid proto.Vid
	// start vid in next batch
	nextVid proto.Vid

	firstPrepare bool

	taskSwitch taskswitch.ISwitcher
	tbl        db.IInspectCheckPointTable
	volsGetter IVolsGetter

	repairShardSender IRepairShardSender
	sendDeduplicator  *badShardDeduplicator

	completeTaskCounter counter.Counter
	timeoutCounter      counter.Counter

	cfg *VolumeInspectMgrCfg
}

// NewVolumeInspectMgr returns inspect task manager
func NewVolumeInspectMgr(
	tbl db.IInspectCheckPointTable,
	volsGetter IVolsGetter,
	repairShardSender IRepairShardSender,
	taskSwitch taskswitch.ISwitcher, cfg *VolumeInspectMgrCfg) *VolumeInspectMgr {
	return &VolumeInspectMgr{
		Closer:            closer.New(),
		tasks:             make(map[string]*inspectTaskInfo),
		acquireEnable:     false,
		firstPrepare:      true,
		taskSwitch:        taskSwitch,
		tbl:               tbl,
		volsGetter:        volsGetter,
		repairShardSender: repairShardSender,
		sendDeduplicator:  newBadShardDeduplicator(defaultDuplicateCnt),
		cfg:               cfg,
	}
}

// Enabled returns true if task switch status
func (mgr *VolumeInspectMgr) Enabled() bool {
	return mgr.taskSwitch.Enabled()
}

// Run run inspect task manager
func (mgr *VolumeInspectMgr) Run() {
	go mgr.run()
}

func (mgr *VolumeInspectMgr) run() {
	t := time.NewTicker(time.Duration(mgr.cfg.InspectIntervalS) * time.Second)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.taskSwitch.WaitEnable()
			mgr.inspectRun()
		case <-mgr.Closer.Done():
			return
		}
	}
}

func (mgr *VolumeInspectMgr) inspectRun() {
	span, ctx := trace.StartSpanFromContext(context.Background(), "inspector.run")
	defer span.Finish()

	mgr.prepare(ctx)
	mgr.waitCompleted(ctx)
	mgr.finish(ctx)
}

func (mgr *VolumeInspectMgr) enableAcquire(enable bool) {
	mgr.acquireEnableL.Lock()
	defer mgr.acquireEnableL.Unlock()
	mgr.acquireEnable = enable
}

func (mgr *VolumeInspectMgr) canAcquire() bool {
	mgr.acquireEnableL.Lock()
	defer mgr.acquireEnableL.Unlock()
	return mgr.acquireEnable
}

func (mgr *VolumeInspectMgr) getStartVid(ctx context.Context) proto.Vid {
	if mgr.firstPrepare {
		mgr.firstPrepare = false
		ck, err := mgr.tbl.GetCheckPoint(ctx)
		if err == nil {
			return ck.StartVid
		}
		log.Warnf("firstPrepare get check point failed: err[%+v]", err)
		return zeroVid
	}

	if !mgr.allVolVisited() {
		return mgr.nextVid
	}
	return zeroVid
}

func (mgr *VolumeInspectMgr) prepare(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)

	var (
		err     error
		vols    []*client.VolumeInfoSimple
		nextVid proto.Vid
		volCnt  int
	)

	mgr.startVid = mgr.getStartVid(ctx)
	startVid := mgr.startVid
	span.Infof("start prepare inspect task: start vid[%d]", startVid)

	for volCnt < mgr.cfg.InspectBatch {
		remainCnt := mgr.cfg.InspectBatch - volCnt
		listStep := mgr.cfg.ListVolStep
		if remainCnt <= mgr.cfg.ListVolStep {
			listStep = remainCnt
		}

		span.Debugf("prepare inspect task: start vid[%d], list step[%d]", startVid, listStep)
		vols, nextVid, err = mgr.volsGetter.ListVolume(ctx, startVid, listStep)
		if err != nil {
			span.Errorf("list volume failed: err[%+v]", err)
			time.Sleep(defaultPrepareFailSleepS * time.Second)
			continue
		}

		if len(vols) == 0 {
			break
		}

		for _, vol := range vols {
			if vol.IsActive() {
				span.Infof("volume is active and skip: vid[%d]", vol.Vid)
				continue
			}

			taskID := mgr.genTaskID(vol)
			mgr.tasks[taskID] = &inspectTaskInfo{
				t:           mgr.genInspectTask(taskID, vol),
				ret:         nil,
				acquireTime: nil,
			}
			span.Debugf("prepare inspect task: vid[%d], task_id[%s]", vol.Vid, taskID)
			volCnt++
		}

		startVid = nextVid
		time.Sleep(time.Duration(mgr.cfg.ListVolIntervalMs) * time.Millisecond)
	}

	mgr.nextVid = nextVid

	span.Infof("prepare finished: next vid[%d], task count[%d]", nextVid, len(mgr.tasks))
}

// AcquireInspect acquire inspect task
func (mgr *VolumeInspectMgr) AcquireInspect(ctx context.Context) (*proto.InspectTask, error) {
	if !mgr.canAcquire() {
		return nil, errForbiddenAcquire
	}

	if !mgr.taskSwitch.Enabled() {
		return nil, proto.ErrTaskPaused
	}

	mgr.tasksL.Lock()
	defer mgr.tasksL.Unlock()

	for _, task := range mgr.tasks {
		if task.tryAcquire() == nil {
			return task.t, nil
		}
	}

	return nil, proto.ErrTaskEmpty
}

// CompleteInspect complete inspect task
func (mgr *VolumeInspectMgr) CompleteInspect(ctx context.Context, ret *proto.InspectRet) {
	span := trace.SpanFromContextSafe(ctx)

	if !mgr.canAcquire() {
		return
	}

	mgr.tasksL.Lock()
	defer mgr.tasksL.Unlock()

	taskID := ret.TaskID
	if _, ok := mgr.tasks[taskID]; !ok {
		span.Warnf("inspect task not found: task_id[%s]", taskID)
		return
	}

	mgr.tasks[taskID].complete(ret)
	mgr.completeTaskCounter.Add()

	span.Debugf("inspect complete: task_id[%s]", taskID)
}

func (mgr *VolumeInspectMgr) waitCompleted(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start wait completed...")

	mgr.enableAcquire(true)
	defer mgr.enableAcquire(false)

	t := time.NewTicker(time.Duration(mgr.cfg.TimeoutMs) * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			span.Debugf("check all task completed")
			if mgr.allTaskCompleted() {
				return
			}
		case <-mgr.Closer.Done():
			return
		}
	}
}

func (mgr *VolumeInspectMgr) allTaskCompleted() bool {
	mgr.tasksL.Lock()
	defer mgr.tasksL.Unlock()

	for _, task := range mgr.tasks {
		if task.running(time.Duration(mgr.cfg.TimeoutMs)) {
			return false
		}
	}
	return true
}

func (mgr *VolumeInspectMgr) finish(ctx context.Context) {
	span := trace.SpanFromContextSafe(ctx)
	span.Infof("start finish inspect tasks...")

	mgr.tasksL.Lock()
	defer mgr.tasksL.Unlock()

	// collect missed bids
	var missedShards [][]*proto.MissedShard
	for _, task := range mgr.tasks {
		if task.hasMissedShard() {
			missedShards = append(missedShards, task.ret.MissedShards)
			continue
		}
	}

	// clear & stats tasks
	for taskID, task := range mgr.tasks {
		span.Debugf("check task and clear: task_id[%s]", taskID)
		if task.completed() {
			span.Debugf("inspect task is completed: task_id[%s]", taskID)
		}

		if task.timeout(time.Duration(mgr.cfg.TimeoutMs)) {
			span.Debugf("inspect timeout: task_id[%s]", taskID)
			mgr.timeoutCounter.Add()
		}
		delete(mgr.tasks, taskID)
	}

	// post repair shard msg
	for _, volMissedShards := range missedShards {
		vid := volMissedShards[0].Vuid.Vid()

		volInfo, err := mgr.volsGetter.GetVolumeInfo(ctx, vid)
		if err != nil {
			span.Errorf("get volume info failed: err[%+v]", err)
			continue
		}

		if volInfo.IsActive() {
			span.Infof("volume is active and will skip: vid[%d]", volInfo.Vid)
			continue
		}

		bidsBads, err := mgr.collectVolInspectBads(ctx, volMissedShards)
		if err != nil {
			span.Errorf("collect volume inspect bads failed: vid[%d], err[%+v]", vid, err)
			continue
		}

		for bid, bads := range bidsBads {
			span.Infof("inspect missed: vid[%d], bid[%d], shards[%+v]", vid, bid, bads)
			base.InsistOn(ctx, "send shard repair msg failed", func() error {
				return mgr.trySendShardRepairMsg(ctx, vid, bid, bads)
			})
		}
	}

	err := retry.Timed(3, 200).On(func() error {
		return mgr.tbl.SaveCheckPoint(ctx, mgr.nextVid)
	})
	if err != nil {
		span.Warnf("save checkpoint failed: err[%+v]", err)
	}
}

func (mgr *VolumeInspectMgr) collectVolInspectBads(
	ctx context.Context,
	volMissedShards []*proto.MissedShard) (bidsMissed map[proto.BlobID][]uint8, err error,
) {
	span := trace.SpanFromContextSafe(ctx)
	if len(volMissedShards) == 0 {
		return
	}
	vid := volMissedShards[0].Vuid.Vid()
	for _, missedShard := range volMissedShards {
		if missedShard.Vuid.Vid() != vid {
			span.Errorf("all missed shard vid should be same: missed vid[%d], vid[%d]", missedShard.Vuid.Vid(), vid)
			err = errors.New("unexpect:vid not same")
			return
		}
	}

	bidMissedVuid := make(map[proto.BlobID]map[proto.Vuid]struct{})
	for _, missedShard := range volMissedShards {
		bid := missedShard.Bid
		vuid := missedShard.Vuid
		_, ok := bidMissedVuid[bid]
		if !ok {
			bidMissedVuid[bid] = make(map[proto.Vuid]struct{})
		}
		bidMissedVuid[bid][vuid] = struct{}{}
	}

	bidsMissed = make(map[proto.BlobID][]uint8)
	for bid, missVuids := range bidMissedVuid {
		var bads []uint8
		for vuid := range missVuids {
			bads = append(bads, vuid.Index())
		}

		sortBads(bads)
		bidsMissed[bid] = bads
	}
	return
}

func (mgr *VolumeInspectMgr) trySendShardRepairMsg(ctx context.Context, vid proto.Vid, bid proto.BlobID, badIdxs []uint8) error {
	span := trace.SpanFromContextSafe(ctx)
	if mgr.sendDeduplicator.reduplicate(vid, bid, badIdxs) {
		span.Infof("volume has send shard repair msg: vid[%d], bid[%d], bad idxs[%+v]", vid, bid, badIdxs)
		return nil
	}

	err := mgr.repairShardSender.SendShardRepairMsg(ctx, vid, bid, badIdxs)
	if err != nil {
		return err
	}
	span.Infof("send shard repair msg success: vid[%d], bid[%d], bad idxs[%+v]", vid, bid, badIdxs)

	mgr.sendDeduplicator.add(vid, bid, badIdxs)
	return nil
}

func (mgr *VolumeInspectMgr) allVolVisited() bool {
	return mgr.startVid == mgr.nextVid
}

func (mgr *VolumeInspectMgr) genTaskID(vol *client.VolumeInfoSimple) string {
	return base.GenTaskID("inspect", vol.Vid)
}

func (mgr *VolumeInspectMgr) genInspectTask(taskID string, vol *client.VolumeInfoSimple) *proto.InspectTask {
	return &proto.InspectTask{
		TaskId:   taskID,
		Mode:     vol.CodeMode,
		Replicas: vol.VunitLocations,
	}
}

// GetTaskStats return task stats
func (mgr *VolumeInspectMgr) GetTaskStats() (finished, timeout [counter.SLOT]int) {
	finished = mgr.completeTaskCounter.Show()
	timeout = mgr.timeoutCounter.Show()
	return
}

func sortBads(bads []uint8) {
	sort.Slice(bads, func(i, j int) bool {
		return bads[i] < bads[j]
	})
}
