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

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

type ClusterMgrConfigAPI interface {
	GetConfig(ctx context.Context, key string) (val string, err error)
}

type ClusterMgrVolumeAPI interface {
	GetVolumeInfo(ctx context.Context, Vid proto.Vid) (ret *VolumeInfoSimple, err error)
	LockVolume(ctx context.Context, Vid proto.Vid) (err error)
	UnlockVolume(ctx context.Context, Vid proto.Vid) (err error)
	UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error)
	AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (ret *AllocVunitInfo, err error)
	ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID) (err error)
	ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (ret []*VunitInfoSimple, err error)
	ListVolume(ctx context.Context, marker proto.Vid, count int) (volInfo []*VolumeInfoSimple, retVid proto.Vid, err error)
}

type ClusterMgrDiskAPI interface {
	ListClusterDisks(ctx context.Context) (disks []*DiskInfoSimple, err error)
	ListBrokenDisks(ctx context.Context, count int) (disks []*DiskInfoSimple, err error)
	ListRepairingDisks(ctx context.Context) (disks []*DiskInfoSimple, err error)
	ListDropDisks(ctx context.Context) (disks []*DiskInfoSimple, err error)
	SetDiskRepairing(ctx context.Context, diskID proto.DiskID) (err error)
	SetDiskRepaired(ctx context.Context, diskID proto.DiskID) (err error)
	SetDiskDropped(ctx context.Context, diskID proto.DiskID) (err error)
	GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *DiskInfoSimple, err error)
}

type ClusterMgrServiceAPI interface {
	Register(ctx context.Context, info RegisterInfo) error
	GetService(ctx context.Context, name string, clusterID proto.ClusterID) (hosts []string, err error)
}

type ClusterMgrTaskAPI interface {
	UpdateMigrateTask(ctx context.Context, value *proto.MigrateTask) (err error)
	AddMigrateTask(ctx context.Context, value *proto.MigrateTask) (err error)
	GetMigrateTask(ctx context.Context, taskType proto.TaskType, key string) (task *proto.MigrateTask, err error)
	DeleteMigrateTask(ctx context.Context, key string) (err error)
	ListMigrateTasks(ctx context.Context, taskType proto.TaskType, args *cmapi.ListKvOpts) (tasks []*proto.MigrateTask, marker string, err error)
	ListAllMigrateTasks(ctx context.Context, taskType proto.TaskType) (tasks []*proto.MigrateTask, err error)
	ListAllMigrateTasksByDiskID(ctx context.Context, taskType proto.TaskType, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error)
	AddMigratingDisk(ctx context.Context, value *MigratingDiskMeta) (err error)
	DeleteMigratingDisk(ctx context.Context, taskType proto.TaskType, diskID proto.DiskID) (err error)
	GetMigratingDisk(ctx context.Context, taskType proto.TaskType, diskID proto.DiskID) (meta *MigratingDiskMeta, err error)
	ListMigratingDisks(ctx context.Context, taskType proto.TaskType) (disks []*MigratingDiskMeta, err error)
	GetVolumeInspectCheckPoint(ctx context.Context) (ck *proto.VolumeInspectCheckPoint, err error)
	SetVolumeInspectCheckPoint(ctx context.Context, startVid proto.Vid) (err error)
	GetConsumeOffset(taskType proto.TaskType, topic string, partition int32) (offset int64, err error)
	SetConsumeOffset(taskType proto.TaskType, topic string, partition int32, offset int64) (err error)
}

// ClusterMgrAPI define the interface of clustermgr used by scheduler
type ClusterMgrAPI interface {
	ClusterMgrConfigAPI
	ClusterMgrVolumeAPI
	ClusterMgrDiskAPI
	ClusterMgrServiceAPI
	ClusterMgrTaskAPI
}

// migrate task key
//  - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  |  task_type  |  disk_id  |  volume_id  | random_id |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - -
//	for example:
//  	balance-10-18-cbkgq4ic605btusi7g90
//		disk_repair-6-1-cbkgq9qc605btusi7gf0
//		disk_drop-6-12-cbkgq9qc605btusi7gg0
//		manual_migrate-6-18-cbkgq9qc605btusi7gj0
//
//	migrating disk key
//  - - - - - - - - - - - - - - - - - - - - - - -
//  | _migratingDiskPrefix | task_type | disk_id |
//  - - - - - - - - - - - - - - - - - - - - - - -
//  for example:
// 		migrating-disk_repair-1
//		migrating-disk_drop-2
//
// volume inspect checkpoint key
//  - - - - - - - - - - - - - -
//  | {task_type} | _checkPoint |
//  - - - - - - - - - - - - - -
//  for example:
//		volume_inspect-checkpoint
//
// kafka consume offset key
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//  | {task_type} | _consumeOffset | {topic} | {partition} |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//	for example:
//		blob_delete-consume_offset-blob_delete-1
//		shard_repair-consume_offset-shard_repair-2
const (
	_delimiter           = "-"
	_migratingDiskPrefix = "migrating"
	_checkPoint          = "checkpoint"
	_consumeOffset       = "consume_offset"
)

var (
	defaultListDiskNum    = 1000
	defaultListDiskMarker = proto.DiskID(0)
	defaultListTaskNum    = 1000
	defaultListTaskMarker = ""
)

type MigratingDiskMeta struct {
	Disk     *DiskInfoSimple `json:"disk"`
	TaskType proto.TaskType  `json:"task_type"`
	Ctime    string          `json:"ctime"`
}

func (d *MigratingDiskMeta) ID() string {
	return genMigratingDiskID(d.TaskType, d.Disk.DiskID)
}

func genMigratingDiskID(taskType proto.TaskType, diskID proto.DiskID) string {
	return fmt.Sprintf("%s%d", genMigratingDiskPrefix(taskType), diskID)
}

func genMigratingDiskPrefix(taskType proto.TaskType) string {
	return fmt.Sprintf("%s%s%s%s", _migratingDiskPrefix, _delimiter, taskType, _delimiter)
}

// GenMigrateTaskID return uniq task id
func GenMigrateTaskID(taskType proto.TaskType, diskID proto.DiskID, volumeID proto.Vid) string {
	return fmt.Sprintf("%s%d%s%s", GenMigrateTaskPrefixByDiskID(taskType, diskID), volumeID, _delimiter, xid.New().String())
}

func GenMigrateTaskPrefix(taskType proto.TaskType) string {
	return fmt.Sprintf("%s%s", taskType, _delimiter)
}

func GenMigrateTaskPrefixByDiskID(taskType proto.TaskType, diskID proto.DiskID) string {
	return fmt.Sprintf("%s%d%s", GenMigrateTaskPrefix(taskType), diskID, _delimiter)
}

func ValidMigrateTask(taskType proto.TaskType, taskID string) bool {
	return strings.HasPrefix(taskID, GenMigrateTaskPrefix(taskType))
}

type ConsumeOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func genVolumeInspectCheckpointKey() string {
	return proto.TaskTypeVolumeInspect.String() + _delimiter + _checkPoint
}

func genConsumerOffsetKey(taskType proto.TaskType, topic string, partition int32) string {
	return fmt.Sprintf("%s%s%s%s%s%s%d", taskType, _delimiter, _consumeOffset, _delimiter, topic, _delimiter, partition)
}

// VolumeInfoSimple volume info used by scheduler
type VolumeInfoSimple struct {
	Vid            proto.Vid             `json:"vid"`
	CodeMode       codemode.CodeMode     `json:"code_mode"`
	Status         proto.VolumeStatus    `json:"status"`
	VunitLocations []proto.VunitLocation `json:"vunit_locations"`
}

// EqualWith returns whether equal with another.
func (vol *VolumeInfoSimple) EqualWith(volInfo *VolumeInfoSimple) bool {
	if len(vol.VunitLocations) != len(volInfo.VunitLocations) {
		return false
	}
	if vol.Vid != volInfo.Vid ||
		vol.CodeMode != volInfo.CodeMode ||
		vol.Status != volInfo.Status {
		return false
	}
	for i := range vol.VunitLocations {
		if vol.VunitLocations[i] != volInfo.VunitLocations[i] {
			return false
		}
	}
	return true
}

// IsIdle returns true if volume is idle
func (vol *VolumeInfoSimple) IsIdle() bool {
	return vol.Status == proto.VolumeStatusIdle
}

// IsActive returns true if volume is active
func (vol *VolumeInfoSimple) IsActive() bool {
	return vol.Status == proto.VolumeStatusActive
}

func (vol *VolumeInfoSimple) set(info *cmapi.VolumeInfo) {
	vol.Vid = info.Vid
	vol.CodeMode = info.CodeMode
	vol.Status = info.Status
	vol.VunitLocations = make([]proto.VunitLocation, len(info.Units))

	// check volume info
	codeModeInfo := info.CodeMode.Tactic()
	vunitCnt := codeModeInfo.N + codeModeInfo.M + codeModeInfo.L
	if len(info.Units) != vunitCnt {
		log.Panicf("volume %d info unexpect", info.Vid)
	}

	diskIDMap := make(map[proto.DiskID]struct{}, vunitCnt)
	for _, repl := range info.Units {
		if _, ok := diskIDMap[repl.DiskID]; ok {
			log.Panicf("vid %d many chunks on same disk", info.Vid)
		}
		diskIDMap[repl.DiskID] = struct{}{}
	}

	for i := 0; i < len(info.Units); i++ {
		vol.VunitLocations[i] = proto.VunitLocation{
			Vuid:   info.Units[i].Vuid,
			Host:   info.Units[i].Host,
			DiskID: info.Units[i].DiskID,
		}
	}
}

// AllocVunitInfo volume unit info for alloc
type AllocVunitInfo struct {
	proto.VunitLocation
}

// Location returns volume unit location
func (vunit *AllocVunitInfo) Location() proto.VunitLocation {
	return vunit.VunitLocation
}

func (vunit *AllocVunitInfo) set(info *cmapi.AllocVolumeUnit, host string) {
	vunit.Vuid = info.Vuid
	vunit.DiskID = info.DiskID
	vunit.Host = host
}

// VunitInfoSimple volume unit simple info
type VunitInfoSimple struct {
	Vuid   proto.Vuid   `json:"vuid"`
	DiskID proto.DiskID `json:"disk_id"`
	Host   string       `json:"host"`
	Used   uint64       `json:"used"`
}

func (vunit *VunitInfoSimple) set(info *cmapi.VolumeUnitInfo, host string) {
	vunit.Vuid = info.Vuid
	vunit.DiskID = info.DiskID
	vunit.Host = host
	vunit.Used = info.Used
}

// DiskInfoSimple disk simple info
type DiskInfoSimple struct {
	ClusterID    proto.ClusterID  `json:"cluster_id"`
	DiskID       proto.DiskID     `json:"disk_id"`
	Idc          string           `json:"idc"`
	Rack         string           `json:"rack"`
	Host         string           `json:"host"`
	Status       proto.DiskStatus `json:"status"`
	Readonly     bool             `json:"readonly"`
	UsedChunkCnt int64            `json:"used_chunk_cnt"`
	MaxChunkCnt  int64            `json:"max_chunk_cnt"`
	FreeChunkCnt int64            `json:"free_chunk_cnt"`
}

// IsHealth return true if disk is health
func (disk *DiskInfoSimple) IsHealth() bool {
	return disk.Status == proto.DiskStatusNormal
}

// IsBroken return true if disk is broken
func (disk *DiskInfoSimple) IsBroken() bool {
	return disk.Status == proto.DiskStatusBroken
}

// IsDropped return true if disk is dropped
func (disk *DiskInfoSimple) IsDropped() bool {
	return disk.Status == proto.DiskStatusDropped
}

// IsRepaired return true if disk is repaired
func (disk *DiskInfoSimple) IsRepaired() bool {
	return disk.Status == proto.DiskStatusRepaired
}

// CanDropped  disk can drop when disk is normal or has repaired or has dropped
// for simplicity we not allow to set disk status dropped
// when disk is repairing
func (disk *DiskInfoSimple) CanDropped() bool {
	if disk.Status == proto.DiskStatusNormal ||
		disk.Status == proto.DiskStatusRepaired ||
		disk.Status == proto.DiskStatusDropped {
		return true
	}
	return false
}

func (disk *DiskInfoSimple) set(info *blobnode.DiskInfo) {
	disk.ClusterID = info.ClusterID
	disk.Idc = info.Idc
	disk.Rack = info.Rack
	disk.Host = info.Host
	disk.DiskID = info.DiskID
	disk.Status = info.Status
	disk.Readonly = info.Readonly
	disk.UsedChunkCnt = info.UsedChunkCnt
	disk.MaxChunkCnt = info.MaxChunkCnt
	disk.FreeChunkCnt = info.FreeChunkCnt
}

// RegisterInfo register info use for clustermgr
type RegisterInfo struct {
	ClusterID          uint64 `json:"cluster_id"`
	Name               string `json:"name"`
	Host               string `json:"host"`
	Idc                string `json:"idc"`
	HeartbeatIntervalS uint32 `json:"heartbeat_interval_s"`
	HeartbeatTicks     uint32 `json:"heartbeat_ticks"`
	ExpiresTicks       uint32 `json:"expires_ticks"`
}

// IClusterManager define the interface of clustermgr
type IClusterManager interface {
	GetConfig(ctx context.Context, key string) (ret string, err error)
	GetVolumeInfo(ctx context.Context, args *cmapi.GetVolumeArgs) (ret *cmapi.VolumeInfo, err error)
	LockVolume(ctx context.Context, args *cmapi.LockVolumeArgs) (err error)
	UnlockVolume(ctx context.Context, args *cmapi.UnlockVolumeArgs) (err error)
	UpdateVolume(ctx context.Context, args *cmapi.UpdateVolumeArgs) (err error)
	AllocVolumeUnit(ctx context.Context, args *cmapi.AllocVolumeUnitArgs) (ret *cmapi.AllocVolumeUnit, err error)
	ReleaseVolumeUnit(ctx context.Context, args *cmapi.ReleaseVolumeUnitArgs) (err error)
	ListVolumeUnit(ctx context.Context, args *cmapi.ListVolumeUnitArgs) ([]*cmapi.VolumeUnitInfo, error)
	ListVolume(ctx context.Context, args *cmapi.ListVolumeArgs) (ret cmapi.ListVolumes, err error)
	ListDisk(ctx context.Context, args *cmapi.ListOptionArgs) (ret cmapi.ListDiskRet, err error)
	ListDroppingDisk(ctx context.Context) (ret []*blobnode.DiskInfo, err error)
	SetDisk(ctx context.Context, id proto.DiskID, status proto.DiskStatus) (err error)
	DiskInfo(ctx context.Context, id proto.DiskID) (ret *blobnode.DiskInfo, err error)
	DroppedDisk(ctx context.Context, id proto.DiskID) (err error)
	RegisterService(ctx context.Context, node cmapi.ServiceNode, tickInterval, heartbeatTicks, expiresTicks uint32) (err error)
	GetService(ctx context.Context, args cmapi.GetServiceArgs) (info cmapi.ServiceInfo, err error)
	GetKV(ctx context.Context, key string) (ret cmapi.GetKvRet, err error)
	DeleteKV(ctx context.Context, key string) (err error)
	SetKV(ctx context.Context, key string, value []byte) (err error)
	ListKV(ctx context.Context, args *cmapi.ListKvOpts) (ret cmapi.ListKvRet, err error)
}

// clustermgrClient clustermgr client
type clustermgrClient struct {
	client IClusterManager
	rwLock sync.RWMutex
}

func NewClusterMgrClient(conf *cmapi.Config) ClusterMgrAPI {
	return &clustermgrClient{
		client: cmapi.New(conf),
		rwLock: sync.RWMutex{},
	}
}

// GetConfig returns config by config key
func (c *clustermgrClient) GetConfig(ctx context.Context, key string) (val string, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("get config: args key[%s]", key)
	ret, err := c.client.GetConfig(ctx, key)
	if err != nil {
		span.Errorf("get config failed: err[%+v]", err)
		return
	}
	span.Debugf("get config ret: config[%s]", ret)
	return ret, err
}

// GetVolumeInfo returns volume info
func (c *clustermgrClient) GetVolumeInfo(ctx context.Context, vid proto.Vid) (*VolumeInfoSimple, error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("get volume info: args vid[%d]", vid)
	info, err := c.client.GetVolumeInfo(ctx, &cmapi.GetVolumeArgs{Vid: vid})
	if err != nil {
		span.Errorf("get volume info failed: err[%+v]", err)
		return nil, err
	}
	span.Debugf("get volume info ret: volume[%+v]", *info)
	ret := &VolumeInfoSimple{}
	ret.set(info)
	return ret, nil
}

// LockVolume lock volume
func (c *clustermgrClient) LockVolume(ctx context.Context, vid proto.Vid) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("lock volume: args vid[%d]", vid)
	err = c.client.LockVolume(ctx, &cmapi.LockVolumeArgs{Vid: vid})
	span.Debugf("lock volume ret: err[%+v]", err)
	return
}

// UnlockVolume unlock volume
func (c *clustermgrClient) UnlockVolume(ctx context.Context, vid proto.Vid) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("unlock volume: args vid[%d]", vid)
	err = c.client.UnlockVolume(ctx, &cmapi.UnlockVolumeArgs{Vid: vid})
	span.Debugf("unlock volume ret: err[%+v]", err)
	if rpc.DetectStatusCode(err) == errcode.CodeUnlockNotAllow {
		span.Infof("unlock volume failed but deem lock success: err[%+v], code[%d]", err, rpc.DetectStatusCode(err))
		return nil
	}

	return
}

// UpdateVolume update volume
func (c *clustermgrClient) UpdateVolume(ctx context.Context, newVuid, oldVuid proto.Vuid, newDiskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Infof("update volume: args new vuid[%d], old vuid[%d], new disk_id[%d]", newVuid, oldVuid, newDiskID)
	err = c.client.UpdateVolume(ctx, &cmapi.UpdateVolumeArgs{NewVuid: newVuid, OldVuid: oldVuid, NewDiskID: newDiskID})
	span.Infof("update volume ret: err %+v", err)
	return
}

// AllocVolumeUnit alloc volume unit
func (c *clustermgrClient) AllocVolumeUnit(ctx context.Context, vuid proto.Vuid) (*AllocVunitInfo, error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("alloc volume unit: args vuid[%d]", vuid)
	ret := &AllocVunitInfo{}
	info, err := c.client.AllocVolumeUnit(ctx, &cmapi.AllocVolumeUnitArgs{Vuid: vuid})
	if err != nil {
		span.Errorf("alloc volume unit failed: err[%+v]", err)
		return nil, err
	}
	span.Debugf("alloc volume unit ret: unit[%+v]", *info)

	diskInfo, err := c.client.DiskInfo(ctx, info.DiskID)
	if err != nil {
		return nil, err
	}
	span.Debugf("get disk info ret: disk[%+v]", diskInfo)

	ret.set(info, diskInfo.Host)
	return ret, err
}

// ReleaseVolumeUnit release volume unit
func (c *clustermgrClient) ReleaseVolumeUnit(ctx context.Context, vuid proto.Vuid, diskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("release volume unit: args vuid[%d], disk_id[%d]", vuid, diskID)
	err = c.client.ReleaseVolumeUnit(ctx, &cmapi.ReleaseVolumeUnitArgs{Vuid: vuid, DiskID: diskID})
	span.Debugf("release volume unit ret: err[%+v]", err)

	return
}

// ListDiskVolumeUnits list disk volume units
func (c *clustermgrClient) ListDiskVolumeUnits(ctx context.Context, diskID proto.DiskID) (rets []*VunitInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("list disk volume units: args disk_id[%d]", diskID)
	infos, err := c.client.ListVolumeUnit(ctx, &cmapi.ListVolumeUnitArgs{DiskID: diskID})
	if err != nil {
		span.Errorf("list disk volume units failed: err[%+v]", err)
		return nil, err
	}

	for idx, info := range infos {
		span.Debugf("list disk volume units ret: idx[%d], info[%+v]", idx, *info)
	}

	diskInfo, err := c.client.DiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("get disk info failed: err[%+v]", err)
		return nil, err
	}
	span.Debugf("get disk info ret: disk[%+v]", *diskInfo)

	for _, info := range infos {
		ele := VunitInfoSimple{}
		ele.set(info, diskInfo.Host)
		rets = append(rets, &ele)
	}
	return rets, nil
}

// ListVolume list volume
func (c *clustermgrClient) ListVolume(ctx context.Context, marker proto.Vid, count int) (rets []*VolumeInfoSimple, nextVid proto.Vid, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	vols, err := c.client.ListVolume(ctx, &cmapi.ListVolumeArgs{Marker: marker, Count: count})
	if err != nil {
		return
	}
	for index := range vols.Volumes {
		ret := &VolumeInfoSimple{}
		ret.set(vols.Volumes[index])
		rets = append(rets, ret)
	}
	nextVid = vols.Marker
	return
}

// ListClusterDisks list all disks
func (c *clustermgrClient) ListClusterDisks(ctx context.Context) (disks []*DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	return c.listAllDisks(ctx, proto.DiskStatusNormal)
}

// ListBrokenDisks list all broken disks
func (c *clustermgrClient) ListBrokenDisks(ctx context.Context, count int) (disks []*DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	return c.listDisks(ctx, proto.DiskStatusBroken, count)
}

// ListRepairingDisks list repairing disks
func (c *clustermgrClient) ListRepairingDisks(ctx context.Context) (disks []*DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()
	return c.listAllDisks(ctx, proto.DiskStatusRepairing)
}

func (c *clustermgrClient) listAllDisks(ctx context.Context, status proto.DiskStatus) (disks []*DiskInfoSimple, err error) {
	span := trace.SpanFromContextSafe(ctx)
	marker := defaultListDiskMarker
	for {
		args := &cmapi.ListOptionArgs{
			Status: status,
			Count:  defaultListDiskNum,
			Marker: marker,
		}
		selectDisks, selectMarker, err := c.listDisk(ctx, args)
		if err != nil {
			span.Errorf("list disk failed: err[%+v]", err)
			return nil, err
		}

		marker = selectMarker
		disks = append(disks, selectDisks...)
		if marker == defaultListDiskMarker {
			break
		}
	}
	return
}

func (c *clustermgrClient) listDisks(ctx context.Context, status proto.DiskStatus, count int) (disks []*DiskInfoSimple, err error) {
	span := trace.SpanFromContextSafe(ctx)

	marker := defaultListDiskMarker
	needDiskCount := count
	for {
		args := &cmapi.ListOptionArgs{
			Status: status,
			Count:  needDiskCount,
			Marker: marker,
		}
		selectDisks, selectMarker, err := c.listDisk(ctx, args)
		if err != nil {
			span.Errorf("list disk failed: err[%+v]", err)
			return nil, err
		}

		marker = selectMarker
		disks = append(disks, selectDisks...)
		needDiskCount -= len(disks)
		if marker == defaultListDiskMarker || needDiskCount <= 0 {
			break
		}
	}
	return
}

func (c *clustermgrClient) listDisk(ctx context.Context, args *cmapi.ListOptionArgs) (disks []*DiskInfoSimple, marker proto.DiskID, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("list disk: args[%+v]", *args)
	infos, err := c.client.ListDisk(ctx, args)
	if err != nil {
		span.Errorf("list disk failed: err[%+v]", err)
		return nil, defaultListDiskMarker, err
	}
	marker = infos.Marker
	for _, info := range infos.Disks {
		span.Debugf("list disk ret: disk[%+v]", *info)
		ele := DiskInfoSimple{}
		ele.set(info)
		disks = append(disks, &ele)
	}
	return
}

// ListDropDisks list drop disks, may contain {DiskStatusNormal,DiskStatusReadOnly,DiskStatusBroken,DiskStatusRepairing,DiskStatusRepaired} disks
func (c *clustermgrClient) ListDropDisks(ctx context.Context) (disks []*DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	span := trace.SpanFromContextSafe(ctx)

	infos, err := c.client.ListDroppingDisk(ctx)
	if err != nil {
		span.Errorf("list drop disks failed: err[%+v]", err)
		return nil, err
	}
	span.Infof("list drop disks: len[%d]", len(infos))
	for _, info := range infos {
		span.Debugf("list drop disks ret: disk[%+v]", *info)
		disk := DiskInfoSimple{}
		disk.set(info)
		span.Infof("disk status: [%s]", disk.Status.String())
		if disk.IsHealth() {
			disks = append(disks, &disk)
		}
	}
	return disks, nil
}

// SetDiskRepairing set disk repairing
func (c *clustermgrClient) SetDiskRepairing(ctx context.Context, diskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("set disk repairing: args disk_id[%d], status[%s]", diskID, proto.DiskStatusRepairing.String())
	err = c.setDiskStatus(ctx, diskID, proto.DiskStatusRepairing)
	span.Debugf("set disk repairing ret: err[%+v]", err)
	return
}

// SetDiskRepaired set disk repaired
func (c *clustermgrClient) SetDiskRepaired(ctx context.Context, diskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("set disk repaired: args disk_id[%d], disk status[%s]", diskID, proto.DiskStatusRepaired.String())
	err = c.setDiskStatus(ctx, diskID, proto.DiskStatusRepaired)
	span.Debugf("set disk repaired ret: err[%+v]", err)
	return
}

// SetDiskDropped set disk dropped
func (c *clustermgrClient) SetDiskDropped(ctx context.Context, diskID proto.DiskID) (err error) {
	c.rwLock.Lock()
	defer c.rwLock.Unlock()
	span := trace.SpanFromContextSafe(ctx)

	info, err := c.client.DiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("get disk info failed: disk_id[%d], err[%+v]", diskID, err)
		return err
	}

	disk := &DiskInfoSimple{}
	disk.set(info)
	if disk.IsDropped() {
		return nil
	}

	if !disk.CanDropped() {
		return errcode.ErrCanNotDropped
	}

	span.Debugf("set disk dropped: args disk_id[%d], status[%s]", diskID, proto.DiskStatusDropped.String())
	err = c.client.DroppedDisk(ctx, diskID)
	span.Debugf("set disk dropped ret: err[%+v]", err)
	return
}

func (c *clustermgrClient) setDiskStatus(ctx context.Context, diskID proto.DiskID, status proto.DiskStatus) (err error) {
	return c.client.SetDisk(ctx, diskID, status)
}

// GetDiskInfo returns disk info
func (c *clustermgrClient) GetDiskInfo(ctx context.Context, diskID proto.DiskID) (ret *DiskInfoSimple, err error) {
	c.rwLock.RLock()
	defer c.rwLock.RUnlock()

	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("get disk info: args disk_id[%d]", diskID)
	info, err := c.client.DiskInfo(ctx, diskID)
	if err != nil {
		span.Errorf("get disk info failed: err[%+v]", err)
		return nil, err
	}
	span.Debugf("get disk info ret: disk[%+v]", *info)
	ret = &DiskInfoSimple{}
	ret.set(info)
	return ret, nil
}

// Register register service
func (c *clustermgrClient) Register(ctx context.Context, info RegisterInfo) error {
	node := cmapi.ServiceNode{
		ClusterID: info.ClusterID,
		Name:      info.Name,
		Host:      info.Host,
		Idc:       info.Idc,
	}
	return c.client.RegisterService(ctx, node, info.HeartbeatIntervalS, info.HeartbeatTicks, info.ExpiresTicks)
}

// GetService returns services
func (c *clustermgrClient) GetService(ctx context.Context, name string, clusterID proto.ClusterID) (hosts []string, err error) {
	svrInfos, err := c.client.GetService(ctx, cmapi.GetServiceArgs{Name: name})
	if err != nil {
		return nil, err
	}
	for _, s := range svrInfos.Nodes {
		if clusterID == proto.ClusterID(s.ClusterID) {
			hosts = append(hosts, s.Host)
		}
	}
	return
}

// AddMigrateTask adds migrate task
func (c *clustermgrClient) AddMigrateTask(ctx context.Context, value *proto.MigrateTask) (err error) {
	value.Ctime = time.Now().String()
	value.MTime = value.Ctime

	return c.setTask(ctx, value.TaskID, value)
}

// UpdateMigrateTask updates migrate task
func (c *clustermgrClient) UpdateMigrateTask(ctx context.Context, value *proto.MigrateTask) (err error) {
	value.MTime = time.Now().String()
	return c.setTask(ctx, value.TaskID, value)
}

func (c *clustermgrClient) setTask(ctx context.Context, key string, value interface{}) (err error) {
	val, err := json.Marshal(value)
	if err != nil {
		return
	}
	return c.client.SetKV(ctx, key, val)
}

// GetMigrateTask returns migrate task
func (c *clustermgrClient) GetMigrateTask(ctx context.Context, taskType proto.TaskType, key string) (task *proto.MigrateTask, err error) {
	val, err := c.client.GetKV(ctx, key)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(val.Value, &task)
	if err != nil {
		return nil, err
	}
	if task.TaskType != taskType {
		return nil, errcode.ErrIllegalTaskType
	}
	return
}

// DeleteMigrateTask deletes migrate task and if will get nil if the key does not exits
func (c *clustermgrClient) DeleteMigrateTask(ctx context.Context, key string) (err error) {
	return c.client.DeleteKV(ctx, key)
}

// ListAllMigrateTasksByDiskID returns all migrate task with disk_id
func (c *clustermgrClient) ListAllMigrateTasksByDiskID(ctx context.Context, taskType proto.TaskType, diskID proto.DiskID) (tasks []*proto.MigrateTask, err error) {
	return c.listAllMigrateTasks(ctx, GenMigrateTaskPrefixByDiskID(taskType, diskID), taskType)
}

// ListAllMigrateTasks returns all migrate task
func (c *clustermgrClient) ListAllMigrateTasks(ctx context.Context, taskType proto.TaskType) (tasks []*proto.MigrateTask, err error) {
	return c.listAllMigrateTasks(ctx, GenMigrateTaskPrefix(taskType), taskType)
}

// ListMigrateTasks returns migrate task base on page size
func (c *clustermgrClient) ListMigrateTasks(ctx context.Context, taskType proto.TaskType, args *cmapi.ListKvOpts) (tasks []*proto.MigrateTask, marker string, err error) {
	return c.listMigrateTasks(ctx, taskType, args)
}

func (c *clustermgrClient) listMigrateTasks(ctx context.Context, taskType proto.TaskType, args *cmapi.ListKvOpts) (tasks []*proto.MigrateTask, marker string, err error) {
	span := trace.SpanFromContextSafe(ctx)
	ret, err := c.client.ListKV(ctx, args)
	if err != nil {
		span.Errorf("list task failed: err[%+v]", err)
		return nil, marker, err
	}
	for _, v := range ret.Kvs {
		var task *proto.MigrateTask
		err = json.Unmarshal(v.Value, &task)
		if err != nil {
			span.Errorf("unmarshal task failed: err[%+v]", err)
			return nil, marker, err
		}
		if task.TaskType != taskType {
			span.Errorf("task type is invalid: expected[%s], actual[%s]", taskType, task.TaskType)
			continue
		}
		tasks = append(tasks, task)
	}
	marker = ret.Marker
	return
}

func (c *clustermgrClient) listAllMigrateTasks(ctx context.Context, prefix string, taskType proto.TaskType) (tasks []*proto.MigrateTask, err error) {
	marker := defaultListTaskMarker
	for {
		args := &cmapi.ListKvOpts{
			Prefix: prefix,
			Count:  defaultListTaskNum,
			Marker: marker,
		}
		ret, marker, err := c.listMigrateTasks(ctx, taskType, args)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, ret...)
		if marker == defaultListTaskMarker {
			break
		}
	}
	return
}

// AddMigratingDisk adds migrating disk meta
func (c *clustermgrClient) AddMigratingDisk(ctx context.Context, value *MigratingDiskMeta) (err error) {
	value.Ctime = time.Now().String()
	return c.setTask(ctx, value.ID(), value)
}

// DeleteMigratingDisk deletes migrating disk meta
func (c *clustermgrClient) DeleteMigratingDisk(ctx context.Context, taskType proto.TaskType, diskID proto.DiskID) (err error) {
	return c.client.DeleteKV(ctx, genMigratingDiskID(taskType, diskID))
}

// GetMigratingDisk returns migrating disk meta
func (c *clustermgrClient) GetMigratingDisk(ctx context.Context, taskType proto.TaskType, diskID proto.DiskID) (meta *MigratingDiskMeta, err error) {
	span := trace.SpanFromContextSafe(ctx)
	ret, err := c.client.GetKV(ctx, genMigratingDiskID(taskType, diskID))
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(ret.Value, &meta); err != nil {
		return nil, err
	}
	if meta.TaskType != taskType {
		span.Errorf("task type is invalid: expected[%s], actual[%s]", taskType, meta.TaskType)
		return meta, errcode.ErrIllegalTaskType
	}
	if meta.Disk.DiskID != diskID {
		span.Errorf("disk_id is invalid: expected[%s], actual[%s]", diskID, meta.Disk.DiskID)
		return meta, errcode.ErrIllegalTaskType
	}
	return
}

// ListMigratingDisks returns all migrating disks
func (c *clustermgrClient) ListMigratingDisks(ctx context.Context, taskType proto.TaskType) (disks []*MigratingDiskMeta, err error) {
	span := trace.SpanFromContextSafe(ctx)

	prefix := genMigratingDiskPrefix(taskType)
	marker := defaultListTaskMarker
	for {
		args := &cmapi.ListKvOpts{
			Prefix: prefix,
			Count:  defaultListTaskNum,
			Marker: marker,
		}
		ret, err := c.client.ListKV(ctx, args)
		if err != nil {
			span.Errorf("list task failed: err[%+v]", err)
			return nil, err
		}

		for _, v := range ret.Kvs {
			var task *MigratingDiskMeta
			err = json.Unmarshal(v.Value, &task)
			if err != nil {
				span.Errorf("unmarshal task failed: err[%+v]", err)
				return nil, err
			}
			if task.TaskType != taskType {
				span.Errorf("task type is invalid: expected[%s], actual[%s]", taskType, task.TaskType)
				continue
			}
			disks = append(disks, task)
		}
		marker = ret.Marker
		if marker == defaultListTaskMarker {
			break
		}
	}
	return
}

func (c *clustermgrClient) GetVolumeInspectCheckPoint(ctx context.Context) (ck *proto.VolumeInspectCheckPoint, err error) {
	ret, err := c.client.GetKV(ctx, genVolumeInspectCheckpointKey())
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(ret.Value, &ck)
	return
}

func (c *clustermgrClient) SetVolumeInspectCheckPoint(ctx context.Context, startVid proto.Vid) (err error) {
	checkPoint := &proto.VolumeInspectCheckPoint{
		StartVid: startVid,
		Ctime:    time.Now().String(),
	}
	checkPointBytes, err := json.Marshal(checkPoint)
	if err != nil {
		return err
	}
	return c.client.SetKV(ctx, genVolumeInspectCheckpointKey(), checkPointBytes)
}

func (c *clustermgrClient) GetConsumeOffset(taskType proto.TaskType, topic string, partition int32) (offset int64, err error) {
	ret, err := c.client.GetKV(context.Background(), genConsumerOffsetKey(taskType, topic, partition))
	if err != nil {
		return offset, err
	}
	var consumeOffset ConsumeOffset
	err = json.Unmarshal(ret.Value, &consumeOffset)
	return consumeOffset.Offset, err
}

func (c *clustermgrClient) SetConsumeOffset(taskType proto.TaskType, topic string, partition int32, offset int64) (err error) {
	consumeOffset := &ConsumeOffset{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}
	consumeOffsetBytes, err := json.Marshal(consumeOffset)
	if err != nil {
		return err
	}
	return c.client.SetKV(context.Background(), genConsumerOffsetKey(taskType, topic, partition), consumeOffsetBytes)
}
