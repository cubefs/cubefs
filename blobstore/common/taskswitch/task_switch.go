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

package taskswitch

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

type ISwitcher interface {
	Enabled() bool
	WaitEnable()
}

// task switch name
const (
	DiskRepairSwitchName    = "disk_repair"
	BalanceSwitchName       = "balance"
	DiskDropSwitchName      = "disk_drop"
	BlobDeleteSwitchName    = "blob_delete"
	ShardRepairSwitchName   = "shard_repair"
	VolumeInspectSwitchName = "volume_inspect"
)

const (
	syncTaskStatusIntervalS = 15
	SwitchOpen              = "true"
	SwitchClose             = "false"
)

var (
	ErrConflictSwitch = errors.New("switch has existed")
	ErrNoSuchSwitch   = errors.New("no such switch")
)

type TaskSwitch struct {
	mu      sync.Mutex
	enabled bool
	wg      sync.WaitGroup
}

func newTaskSwitch() *TaskSwitch {
	c := &TaskSwitch{
		enabled: true,
	}
	c.Disable()
	return c
}

func NewEnabledTaskSwitch() *TaskSwitch {
	taskSwitch := newTaskSwitch()
	taskSwitch.Enable()
	return taskSwitch
}

func (s *TaskSwitch) Enable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.enabled {
		return
	}
	s.enabled = true
	s.wg.Done()
}

func (s *TaskSwitch) Disable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.enabled {
		return
	}
	s.enabled = false
	s.wg.Add(1)
}

func (s *TaskSwitch) Enabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enabled
}

func (s *TaskSwitch) WaitEnable() {
	s.wg.Wait()
}

type ConfigGetter interface {
	GetConfig(ctx context.Context, key string) (val string, err error)
}

type SwitchMgr struct {
	switchs     map[string]*TaskSwitch
	mu          sync.Mutex
	cmCfgGetter ConfigGetter
}

func NewSwitchMgr(cmCli ConfigGetter) *SwitchMgr {
	sm := SwitchMgr{
		switchs:     make(map[string]*TaskSwitch),
		cmCfgGetter: cmCli,
	}
	go sm.loopUpdate()
	return &sm
}

func (sm *SwitchMgr) loopUpdate() {
	for {
		sm.update()
		time.Sleep(syncTaskStatusIntervalS * time.Second)
	}
}

func (sm *SwitchMgr) update() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	span, ctx := trace.StartSpanFromContext(context.Background(), "")

	for switchName, taskSwitch := range sm.switchs {
		statusStr, err := sm.cmCfgGetter.GetConfig(ctx, switchName)
		if err != nil {
			span.Errorf("Get Fail switchName %s err %v", switchName, err)
			continue
		}

		if switchStatus(statusStr) {
			taskSwitch.Enable()
			continue
		}
		taskSwitch.Disable()
	}
}

func (sm *SwitchMgr) AddSwitch(switchName string) (*TaskSwitch, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.switchs[switchName]; ok {
		return nil, ErrConflictSwitch
	}
	sm.switchs[switchName] = newTaskSwitch()
	return sm.switchs[switchName], nil
}

func (sm *SwitchMgr) DelSwitch(switchName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.switchs[switchName]; ok {
		delete(sm.switchs, switchName)
		return nil
	}
	return ErrNoSuchSwitch
}

func switchStatus(statusStr string) (open bool) {
	switch statusStr {
	case SwitchOpen:
		return true
	case SwitchClose:
		return false
	default:
		return false
	}
}
