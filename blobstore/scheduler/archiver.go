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
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/scheduler/base"
	"github.com/cubefs/cubefs/blobstore/scheduler/db"
	"github.com/cubefs/cubefs/blobstore/util/closer"
)

// IArchiver define the interface of archive manager
type IArchiver interface {
	RegisterTables(tables ...db.IRecordSrcTbl)
	Run()
	closer.Closer
}

// ArchiveStoreConfig archive store config
type ArchiveStoreConfig struct {
	ArchiveIntervalMin int `json:"archive_interval_min"`
	// recode will archive util delete ArchiveDelayMin
	ArchiveDelayMin int `json:"archive_delay_min"`
}

// ArchiveStoreMgr archive store
type ArchiveStoreMgr struct {
	closer.Closer

	archTbl db.IArchiveTable

	mu        sync.Mutex
	srcTables map[string]db.IRecordSrcTbl

	cfg ArchiveStoreConfig
}

// NewArchiveStoreMgr returns archive store manager
func NewArchiveStoreMgr(table db.IArchiveTable, cfg ArchiveStoreConfig) *ArchiveStoreMgr {
	return &ArchiveStoreMgr{
		Closer:    closer.New(),
		archTbl:   table,
		srcTables: make(map[string]db.IRecordSrcTbl),
		cfg:       cfg,
	}
}

// Run archive store task
func (mgr *ArchiveStoreMgr) Run() {
	go mgr.run()
}

func (mgr *ArchiveStoreMgr) run() {
	t := time.NewTicker(time.Duration(mgr.cfg.ArchiveIntervalMin) * time.Minute)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			mgr.storeRun()
		case <-mgr.Closer.Done():
			return
		}
	}
}

func (mgr *ArchiveStoreMgr) storeRun() {
	_, ctx := trace.StartSpanFromContext(context.Background(), "ArchiveStoreMgr")

	mgr.mu.Lock()
	for _, src := range mgr.srcTables {
		mgr.store(ctx, src)
	}
	mgr.mu.Unlock()
}

func (mgr *ArchiveStoreMgr) store(ctx context.Context, src db.IRecordSrcTbl) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("archive store: src[%s]", src.Name())

	deleteTasks, err := src.QueryMarkDeleteTasks(ctx, mgr.cfg.ArchiveDelayMin)
	if err != nil {
		span.Errorf("query delete tasks failed: src[%s], err[%+v]", src.Name(), err)
		return
	}

	shouldRemove := make([]*proto.ArchiveRecord, 0, len(deleteTasks))
	for _, task := range deleteTasks {
		archivedRecord, err := mgr.archTbl.FindTask(ctx, task.TaskID)
		if archivedRecord != nil {
			span.Infof("task has been archived: task_id [%s]", task.TaskID)
			shouldRemove = append(shouldRemove, task)
			continue
		}
		if err != nil && err != base.ErrNoDocuments {
			span.Errorf("find task in archive table failed: task_id[%s], err[%+v]", task.TaskID, err)
			continue
		}

		err = mgr.archTbl.Insert(ctx, task)
		if err != nil {
			span.Errorf("insert task into archive table failed: task_id[%s], err[%+v]", task.TaskID, err)
			continue
		}

		shouldRemove = append(shouldRemove, task)
	}

	for _, task := range shouldRemove {
		archivedRecord, err := mgr.archTbl.FindTask(ctx, task.TaskID)
		if archivedRecord == nil {
			span.Warnf("task not find in archive table: task_id[%s], err[%+v]", task.TaskID, err)
			continue
		}

		err = src.RemoveMarkDelete(ctx, task.TaskID)
		if err != nil {
			span.Errorf("remove task failed: task_id[%s], err[%+v]", task.TaskID, err)
			continue
		}
		span.Debugf("archived: task_id[%s] task[%+v]", task.TaskID, task)
	}
}

// RegisterTables register tables to archive.
func (mgr *ArchiveStoreMgr) RegisterTables(tables ...db.IRecordSrcTbl) {
	mgr.mu.Lock()
	for _, src := range tables {
		mgr.srcTables[src.Name()] = src
	}
	mgr.mu.Unlock()
}
