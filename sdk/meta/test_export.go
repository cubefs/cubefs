// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package meta

import (
	"sync"
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/btree"
	"golang.org/x/time/rate"
)

// NewTestMetaWrapperWithLeader wires a minimal MetaWrapper to a mock meta TCP leader for cross-package tests.
func NewTestMetaWrapperWithLeader(t testing.TB, leaderAddr string) *MetaWrapper {
	t.Helper()
	mw := &MetaWrapper{
		volname:          "ut-vol",
		metaSendTimeout:  30,
		conns:            util.NewConnectPool(),
		partitions:       make(map[uint64]*MetaPartition),
		ranges:           btree.New(32),
		dirtyInodes:      newDirtyInodeCache(DirtyInodeTTL, MaxDirtyInodeCache),
		forceUpdate:      make(chan struct{}, 1),
		forceUpdateLimit: rate.NewLimiter(1, MinForceUpdateMetaPartitionsInterval),
		uniqidRangeMap:   make(map[uint64]*uniqidRange),
	}
	mw.partCond = sync.NewCond(&mw.partMutex)
	if c, ok := t.(interface{ Cleanup(func()) }); ok {
		c.Cleanup(func() { mw.conns.Close() })
	}
	mp := &MetaPartition{
		PartitionID: 11,
		Start:       1,
		End:         1 << 20,
		LeaderAddr:  leaderAddr,
		Members:     []string{leaderAddr},
	}
	mw.addPartition(mp)
	return mw
}
