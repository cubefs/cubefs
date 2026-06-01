// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package metanode

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStartFreeOSMemory_skipsWhenAlreadyFreeing(t *testing.T) {
	t.Parallel()
	m := &metadataManager{memFreeing: true}
	m.startFreeOSMemory()
	require.True(t, m.memFreeing)
}

func TestStartFreeOSMemory_clearsMemFreeingAfterRun(t *testing.T) {
	m := &metadataManager{}
	m.startFreeOSMemory()

	require.Eventually(t, func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return !m.memFreeing
	}, 5*time.Second, 10*time.Millisecond)
}

// triggerFreeOSMemoryLikePartitionAdmin mirrors opDeleteMetaPartition / opBackupEmptyMetaPartition.
func triggerFreeOSMemoryLikePartitionAdmin(m *metadataManager) {
	go func() {
		m.startFreeOSMemory()
	}()
}

func TestPartitionAdminFreeOSMemoryPattern_skipsWhileInflight(t *testing.T) {
	m := &metadataManager{memFreeing: true}
	for i := 0; i < 16; i++ {
		triggerFreeOSMemoryLikePartitionAdmin(m)
	}
	require.True(t, m.memFreeing)
}

func TestPartitionAdminFreeOSMemoryPattern_completesAfterRun(t *testing.T) {
	m := &metadataManager{}
	triggerFreeOSMemoryLikePartitionAdmin(m)

	require.Eventually(t, func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return !m.memFreeing
	}, 5*time.Second, 10*time.Millisecond)
}
