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

func TestPartitionAdminFreeOSMemoryPattern_matchesOpDeletePath(t *testing.T) {
	m := &metadataManager{}
	triggerFreeOSMemoryLikePartitionAdmin(m)

	require.Eventually(t, func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.memFreeing
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return !m.memFreeing
	}, 5*time.Second, 10*time.Millisecond)
}
