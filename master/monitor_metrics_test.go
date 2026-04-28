// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Package master requires the same build environment as production (e.g. RocksDB headers for CGO).

package master

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMonitorMetrics_nodeStatTagLabel(t *testing.T) {
	t.Parallel()
	require.Equal(t, "null", nodeStatTagLabel(""))
	require.Equal(t, "ssd", nodeStatTagLabel("ssd"))
	require.Equal(t, "pool-tag", nodeStatTagLabel("pool-tag"))
}
