// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package metanode

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
)

func TestFollowerReadLeaseTime_defaultFallback(t *testing.T) {
	atomic.StoreUint64(&nodeInfo.followerReadLeaseTime, 0)
	t.Cleanup(func() {
		atomic.StoreUint64(&nodeInfo.followerReadLeaseTime, 0)
	})

	require.EqualValues(t, proto.DefaultFollowerReadLeaseTimeSec, FollowerReadLeaseTime())
	require.EqualValues(t, proto.DefaultFollowerReadLeaseTimeSec, DefaultFollowerReadLeaseTime)
}

func TestUpdateFollowerReadLeaseTime(t *testing.T) {
	t.Cleanup(func() {
		atomic.StoreUint64(&nodeInfo.followerReadLeaseTime, 0)
	})

	updateFollowerReadLeaseTime(proto.DefaultFollowerReadLeaseTimeSec)
	require.EqualValues(t, proto.DefaultFollowerReadLeaseTimeSec, FollowerReadLeaseTime())

	updateFollowerReadLeaseTime(proto.MaxFollowerReadLeaseTimeSec)
	require.EqualValues(t, proto.MaxFollowerReadLeaseTimeSec, FollowerReadLeaseTime())

	updateFollowerReadLeaseTime(proto.MaxFollowerReadLeaseTimeSec + 500)
	require.EqualValues(t, proto.MaxFollowerReadLeaseTimeSec, FollowerReadLeaseTime())

	updateFollowerReadLeaseTime(1)
	require.EqualValues(t, proto.MinFollowerReadLeaseTimeSec, FollowerReadLeaseTime())
}
