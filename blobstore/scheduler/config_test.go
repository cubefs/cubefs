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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigCheckAndFix(t *testing.T) {
	cfg := &Config{}
	err := cfg.fixConfig()
	require.Error(t, err, errIllegalClusterID.Error())

	cfg.ClusterID = 1
	err = cfg.fixConfig()
	require.Error(t, err, errInvalidMembers)

	cfg.Services.Members = map[uint64]string{1: "127.0.0.1:9800"}
	err = cfg.fixConfig()
	require.Error(t, err, errInvalidLeader)

	cfg.Services.Leader = 1
	err = cfg.fixConfig()
	require.Error(t, err, errInvalidNodeID)

	cfg.Services.NodeID = 1
	err = cfg.fixConfig()
	require.NoError(t, err)
	require.True(t, cfg.IsLeader())
	require.Equal(t, "127.0.0.1:9800", cfg.Leader())
	require.Nil(t, cfg.Follower())
	require.Equal(t, defaultDeleteDelayH, cfg.BlobDelete.SafeDelayTimeH)
	cfg.Services.Members[2] = "127.0.0.1:9880"
	require.Equal(t, "127.0.0.1:9880", cfg.Follower()[0])

	cfg.Services.NodeID = 1
	cfg.BlobDelete.SafeDelayTimeH = -1
	err = cfg.fixConfig()
	require.NoError(t, err)
	require.Equal(t, defaultDeleteNoDelay, cfg.BlobDelete.SafeDelayTimeH)
	require.Equal(t, defaultDeleteHourRangeTo, cfg.BlobDelete.DeleteHourRange.To)

	testCases := []struct {
		hourRange HourRange
		err       error
	}{
		{
			hourRange: HourRange{From: 1, To: 0},
			err:       errInvalidHourRange,
		},
		{
			hourRange: HourRange{From: 1, To: 25},
			err:       errInvalidHourRange,
		},
		{
			hourRange: HourRange{From: -2, To: -1},
			err:       errInvalidHourRange,
		},
		{
			hourRange: HourRange{From: 25, To: 26},
			err:       errInvalidHourRange,
		},
	}
	for _, test := range testCases {
		cfg.BlobDelete.DeleteHourRange = test.hourRange
		err = cfg.fixConfig()
		require.True(t, errors.Is(err, test.err))
	}
}
