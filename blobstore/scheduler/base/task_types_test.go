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

package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommonCheckAndFix(t *testing.T) {
	cfg := TaskCommonConfig{}
	cfg.CheckAndFix()
	require.Equal(t, defaultPrepareQueueRetryDelayS, cfg.PrepareQueueRetryDelayS)
	require.Equal(t, defaultFinishQueueRetryDelayS, cfg.FinishQueueRetryDelayS)
	require.Equal(t, defaultCancelPunishDurationS, cfg.CancelPunishDurationS)
	require.Equal(t, defaultWorkQueueSize, cfg.WorkQueueSize)
	require.Equal(t, defaultCollectIntervalS, cfg.CollectTaskIntervalS)
	require.Equal(t, defaultCheckTaskIntervalS, cfg.CheckTaskIntervalS)
}
