// Copyright 2018 The CubeFS Authors.
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

package wrapper

import "time"

const (
	// DefaultDpMasterCheckInterval matches master default IntervalToCheckDataPartition (5 seconds).
	DefaultDpMasterCheckInterval = 5 * time.Second

	// DefaultDpPullInterval is how often the client Wrapper refreshes the data partition view from master.
	DefaultDpPullInterval = time.Minute

	// DefaultFlowUploadActiveTick is the ticker interval when client flow upload is in active mode.
	DefaultFlowUploadActiveTick = 5 * time.Second

	// DefaultExtentAllocRetryBaseIntervalMs is the base backoff (milliseconds) for extent allocation retry
	// when no custom interval is configured via SetExentRetryArgs.
	DefaultExtentAllocRetryBaseIntervalMs = 100
)
