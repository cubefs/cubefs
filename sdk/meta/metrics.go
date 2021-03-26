// Copyright 2018 The Chubao Authors.
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

package meta

import (
	"github.com/chubaofs/chubaofs/util/exporter"
)

const (
	MetricSdkMetaOpName = "sdk_meta_op"
)

var (
	SdkMetaOpLabels = []string{"op"}
)

type SdkMetaMetrics struct {
	MetricOpTpc *exporter.TimePointCountVec
}

var (
	metrics *SdkMetaMetrics
)

func RegisterMetrics() {
	if metrics == nil {
		metrics = &SdkMetaMetrics{
			MetricOpTpc: exporter.NewTPCntVec(MetricSdkMetaOpName, "", SdkMetaOpLabels),
		}
	}
}
