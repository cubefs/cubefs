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

package diskmgr

import (
	"context"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func TestMetricReport(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	initTestDiskMgrDisks(t, testDiskMgr, 1, 10, testIdcs...)

	testDiskMgr.refresh(ctx)
	testDiskMgr.Report(ctx, "test-region", 1, "true")
}
