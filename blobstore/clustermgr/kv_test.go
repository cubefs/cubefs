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

package clustermgr

import (
	"context"
	"fmt"
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/stretchr/testify/require"
)

func TestKV(t *testing.T) {
	testService := initTestService(t)
	defer clear(testService)
	defer testService.Close()
	testClusterClient := initTestClusterClient(testService)

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	err := testClusterClient.SetKV(ctx, "test1", []byte("value"))
	require.NoError(t, err)
}

func BenchmarkService_KvSet(b *testing.B) {
	testService := initTestService(&testing.T{})
	defer clear(testService)
	defer testService.Close()
	testClusterClient := initTestClusterClient(testService)

	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	testCases := []struct {
		key   string
		value []byte
	}{
		{key: "test1", value: []byte("migrate-1023-1236-repair-task")},
		{key: "test2", value: []byte("repair-1")},
		{key: "test3", value: []byte("")},
		{key: "test4", value: []byte("test-repair")},
	}

	b.ResetTimer()
	for i, tCase := range testCases {
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			for ii := 0; ii < b.N; ii++ {
				testClusterClient.SetKV(ctx, tCase.key, tCase.value)
			}
		})
	}
}
