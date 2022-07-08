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

package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord(t *testing.T) {
	err := NewLogError("123.txt", 10, "test error")
	require.True(t, IsLogError(err))
	require.Equal(t, "invalid data at 123.txt:10 (test error)", err.Error())

	var recType recordType
	recType = EntryType
	require.Equal(t, "entry", recType.String())
	recType = IndexType
	require.Equal(t, "index", recType.String())
	recType = FooterType
	require.Equal(t, "footer", recType.String())
	recType = recordType(4)
	require.Equal(t, "unknown(4)", recType.String())
}
