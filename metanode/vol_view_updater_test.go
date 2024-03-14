// Copyright 2024 The CubeFS Authors.
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

package metanode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVolViewUpdater(t *testing.T) {
	updater := NewVolViewUpdater()
	mp := &metaPartition{
		config: &MetaPartitionConfig{
			VolName: "testVol",
		},
	}
	err := updater.Register(mp)
	require.NoError(t, err)
	err = updater.Register(mp)
	require.ErrorIs(t, err, ErrMpExistsInVolViewUpdater)
	err = updater.Unregister(mp)
	require.NoError(t, err)
	err = updater.Unregister(mp)
	require.ErrorIs(t, err, ErrMpNotFoundInVolViewUpdater)
}
