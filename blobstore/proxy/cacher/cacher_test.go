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

package cacher

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

var (
	A = gomock.Any()
	C = gomock.NewController
)

func newCacher(t gomock.TestReporter, expiration int) (Cacher, *mocks.MockClientAPI, func()) {
	cmCli := mocks.NewMockClientAPI(C(t))
	basePath := path.Join(os.TempDir(), fmt.Sprintf("proxy-cacher-%d", rand.Intn(1000)+1000))
	cacher, _ := New(1, ConfigCache{
		DiskvBasePath:     basePath,
		VolumeExpirationS: expiration,
		DiskExpirationS:   expiration,
	}, cmCli)
	return cacher, cmCli, func() { os.RemoveAll(basePath) }
}

func TestProxyCacherConfigVolume(t *testing.T) {
	config := ConfigCache{}
	getCacher := func() *cacher {
		c, err := New(1, config, nil)
		require.NoError(t, err)
		return c.(*cacher)
	}

	for _, cs := range []struct {
		capacity, expCapacity     int
		expiration, expExpiration int
	}{
		{0, _defaultCapacity, 0, _defaultExpirationS},
		{-100, _defaultCapacity, 0, _defaultExpirationS},
		{-100, _defaultCapacity, -1, _defaultExpirationS},
		{1 << 11, 1 << 11, 600, 600},
	} {
		config.VolumeCapacity = cs.capacity
		config.VolumeExpirationS = cs.expiration
		c := getCacher()
		require.Equal(t, cs.expCapacity, c.config.VolumeCapacity)
		require.Equal(t, cs.expExpiration, c.config.VolumeExpirationS)
	}
}
