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
	"testing"

	"github.com/stretchr/testify/require"

	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
)

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

func TestProxyCacherConfigPath(t *testing.T) {
	for _, cs := range []struct {
		key   string
		paths []string
	}{
		{"", []string{}},
		{"akey", []string{}},
		{"-id", []string{"8b", "d5"}},
		{"volume-", []string{"fc", "08"}},
		{"volume-111", []string{"59", "90"}},
		{"volume-111-", []string{"cb", "dc"}},
		{"volume-111-10", []string{"cf", "77"}},
		{"disk-111", []string{"a6", "51"}},
		{"disk-111-10", []string{"17", "3a"}},
		{diskvKeyVolume(111), []string{"59", "90"}},
		{diskvKeyDisk(111), []string{"a6", "51"}},
	} {
		require.Equal(t, cs.paths, diskvPathTransform(cs.key))
	}
}
