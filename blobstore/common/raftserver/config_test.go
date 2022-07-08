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

package raftserver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type configSM struct{}

func (sm *configSM) Apply(data [][]byte, index uint64) error {
	return nil
}

func (sm *configSM) ApplyMemberChange(cc ConfChange, index uint64) error {
	return nil
}

func (sm *configSM) Snapshot() (Snapshot, error) {
	return nil, nil
}

func (sm *configSM) ApplySnapshot(meta SnapshotMeta, st Snapshot) error {
	return nil
}

func (sm *configSM) LeaderChange(leader uint64, host string) {
}

func TestConfig(t *testing.T) {
	cfg := &Config{}
	require.NotNil(t, cfg.Verify())

	cfg.NodeId = 1
	require.NotNil(t, cfg.Verify())

	cfg.ListenPort = 80
	require.NotNil(t, cfg.Verify())

	cfg.WalDir = "/tmp/wal"
	require.NotNil(t, cfg.Verify())

	cfg.SM = &configSM{}
	require.Nil(t, cfg.Verify())
}
