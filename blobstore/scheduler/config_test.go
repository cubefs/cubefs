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

package scheduler

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/kafka"
)

func TestConfigCheckAndFix(t *testing.T) {
	cfg := &Config{}
	err := cfg.fixConfig()
	require.Error(t, err, errIllegalClusterID.Error())

	cfg.ClusterID = 1
	err = cfg.fixConfig()
	require.Error(t, err, errInvalidRegisterHost)

	cfg.ServiceRegister.Host = "http://127.0.0.1:9800"
	cfg.IsLeader = true
	cfg.Kafka.Version = "v2.3.aa"
	err = cfg.fixConfig()
	require.Error(t, err, errInvalidKafka)

	cfg.Kafka.Version = "0.10.0.0"
	err = cfg.fixConfig() // ok
	require.NoError(t, err)

	cfg.Kafka.Version = "2.1.0"
	err = cfg.fixConfig() // ok
	require.NoError(t, err)
	require.True(t, cfg.IsLeader)
	require.Equal(t, "http://127.0.0.1:9800", cfg.ServiceRegister.Host)
	require.Equal(t, defaultMemberRefreshIntervalS, cfg.MemberRefreshIntervalS)

	cfg.MemberRefreshIntervalS = 15
	err = cfg.fixConfig()
	require.NoError(t, err)
	require.Equal(t, 15, cfg.MemberRefreshIntervalS)
	require.Equal(t, defaultDeleteDelayH, cfg.BlobDelete.SafeDelayTimeH)
	require.Equal(t, sarama.V2_1_0_0, kafka.DefaultKafkaVersion)

	cfg.BlobDelete.SafeDelayTimeH = -1
	err = cfg.fixConfig()
	require.NoError(t, err)
	require.Equal(t, defaultDeleteNoDelay, cfg.BlobDelete.SafeDelayTimeH)
}

func TestConfigBalanceGateOptIn(t *testing.T) {
	cfg := &Config{ClusterID: 1}
	cfg.ServiceRegister.Host = "http://127.0.0.1:9800"
	cfg.Kafka.Version = "2.1.0"
	require.NoError(t, cfg.fixConfig())

	// balance gates stay off until explicitly configured
	require.Equal(t, int64(0), cfg.Balance.SrcLagMaxFreeChunkCnt)
	require.Equal(t, int64(0), cfg.Balance.DstLeadFreeChunkCnt)
	require.Equal(t, int64(0), cfg.Balance.MigrateConfig.DstLeadFreeChunkCnt)

	cfg.Balance.SrcLagMaxFreeChunkCnt = 10
	cfg.Balance.DstLeadFreeChunkCnt = 5
	require.NoError(t, cfg.fixConfig())
	require.Equal(t, int64(10), cfg.Balance.SrcLagMaxFreeChunkCnt)
	require.Equal(t, int64(5), cfg.Balance.DstLeadFreeChunkCnt)
	// migrate only receives the value via NewBalanceMgr pass-down
	require.Equal(t, int64(0), cfg.Balance.MigrateConfig.DstLeadFreeChunkCnt)
}
