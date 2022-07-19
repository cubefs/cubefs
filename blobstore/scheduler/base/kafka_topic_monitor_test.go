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

package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestNewKafkaTopicMonitor(t *testing.T) {
	broker := newBroker(t)
	defer broker.Close()
	cfg := &KafkaConfig{
		Topic:      testTopic,
		BrokerList: []string{broker.Addr()},
		Partitions: []int32{0},
	}

	access := newMockAccess(nil)
	monitor, err := NewKafkaTopicMonitor(proto.TaskTypeBlobDelete, proto.ClusterID(1), cfg, access, 0)
	go func() {
		monitor.Run()
	}()
	time.Sleep(time.Second * 3)
	require.NoError(t, err)

	cfg.BrokerList = []string{}
	monitor, err = NewKafkaTopicMonitor(proto.TaskTypeBlobDelete, proto.ClusterID(1), cfg, access, 0)
	require.Error(t, err)
}
