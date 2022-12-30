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

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func TestNewKafkaTopicMonitor(t *testing.T) {
	broker := newBroker(t)
	defer broker.Close()
	cfg := &KafkaConfig{
		Topic:      testTopic,
		BrokerList: []string{broker.Addr()},
	}

	access := newMockAccess(nil)
	monitor, err := NewKafkaTopicMonitor(proto.TaskTypeBlobDelete, 1, cfg, access, 0)
	require.NoError(t, err)
	go func() {
		monitor.Run()
	}()

	cfg.BrokerList = []string{}
	_, err = NewKafkaTopicMonitor(proto.TaskTypeBlobDelete, 1, cfg, access, 0)
	require.Error(t, err)
}
