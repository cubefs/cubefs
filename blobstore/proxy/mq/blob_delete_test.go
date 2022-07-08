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

package mq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var ErrSendMessage = errors.New("fake send message failed")

func TestBlobDeleteMgr_sendDeleteMsg(t *testing.T) {
	mockProducer := newProducer()
	mgr := BlobDeleteMgr{
		topic:        "test",
		delMsgSender: mockProducer,
	}
	testCases := []struct {
		args *proxy.DeleteArgs
		err  error
	}{
		{
			args: &proxy.DeleteArgs{
				ClusterID: 0,
				Blobs:     []proxy.BlobDelete{{Vid: 1, Bid: 1000}},
			},
			err: nil,
		}, {
			args: &proxy.DeleteArgs{
				ClusterID: 0,
				Blobs:     []proxy.BlobDelete{{Vid: 1, Bid: 1000}, {Vid: 1, Bid: 1000}},
			},
			err: ErrSendMessage,
		},
	}
	for _, tc := range testCases {
		err := mgr.SendDeleteMsg(context.Background(), tc.args)
		require.Equal(t, true, errors.Is(err, tc.err))
	}
}

func TestNewBlobDeleteMgr(t *testing.T) {
	seedBroker, leader := NewBrokers(t)

	mgr, err := NewBlobDeleteMgr(BlobDeleteConfig{
		Topic:        "my_topic",
		MsgSenderCfg: kafka.ProducerCfg{BrokerList: []string{seedBroker.Addr()}},
	})
	require.NoError(t, err)

	info := &proxy.DeleteArgs{
		ClusterID: 0,
		Blobs:     []proxy.BlobDelete{{Vid: 1, Bid: 1000}},
	}

	for i := 0; i < 10; i++ {
		err := mgr.SendDeleteMsg(context.Background(), info)
		require.NoError(t, err)
	}

	leader.Close()
	seedBroker.Close()

	_, err = NewBlobDeleteMgr(BlobDeleteConfig{
		Topic:        "",
		MsgSenderCfg: kafka.ProducerCfg{},
	})
	require.Error(t, err)
}
