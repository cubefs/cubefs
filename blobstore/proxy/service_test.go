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

package proxy

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/kafka"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/proxy/allocator"
	"github.com/cubefs/cubefs/blobstore/proxy/mock"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

var (
	errCodeMode = errors.New("codeMode not exist")
	errBidCount = errors.New("count too large")
	ctx         = context.Background()

	proxyServer *httptest.Server
	once        sync.Once
)

func runMockService(s *Service) string {
	once.Do(func() {
		proxyServer = httptest.NewServer(NewHandler(s))
	})
	return proxyServer.URL
}

func newMockService(t *testing.T) *Service {
	ctr := gomock.NewController(t)

	blobDeleteMgr := mock.NewMockBlobDeleteHandler(ctr)
	blobDeleteMgr.EXPECT().SendDeleteMsg(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, info *proxy.DeleteArgs) error {
			if len(info.Blobs) > 1 {
				return errors.New("fake send delete message failed")
			}
			return nil
		},
	)

	shardRepairMgr := mock.NewMockShardRepairHandler(ctr)
	shardRepairMgr.EXPECT().SendShardRepairMsg(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, info *proxy.ShardRepairArgs) error {
			if info.Vid == 100 {
				return errors.New("fake send shard repair message failed")
			}
			return nil
		})

	volumeMgr := mock.NewMockVolumeMgr(ctr)
	volumeMgr.EXPECT().Alloc(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, args *proxy.AllocVolsArgs) (allocVols []proxy.AllocRet, err error) {
			if args.CodeMode != codemode.EC6P6 && args.CodeMode != codemode.EC15P12 {
				return nil, errCodeMode
			}
			if args.BidCount > 10000 || args.BidCount < 1 {
				return nil, errBidCount
			}
			return nil, nil
		})
	volumeMgr.EXPECT().List(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(ctx context.Context, codeMode codemode.CodeMode) (vids []proto.Vid, volumes []clustermgr.AllocVolumeInfo, err error) {
			if codeMode != codemode.EC6P6 && codeMode != codemode.EC15P12 {
				return nil, nil, errCodeMode
			}
			return nil, nil, nil
		})

	return &Service{
		Config: Config{
			VolConfig: allocator.VolConfig{
				ClusterID: 1,
			},
		},
		shardRepairMgr: shardRepairMgr,
		blobDeleteMgr:  blobDeleteMgr,
		volumeMgr:      volumeMgr,
	}
}

func newClient() rpc.Client {
	return rpc.NewClient(&rpc.Config{})
}

func TestService_New(t *testing.T) {
	// interface test
	seedBroker, leader := newBrokersWith2Responses(t)
	ctrl := gomock.NewController(t)
	cmcli := mock.ProxyMockClusterMgrCli(ctrl)

	defer ctrl.Finish()
	defer seedBroker.Close()
	defer leader.Close()

	testCases := []struct {
		cfg Config
	}{
		// todo wait cm chang rpc
		{
			cfg: Config{
				MQ: MQConfig{
					BlobDeleteTopic:          "test1",
					ShardRepairTopic:         "test2",
					ShardRepairPriorityTopic: "test3",
					MsgSender: kafka.ProducerCfg{
						BrokerList: []string{seedBroker.Addr()},
						TimeoutMs:  1,
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		New(tc.cfg, cmcli)
	}
}

func TestService_MQ(t *testing.T) {
	runMockService(newMockService(t))
	cli := newClient()

	deleteCases := []struct {
		args proxy.DeleteArgs
		code int
	}{
		{
			args: proxy.DeleteArgs{
				ClusterID: 1,
				Blobs:     []proxy.BlobDelete{{Bid: 0, Vid: 0}},
			},
			code: 200,
		},
		{
			args: proxy.DeleteArgs{
				ClusterID: 2,
				Blobs:     []proxy.BlobDelete{{Bid: 0, Vid: 0}},
			},
			code: 706,
		},
		{
			args: proxy.DeleteArgs{
				ClusterID: 1,
				Blobs:     []proxy.BlobDelete{{Bid: 0, Vid: 0}, {Bid: 1, Vid: 1}},
			},
			code: 500,
		},
	}
	for _, tc := range deleteCases {
		err := cli.PostWith(ctx, proxyServer.URL+"/deletemsg", nil, tc.args)
		require.Equal(t, tc.code, rpc.DetectStatusCode(err))
	}

	shardRepairCases := []struct {
		args proxy.ShardRepairArgs
		code int
	}{
		{
			args: proxy.ShardRepairArgs{
				ClusterID: 1,
				Bid:       1,
				Vid:       1,
				BadIdxes:  nil,
				Reason:    "",
			},
			code: 200,
		},
		{
			args: proxy.ShardRepairArgs{
				ClusterID: 2,
				Bid:       1,
				Vid:       1,
				BadIdxes:  nil,
				Reason:    "",
			},
			code: 706,
		},
		{
			args: proxy.ShardRepairArgs{
				ClusterID: 1,
				Bid:       1,
				Vid:       100,
				BadIdxes:  nil,
				Reason:    "",
			},
			code: 500,
		},
	}
	for _, tc := range shardRepairCases {
		err := cli.PostWith(ctx, proxyServer.URL+"/repairmsg", nil, tc.args)
		require.Equal(t, tc.code, rpc.DetectStatusCode(err))
	}
}

func TestService_Allocator(t *testing.T) {
	url := runMockService(newMockService(t))
	cli := newClient()
	allocURL := url + "/volume/alloc"
	{
		args := proxy.AllocVolsArgs{
			Fsize:    100,
			BidCount: 1,
			CodeMode: codemode.CodeMode(2),
		}

		err := cli.PostWith(ctx, allocURL, nil, args)
		require.NoError(t, err)
	}
	{
		args := proxy.AllocVolsArgs{
			Fsize:    100,
			BidCount: 1,
			CodeMode: codemode.CodeMode(3),
		}

		err := cli.PostWith(ctx, allocURL, nil, args)
		require.Error(t, err)
		require.Equal(t, errCodeMode.Error(), err.Error())
	}
	{
		args := proxy.AllocVolsArgs{
			Fsize:    100,
			BidCount: 10001,
			CodeMode: codemode.CodeMode(2),
		}
		err := cli.PostWith(ctx, allocURL, nil, args)
		require.Error(t, err)
		require.Equal(t, errBidCount.Error(), err.Error())
	}
	{
		args := proxy.AllocVolsArgs{
			Fsize:    100,
			BidCount: 0,
			CodeMode: codemode.CodeMode(2),
		}
		err := cli.PostWith(ctx, allocURL, nil, args)
		require.Error(t, err)
		require.Equal(t, errcode.ErrIllegalArguments.Error(), err.Error())
	}
	{
		err := cli.GetWith(ctx, url+"/volume/list?code_mode=0", nil)
		require.Error(t, err)
		require.Equal(t, errcode.ErrIllegalArguments.Error(), err.Error())
	}
	{
		err := cli.GetWith(ctx, url+"/volume/list?code_mode=3", nil)
		require.Error(t, err)
		require.Equal(t, errCodeMode.Error(), err.Error())
	}
	{
		err := cli.GetWith(ctx, url+"/volume/list?code_mode=2", nil)
		require.NoError(t, err)
	}
	{
		err := cli.GetWith(ctx, url+"/volume/list?code_mode=2", nil)
		require.NoError(t, err)
	}
}

func TestConfigFix(t *testing.T) {
	testCases := []struct {
		cfg *Config
		err error
	}{
		{cfg: &Config{}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test"}}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test", ShardRepairTopic: "test1"}}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test", ShardRepairTopic: "test", ShardRepairPriorityTopic: "test3"}}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test", ShardRepairTopic: "test1", ShardRepairPriorityTopic: "test"}}, err: ErrIllegalTopic},
		{cfg: &Config{MQ: MQConfig{BlobDeleteTopic: "test", ShardRepairTopic: "test1", ShardRepairPriorityTopic: "test3"}}, err: nil},
	}

	for _, tc := range testCases {
		err := tc.cfg.checkAndFix()
		require.Equal(t, true, errors.Is(err, tc.err))
		tc.cfg.shardRepairCfg()
		tc.cfg.blobDeleteCfg()
	}
}

func newBrokersWith2Responses(t *testing.T) (*sarama.MockBroker, *sarama.MockBroker) {
	kafka.DefaultKafkaVersion = sarama.V0_9_0_1

	seedBroker := sarama.NewMockBrokerAddr(t, 1, "127.0.0.1:0")
	leader := sarama.NewMockBrokerAddr(t, 2, "127.0.0.1:0")

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), nil, nil, nil, 0)
	seedBroker.Returns(metadataResponse)
	seedBroker.Returns(metadataResponse)

	return seedBroker, leader
}
