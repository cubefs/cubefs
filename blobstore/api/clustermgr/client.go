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

package clustermgr

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

const (
	MemberTypeMin = MemberType(iota)
	MemberTypeLearner
	MemberTypeNormal
	MemberTypeMax
)

var retryCodes = []int{
	http.StatusInternalServerError,
	http.StatusBadGateway,
	http.StatusGatewayTimeout,
	int(errors.ErrRaftReadIndex),
}

type MemberType uint8

type Config struct {
	rpc.LbConfig
}

type Client struct {
	rpc.Client
}

var _ ClientAPI = (*Client)(nil)

var defaultShouldRetry = func(code int, err error) bool {
	for i := range retryCodes {
		if code == retryCodes[i] {
			return true
		}
	}

	return err != nil
}

func New(cfg *Config) *Client {
	if cfg.ShouldRetry == nil {
		cfg.ShouldRetry = defaultShouldRetry
	}
	return &Client{rpc.NewLbClient(&cfg.LbConfig, nil)}
}

type BidScopeArgs struct {
	Count uint64 `json:"count"`
}

type BidScopeRet struct {
	StartBid proto.BlobID `json:"start_bid"`
	EndBid   proto.BlobID `json:"end_bid"`
}

// BidAlloc return available bid scope
func (c *Client) AllocBid(ctx context.Context, args *BidScopeArgs) (ret *BidScopeRet, err error) {
	ret = &BidScopeRet{}
	err = c.PostWith(ctx, "/bid/alloc", ret, args)
	return
}

type AddMemberArgs struct {
	PeerID     uint64     `json:"peer_id"`
	Host       string     `json:"host"`
	MemberType MemberType `json:"member_type"`
	NodeHost   string     `json:"node_host"`
}

type MemberContext struct {
	NodeHost string `json:"node_host"`
}

func (mc *MemberContext) Marshal() ([]byte, error) {
	return json.Marshal(mc)
}

func (mc *MemberContext) Unmarshal(data []byte) error {
	return json.Unmarshal(data, mc)
}

type RemoveMemberArgs struct {
	PeerID uint64 `json:"peer_id"`
}

// AddMember add new member(normal or learner) into raft cluster
func (c *Client) AddMember(ctx context.Context, args *AddMemberArgs) (err error) {
	err = c.PostWith(ctx, "/member/add", nil, args)
	return
}

// RemoveMember remove member from raft cluster
func (c *Client) RemoveMember(ctx context.Context, peerID uint64) (err error) {
	err = c.PostWith(ctx, "/member/remove", nil, &RemoveMemberArgs{PeerID: peerID})
	return
}

// RemoveMember remove member from raft cluster
func (c *Client) TransferLeadership(ctx context.Context, transfereeID uint64) (err error) {
	err = c.PostWith(ctx, "/leadership/transfer", nil, &RemoveMemberArgs{PeerID: transfereeID})
	return
}

// Stat return cluster's statics info, like space info and raft status and so on
func (c *Client) Stat(ctx context.Context) (ret *StatInfo, err error) {
	ret = &StatInfo{}
	err = c.GetWith(ctx, "/stat", ret)
	return
}

func (c *Client) Snapshot(ctx context.Context) (*http.Response, error) {
	return c.Get(ctx, "/snapshot/dump")
}
