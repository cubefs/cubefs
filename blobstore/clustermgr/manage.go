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
	"io"
	"os"
	"strconv"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/configmgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

/*
	manage.go implements cluster manage API
*/

func (s *Service) MemberAdd(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.AddMemberArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept MemberAdd request, args: %v", args)

	if args.MemberType <= clustermgr.MemberTypeMin || args.MemberType >= clustermgr.MemberTypeMax {
		span.Warnf("invalid member type, valid range(%d-%d)", clustermgr.MemberTypeLearner, clustermgr.MemberTypeNormal)
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	status := s.raftNode.Status()
	for i := range status.Peers {
		if status.Peers[i].Id == args.PeerID || status.Peers[i].Host == args.Host {
			c.RespondError(apierrors.ErrDuplicatedMemberInfo)
			return
		}
	}

	var err error
	mc, err := marshalMemberContext(args.NodeHost)
	if err != nil {
		c.RespondError(err)
		return
	}

	switch args.MemberType {
	case clustermgr.MemberTypeLearner:
		err = s.raftNode.AddMember(ctx, raftserver.Member{NodeID: args.PeerID, Host: args.Host, Learner: true, Context: mc})
	case clustermgr.MemberTypeNormal:
		err = s.raftNode.AddMember(ctx, raftserver.Member{NodeID: args.PeerID, Host: args.Host, Learner: false, Context: mc})
	default:
	}
	c.RespondError(err)
}

func (s *Service) MemberRemove(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.RemoveMemberArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept MemberRemove request, args: %v", args)

	if !s.checkPeerIDExist(args.PeerID) {
		span.Warnf("peer_id not exist")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	// not allow to remove leader directly, must transfer leadership firstly
	if args.PeerID == s.raftNode.Status().Leader {
		c.RespondError(apierrors.ErrRequestNotAllow)
		return
	}

	if err := s.raftNode.RemoveMember(ctx, args.PeerID); err != nil {
		c.RespondError(err)
	}
}

func (s *Service) LeadershipTransfer(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.RemoveMemberArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Infof("accept LeadershipTransfer request, args: %v", args)

	if !s.checkPeerIDExist(args.PeerID) {
		span.Warnf("peer_id not exist")
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	s.raftNode.TransferLeadership(ctx, s.raftNode.Status().Id, args.PeerID)
}

func (s *Service) Stat(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	span.Info("accept Stat request")

	ret := new(clustermgr.StatInfo)
	ret.RaftStatus = s.raftNode.Status()
	ret.LeaderHost = s.raftNode.GetLeaderHost()
	ret.BlobNodeSpaceStat = *(s.BlobNodeMgr.Stat(ctx, proto.DiskTypeHDD))
	ret.ShardNodeSpaceStat = *(s.ShardNodeMgr.Stat(ctx, proto.DiskTypeNVMeSSD))
	ret.VolumeStat = s.VolumeMgr.Stat(ctx)
	ret.ReadOnly = s.getClusterReadonlyStatus(ctx)
	c.RespondJSON(ret)
}

// SnapshotDump will dump all data using snapshot
func (s *Service) SnapshotDump(c *rpc.Context) {
	span := trace.SpanFromContextSafe(c.Request.Context())
	span.Info("accept SnapshotDump request")

	snapshot, err := s.Snapshot()
	if err != nil {
		c.RespondError(err)
		return
	}
	defer snapshot.Close()
	c.Writer.Header().Set(clustermgr.RaftSnapshotIndexHeaderKey, strconv.FormatUint(snapshot.Index(), 10))
	c.Writer.Header().Set(clustermgr.RaftSnapshotNameHeaderKey, snapshot.Name())
	c.RespondStatus(206)

	for {
		buf, err := snapshot.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			span.Errorf("read snapshot failed: %s", err.Error())
			return
		}
		n, err := c.Writer.Write(buf)
		if err != nil {
			span.Warnf("write snapshot data failed: %s", err.Error())
			return
		}
		if n != len(buf) {
			span.Warnf("write snapshpot data failed: %s", io.ErrShortWrite)
			return
		}
	}
}

// ClusterSetReadonly switches cluster readonly mode and persists it in config kv.
func (s *Service) ClusterSetReadonly(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.SetClusterReadonlyArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Warnf("accept ClusterSetReadonly request, args: %v", args)

	setArgs := &clustermgr.ConfigSetArgs{
		Key:   proto.ClusterReadonlyKey,
		Value: util.Any2String(args.Readonly),
	}
	data, err := json.Marshal(setArgs)
	if err != nil {
		c.RespondError(errors.Info(apierrors.ErrIllegalArguments).Detail(err))
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.ConfigMgr.GetModuleName(), configmgr.OperTypeSetConfig, data, base.ProposeContext{ReqID: span.TraceID()})
	if err = s.raftNode.Propose(ctx, proposeInfo); err != nil {
		span.Error(err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
	c.Respond()
}

func (s *Service) checkPeerIDExist(peerID uint64) bool {
	peers := s.raftNode.Status().Peers
	found := false
	for i := range peers {
		if peerID == peers[i].Id {
			found = true
		}
	}
	return found
}

func (s *Service) getClusterReadonlyStatus(ctx context.Context) bool {
	span := trace.SpanFromContextSafe(ctx)
	readonlyStatus, err := s.ConfigMgr.Get(ctx, proto.ClusterReadonlyKey) // linear read is not required.
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			span.Warnf("get clusterReadonlyStatus configMgr get failed %v", err)
		}
		return s.Config.Readonly
	}
	var ret bool
	err = util.String2Any(readonlyStatus, &ret)
	if err != nil {
		span.Warnf("get clusterReadonlyStatus parse failed %v, readonlyStatus %s", err, readonlyStatus)
		return s.Config.Readonly
	}
	return ret
}

func marshalMemberContext(host string) ([]byte, error) {
	if host == "" {
		return nil, apierrors.ErrIllegalArguments
	}
	memberContext := &clustermgr.MemberContext{NodeHost: host}
	return memberContext.Marshal()
}
