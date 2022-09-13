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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/volumemgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	maxReportChunkBodyLength = 1 << 23
)

func (s *Service) VolumeGet(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.GetVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeGet request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	ret, err := s.VolumeMgr.GetVolumeInfo(ctx, args.Vid)
	if err != nil {
		span.Errorf("get volume error,vid is: %v, error:%v", args.Vid, err)
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) VolumeList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeList request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	volInfos, err := s.VolumeMgr.ListVolumeInfo(ctx, args)
	if err != nil && err != kvstore.ErrNotFound {
		span.Errorf("list volume error,args is: %v, error:%v", args, err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}

	if len(volInfos) > 0 {
		c.RespondJSON(&clustermgr.ListVolumes{Volumes: volInfos, Marker: volInfos[len(volInfos)-1].Vid})
	}
}

// transport to primary and params check
func (s *Service) VolumeAlloc(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.AllocVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeAlloc request, args: %v", args)

	// allocator init, direct return allocated volume back
	if args.IsInit {
		c.RespondJSON(s.VolumeMgr.ListAllocatedVolume(ctx, clientIP(c.Request), args.CodeMode))
		return
	}

	if args.Count <= 0 {
		c.RespondError(apierrors.ErrAllocVolumeInvalidParams)
		return
	}

	ret, err := s.VolumeMgr.AllocVolume(ctx, args.CodeMode, args.Count, clientIP(c.Request))
	if err != nil {
		span.Errorf("alloc volume error:%v", err)
		c.RespondError(err)
		return
	}
	if ret != nil {
		span.Debugf("alloc volumes %v to %v", ret.AllocVolumeInfos, clientIP(c.Request))
		c.RespondJSON(ret)
	}
}

func (s *Service) VolumeAllocatedList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListAllocatedVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeAllocatedList request, request ip is %v", args.Host)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	c.RespondJSON(s.VolumeMgr.ListAllocatedVolume(ctx, args.Host, args.CodeMode))
}

func clientIP(r *http.Request) string {
	ip := r.Header.Get("X-Real-Ip")
	if ip == "" {
		ip = r.Header.Get("X-Forwarded-For")
	}
	if ip == "" {
		ip = r.RemoteAddr
	}

	if strings.Contains(ip, ":") {
		index := strings.Index(ip, ":")
		ip = ip[:index]
	}

	return ip
}

func (s *Service) VolumeUpdate(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.UpdateVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeUpdate request, args: %v", args)

	err := s.VolumeMgr.PreUpdateVolumeUnit(ctx, args)
	if err != nil {
		if err == volumemgr.ErrRepeatUpdateUnit {
			span.Info("repeat update volume unit, ignore and return success")
			return
		}
		span.Errorf("update volume error:%v", err)
		c.RespondError(err)
		return
	}
	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.VolumeMgr.GetModuleName(), volumemgr.OperTypeUpdateVolumeUnit, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Errorf("raft propose error:%v", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}

func (s *Service) VolumeRetain(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.RetainVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeRetain request,args: %v,request ip is %v", args, clientIP(c.Request))

	retainVolumes, err := s.VolumeMgr.PreRetainVolume(ctx, args.Tokens, clientIP(c.Request))
	if err != nil {
		span.Errorf("retain volume error:%v", err)
		c.RespondError(err)
		return
	}
	if retainVolumes == nil {
		return
	}

	data, err := json.Marshal(retainVolumes)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", retainVolumes, err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.VolumeMgr.GetModuleName(), volumemgr.OperTypeRetainVolume, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Errorf("raft propose error:%v", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
	c.RespondJSON(retainVolumes)
}

func (s *Service) VolumeLock(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.LockVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeLock request, args: %v", args)

	c.RespondError(s.VolumeMgr.LockVolume(ctx, args.Vid))
}

func (s *Service) VolumeUnlock(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.UnlockVolumeArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeUnlock request, args: %v", args)

	c.RespondError(s.VolumeMgr.UnlockVolume(ctx, args.Vid))
}

func (s *Service) VolumeUnitAlloc(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.AllocVolumeUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeUnitAlloc request, args: %v", args)

	ret, err := s.VolumeMgr.AllocVolumeUnit(ctx, args.Vuid)
	if err != nil {
		span.Error("alloc volumeUnit failed, err: ", errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(ret)
}

func (s *Service) VolumeUnitList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListVolumeUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeUnitList request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	vuInfos, err := s.VolumeMgr.ListVolumeUnitInfo(ctx, args)
	if err != nil {
		span.Error(errors.Detail(err))
		c.RespondError(err)
		return
	}
	c.RespondJSON(&clustermgr.ListVolumeUnitInfos{VolumeUnitInfos: vuInfos})
}

// direct use blobnode client release chunk
func (s *Service) VolumeUnitRelease(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ReleaseVolumeUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept VolumeUnitRelease request, args: %v", args)

	c.RespondError(s.VolumeMgr.ReleaseVolumeUnit(ctx, args.Vuid, args.DiskID, false))
}

func (s *Service) ChunkReport(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if c.Request.ContentLength > maxReportChunkBodyLength {
		span.Warnf("chunk report body length exceed max limit[%d], body length %d", maxReportChunkBodyLength, c.Request.ContentLength)
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	args := &clustermgr.ReportChunkArgs{}
	writer := bytes.NewBuffer(make([]byte, 0, c.Request.ContentLength))
	reader := io.TeeReader(c.Request.Body, writer)

	if err := args.Decode(reader); err != nil {
		span.Errorf("decode report chunk arguments failed, err: %v", err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}

	span.Debugf("accept ChunkReport request, args: %v", args)

	proposeInfo := base.EncodeProposeInfo(s.VolumeMgr.GetModuleName(), volumemgr.OperTypeChunkReport, writer.Bytes(), base.ProposeContext{ReqID: span.TraceID()})
	err := s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Errorf("raft propose error:%v", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}

func (s *Service) ChunkSetCompact(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.SetCompactChunkArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept ChunkSetCompact request, args: %v", args)

	vid := args.Vuid.Vid()
	index := args.Vuid.Index()
	volInfo, err := s.VolumeMgr.GetVolumeInfo(ctx, vid)
	if err != nil {
		c.RespondError(err)
		return
	}
	if index >= uint8(len(volInfo.Units)) || volInfo.Units[index].Vuid != args.Vuid {
		c.RespondError(apierrors.ErrVolumeUnitNotExist)
		return
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.VolumeMgr.GetModuleName(), volumemgr.OperTypeChunkSetCompact, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}

func (s *Service) AdminUpdateVolume(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.VolumeInfoBase)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept AdminUpdateVolume request, args: %v", args)

	volume, err := s.VolumeMgr.GetVolumeInfo(ctx, args.Vid)
	if err != nil {
		c.RespondError(err)
		return
	}
	if args.Status.IsValid() {
		volume.Status = args.Status
	}
	if args.CodeMode.IsValid() {
		volume.CodeMode = args.CodeMode
	}
	if args.Total > 0 {
		volume.Total = args.Total
	}
	if args.Used > 0 {
		volume.Used = args.Used
	}
	if args.Free > 0 {
		volume.Free = args.Free
	}

	data, err := json.Marshal(volume)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(apierrors.ErrCMUnexpect)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.VolumeMgr.GetModuleName(), volumemgr.OperTypeAdminUpdateVolume, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Error("raft propose failed, err: ", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}

func (s *Service) AdminUpdateVolumeUnit(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.AdminUpdateUnitArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept AdminUpdateVolumeUnit request, args: %v", args)

	_, err := s.VolumeMgr.GetVolumeInfo(ctx, args.Vuid.Vid())
	if err != nil {
		c.RespondError(err)
		return
	}

	if !proto.IsValidEpoch(args.Epoch) || !proto.IsValidEpoch(args.NextEpoch) {
		span.Errorf("epoch:%d or nextEpoch:%d not valid", args.Epoch, args.NextEpoch)
		c.RespondError(errors.New("args not valid"))
		return
	}
	if args.DiskID > 0 {
		_, err := s.DiskMgr.GetDiskInfo(ctx, args.DiskID)
		if err != nil {
			c.RespondError(err)
			return
		}
	}

	data, err := json.Marshal(args)
	if err != nil {
		span.Errorf("json marshal failed, args: %v, error: %v", args, err)
		c.RespondError(err)
		return
	}
	proposeInfo := base.EncodeProposeInfo(s.VolumeMgr.GetModuleName(), volumemgr.OperTypeAdminUpdateVolumeUnit, data, base.ProposeContext{ReqID: span.TraceID()})
	err = s.raftNode.Propose(ctx, proposeInfo)
	if err != nil {
		span.Errorf("raft propose failed, err:%v ", err)
		c.RespondError(apierrors.ErrRaftPropose)
		return
	}
}

func (s *Service) V2VolumeList(c *rpc.Context) {
	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)
	args := new(clustermgr.ListVolumeV2Args)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}
	span.Debugf("accept V2VolumeList request, args: %v", args)

	if err := s.raftNode.ReadIndex(ctx); err != nil {
		span.Errorf("read index error: %v", err)
		c.RespondError(apierrors.ErrRaftReadIndex)
		return
	}

	if !args.Status.IsValid() {
		span.Warnf("invalid status[%d]", args.Status)
		c.RespondError(apierrors.ErrIllegalArguments)
		return
	}
	// currently, there is no necessary to list all idle volumes
	// we can use list volume to get all idle volumes
	if args.Status == proto.VolumeStatusIdle {
		c.RespondError(apierrors.ErrNotSupportIdle)
		return
	}

	volInfos, err := s.VolumeMgr.ListVolumeInfoV2(ctx, args.Status)
	if err != nil {
		span.Errorf("list volume failed, error: %s", err.Error())
		c.RespondError(err)
		return
	}
	if len(volInfos) > 0 {
		c.RespondJSON(&clustermgr.ListVolumes{Volumes: volInfos})
		return
	}
}
