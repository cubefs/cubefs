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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"unsafe"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type GetVolumeArgs struct {
	Vid proto.Vid `json:"vid"`
}

type Unit struct {
	Vuid   proto.Vuid   `json:"vuid"`
	DiskID proto.DiskID `json:"disk_id"`
	Host   string       `json:"host"`
}

type VolumeInfo struct {
	Units []Unit `json:"units"`
	VolumeInfoBase
}

type VolumeInfoBase struct {
	Vid            proto.Vid          `json:"vid"`
	CodeMode       codemode.CodeMode  `json:"code_mode"`
	Status         proto.VolumeStatus `json:"status"`
	HealthScore    int                `json:"health_score"`
	Total          uint64             `json:"total"`
	Free           uint64             `json:"free"`
	Used           uint64             `json:"used"`
	CreateByNodeID uint64             `json:"create_by_node_id"`
}

type AllocVolumeInfo struct {
	VolumeInfo
	Token      string `json:"token"`
	ExpireTime int64  `json:"expire_time"`
}

func (c *Client) GetVolumeInfo(ctx context.Context, args *GetVolumeArgs) (ret *VolumeInfo, err error) {
	ret = &VolumeInfo{}
	err = c.GetWith(ctx, "/volume/get?vid="+args.Vid.ToString(), ret)
	return
}

type ListVolumeArgs struct {
	// list volume info after Marker marker
	Marker proto.Vid `json:"marker,omitempty"`
	// one page count
	Count int `json:"count"`
}

type ListVolumeV2Args struct {
	Status proto.VolumeStatus `json:"status"`
}

type ListVolumes struct {
	Volumes []*VolumeInfo `json:"volumes"`
	Marker  proto.Vid     `json:"marker"`
}

func (c *Client) ListVolume(ctx context.Context, args *ListVolumeArgs) (ret ListVolumes, err error) {
	err = c.GetWith(ctx, fmt.Sprintf("/volume/list?marker=%d&count=%d", args.Marker, args.Count), &ret)
	return
}

func (c *Client) ListVolumeV2(ctx context.Context, args *ListVolumeV2Args) (ret ListVolumes, err error) {
	err = c.GetWith(ctx, fmt.Sprintf("/v2/volume/list?status=%d", args.Status), &ret)
	return
}

type AllocVolumeArgs struct {
	IsInit   bool              `json:"is_init"`
	CodeMode codemode.CodeMode `json:"code_mode"`
	Count    int               `json:"count"`
}

type AllocatedVolumeInfos struct {
	AllocVolumeInfos []AllocVolumeInfo `json:"alloc_volume_infos"`
}

func (c *Client) AllocVolume(ctx context.Context, args *AllocVolumeArgs) (ret AllocatedVolumeInfos, err error) {
	err = c.PostWith(ctx, "/volume/alloc", &ret, args)
	return
}

type UpdateVolumeArgs struct {
	NewVuid   proto.Vuid   `json:"new_vuid"`
	NewDiskID proto.DiskID `json:"new_disk_id"`
	OldVuid   proto.Vuid   `json:"old_vuid"`
}

func (c *Client) UpdateVolume(ctx context.Context, args *UpdateVolumeArgs) (err error) {
	err = c.PostWith(ctx, "/volume/update", nil, args)
	return
}

type RetainVolumeArgs struct {
	Tokens []string `json:"tokens"`
}

type RetainVolume struct {
	Token string `json:"token"`
	// unixTime
	ExpireTime int64 `json:"expire_time"`
}

type RetainVolumes struct {
	RetainVolTokens []RetainVolume `json:"retain_vol_tokens"`
}

func (c *Client) RetainVolume(ctx context.Context, args *RetainVolumeArgs) (ret RetainVolumes, err error) {
	err = c.PostWith(ctx, "/volume/retain", &ret, args)
	return
}

type LockVolumeArgs struct {
	Vid proto.Vid `json:"vid"`
}

func (c *Client) LockVolume(ctx context.Context, args *LockVolumeArgs) (err error) {
	err = c.PostWith(ctx, "/volume/lock", nil, args)
	return
}

type UnlockVolumeArgs struct {
	Vid proto.Vid `json:"vid"`
}

func (c *Client) UnlockVolume(ctx context.Context, args *UnlockVolumeArgs) (err error) {
	err = c.PostWith(ctx, "/volume/unlock", nil, args)
	return
}

type AllocVolumeUnitArgs struct {
	Vuid proto.Vuid `json:"vuid"`
}

type AllocVolumeUnit struct {
	Vuid   proto.Vuid   `json:"vuid"`
	DiskID proto.DiskID `json:"disk_id"`
}

func (c *Client) AllocVolumeUnit(ctx context.Context, args *AllocVolumeUnitArgs) (ret *AllocVolumeUnit, err error) {
	err = c.PostWith(ctx, "/volume/unit/alloc", &ret, args)
	return
}

type ReleaseVolumeUnitArgs struct {
	Vuid   proto.Vuid   `json:"vuid"`
	DiskID proto.DiskID `json:"disk_id"`
}

func (c *Client) ReleaseVolumeUnit(ctx context.Context, args *ReleaseVolumeUnitArgs) (err error) {
	err = c.PostWith(ctx, "/volume/unit/release", nil, args)
	return
}

type ListVolumeUnitArgs struct {
	DiskID proto.DiskID `json:"disk_id"`
}

type VolumeUnitInfo struct {
	Vuid       proto.Vuid   `json:"vuid"`
	DiskID     proto.DiskID `json:"disk_id"`
	Total      uint64       `json:"total"`
	Free       uint64       `json:"free"`
	Used       uint64       `json:"used"`
	Compacting bool         `json:"compact"`
	Host       string       `json:"host"`
}

type ListVolumeUnitInfos struct {
	VolumeUnitInfos []*VolumeUnitInfo `json:"volume_unit_infos"`
}

func (c *Client) ListVolumeUnit(ctx context.Context, args *ListVolumeUnitArgs) ([]*VolumeUnitInfo, error) {
	ret := &ListVolumeUnitInfos{}
	err := c.GetWith(ctx, "/volume/unit/list?disk_id="+args.DiskID.ToString(), &ret)
	return ret.VolumeUnitInfos, err
}

type ReportChunkArgs struct {
	ChunkInfos []blobnode.ChunkInfo `json:"chunk_infos"`
}

func (r *ReportChunkArgs) Encode() ([]byte, error) {
	count := uint32(len(r.ChunkInfos))
	if count == 0 {
		return nil, errors.New("chunk infos is empty")
	}
	size := uint32(unsafe.Sizeof(r.ChunkInfos[0]))
	w := bytes.NewBuffer(make([]byte, 0, size*count+8))
	var retErr error
	write := func(field interface{}) {
		if retErr != nil {
			return
		}
		retErr = binary.Write(w, binary.BigEndian, field)
	}

	write(count)
	for _, chunkInfo := range r.ChunkInfos {
		raw, err := chunkInfo.Id.Marshal()
		if err != nil {
			return nil, err
		}
		w.Write(raw)
		write(chunkInfo.Vuid)
		write(chunkInfo.DiskID)
		write(chunkInfo.Total)
		write(chunkInfo.Used)
		write(chunkInfo.Free)
		write(chunkInfo.Size)
		write(chunkInfo.Status)
		write(chunkInfo.Compacting)
		if retErr != nil {
			return nil, retErr
		}
	}
	return w.Bytes(), nil
}

func (r *ReportChunkArgs) Decode(reader io.Reader) error {
	count := uint32(0)
	var retErr error
	read := func(field interface{}) {
		if retErr != nil {
			return
		}
		retErr = binary.Read(reader, binary.BigEndian, field)
	}

	read(&count)
	r.ChunkInfos = make([]blobnode.ChunkInfo, count)
	for i := range r.ChunkInfos {
		raw := make([]byte, blobnode.ChunkIdEncodeLen)
		n, err := io.ReadFull(reader, raw)
		if n != blobnode.ChunkIdEncodeLen || err != nil {
			return fmt.Errorf("invalid source reader, err: %v", err)
		}
		if err != nil {
			return err
		}
		r.ChunkInfos[i].Id.Unmarshal(raw)
		read(&r.ChunkInfos[i].Vuid)
		read(&r.ChunkInfos[i].DiskID)
		read(&r.ChunkInfos[i].Total)
		read(&r.ChunkInfos[i].Used)
		read(&r.ChunkInfos[i].Free)
		read(&r.ChunkInfos[i].Size)
		read(&r.ChunkInfos[i].Status)
		read(&r.ChunkInfos[i].Compacting)
		if retErr != nil {
			return retErr
		}
	}
	return nil
}

func (c *Client) ReportChunk(ctx context.Context, args *ReportChunkArgs) (err error) {
	b, err := args.Encode()
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, "/chunk/report", bytes.NewReader(b))
	if err != nil {
		return err
	}
	request.ContentLength = int64(len(b))
	request.Header.Set(rpc.HeaderContentType, rpc.MIMEStream)
	resp, err := c.Do(ctx, request)
	if err != nil {
		return
	}
	if resp.StatusCode/100 != 2 {
		err = rpc.ParseResponseErr(resp)
	}
	resp.Body.Close()
	return
}

type SetCompactChunkArgs struct {
	Vuid       proto.Vuid `json:"vuid"`
	Compacting bool       `json:"compact"`
}

func (c *Client) SetCompactChunk(ctx context.Context, args *SetCompactChunkArgs) (err error) {
	err = c.PostWith(ctx, "/chunk/set/compact", nil, args)
	return
}

type ListAllocatedVolumeArgs struct {
	Host     string            `json:"host"`
	CodeMode codemode.CodeMode `json:"code_mode"`
}

// list allocated volume return already allocated allocator's volume
func (c *Client) ListAllocatedVolumes(ctx context.Context, args *ListAllocatedVolumeArgs) (ret AllocatedVolumeInfos, err error) {
	err = c.GetWith(ctx, fmt.Sprintf("/volume/allocated/list?host=%s&code_mode=%d", args.Host, args.CodeMode), &ret)
	return
}

type VolumeStatInfo struct {
	TotalVolume       int `json:"total_volume"`
	IdleVolume        int `json:"idle_volume"`
	AllocatableVolume int `json:"can_alloc_volume"`
	ActiveVolume      int `json:"active_volume"`
	LockVolume        int `json:"lock_volume"`
	UnlockingVolume   int `json:"unlocking_volume"`
}

type AdminUpdateUnitArgs struct {
	Epoch     uint32 `json:"epoch"`
	NextEpoch uint32 `json:"next_epoch"`
	VolumeUnitInfo
}
