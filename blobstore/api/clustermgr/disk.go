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
	"errors"
	"fmt"

	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type DiskInfoArgs struct {
	DiskID proto.DiskID `json:"disk_id"`
}

type DiskIDAllocRet struct {
	DiskID proto.DiskID `json:"disk_id"`
}

type DiskSetArgs struct {
	DiskID proto.DiskID     `json:"disk_id"`
	Status proto.DiskStatus `json:"status"`
}

type ListOptionArgs struct {
	Idc    string           `json:"idc,omitempty"`
	Rack   string           `json:"rack,omitempty"`
	Host   string           `json:"host,omitempty"`
	Status proto.DiskStatus `json:"status,omitempty"`
	// list disk info after marker
	Marker proto.DiskID `json:"marker,omitempty"`
	// one page count
	Count int `json:"count,omitempty"`
}

type ListDiskRet struct {
	Disks  []*blobnode.DiskInfo `json:"disks"`
	Marker proto.DiskID         `json:"marker"`
}

type DisksHeartbeatArgs struct {
	Disks []*blobnode.DiskHeartBeatInfo `json:"disks"`
}

type DisksHeartbeatRet struct {
	Disks []*DiskHeartbeatRet `json:"disks"`
}

type DiskHeartbeatRet struct {
	DiskID   proto.DiskID     `json:"disk_id"`
	Status   proto.DiskStatus `json:"status"`
	ReadOnly bool             `json:"read_only"`
}

type DiskStatInfo struct {
	IDC            string `json:"idc"`
	Total          int    `json:"total"`
	TotalChunk     int64  `json:"total_chunk"`
	TotalFreeChunk int64  `json:"total_free_chunk"`
	Available      int    `json:"available"`
	Readonly       int    `json:"readonly"`
	Expired        int    `json:"expired"`
	Broken         int    `json:"broken"`
	Repairing      int    `json:"repairing"`
	Repaired       int    `json:"repaired"`
	Dropping       int    `json:"dropping"`
	Dropped        int    `json:"dropped"`
}

type SpaceStatInfo struct {
	TotalSpace     int64          `json:"total_space"`
	FreeSpace      int64          `json:"free_space"`
	UsedSpace      int64          `json:"used_space"`
	WritableSpace  int64          `json:"writable_space"`
	TotalBlobNode  int64          `json:"total_blob_node"`
	TotalDisk      int64          `json:"total_disk"`
	DisksStatInfos []DiskStatInfo `json:"disk_stat_infos"`
}

type DiskAccessArgs struct {
	DiskID   proto.DiskID `json:"disk_id"`
	Readonly bool         `json:"readonly"`
}

// DiskIDAlloc alloc diskID from cluster manager
func (c *Client) AllocDiskID(ctx context.Context) (proto.DiskID, error) {
	ret := &DiskIDAllocRet{}
	err := c.PostWith(ctx, "/diskid/alloc", ret, rpc.NoneBody)
	if err != nil {
		return 0, err
	}
	return ret.DiskID, nil
}

// DiskInfo get disk info from cluster manager
func (c *Client) DiskInfo(ctx context.Context, id proto.DiskID) (ret *blobnode.DiskInfo, err error) {
	ret = &blobnode.DiskInfo{}
	err = c.GetWith(ctx, "/disk/info?disk_id="+id.ToString(), ret)
	return
}

// AddDisk add/register a new disk into cluster manager
func (c *Client) AddDisk(ctx context.Context, info *blobnode.DiskInfo) (err error) {
	err = c.PostWith(ctx, "/disk/add", nil, info)
	return
}

// SetDisk set disk status
func (c *Client) SetDisk(ctx context.Context, id proto.DiskID, status proto.DiskStatus) (err error) {
	if !status.IsValid() {
		return errors.New("invalid status")
	}
	return c.PostWith(ctx, "/disk/set", nil, &DiskSetArgs{DiskID: id, Status: status})
}

// ListHostDisk list specified host disk info from cluster manager
func (c *Client) ListHostDisk(ctx context.Context, host string) (ret []*blobnode.DiskInfo, err error) {
	listRet := ListDiskRet{}
	opt := &ListOptionArgs{Host: host, Count: 200}
	for {
		listRet, err = c.ListDisk(ctx, opt)
		if err != nil || len(listRet.Disks) == 0 {
			return
		}
		opt.Marker = listRet.Marker
		ret = append(ret, listRet.Disks...)
	}
}

// ListDisk list disk info from cluster manager
// when ListOptionArgs is default value, defalut return 10 diskInfos
func (c *Client) ListDisk(ctx context.Context, options *ListOptionArgs) (ret ListDiskRet, err error) {
	err = c.GetWith(ctx, fmt.Sprintf(
		"/disk/list?idc=%s&rack=%s&host=%s&status=%d&marker=%d&count=%d",
		options.Idc,
		options.Rack,
		options.Host,
		options.Status,
		options.Marker,
		options.Count,
	), &ret)
	return
}

// HeartbeatDisk report blobnode disk latest capacity info to cluster manager
func (c *Client) HeartbeatDisk(ctx context.Context, infos []*blobnode.DiskHeartBeatInfo) (ret []*DiskHeartbeatRet, err error) {
	result := &DisksHeartbeatRet{}
	args := &DisksHeartbeatArgs{Disks: infos}
	err = c.PostWith(ctx, "/disk/heartbeat", result, args)
	ret = result.Disks
	return
}

func (c *Client) DropDisk(ctx context.Context, id proto.DiskID) (err error) {
	err = c.PostWith(ctx, "/disk/drop", nil, &DiskInfoArgs{DiskID: id})
	return
}

func (c *Client) DroppedDisk(ctx context.Context, id proto.DiskID) (err error) {
	err = c.PostWith(ctx, "/disk/dropped", nil, &DiskInfoArgs{DiskID: id})
	return
}

func (c *Client) ListDroppingDisk(ctx context.Context) (ret []*blobnode.DiskInfo, err error) {
	result := &ListDiskRet{}
	err = c.GetWith(ctx, "/disk/droppinglist", result)
	ret = result.Disks
	return
}

func (c *Client) SetReadonlyDisk(ctx context.Context, id proto.DiskID, readonly bool) (err error) {
	err = c.PostWith(ctx, "/disk/access", nil, &DiskAccessArgs{DiskID: id, Readonly: readonly})
	return
}
