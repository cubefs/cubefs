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
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

func NewHandler(service *Service) *rpc.Router {
	//===================config=====================
	rpc.RegisterArgsParser(&clustermgr.ConfigArgs{}, "json")

	// POST "/config/set?key={key}&value={value}"
	rpc.POST("/config/set", service.ConfigSet, rpc.OptArgsBody())

	rpc.GET("/config/get", service.ConfigGet, rpc.OptArgsQuery())

	rpc.POST("/config/delete", service.ConfigDelete, rpc.OptArgsQuery())

	rpc.GET("/config/list", service.ConfigList)

	//==================disk==========================
	rpc.RegisterArgsParser(&clustermgr.DiskInfoArgs{}, "json")
	rpc.RegisterArgsParser(&clustermgr.ListOptionArgs{}, "json")

	rpc.POST("/diskid/alloc", service.DiskIdAlloc)

	rpc.GET("/disk/info", service.DiskInfo, rpc.OptArgsQuery())

	rpc.POST("/disk/add", service.DiskAdd, rpc.OptArgsBody())

	rpc.POST("/disk/set", service.DiskSet, rpc.OptArgsBody())

	rpc.GET("/disk/list", service.DiskList, rpc.OptArgsQuery())

	rpc.POST("/disk/heartbeat", service.DiskHeartbeat, rpc.OptArgsBody())

	rpc.POST("/disk/drop", service.DiskDrop, rpc.OptArgsBody())

	rpc.POST("/disk/dropped", service.DiskDropped, rpc.OptArgsBody())

	rpc.GET("/disk/droppinglist", service.DiskDroppingList)

	rpc.POST("/disk/access", service.DiskAccess, rpc.OptArgsBody())

	rpc.POST("/admin/disk/update", service.AdminDiskUpdate, rpc.OptArgsBody())

	//==================service==========================
	rpc.RegisterArgsParser(&clustermgr.GetServiceArgs{}, "json")

	rpc.POST("/service/register", service.ServiceRegister, rpc.OptArgsBody())

	rpc.POST("/service/unregister", service.ServiceUnregister, rpc.OptArgsBody())

	rpc.GET("/service/get", service.ServiceGet, rpc.OptArgsQuery())

	rpc.POST("/service/heartbeat", service.ServiceHeartbeat, rpc.OptArgsBody())

	rpc.GET("/service/list", service.ServiceList)

	//==================volume==========================
	rpc.RegisterArgsParser(&clustermgr.GetVolumeArgs{}, "json")
	rpc.RegisterArgsParser(&clustermgr.ListVolumeArgs{}, "json")
	rpc.RegisterArgsParser(&clustermgr.ListVolumeV2Args{}, "json")
	rpc.RegisterArgsParser(&clustermgr.ListVolumeUnitArgs{}, "json")
	rpc.RegisterArgsParser(&clustermgr.ListAllocatedVolumeArgs{}, "json")

	rpc.GET("/volume/get", service.VolumeGet, rpc.OptArgsQuery())

	rpc.GET("/volume/list", service.VolumeList, rpc.OptArgsQuery())

	rpc.GET("/v2/volume/list", service.V2VolumeList, rpc.OptArgsQuery())

	rpc.POST("/volume/alloc", service.VolumeAlloc, rpc.OptArgsBody())

	rpc.POST("/volume/update", service.VolumeUpdate, rpc.OptArgsBody())

	rpc.POST("/volume/retain", service.VolumeRetain, rpc.OptArgsBody())

	rpc.POST("/volume/lock", service.VolumeLock, rpc.OptArgsBody())

	rpc.POST("/volume/unlock", service.VolumeUnlock, rpc.OptArgsBody())

	rpc.POST("/volume/unit/alloc", service.VolumeUnitAlloc, rpc.OptArgsBody())

	rpc.POST("/volume/unit/release", service.VolumeUnitRelease, rpc.OptArgsBody())

	rpc.GET("/volume/unit/list", service.VolumeUnitList, rpc.OptArgsQuery())

	rpc.GET("/volume/allocated/list", service.VolumeAllocatedList, rpc.OptArgsQuery())

	rpc.POST("/admin/update/volume/unit", service.AdminUpdateVolumeUnit, rpc.OptArgsBody())

	rpc.POST("/admin/update/volume", service.AdminUpdateVolume, rpc.OptArgsBody())

	//==================chunk==========================

	rpc.POST("/chunk/report", service.ChunkReport, rpc.OptArgsBody())

	rpc.POST("/chunk/set/compact", service.ChunkSetCompact, rpc.OptArgsBody())

	//==================srv==========================

	rpc.POST("/bid/alloc", service.BidAlloc, rpc.OptArgsBody())

	//==================manage==========================

	rpc.POST("/member/add", service.MemberAdd, rpc.OptArgsBody())

	rpc.POST("/member/remove", service.MemberRemove, rpc.OptArgsBody())

	rpc.POST("/leadership/transfer", service.LeadershipTransfer, rpc.OptArgsBody())

	rpc.GET("/stat", service.Stat)

	rpc.GET("/snapshot/dump", service.SnapshotDump)

	return rpc.DefaultRouter
}
