// Copyright 2023 The CubeFS Authors.
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

package mocktest

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

type mockAdminAPI struct {
	master.AdminAPI
}

func (api *mockAdminAPI) GetCluster() (*proto.ClusterView, error) {
	return &proto.ClusterView{
		Name:               "cfs_dev",
		CreateTime:         "2023-04-29 16:59:54",
		LeaderAddr:         "172.16.1.101:17010",
		DisableAutoAlloc:   false,
		MetaNodeThreshold:  0.75,
		Applied:            123,
		MaxDataPartitionID: 20,
		MaxMetaNodeID:      20,
		MaxMetaPartitionID: 3,
		DataNodeStatInfo: &proto.NodeStatInfo{
			TotalGB:     215,
			UsedGB:      177,
			IncreasedGB: 0,
			UsedRatio:   "0.826",
		},
		MetaNodeStatInfo: &proto.NodeStatInfo{
			TotalGB:     9,
			UsedGB:      0,
			IncreasedGB: 0,
			UsedRatio:   "0.037",
		},
		VolStatInfo: []*proto.VolStatInfo{
			{
				Name:                  "vol1",
				TotalSize:             107374182400,
				UsedSize:              0,
				UsedRatio:             "0.000",
				CacheTotalSize:        0,
				CacheUsedSize:         0,
				CacheUsedRatio:        "0.00",
				EnableToken:           false,
				InodeCount:            1,
				DpReadOnlyWhenVolFull: false,
			},
		},
		BadPartitionIDs:     []proto.BadPartitionView{},
		BadMetaPartitionIDs: []proto.BadPartitionView{},
		MasterNodes: []proto.NodeView{
			{
				Addr:       "172.16.1.101:17010",
				Status:     true,
				DomainAddr: "",
				ID:         1,
				IsWritable: false,
			},
			{
				Addr:       "172.16.1.102:17010",
				Status:     true,
				DomainAddr: "",
				ID:         2,
				IsWritable: false,
			},
			{
				Addr:       "172.16.1.103:17010",
				Status:     true,
				DomainAddr: "",
				ID:         3,
				IsWritable: false,
			},
		},
		MetaNodes: []proto.NodeView{
			{
				Addr:       "172.16.1.101:17210",
				Status:     false,
				DomainAddr: "",
				ID:         2,
				IsWritable: true,
			},
			{
				Addr:       "172.16.1.102:17210",
				Status:     false,
				DomainAddr: "",
				ID:         3,
				IsWritable: true,
			},
			{
				Addr:       "172.16.1.103:17210",
				Status:     false,
				DomainAddr: "",
				ID:         4,
				IsWritable: true,
			},
		},
		DataNodes: []proto.NodeView{
			{
				Addr:       "172.16.1.101:17310",
				Status:     false,
				DomainAddr: "",
				ID:         2,
				IsWritable: false,
			},
			{
				Addr:       "172.16.1.102:17310",
				Status:     false,
				DomainAddr: "",
				ID:         3,
				IsWritable: false,
			},
			{
				Addr:       "172.16.1.103:17310",
				Status:     false,
				DomainAddr: "",
				ID:         4,
				IsWritable: false,
			},
		},
	}, nil
}

func (api *mockAdminAPI) GetClusterNodeInfo() (*proto.ClusterNodeInfo, error) {
	return &proto.ClusterNodeInfo{}, nil
}

func (api *mockAdminAPI) GetClusterIP() (*proto.ClusterIP, error) {
	return &proto.ClusterIP{}, nil
}

func (api *mockAdminAPI) GetClusterParas() (map[string]string, error) {
	return map[string]string{}, nil
}

func (api *mockAdminAPI) GetClusterStat() (cs *proto.ClusterStatInfo, err error) {
	return &proto.ClusterStatInfo{
		DataNodeStatInfo: &proto.NodeStatInfo{},
		MetaNodeStatInfo: &proto.NodeStatInfo{},
		ZoneStatInfo:     map[string]*proto.ZoneStat{},
	}, nil
}

func (api *mockAdminAPI) IsFreezeCluster(isFreeze bool) (err error) {
	return nil
}

func (api *mockAdminAPI) SetMetaNodeThreshold(threshold float64) (err error) {
	return nil
}

func (api *mockAdminAPI) SetClusterParas(batchCount string, markDeleteRate string, deleteWorkerSleepMs string, autoRepairRate string, loadFactor string, maxDpCntLimit string) (err error) {
	return nil
}

func (api *mockAdminAPI) GetDataPartition(volName string, partitionID uint64) (partition *proto.DataPartitionInfo, err error) {
	return &proto.DataPartitionInfo{
		PartitionID:              0,
		PartitionTTL:             0,
		PartitionType:            0,
		LastLoadedTime:           0,
		ReplicaNum:               0,
		Status:                   0,
		Recover:                  false,
		Replicas:                 []*proto.DataReplica{},
		Hosts:                    []string{},
		Peers:                    []proto.Peer{},
		Zones:                    []string{},
		MissingNodes:             map[string]int64{},
		VolName:                  "",
		VolID:                    0,
		OfflinePeerID:            0,
		FileInCoreMap:            map[string]*proto.FileInCore{},
		IsRecover:                false,
		FilesWithMissingReplica:  map[string]int64{},
		SingleDecommissionStatus: 0,
		SingleDecommissionAddr:   "",
		RdOnly:                   false,
		IsDiscard:                false,
	}, nil
}

func (api *mockAdminAPI) DiagnoseDataPartition(ignoreDiscardDp bool) (diagnosis *proto.DataPartitionDiagnosis, err error) {
	return &proto.DataPartitionDiagnosis{
		InactiveDataNodes:           []string{"172.16.1.101:17310", "172.16.1.102:17310"},
		CorruptDataPartitionIDs:     []uint64{1, 2},
		LackReplicaDataPartitionIDs: []uint64{1, 2},
		BadDataPartitionIDs: []proto.BadPartitionView{
			{
				Path:         "/test1",
				PartitionIDs: []uint64{1, 2},
			},
			{
				Path:         "/test2",
				PartitionIDs: []uint64{3, 4},
			},
		},
		BadReplicaDataPartitionIDs: []uint64{1, 2},
		RepFileCountDifferDpIDs:    []uint64{1, 2},
		RepUsedSizeDifferDpIDs:     []uint64{1, 2},
		ExcessReplicaDpIDs:         []uint64{1, 2},
	}, nil
}

func (api *mockAdminAPI) DiagnoseMetaPartition() (diagnosis *proto.MetaPartitionDiagnosis, err error) {
	return &proto.MetaPartitionDiagnosis{
		InactiveMetaNodes:           []string{"172.16.1.101:17310", "172.16.1.102:17310"},
		CorruptMetaPartitionIDs:     []uint64{1, 2},
		LackReplicaMetaPartitionIDs: []uint64{1, 2},
		BadMetaPartitionIDs: []proto.BadPartitionView{
			{
				Path:         "/test1",
				PartitionIDs: []uint64{1, 2},
			},
			{
				Path:         "/test2",
				PartitionIDs: []uint64{3, 4},
			},
		},
		BadReplicaMetaPartitionIDs:                 []uint64{1, 2},
		ExcessReplicaMetaPartitionIDs:              []uint64{1, 2},
		InodeCountNotEqualReplicaMetaPartitionIDs:  []uint64{1, 2},
		MaxInodeNotEqualReplicaMetaPartitionIDs:    []uint64{1, 2},
		DentryCountNotEqualReplicaMetaPartitionIDs: []uint64{1, 2},
	}, nil
}

func (api *mockAdminAPI) DecommissionDataPartition(dataPartitionID uint64, nodeAddr string, raftForce bool) (err error) {
	return nil
}

func (api *mockAdminAPI) DecommissionMetaPartition(dataPartitionID uint64, nodeAddr string) (err error) {
	return nil
}

func (api *mockAdminAPI) AddDataReplica(dataPartitionID uint64, nodeAddr string) (err error) {
	return nil
}

func (api *mockAdminAPI) AddMetaReplica(metaPartitionID uint64, nodeAddr string) (err error) {
	return nil
}

func (api *mockAdminAPI) DeleteDataReplica(dataPartitionID uint64, nodeAddr string) (err error) {
	return nil
}

func (api *mockAdminAPI) DeleteMetaReplica(metaPartitionID uint64, nodeAddr string) (err error) {
	return nil
}

func (api *mockAdminAPI) GetDiscardDataPartition() (DiscardDpInfos *proto.DiscardDataPartitionInfos, err error) {
	return &proto.DiscardDataPartitionInfos{
		DiscardDps: []proto.DataPartitionInfo{
			{
				PartitionID:              1,
				PartitionTTL:             0,
				PartitionType:            0,
				LastLoadedTime:           0,
				ReplicaNum:               0,
				Status:                   0,
				Recover:                  false,
				Replicas:                 []*proto.DataReplica{},
				Hosts:                    []string{},
				Peers:                    []proto.Peer{},
				Zones:                    []string{},
				MissingNodes:             map[string]int64{},
				VolName:                  "",
				VolID:                    0,
				OfflinePeerID:            0,
				FileInCoreMap:            map[string]*proto.FileInCore{},
				IsRecover:                false,
				FilesWithMissingReplica:  map[string]int64{},
				SingleDecommissionStatus: 0,
				SingleDecommissionAddr:   "",
				RdOnly:                   false,
				IsDiscard:                false,
			},
		},
	}, nil
}

func (api *mockAdminAPI) QueryBadDisks() (badDisks *proto.BadDiskInfos, err error) {
	return &proto.BadDiskInfos{
		BadDisks: []proto.BadDiskInfo{
			{
				Address: "172.16.1.101:17310",
				Path:    "/test1",
			},
			{
				Address: "172.16.1.102:17310",
				Path:    "/test2",
			},
		},
	}, nil
}

func (api *mockAdminAPI) ListQuota(volName string) (quotaInfo []*proto.QuotaInfo, err error) {
	return []*proto.QuotaInfo{
		{
			VolName: "vol1",
			QuotaId: 1,
			CTime:   0,
			PathInfos: []proto.QuotaPathInfo{
				{
					FullPath:    "/path1",
					RootInode:   0,
					PartitionId: 0,
				},
			},
			LimitedInfo: proto.QuotaLimitedInfo{},
			UsedInfo:    proto.QuotaUsedInfo{},
			MaxFiles:    18446744073709551615,
			MaxBytes:    18446744073709551615,
			Rsv:         "",
		},
		{
			VolName: "vol1",
			QuotaId: 2,
			CTime:   0,
			PathInfos: []proto.QuotaPathInfo{
				{
					FullPath:    "/path2",
					RootInode:   0,
					PartitionId: 0,
				},
			},
			LimitedInfo: proto.QuotaLimitedInfo{},
			UsedInfo:    proto.QuotaUsedInfo{},
			MaxFiles:    18446744073709551615,
			MaxBytes:    18446744073709551615,
			Rsv:         "",
		},
	}, nil
}

func (api *mockAdminAPI) CreateQuota(volName string, quotaPathInfos []proto.QuotaPathInfo, maxFiles uint64, maxBytes uint64) (quotaId uint32, err error) {
	return 1, nil
}

func (api *mockAdminAPI) UpdateQuota(volName string, quotaId string, maxFiles uint64, maxBytes uint64) (err error) {
	return nil
}

func (api *mockAdminAPI) DeleteQuota(volName string, quotaId string) (err error) {
	return nil
}

func (api *mockAdminAPI) GetQuota(volName string, quotaId string) (quotaInfo *proto.QuotaInfo, err error) {
	return &proto.QuotaInfo{
		VolName:     "vol1",
		QuotaId:     1,
		CTime:       0,
		PathInfos:   nil,
		LimitedInfo: proto.QuotaLimitedInfo{},
		UsedInfo:    proto.QuotaUsedInfo{},
		MaxFiles:    18446744073709551615,
		MaxBytes:    18446744073709551615,
		Rsv:         "",
	}, nil
}

func (api *mockAdminAPI) ListQuotaAll() (volsInfo []*proto.VolInfo, err error) {
	return []*proto.VolInfo{
		{
			Name:                  "vol1",
			Owner:                 "cfs",
			CreateTime:            0,
			Status:                0,
			TotalSize:             0,
			UsedSize:              0,
			DpReadOnlyWhenVolFull: false,
		},
	}, nil
}

func (api *mockAdminAPI) ListVols(keywords string) (volsInfo []*proto.VolInfo, err error) {
	return []*proto.VolInfo{
		{
			Name:                  "vol1",
			Owner:                 "cfs",
			CreateTime:            0,
			Status:                0,
			TotalSize:             0,
			UsedSize:              0,
			DpReadOnlyWhenVolFull: false,
		},
	}, nil
}

func (api *mockAdminAPI) CreateVolName(volName, owner string, capacity uint64, deleteLockTime int64, crossZone, normalZonesFirst bool, business string,
	mpCount, replicaNum, size, volType int, followerRead bool, zoneName, cacheRuleKey string, ebsBlkSize,
	cacheCapacity, cacheAction, cacheThreshold, cacheTTL, cacheHighWater, cacheLowWater, cacheLRUInterval int,
	dpReadOnlyWhenVolFull bool, txMask string, txTimeout uint32, txConflictRetryNum int64, txConflictRetryInterval int64, optEnableQuota string) (err error) {
	return nil
}

func (api *mockAdminAPI) GetVolumeSimpleInfo(volName string) (vv *proto.SimpleVolView, err error) {
	return &proto.SimpleVolView{
		ID:                      1,
		Name:                    "vol1",
		Owner:                   "user1",
		ZoneName:                "zone1",
		DpReplicaNum:            3,
		MpReplicaNum:            3,
		InodeCount:              1,
		DentryCount:             0,
		MaxMetaPartitionID:      3,
		Status:                  0,
		Capacity:                100,
		RwDpCnt:                 6,
		MpCnt:                   3,
		DpCnt:                   20,
		FollowerRead:            false,
		NeedToLowerReplica:      false,
		Authenticate:            false,
		CrossZone:               false,
		DefaultPriority:         false,
		DomainOn:                false,
		CreateTime:              "2023-04-29 17:27:17",
		EnableToken:             false,
		EnablePosixAcl:          false,
		EnableTransaction:       "create|mkdir",
		TxTimeout:               1,
		TxConflictRetryNum:      10,
		TxConflictRetryInterval: 20,
		Description:             "",
		DpSelectorName:          "",
		DpSelectorParm:          "",
		DefaultZonePrior:        false,
		DpReadOnlyWhenVolFull:   false,
		VolType:                 1,
		ObjBlockSize:            0,
		CacheCapacity:           0,
		CacheAction:             0,
		CacheThreshold:          0,
		CacheHighWater:          0,
		CacheLowWater:           0,
		CacheLruInterval:        0,
		CacheTtl:                0,
		CacheRule:               "",
		PreloadCapacity:         0,
		Uids:                    nil,
	}, nil
}

func (api *mockAdminAPI) UpdateVolume(vv *proto.SimpleVolView, txTimeout int64, txMask string, txForceReset bool, txConflictRetryNum int64, txConflictRetryInterval int64, txOpLimit int) (err error) {
	return nil
}

func (api *mockAdminAPI) DeleteVolume(volName string, authKey string) (err error) {
	return nil
}

func (api *mockAdminAPI) CreateDataPartition(volName string, count int) (err error) {
	return nil
}

func (api *mockAdminAPI) ListZones() (zoneViews []*proto.ZoneView, err error) {
	return []*proto.ZoneView{
		{
			Name:   "zone1",
			Status: "available",
			NodeSet: map[uint64]*proto.NodeSetView{
				1: {
					DataNodeLen: 3,
					MetaNodeLen: 3,
					MetaNodes: []proto.NodeView{
						{
							Addr:       "172.16.1.101:17210",
							Status:     false,
							DomainAddr: "172.16.1.101:17010",
							ID:         1,
							IsWritable: false,
						},
					},
					DataNodes: []proto.NodeView{
						{
							Addr:       "172.16.1.101:17310",
							Status:     false,
							DomainAddr: "172.16.1.101:17010",
							ID:         1,
							IsWritable: false,
						},
					},
				},
			},
		},
	}, nil
}

func (api *mockAdminAPI) Topo() (topo *proto.TopologyView, err error) {
	return &proto.TopologyView{
		Zones: []*proto.ZoneView{
			{
				Name:   "zone1",
				Status: "available",
				NodeSet: map[uint64]*proto.NodeSetView{
					1: {
						DataNodeLen: 3,
						MetaNodeLen: 3,
						MetaNodes: []proto.NodeView{
							{
								Addr:       "172.16.1.101:17210",
								Status:     false,
								DomainAddr: "172.16.1.101:17010",
								ID:         1,
								IsWritable: false,
							},
						},
						DataNodes: []proto.NodeView{
							{
								Addr:       "172.16.1.101:17310",
								Status:     false,
								DomainAddr: "172.16.1.101:17010",
								ID:         1,
								IsWritable: false,
							},
						},
					},
				},
			},
		},
	}, nil
}

func (api *mockAdminAPI) SetForbidMpDecommission(disable bool) (err error) {
	return nil
}

func (api *mockAdminAPI) VolShrink(volName string, capacity uint64, authKey string) (err error) {
	return nil
}

func (api *mockAdminAPI) VolExpand(volName string, capacity uint64, authKey string) (err error) {
	return nil
}
