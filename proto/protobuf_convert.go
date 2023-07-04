package proto

import (
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/exporter/backend/ump"
)

func ConvertDataPartitionsViewPb(view *DataPartitionsViewPb) *DataPartitionsView {
	dpResps := make([]*DataPartitionResponse, 0, len(view.DataPartitions))
	for _, dpPb := range view.DataPartitions {
		dp := ConvertDataPartitionResponsePb(dpPb)
		dpResps = append(dpResps, dp)
	}
	return &DataPartitionsView{dpResps}
}

func ConvertDataPartitionsView(view *DataPartitionsView) *DataPartitionsViewPb {
	dpRespsPb := make([]*DataPartitionResponsePb, 0, len(view.DataPartitions))
	for _, dp := range view.DataPartitions {
		dpPb := ConvertDataPartitionResponse(dp)
		dpRespsPb = append(dpRespsPb, dpPb)
	}
	return &DataPartitionsViewPb{DataPartitions: dpRespsPb}
}

func ConvertDataPartitionResponsePb(dataPartition *DataPartitionResponsePb) *DataPartitionResponse {
	dp := &DataPartitionResponse{
		PartitionID:     dataPartition.PartitionID,
		Status:          int8(dataPartition.Status),
		ReplicaNum:      uint8(dataPartition.ReplicaNum),
		Hosts:           make([]string, 0),
		LeaderAddr:      dataPartition.LeaderAddr,
		Epoch:           dataPartition.Epoch,
		IsRecover:       dataPartition.IsRecover,
		IsFrozen:        dataPartition.IsFrozen,
		CreateTime:      dataPartition.CreateTime,
		MediumType:      dataPartition.MediumType,
		Total:           dataPartition.Total,
		Used:            dataPartition.Used,
		EcMigrateStatus: uint8(dataPartition.EcMigrateStatus),
		EcHosts:         dataPartition.EcHosts,
		EcDataNum:       uint8(dataPartition.EcDataNum),
		EcMaxUnitSize:   dataPartition.EcMaxUnitSize,
	}
	if dataPartition.Hosts != nil {
		dp.Hosts = dataPartition.Hosts
	}
	return dp
}

func ConvertDataPartitionResponse(dataPartition *DataPartitionResponse) *DataPartitionResponsePb {
	return &DataPartitionResponsePb{
		PartitionID:     dataPartition.PartitionID,
		Status:          int32(dataPartition.Status),
		ReplicaNum:      uint32(dataPartition.ReplicaNum),
		Hosts:           dataPartition.Hosts,
		LeaderAddr:      dataPartition.LeaderAddr,
		Epoch:           dataPartition.Epoch,
		IsRecover:       dataPartition.IsRecover,
		IsFrozen:        dataPartition.IsFrozen,
		CreateTime:      dataPartition.CreateTime,
		MediumType:      dataPartition.MediumType,
		Total:           dataPartition.Total,
		Used:            dataPartition.Used,
		EcMigrateStatus: uint32(dataPartition.EcMigrateStatus),
		EcHosts:         dataPartition.EcHosts,
		EcDataNum:       uint32(dataPartition.EcDataNum),
		EcMaxUnitSize:   dataPartition.EcMaxUnitSize,
	}
}

func ConvertMetaPartitionViews(metaPartitions []*MetaPartitionView) *MetaPartitionViewsPb {
	mpPbs := make([]*MetaPartitionViewPb, 0, len(metaPartitions))
	for _, metaPartition := range metaPartitions {
		mpPb := &MetaPartitionViewPb{
			PartitionID: metaPartition.PartitionID,
			Start:       metaPartition.Start,
			End:         metaPartition.End,
			MaxInodeID:  metaPartition.MaxInodeID,
			InodeCount:  metaPartition.InodeCount,
			DentryCount: metaPartition.DentryCount,
			MaxExistIno: metaPartition.MaxExistIno,
			IsRecover:   metaPartition.IsRecover,
			Members:     metaPartition.Members,
			Learners:    metaPartition.Learners,
			LeaderAddr:  metaPartition.LeaderAddr,
			Status:      int32(metaPartition.Status),
			StoreMode:   uint32(metaPartition.StoreMode),
			MemCount:    uint32(metaPartition.MemCount),
			RocksCount:  uint32(metaPartition.RocksCount),
		}
		mpPbs = append(mpPbs, mpPb)
	}
	return &MetaPartitionViewsPb{MetaPartitions: mpPbs}
}

func ConvertMetaPartitionViewsPb(metaPartitions *MetaPartitionViewsPb) []*MetaPartitionView {
	mps := make([]*MetaPartitionView, 0, len(metaPartitions.MetaPartitions))
	for _, metaPartition := range metaPartitions.MetaPartitions {
		mp := &MetaPartitionView{
			PartitionID: metaPartition.PartitionID,
			Start:       metaPartition.Start,
			End:         metaPartition.End,
			MaxInodeID:  metaPartition.MaxInodeID,
			InodeCount:  metaPartition.InodeCount,
			DentryCount: metaPartition.DentryCount,
			MaxExistIno: metaPartition.MaxExistIno,
			IsRecover:   metaPartition.IsRecover,
			Members:     make([]string, 0),
			Learners:    make([]string, 0),
			LeaderAddr:  metaPartition.LeaderAddr,
			Status:      int8(metaPartition.Status),
			StoreMode:   StoreMode(metaPartition.StoreMode),
			MemCount:    uint8(metaPartition.MemCount),
			RocksCount:  uint8(metaPartition.RocksCount),
		}
		if metaPartition.Members != nil {
			mp.Members = metaPartition.Members
		}
		if metaPartition.Learners != nil {
			mp.Learners = metaPartition.Learners
		}
		mps = append(mps, mp)
	}
	return mps
}

func ConvertEcPartitionResponsePbs(ecPartitions []*EcPartitionResponsePb) []*EcPartitionResponse {
	eps := make([]*EcPartitionResponse, 0, len(ecPartitions))
	for _, epPb := range ecPartitions {
		ep := &EcPartitionResponse{
			PartitionID:    epPb.PartitionID,
			Status:         int8(epPb.Status),
			ReplicaNum:     uint8(epPb.ReplicaNum),
			Hosts:          make([]string, 0),
			LeaderAddr:     epPb.LeaderAddr,
			DataUnitsNum:   uint8(epPb.DataUnitsNum),
			ParityUnitsNum: uint8(epPb.ParityUnitsNum),
		}
		if epPb.Hosts != nil {
			ep.Hosts = epPb.Hosts
		}
		eps = append(eps, ep)
	}
	return eps
}

func ConvertEcPartitionResponses(ecPartitions []*EcPartitionResponse) []*EcPartitionResponsePb {
	epPbs := make([]*EcPartitionResponsePb, 0, len(ecPartitions))
	for _, ep := range ecPartitions {
		epPb := &EcPartitionResponsePb{
			PartitionID:    ep.PartitionID,
			Status:         int32(ep.Status),
			ReplicaNum:     uint32(ep.ReplicaNum),
			Hosts:          ep.Hosts,
			LeaderAddr:     ep.LeaderAddr,
			DataUnitsNum:   uint32(ep.DataUnitsNum),
			ParityUnitsNum: uint32(ep.ParityUnitsNum),
		}
		epPbs = append(epPbs, epPb)
	}
	return epPbs
}

func ConvertVolViewPb(vv *VolViewPb) *VolView {
	volView := &VolView{
		Name:              vv.Name,
		Owner:             vv.Owner,
		Status:            uint8(vv.Status),
		FollowerRead:      vv.FollowerRead,
		ForceROW:          vv.ForceROW,
		EnableWriteCache:  vv.EnableWriteCache,
		CrossRegionHAType: CrossRegionHAType(vv.CrossRegionHAType),
		MetaPartitions:    make([]*MetaPartitionView, 0),
		DataPartitions:    make([]*DataPartitionResponse, 0),
		EcPartitions:      make([]*EcPartitionResponse, 0),
		OSSSecure:         vv.OSSSecure,
		OSSBucketPolicy:   BucketAccessPolicy(vv.OSSBucketPolicy),
		CreateTime:        vv.CreateTime,
		ConnConfig:        vv.ConnConfig,
		IsSmart:           vv.IsSmart,
		SmartEnableTime:   vv.SmartEnableTime,
		SmartRules:        make([]string, 0),
	}

	if vv.MetaPartitions != nil {
		volView.MetaPartitions = ConvertMetaPartitionViewsPb(&MetaPartitionViewsPb{MetaPartitions: vv.MetaPartitions})
	}
	if vv.DataPartitions != nil {
		volView.DataPartitions = ConvertDataPartitionsViewPb(&DataPartitionsViewPb{DataPartitions: vv.DataPartitions}).DataPartitions
	}
	if vv.EcPartitions != nil {
		volView.EcPartitions = ConvertEcPartitionResponsePbs(vv.EcPartitions)
	}
	if vv.SmartRules != nil {
		volView.SmartRules = vv.SmartRules
	}
	return volView
}

func ConvertVolView(vv *VolView) *VolViewPb {
	return &VolViewPb{
		Name:              vv.Name,
		Owner:             vv.Owner,
		Status:            uint32(vv.Status),
		FollowerRead:      vv.FollowerRead,
		ForceROW:          vv.ForceROW,
		EnableWriteCache:  vv.EnableWriteCache,
		CrossRegionHAType: uint32(vv.CrossRegionHAType),
		MetaPartitions:    ConvertMetaPartitionViews(vv.MetaPartitions).MetaPartitions,
		DataPartitions:    ConvertDataPartitionsView(&DataPartitionsView{DataPartitions: vv.DataPartitions}).DataPartitions,
		EcPartitions:      ConvertEcPartitionResponses(vv.EcPartitions),
		OSSSecure:         vv.OSSSecure,
		OSSBucketPolicy:   uint32(vv.OSSBucketPolicy),
		CreateTime:        vv.CreateTime,
		ConnConfig:        vv.ConnConfig,
		IsSmart:           vv.IsSmart,
		SmartEnableTime:   vv.SmartEnableTime,
		SmartRules:        vv.SmartRules,
	}
}

func ConvertTokensPb(tokens map[string]*TokenPb) map[string]*Token {
	res := make(map[string]*Token, len(tokens))
	for k, v := range tokens {
		t := &Token{
			TokenType: int8(v.TokenType),
			Value:     v.Value,
			VolName:   v.VolName,
		}
		res[k] = t
	}
	return res
}

func ConvertTokens(tokens map[string]*Token) map[string]*TokenPb {
	res := make(map[string]*TokenPb, len(tokens))
	for k, v := range tokens {
		t := &TokenPb{
			TokenType: int32(v.TokenType),
			Value:     v.Value,
			VolName:   v.VolName,
		}
		res[k] = t
	}
	return res
}

func ConvertSimpleVolViewPb(vv *SimpleVolViewPb) *SimpleVolView {
	simpleVolView := &SimpleVolView{
		ID:                    vv.ID,
		Name:                  vv.Name,
		Owner:                 vv.Owner,
		ZoneName:              vv.ZoneName,
		DpReplicaNum:          uint8(vv.DpReplicaNum),
		MpReplicaNum:          uint8(vv.MpReplicaNum),
		DpLearnerNum:          uint8(vv.DpLearnerNum),
		MpLearnerNum:          uint8(vv.MpLearnerNum),
		InodeCount:            vv.InodeCount,
		DentryCount:           vv.DentryCount,
		MaxMetaPartitionID:    vv.MaxMetaPartitionID,
		Status:                uint8(vv.Status),
		Capacity:              vv.Capacity,
		DpWriteableThreshold:  vv.DpWriteableThreshold,
		RwDpCnt:               int(vv.RwDpCnt),
		MpCnt:                 int(vv.MpCnt),
		DpCnt:                 int(vv.DpCnt),
		FollowerRead:          vv.FollowerRead,
		NearRead:              vv.NearRead,
		NeedToLowerReplica:    vv.NeedToLowerReplica,
		Authenticate:          vv.Authenticate,
		VolWriteMutexEnable:   vv.VolWriteMutexEnable,
		CrossZone:             vv.CrossZone,
		AutoRepair:            vv.AutoRepair,
		CreateTime:            vv.CreateTime,
		EnableToken:            vv.EnableToken,
		ForceROW:               vv.ForceROW,
		EnableWriteCache:       vv.EnableWriteCache,
		CrossRegionHAType:      CrossRegionHAType(vv.CrossRegionHAType),
		Tokens:                 ConvertTokensPb(vv.Tokens),
		Description:            vv.Description,
		DpSelectorName:         vv.DpSelectorName,
		DpSelectorParm:         vv.DpSelectorParm,
		Quorum:                 int(vv.Quorum),
		OSSBucketPolicy:        BucketAccessPolicy(vv.OSSBucketPolicy),
		DPConvertMode:          ConvertMode(vv.DPConvertMode),
		MPConvertMode:          ConvertMode(vv.MPConvertMode),
		MasterRegionZone:       vv.MasterRegionZone,
		SlaveRegionZone:        vv.SlaveRegionZone,
		ConnConfig:             vv.ConnConfig,
		ExtentCacheExpireSec:   vv.ExtentCacheExpireSec,
		DpMetricsReportConfig:  vv.DpMetricsReportConfig,
		DpFolReadDelayConfig:   vv.DpFolReadDelayConfig,
		FolReadHostWeight:      int(vv.FolReadHostWeight),
		RwMpCnt:                int(vv.RwMpCnt),
		MinWritableMPNum:       int(vv.MinWritableMPNum),
		MinWritableDPNum:       int(vv.MinWritableDPNum),
		TrashRemainingDays:     vv.TrashRemainingDays,
		DefaultStoreMode:       StoreMode(vv.DefaultStoreMode),
		ConvertState:           VolConvertState(vv.ConvertState),
		MpLayout:               vv.MpLayout,
		TotalSizeGB:            vv.TotalSizeGB,
		UsedSizeGB:             vv.UsedSizeGB,
		TotalSize:              vv.TotalSize,
		UsedSize:               vv.UsedSize,
		UsedRatio:              vv.UsedRatio,
		FileAvgSize:            vv.FileAvgSize,
		CreateStatus:           VolCreateStatus(vv.CreateStatus),
		IsSmart:                vv.IsSmart,
		SmartEnableTime:        vv.SmartEnableTime,
		SmartRules:             make([]string, 0),
		CompactTag:             vv.CompactTag,
		CompactTagModifyTime:   vv.CompactTagModifyTime,
		EcEnable:               vv.EcEnable,
		EcDataNum:              uint8(vv.EcDataNum),
		EcParityNum:            uint8(vv.EcParityNum),
		EcWaitTime:             vv.EcWaitTime,
		EcSaveTime:             vv.EcSaveTime,
		EcTimeOut:              vv.EcTimeOut,
		EcRetryWait:            vv.EcRetryWait,
		EcMaxUnitSize:          vv.EcMaxUnitSize,
		ChildFileMaxCount:      vv.ChildFileMaxCount,
		TrashCleanInterval:     vv.TrashCleanInterval,
		BatchDelInodeCnt:       vv.BatchDelInodeCnt,
		DelInodeInterval:       vv.DelInodeInterval,
		UmpCollectWay:          exporter.UMPCollectMethod(ump.CollectMethod(vv.UmpCollectWay)),
		EnableBitMapAllocator:  vv.EnableBitMapAllocator,
		TrashCleanDuration:     vv.TrashCleanDuration,
		TrashCleanMaxCount:     vv.TrashCleanMaxCount,
		NewVolName:             vv.NewVolName,
		NewVolID:               vv.NewVolID,
		OldVolName:             vv.OldVolName,
		FinalVolStatus:         uint8(vv.FinalVolStatus),
		RenameConvertStatus:    VolRenameConvertStatus(vv.RenameConvertStatus),
		MarkDeleteTime:         vv.MarkDeleteTime,
		RemoteCacheBoostEnable: vv.RemoteCacheBoostEnable,
		RemoteCacheAutoPrepare: vv.RemoteCacheAutoPrepare,
		RemoteCacheBoostPath:   vv.RemoteCacheBoostPath,
		RemoteCacheTTL:         vv.RemoteCacheTTL,
	}
	if vv.SmartRules != nil {
		simpleVolView.SmartRules = vv.SmartRules
	}
	return simpleVolView
}

func ConvertSimpleVolView(vv *SimpleVolView) *SimpleVolViewPb {
	return &SimpleVolViewPb{
		ID:                    vv.ID,
		Name:                  vv.Name,
		Owner:                 vv.Owner,
		ZoneName:              vv.ZoneName,
		DpReplicaNum:          uint32(vv.DpReplicaNum),
		MpReplicaNum:          uint32(vv.MpReplicaNum),
		DpLearnerNum:          uint32(vv.DpLearnerNum),
		MpLearnerNum:          uint32(vv.MpLearnerNum),
		InodeCount:            vv.InodeCount,
		DentryCount:           vv.DentryCount,
		MaxMetaPartitionID:    vv.MaxMetaPartitionID,
		Status:                uint32(vv.Status),
		Capacity:              vv.Capacity,
		DpWriteableThreshold:  vv.DpWriteableThreshold,
		RwDpCnt:               int64(vv.RwDpCnt),
		MpCnt:                 int64(vv.MpCnt),
		DpCnt:                 int64(vv.DpCnt),
		FollowerRead:          vv.FollowerRead,
		NearRead:              vv.NearRead,
		NeedToLowerReplica:    vv.NeedToLowerReplica,
		Authenticate:          vv.Authenticate,
		VolWriteMutexEnable:   vv.VolWriteMutexEnable,
		CrossZone:             vv.CrossZone,
		AutoRepair:            vv.AutoRepair,
		CreateTime:            vv.CreateTime,
		EnableToken:           vv.EnableToken,
		ForceROW:              vv.ForceROW,
		EnableWriteCache:      vv.EnableWriteCache,
		CrossRegionHAType:     uint32(vv.CrossRegionHAType),
		Tokens:                ConvertTokens(vv.Tokens),
		Description:           vv.Description,
		DpSelectorName:        vv.DpSelectorName,
		DpSelectorParm:        vv.DpSelectorParm,
		Quorum:                int64(vv.Quorum),
		OSSBucketPolicy:       uint32(vv.OSSBucketPolicy),
		DPConvertMode:         uint32(vv.DPConvertMode),
		MPConvertMode:         uint32(vv.MPConvertMode),
		MasterRegionZone:      vv.MasterRegionZone,
		SlaveRegionZone:       vv.SlaveRegionZone,
		ConnConfig:            vv.ConnConfig,
		ExtentCacheExpireSec:  vv.ExtentCacheExpireSec,
		DpMetricsReportConfig: vv.DpMetricsReportConfig,
		DpFolReadDelayConfig:  vv.DpFolReadDelayConfig,
		FolReadHostWeight:     int64(vv.FolReadHostWeight),
		RwMpCnt:               int64(vv.RwMpCnt),
		MinWritableMPNum:      int64(vv.MinWritableMPNum),
		MinWritableDPNum:      int64(vv.MinWritableDPNum),
		TrashRemainingDays:    vv.TrashRemainingDays,
		DefaultStoreMode:      uint32(vv.DefaultStoreMode),
		ConvertState:          uint32(vv.ConvertState),
		MpLayout:              vv.MpLayout,
		TotalSizeGB:           vv.TotalSizeGB,
		UsedSizeGB:            vv.UsedSizeGB,
		TotalSize:             vv.TotalSize,
		UsedSize:              vv.UsedSize,
		UsedRatio:             vv.UsedRatio,
		FileAvgSize:           vv.FileAvgSize,
		CreateStatus:          uint32(vv.CreateStatus),
		IsSmart:               vv.IsSmart,
		SmartEnableTime:       vv.SmartEnableTime,
		SmartRules:            vv.SmartRules,
		CompactTag:            vv.CompactTag,
		CompactTagModifyTime:  vv.CompactTagModifyTime,
		EcEnable:              vv.EcEnable,
		EcDataNum:             uint32(vv.EcDataNum),
		EcParityNum:            uint32(vv.EcParityNum),
		EcWaitTime:             vv.EcWaitTime,
		EcSaveTime:             vv.EcSaveTime,
		EcTimeOut:              vv.EcTimeOut,
		EcRetryWait:            vv.EcRetryWait,
		EcMaxUnitSize:          vv.EcMaxUnitSize,
		ChildFileMaxCount:      vv.ChildFileMaxCount,
		TrashCleanInterval:     vv.TrashCleanInterval,
		BatchDelInodeCnt:       vv.BatchDelInodeCnt,
		DelInodeInterval:       vv.DelInodeInterval,
		UmpCollectWay:          int64(vv.UmpCollectWay),
		EnableBitMapAllocator:  vv.EnableBitMapAllocator,
		TrashCleanDuration:     vv.TrashCleanDuration,
		TrashCleanMaxCount:     vv.TrashCleanMaxCount,
		NewVolName:             vv.NewVolName,
		NewVolID:               vv.NewVolID,
		OldVolName:             vv.OldVolName,
		FinalVolStatus:         uint32(vv.FinalVolStatus),
		RenameConvertStatus:    uint32(vv.RenameConvertStatus),
		MarkDeleteTime:         vv.MarkDeleteTime,
		RemoteCacheBoostEnable: vv.RemoteCacheBoostEnable,
		RemoteCacheAutoPrepare: vv.RemoteCacheAutoPrepare,
		RemoteCacheBoostPath:   vv.RemoteCacheBoostPath,
		RemoteCacheTTL:         vv.RemoteCacheTTL,
	}
}

func ConvertNodeStatInfoPb(info *NodeStatInfoPb) *NodeStatInfo {
	return &NodeStatInfo{
		TotalGB:            info.TotalGB,
		UsedGB:             info.UsedGB,
		IncreasedGB:        info.IncreasedGB,
		UsedRatio:          info.UsedRatio,
		TotalNodes:         int(info.TotalNodes),
		WritableNodes:      int(info.WritableNodes),
		HighUsedRatioNodes: int(info.HighUsedRatioNodes),
	}
}

func ConvertNodeStatInfo(info *NodeStatInfo) *NodeStatInfoPb {
	return &NodeStatInfoPb{
		TotalGB:            info.TotalGB,
		UsedGB:             info.UsedGB,
		IncreasedGB:        info.IncreasedGB,
		UsedRatio:          info.UsedRatio,
		TotalNodes:         int64(info.TotalNodes),
		WritableNodes:      int64(info.WritableNodes),
		HighUsedRatioNodes: int64(info.HighUsedRatioNodes),
	}
}

func ConvertClusterViewPb(cv *ClusterViewPb) *ClusterView {
	clusterView := &ClusterView{
		Name:                                cv.Name,
		LeaderAddr:                          cv.LeaderAddr,
		DisableAutoAlloc:                    cv.DisableAutoAlloc,
		AutoMergeNodeSet:                    cv.AutoMergeNodeSet,
		NodeSetCapacity:                     int(cv.NodeSetCapacity),
		MetaNodeThreshold:                   cv.MetaNodeThreshold,
		DpRecoverPool:                       cv.DpRecoverPool,
		MpRecoverPool:                       cv.MpRecoverPool,
		DeleteMarkDelVolInterval:            cv.DeleteMarkDelVolInterval,
		Applied:                             cv.Applied,
		MaxDataPartitionID:                  cv.MaxDataPartitionID,
		MaxMetaNodeID:                       cv.MaxMetaNodeID,
		MaxMetaPartitionID:                  cv.MaxMetaPartitionID,
		EcScrubEnable:                       cv.EcScrubEnable,
		EcMaxScrubExtents:                   uint8(cv.EcMaxScrubExtents),
		EcScrubPeriod:                       cv.EcScrubPeriod,
		EcScrubStartTime:                    cv.EcScrubStartTime,
		MaxCodecConcurrent:                  int(cv.MaxCodecConcurrent),
		VolCount:                            int(cv.VolCount),
		DataNodeStatInfo:                    ConvertNodeStatInfoPb(cv.DataNodeStatInfo),
		MetaNodeStatInfo:                    ConvertNodeStatInfoPb(cv.MetaNodeStatInfo),
		EcNodeStatInfo:                      ConvertNodeStatInfoPb(cv.EcNodeStatInfo),
		BadPartitionIDs:                     make([]BadPartitionView, 0),
		BadMetaPartitionIDs:                 make([]BadPartitionView, 0),
		BadEcPartitionIDs:                   make([]BadPartitionView, 0),
		MigratedDataPartitions:              make([]BadPartitionView, 0),
		MigratedMetaPartitions:              make([]BadPartitionView, 0),
		MetaNodes:                           make([]NodeView, 0),
		DataNodes:                           make([]NodeView, 0),
		CodEcnodes:                          make([]NodeView, 0),
		EcNodes:                             make([]NodeView, 0),
		FlashNodes:                          make([]NodeView, 0),
		DataNodeBadDisks:                    make([]DataNodeBadDisksView, 0),
		SchedulerDomain:                     cv.SchedulerDomain,
		ClientPkgAddr:                       cv.ClientPkgAddr,
		UmpJmtpAddr:                         cv.UmpJmtpAddr,
		UmpJmtpBatch:                        cv.UmpJmtpBatch,
		MetaNodeRocksdbDiskThreshold:        cv.MetaNodeRocksdbDiskThreshold,
		MetaNodeMemModeRocksdbDiskThreshold: cv.MetaNodeMemModeRocksdbDiskThreshold,
		RocksDBDiskReservedSpace:            cv.RocksDBDiskReservedSpace,
		LogMaxMB:                            cv.LogMaxMB,
		MetaRockDBWalFileSize:               cv.MetaRockDBWalFileSize,
		MetaRocksWalMemSize:                 cv.MetaRocksWalMemSize,
		MetaRocksLogSize:                    cv.MetaRocksLogSize,
		MetaRocksLogReservedTime:            cv.MetaRocksLogReservedTime,
		MetaRocksLogReservedCnt:             cv.MetaRocksLogReservedCnt,
		MetaRocksFlushWalInterval:           cv.MetaRocksFlushWalInterval,
		MetaRocksDisableFlushFlag:           cv.MetaRocksDisableFlushFlag,
		MetaRocksWalTTL:                     cv.MetaRocksWalTTL,
		MetaDelEKRecordFileMaxMB:            cv.MetaDelEKRecordFileMaxMB,
		MetaTrashCleanInterval:              cv.MetaTrashCleanInterval,
		MetaRaftLogSize:                     cv.MetaRaftLogSize,
		MetaRaftLogCap:                      cv.MetaRaftLogCap,
		BitMapAllocatorMaxUsedFactor:        cv.BitMapAllocatorMaxUsedFactor,
		BitMapAllocatorMinFreeFactor:        cv.BitMapAllocatorMinFreeFactor,
		DisableStrictVolZone:                cv.DisableStrictVolZone,
		AutoUpdatePartitionReplicaNum:       cv.AutoUpdatePartitionReplicaNum,
	}
	if cv.MetaNodes != nil {
		clusterView.MetaNodes = cv.MetaNodes
	}
	if cv.DataNodes != nil {
		clusterView.DataNodes = cv.DataNodes
	}
	if cv.CodEcnodes != nil {
		clusterView.CodEcnodes = cv.CodEcnodes
	}
	if cv.EcNodes != nil {
		clusterView.EcNodes = cv.EcNodes
	}
	if cv.BadPartitionIDs != nil {
		clusterView.BadPartitionIDs = cv.BadPartitionIDs
	}
	if cv.BadMetaPartitionIDs != nil {
		clusterView.BadMetaPartitionIDs = cv.BadMetaPartitionIDs
	}
	if cv.BadEcPartitionIDs != nil {
		clusterView.BadEcPartitionIDs = cv.BadEcPartitionIDs
	}
	if cv.MigratedDataPartitions != nil {
		clusterView.MigratedDataPartitions = cv.MigratedDataPartitions
	}
	if cv.MigratedMetaPartitions != nil {
		clusterView.MigratedMetaPartitions = cv.MigratedMetaPartitions
	}
	if cv.DataNodeBadDisks != nil {
		clusterView.DataNodeBadDisks = cv.DataNodeBadDisks
	}
	if cv.FlashNodes != nil {
		clusterView.FlashNodes = cv.FlashNodes
	}
	return clusterView
}

func ConvertClusterView(cv *ClusterView) *ClusterViewPb {
	return &ClusterViewPb{
		Name:                                cv.Name,
		LeaderAddr:                          cv.LeaderAddr,
		DisableAutoAlloc:                    cv.DisableAutoAlloc,
		AutoMergeNodeSet:                    cv.AutoMergeNodeSet,
		NodeSetCapacity:                     int64(cv.NodeSetCapacity),
		MetaNodeThreshold:                   cv.MetaNodeThreshold,
		DpRecoverPool:                       cv.DpRecoverPool,
		MpRecoverPool:                       cv.MpRecoverPool,
		DeleteMarkDelVolInterval:            cv.DeleteMarkDelVolInterval,
		Applied:                             cv.Applied,
		MaxDataPartitionID:                  cv.MaxDataPartitionID,
		MaxMetaNodeID:                       cv.MaxMetaNodeID,
		MaxMetaPartitionID:                  cv.MaxMetaPartitionID,
		EcScrubEnable:                       cv.EcScrubEnable,
		EcMaxScrubExtents:                   uint32(cv.EcMaxScrubExtents),
		EcScrubPeriod:                       cv.EcScrubPeriod,
		EcScrubStartTime:                    cv.EcScrubStartTime,
		MaxCodecConcurrent:                  int64(cv.MaxCodecConcurrent),
		VolCount:                            int64(cv.VolCount),
		DataNodeStatInfo:                    ConvertNodeStatInfo(cv.DataNodeStatInfo),
		MetaNodeStatInfo:                    ConvertNodeStatInfo(cv.MetaNodeStatInfo),
		EcNodeStatInfo:                      ConvertNodeStatInfo(cv.EcNodeStatInfo),
		BadPartitionIDs:                     cv.BadPartitionIDs,
		BadMetaPartitionIDs:                 cv.BadMetaPartitionIDs,
		BadEcPartitionIDs:                   cv.BadEcPartitionIDs,
		MigratedDataPartitions:              cv.MigratedDataPartitions,
		MigratedMetaPartitions:              cv.MigratedMetaPartitions,
		MetaNodes:                           cv.MetaNodes,
		DataNodes:                           cv.DataNodes,
		CodEcnodes:                          cv.CodEcnodes,
		EcNodes:                             cv.EcNodes,
		DataNodeBadDisks:                    cv.DataNodeBadDisks,
		SchedulerDomain:                     cv.SchedulerDomain,
		ClientPkgAddr:                       cv.ClientPkgAddr,
		UmpJmtpAddr:                         cv.UmpJmtpAddr,
		UmpJmtpBatch:                        cv.UmpJmtpBatch,
		MetaNodeRocksdbDiskThreshold:        cv.MetaNodeRocksdbDiskThreshold,
		MetaNodeMemModeRocksdbDiskThreshold: cv.MetaNodeMemModeRocksdbDiskThreshold,
		RocksDBDiskReservedSpace:            cv.RocksDBDiskReservedSpace,
		LogMaxMB:                            cv.LogMaxMB,
		MetaRockDBWalFileSize:               cv.MetaRockDBWalFileSize,
		MetaRocksWalMemSize:                 cv.MetaRocksWalMemSize,
		MetaRocksLogSize:                    cv.MetaRocksLogSize,
		MetaRocksLogReservedTime:            cv.MetaRocksLogReservedTime,
		MetaRocksLogReservedCnt:             cv.MetaRocksLogReservedCnt,
		MetaRocksFlushWalInterval:           cv.MetaRocksFlushWalInterval,
		MetaRocksDisableFlushFlag:           cv.MetaRocksDisableFlushFlag,
		MetaRocksWalTTL:                     cv.MetaRocksWalTTL,
		MetaDelEKRecordFileMaxMB:            cv.MetaDelEKRecordFileMaxMB,
		MetaTrashCleanInterval:              cv.MetaTrashCleanInterval,
		MetaRaftLogSize:                     cv.MetaRaftLogSize,
		MetaRaftLogCap:                      cv.MetaRaftLogCap,
		BitMapAllocatorMaxUsedFactor:        cv.BitMapAllocatorMaxUsedFactor,
		BitMapAllocatorMinFreeFactor:        cv.BitMapAllocatorMinFreeFactor,
		DisableStrictVolZone:                cv.DisableStrictVolZone,
		AutoUpdatePartitionReplicaNum:       cv.AutoUpdatePartitionReplicaNum,
		FlashNodes:                          cv.FlashNodes,
	}
}
