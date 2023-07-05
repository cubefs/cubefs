package proto

func (from *DiskInfoPb) ConvertToView() *DiskInfo {
	return &DiskInfo{
		Total:         from.Total,
		Used:          from.Used,
		ReservedSpace: from.ReservedSpace,
		Status:        int(from.Status),
		Path:          from.Path,
		UsageRatio:    float64(from.UsageRatio),
	}
}

func (from *PartitionReportPb) ConvertToView() *PartitionReport {
	return &PartitionReport{
		VolName:         from.VolName,
		PartitionID:     from.PartitionID,
		PartitionStatus: int(from.PartitionStatus),
		Total:           from.Total,
		Used:            from.Used,
		DiskPath:        from.DiskPath,
		IsLeader:        from.IsLeader,
		ExtentCount:     int(from.ExtentCount),
		NeedCompare:     from.NeedCompare,
		IsLearner:       from.IsLearner,
		LastUpdateTime:  from.LastUpdateTime,
		IsRecover:       from.IsRecover,
	}
}

func (from *DataNodeHeartbeatResponsePb) ConvertToView() *DataNodeHeartbeatResponse {
	return &DataNodeHeartbeatResponse{
		Total:               from.Total,
		Used:                from.Used,
		Available:           from.Available,
		TotalPartitionSize:  from.TotalPartitionSize,
		RemainingCapacity:   from.RemainingCapacity,
		CreatedPartitionCnt: from.CreatedPartitionCnt,
		MaxCapacity:         from.MaxCapacity,
		HttpPort:            from.HttpPort,
		ZoneName:            from.ZoneName,
		PartitionReports: func(reports []*PartitionReportPb) []*PartitionReport {
			res := make([]*PartitionReport, 0, len(reports))
			for _, report := range reports {
				res = append(res, report.ConvertToView())
			}
			return res
		}(from.PartitionReports),
		Status:   uint8(from.Status),
		Result:   from.Result,
		BadDisks: from.BadDisks,
		DiskInfos: func(diskInfos map[string]*DiskInfoPb) map[string]*DiskInfo {
			res := make(map[string]*DiskInfo)
			for _, disk := range diskInfos {
				res[disk.Path] = disk.ConvertToView()
			}
			return res
		}(from.DiskInfos),
		Version: from.Version,
	}
}

func (from *MetaPartitionReportPb) ConvertToView() *MetaPartitionReport {
	return &MetaPartitionReport{
		PartitionID:       from.PartitionID,
		Start:             from.Start,
		End:               from.End,
		Status:            int(from.Status),
		MaxInodeID:        from.MaxInodeID,
		IsLeader:          from.IsLeader,
		VolName:           from.VolName,
		InodeCnt:          from.InodeCnt,
		DentryCnt:         from.DentryCnt,
		DelInodeCnt:       from.DelInodeCnt,
		DelDentryCnt:      from.DelDentryCnt,
		IsLearner:         from.IsLearner,
		ExistMaxInodeID:   from.ExistMaxInodeID,
		StoreMode:         StoreMode(from.StoreMode),
		ApplyId:           from.ApplyId,
		IsRecover:         from.IsRecover,
		AllocatorInUseCnt: from.AllocatorInUseCnt,
	}
}

func (from *MetaNodeDiskInfoPb) ConvertToView() *MetaNodeDiskInfo {
	return &MetaNodeDiskInfo{
		Path:       from.Path,
		Total:      from.Total,
		Used:       from.Used,
		UsageRatio: float64(from.UsageRatio),
		Status:     int8(from.Status),
		MPCount:    int(from.MPCount),
	}
}

func (from *MetaNodeHeartbeatResponsePb) ConvertToView() *MetaNodeHeartbeatResponse {
	return &MetaNodeHeartbeatResponse{
		ZoneName: from.ZoneName,
		Total:    from.Total,
		Used:     from.Used,
		MetaPartitionReports: func(reports []*MetaPartitionReportPb) []*MetaPartitionReport {
			res := make([]*MetaPartitionReport, 0, len(reports))
			for _, report := range reports {
				res = append(res, report.ConvertToView())
			}
			return res
		}(from.MetaPartitionReports),
		Status:   uint8(from.Status),
		ProfPort: from.ProfPort,
		Result:   from.Result,
		RocksDBDiskInfo: func(disks []*MetaNodeDiskInfoPb) []*MetaNodeDiskInfo {
			res := make([]*MetaNodeDiskInfo, 0, len(disks))
			for _, disk := range disks {
				res = append(res, disk.ConvertToView())
			}
			return res
		}(from.RocksDBDiskInfo),
		Version: from.Version,
	}
}

func (from *HeartbeatAdminTaskPb) ConvertToView() *AdminTask {
	return &AdminTask{
		ID:              from.ID,
		PartitionID:     from.PartitionID,
		OpCode:          uint8(from.OpCode),
		OperatorAddr:    from.OperatorAddr,
		Status:          int8(from.Status),
		SendTime:        from.SendTime,
		CreateTime:      from.CreateTime,
		SendCount:       uint8(from.SendCount),
		ReserveResource: from.ReserveResource,
	}
}

func (from *AdminTask) ConvertToPb() *HeartbeatAdminTaskPb {
	return &HeartbeatAdminTaskPb{
		ID:              from.ID,
		PartitionID:     from.PartitionID,
		OpCode:          uint32(from.OpCode),
		OperatorAddr:    from.OperatorAddr,
		Status:          int32(from.Status),
		SendTime:        from.SendTime,
		CreateTime:      from.CreateTime,
		SendCount:       uint32(from.SendCount),
		ReserveResource: from.ReserveResource,
	}
}

func (from *HeartBeatRequest) ConvertToPb() *HeartBeatRequestPb {
	return &HeartBeatRequestPb{
		CurrTime:   from.CurrTime,
		MasterAddr: from.MasterAddr,
	}
}
