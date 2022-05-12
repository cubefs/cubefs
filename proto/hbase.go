package proto

import (
	"errors"
	"strconv"
)

const (
	ParamNameCluster     = "cluster"
	ParamNameVolume      = "vol"
	ParamNamePartitionID = "partitionID"
	ParamNameAction      = "action"
	ParamNameTable       = "table"
	ParamNameStart       = "start"
	ParamNameEnd         = "end"
	ParamNameLimit       = "limit"
	ParamNameMilliSecond = "ms"
)

const (
	TableNameMinute = "minute"
	TableNameHour   = "hour"
	TableNameDay    = "day"
)

const (
	DefaultHBaseSelectLimit = 100
)

type HBaseRequest struct {
	ClusterId       string
	VolumeName      string
	DataPartitionId uint64
	MetaPartitionId uint64
	Action          int
	TimeType        int8
	StartTime       string
	StopTime        string
	Limit           int
}

type HBaseResponseView struct {
	Response []*HBaseMetricsData
}

type HBaseMetricsData struct {
	Time string
	Num  uint64
	Size uint64
}

func NewHBaseDPRequest(clusterId, volName string, dpId uint64, timeType int8, action int, start, stop string, limit int) *HBaseRequest {
	return &HBaseRequest{
		ClusterId:       clusterId,
		VolumeName:      volName,
		DataPartitionId: dpId,
		Action:          action,
		TimeType:        timeType,
		StartTime:       start,
		StopTime:        stop,
		Limit:           limit,
	}
}

func NewHBaseMPRequest(clusterId, volName string, mpId uint64, action int, start, stop string) *HBaseRequest {
	return &HBaseRequest{
		ClusterId:       clusterId,
		VolumeName:      volName,
		MetaPartitionId: mpId,
		Action:          action,
		StartTime:       start,
		StopTime:        stop,
	}
}

func (hbr *HBaseRequest) ToParam() (params map[string]string, err error) {
	var tableName string
	switch hbr.TimeType {
	case ActionMetricsTimeTypeMinute:
		tableName = TableNameMinute
		break
	case ActionMetricsTimeTypeHour:
		tableName = TableNameHour
		break
	case ActionMetricsTimeTypeDay:
		tableName = TableNameDay
		break
	default:
		return nil, errors.New("invalid time type")
	}
	params = make(map[string]string)
	params[ParamNameCluster] = hbr.ClusterId
	params[ParamNameVolume] = hbr.VolumeName
	params[ParamNamePartitionID] = strconv.FormatUint(hbr.DataPartitionId, 10)
	params[ParamNameAction] = strconv.Itoa(hbr.Action)
	params[ParamNameTable] = tableName
	params[ParamNameStart] = hbr.StartTime
	params[ParamNameEnd] = hbr.StopTime
	if hbr.Limit <= 0 {
		hbr.Limit = DefaultHBaseSelectLimit
	}
	params[ParamNameLimit] = strconv.Itoa(hbr.Limit)
	return
}
