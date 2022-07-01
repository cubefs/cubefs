package proto

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
)

type SmartVolumeView struct {
	Cluster      string
	SmartVolumes []*SmartVolume
}

func NewSmartVolumeView(cluster string, smartVolumes []*SmartVolume) *SmartVolumeView {
	return &SmartVolumeView{
		Cluster:      cluster,
		SmartVolumes: smartVolumes,
	}
}

type SmartVolume struct {
	ClusterId         string
	Name              string
	Owner             string
	Status            uint8
	SmartRules        []string
	TotalSize         uint64
	UsedSize          uint64
	CreateTime        int64
	SmartEnableTime   int64
	FollowerRead      bool
	ForceROW          bool
	CrossRegionHAType CrossRegionHAType
	MetaPartitions    []*MetaPartitionView
	DataPartitions    []*DataPartitionResponse
	OSSSecure         *OSSSecure
	OSSBucketPolicy   BucketAccessPolicy
	LayerPolicies     map[LayerType][]interface{}
}

type ActionMetricsTaskInfo struct {
	ModuleType   string     // 指标维度 1: dp 2: mp
	Action       int        // 操作行为
	DataType     int8       // 1: count 2: size
	TimeType     int8       // 1: minute 2: hour 3: day
	Threshold    uint64     // count表示操作次数，size表示请求数据总量
	LessCount    int        // 最小连续触发次数
	TargetMedium MediumType // 目标介质类型，1:SSD 2:HDD 3:EC
	Metrics      []*HBaseMetricsData
	SourceHosts  []string
	TargetHosts  []string
}

func NewActionMetricsTaskInfo(lp *LayerPolicyActionMetrics, metrics []*HBaseMetricsData, hosts []string) (info string, err error) {
	taskInfo := &ActionMetricsTaskInfo{
		ModuleType:   lp.ModuleType,
		Action:       lp.Action,
		DataType:     lp.DataType,
		TimeType:     lp.TimeType,
		Threshold:    lp.Threshold,
		LessCount:    lp.LessCount,
		TargetMedium: lp.TargetMedium,
		Metrics:      metrics,
		SourceHosts:  hosts,
	}
	if info, err = taskInfo.Marshall(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", LayerTypeActionMetricsString, info), nil
}

func (mt *ActionMetricsTaskInfo) Marshall() (res string, err error) {
	data, err := json.Marshal(mt)
	if err != nil {
		return
	}
	return string(data), nil
}

func UnmarshallActionMetricsTaskInfo(str string) (info *ActionMetricsTaskInfo, err error) {
	info = &ActionMetricsTaskInfo{}
	err = json.Unmarshal([]byte(str), info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

type DPCreateTimeTaskInfo struct {
	TimeType     int8       // 1: minute 2: hour 3: day
	TimeValue    int64      // count: action times，size: action data size
	TargetMedium MediumType // targetMediumType，1:SSD 2:HDD 3:EC
	EffectValue  int64      // the timestamp of data partition create time
	SourceHosts  []string
	TargetHosts  []string
}

func NewDPCreateTimeTaskInfo(lp *LayerPolicyDPCreateTime, dpCreateTime int64, hosts []string) (info string, err error) {
	taskInfo := &DPCreateTimeTaskInfo{
		TimeType:     lp.TimeType,
		TimeValue:    lp.TimeValue,
		TargetMedium: lp.TargetMedium,
		EffectValue:  dpCreateTime,
		SourceHosts:  hosts,
	}
	if info, err = taskInfo.Marshall(); err != nil {
		return "", err
	}
	log.LogDebugf("[NewDPCreateTimeTaskInfo] info(%v)", info)
	return fmt.Sprintf("%s:%s", LayerTypeDPCreateTimeString, info), nil
}

func (dpc *DPCreateTimeTaskInfo) Marshall() (res string, err error) {
	data, err := json.Marshal(dpc)
	if err != nil {
		return
	}
	return string(data), nil
}

func UnmarshallDPCreateTimeTaskInfo(str string) (info *DPCreateTimeTaskInfo, err error) {
	info = &DPCreateTimeTaskInfo{}
	err = json.Unmarshal([]byte(str), info)
	if err != nil {
		return nil, err
	}
	return info, nil
}
