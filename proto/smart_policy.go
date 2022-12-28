package proto

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"strconv"
	"strings"
	"time"
)

type LayerType uint8

const (
	VolumeMaxSmartRules = 10
)

const (
	LayerTypeActionMetrics       LayerType = 1
	LayerTypeDPCreateTime        LayerType = 2
	LayerTypeActionMetricsString           = "actionMetrics"
	LayerTypeDPCreateTimeString            = "dpCreateTime"
)

func CheckLayerPolicy(cluster, volName string, smartRules []string) (err error) {
	if smartRules == nil || len(smartRules) == 0 {
		return errors.New("[CheckLayerPolicy], smart rules is nil")
	}
	if len(smartRules) > VolumeMaxSmartRules {
		return errors.New(fmt.Sprintf("too many smart rules, maxRules(%v), smartRules(%v)", VolumeMaxSmartRules, len(smartRules)))
	}
	var lt LayerType
	for _, rule := range smartRules {
		if lt, err = ParseLayerType(rule); err != nil {
			return errors.New(fmt.Sprintf("%s, invalidRule(%s)", err.Error(), rule))
		}
		switch lt {
		case LayerTypeActionMetrics:
			err = CheckActionMetrics(cluster, volName, rule)
		case LayerTypeDPCreateTime:
			err = CheckDPCreateTime(cluster, volName, rule)
		}
		if err != nil {
			return errors.New(fmt.Sprintf("%s, invalidRule(%s)", err.Error(), rule))
		}
	}

	m := NewLayerPolicyActionMetrics(cluster, volName)
	var tm MediumType
	for _, rule := range smartRules {
		lt, _ = ParseLayerType(rule)
		if lt == LayerTypeActionMetrics {
			items := strings.Split(rule, ":")
			_ = m.ParseTargetMedium(items[7])
		}
		if tm != 0 && tm != m.TargetMedium {
			return errors.New("all action metrics target medium type not same")
		}
		tm = m.TargetMedium
	}
	return
}

func ParseLayerType(originRule string) (lt LayerType, err error) {
	originRule = strings.ReplaceAll(originRule, " ", "")
	items := strings.Split(originRule, ":")
	if len(items) <= 0 {
		log.LogErrorf("invalid origin rule: %v", originRule)
		return 0, errors.New("invalid origin rule")
	}
	itemFirst := items[0]
	if strings.ToLower(itemFirst) == strings.ToLower(LayerTypeActionMetricsString) {
		return LayerTypeActionMetrics, nil
	}
	if strings.ToLower(itemFirst) == strings.ToLower(LayerTypeDPCreateTimeString) {
		return LayerTypeDPCreateTime, nil
	}
	return 0, errors.New("invalid layer type")
}

func CheckActionMetrics(cluster, volName, originRule string) (err error) {
	m := NewLayerPolicyActionMetrics(cluster, volName)
	return m.Parse(originRule)
}

func CheckDPCreateTime(cluster, volName, rule string) (err error) {
	c := NewLayerPolicyDPCreateTime(cluster, volName)
	return c.Parse(rule)
}

func ParseLayerPolicy(cluster, volName, originRule string) (lt LayerType, layerPolicy interface{}, err error) {
	originRule = strings.ReplaceAll(originRule, " ", "")
	lt, err = ParseLayerType(originRule)
	if err != nil {
		log.LogErrorf("[ParseLayerPolicy] parse layer type failed, cluster(%v) ,volumeName(%v), originRule(%v), err(%v)",
			cluster, volName, originRule, err)
		return
	}
	switch lt {
	case LayerTypeActionMetrics:
		am := NewLayerPolicyActionMetrics(cluster, volName)
		if err = am.Parse(originRule); err != nil {
			log.LogErrorf("[ParseLayerPolicy] parse action metrics layer policy failed, cluster(%v) ,volumeName(%v), originRule(%v), err(%v)",
				cluster, volName, originRule, err)
			return
		}
		return lt, am, nil
	case LayerTypeDPCreateTime:
		dpc := NewLayerPolicyDPCreateTime(cluster, volName)
		if err = dpc.Parse(originRule); err != nil {
			log.LogErrorf("[ParseLayerPolicy] parse data partition create time layer policy failed, cluster(%v) ,volumeName(%v), originRule(%v), err(%v)",
				cluster, volName, originRule, err)
			return
		}
		return lt, dpc, nil
	}
	return
}

type LayerPolicy interface {
	Parse(originRule string) (err error)
	CheckDPMigrated(dp *DataPartitionResponse, i interface{}) (res bool)
	String() string
}

type LayerPolicyActionMetrics struct {
	ClusterId    string
	VolumeName   string
	OriginRule   string     // actionMetrics:dp:read:count:minute:2000:5:hdd
	ModuleType   string     // 指标维度 1: dp 2: mp
	Action       int        // 操作行为
	DataType     int8       // 1: count 2: size
	TimeType     int8       // 1: minute 2: hour 3: day
	Threshold    uint64     // count表示操作次数，size表示请求数据总量
	LessCount    int        // 最小连续触发次数
	TargetMedium MediumType // targetMediumType，1:SSD 2:HDD 3:EC
}

func (m *LayerPolicyActionMetrics) Parse(originRule string) (err error) {
	items := strings.Split(originRule, ":")
	m.OriginRule = originRule
	if len(items) != 8 {
		log.LogErrorf("[LayerPolicyActionMetrics Parse] invalid layer originRule, cluster(%v) ,volumeName(%v), originRule(%v)",
			m.ClusterId, m.VolumeName, originRule)
		return errors.New("smart rule items is not enough")
	}
	if err = m.ParseModuleType(items[1]); err != nil {
		log.LogErrorf("[LayerPolicyActionMetrics Parse] invalid layer originRule, invalid module type, cluster(%v), volumeName(%v), originRule(%v), moduleType(%v), err(%v)",
			m.ClusterId, m.VolumeName, originRule, items[1], err)
		return err
	}
	if err = m.ParseAction(items[2]); err != nil {
		log.LogErrorf("[LayerPolicyActionMetrics Parse] invalid layer originRule, invalid action, cluster(%v), volumeName(%v), originRule(%v), action(%v), err(%v)",
			m.ClusterId, m.VolumeName, originRule, items[2], err)
		return err
	}
	if err = m.ParseDataType(items[3]); err != nil {
		log.LogErrorf("[LayerPolicyActionMetrics Parse] invalid layer originRule, invalid data type, cluster(%v), volumeName(%v), originRule(%v), dataType(%v), err(%v)",
			m.ClusterId, m.VolumeName, originRule, items[3], err)
		return err
	}
	if err = m.ParseTimeType(items[4]); err != nil {
		log.LogErrorf("[LayerPolicyActionMetrics Parse] invalid layer originRule, invalid time type, cluster(%v), volumeName(%v), originRule(%v), timeType(%v), err(%v)",
			m.ClusterId, m.VolumeName, originRule, items[4], err)
		return err
	}
	if err = m.ParseThreshold(items[5]); err != nil {
		log.LogErrorf("[LayerPolicyActionMetrics Parse] invalid layer originRule, invalid threshold, cluster(%v), volumeName(%v), originRule(%v), threshold(%v), err(%v)",
			m.ClusterId, m.VolumeName, originRule, items[5], err)
		return err
	}
	if err = m.ParseLessCount(items[6]); err != nil {
		log.LogErrorf("[LayerPolicyActionMetrics Parse] invalid layer originRule, invalid less count, cluster(%v), volumeName(%v), originRule(%v), lessCount(%v), err(%v)",
			m.ClusterId, m.VolumeName, originRule, items[6], err)
		return err
	}
	if err = m.ParseTargetMedium(items[7]); err != nil {
		log.LogErrorf("[LayerPolicyActionMetrics Parse] invalid layer originRule, invalid medium type, cluster(%v), volumeName(%v), originRule(%v), targetMedium(%v), err(%v)",
			m.ClusterId, m.VolumeName, originRule, items[7], err)
		return err
	}
	return nil
}

func (m *LayerPolicyActionMetrics) CheckDPMigrated(dp *DataPartitionResponse, metrics interface{}) bool {
	log.LogInfof("[LayerPolicyActionMetrics CheckDPMigrated] cluster(%v), volName(%v), dpId(%v), policy(%v), metrics(%v)",
		m.ClusterId, m.VolumeName, dp.PartitionID, m.String(), metrics)
	dpMediumType, _ := StrToMediumType(dp.MediumType)
	if dpMediumType >= m.TargetMedium {
		log.LogInfof("[LayerPolicyActionMetrics CheckDPMigrated] dp medium type is more then target medium type, meed not migrate, "+
			"cluster(%v), volName(%v), dpId(%v), dpMediumType(%v), TargetMedium(%v)", m.ClusterId, m.VolumeName, dp.PartitionID, dp.MediumType, m.TargetMedium)
		return false
	}
	metricsData, ok := metrics.([]*HBaseMetricsData)
	if !ok {
		log.LogInfof("[LayerPolicyActionMetrics CheckDPMigrated] param metrics data is invalid, "+
			"cluster(%v), volName(%v), dpId(%v), metrics(%v)", m.ClusterId, m.VolumeName, dp.PartitionID, metrics)
		return false
	}
	for _, item := range metricsData {
		if (m.DataType == DataTypeCount && item.Num >= m.Threshold) ||
			m.DataType == DataTypeSize && item.Size >= m.Threshold {
			return false
		}
	}
	return true
}

func NewLayerPolicyActionMetrics(cluster, vn string) *LayerPolicyActionMetrics {
	return &LayerPolicyActionMetrics{
		ClusterId:  cluster,
		VolumeName: vn,
	}
}

func (m *LayerPolicyActionMetrics) ParseModuleType(moduleType string) error {
	mt := strings.ToLower(moduleType)
	if mt != ModuleTypeMeta && mt != ModuleTypeData {
		return errors.New("invalid module type")
	}
	m.ModuleType = moduleType
	return nil
}

func (m *LayerPolicyActionMetrics) ParseAction(action string) error {
	if m.ModuleType == ModuleTypeMeta {
		metaAction := constructMetaAction()
		if _, ok := metaAction[action]; !ok {
			return errors.New("invalid meta partition action")
		}
		m.Action = metaAction[action]
		return nil
	}
	if m.ModuleType == ModuleTypeData {
		dataAction := constructDataAction()
		if _, ok := dataAction[action]; !ok {
			return errors.New("invalid data partition action")
		}
		m.Action = dataAction[action]
		return nil
	}
	return errors.New("module type can not be empty")
}

func constructDataAction() map[string]int {
	actionData := make(map[string]int)
	for key, value := range ActionDataMap {
		actionData[value] = key
	}
	return actionData
}

func constructMetaAction() map[string]int {
	actionData := make(map[string]int)
	for key, value := range ActionMetaMap {
		actionData[value] = key
	}
	return actionData
}

func (m *LayerPolicyActionMetrics) ParseDataType(dataType string) error {
	dataType = strings.ToLower(dataType)
	if dataType == DataTypeStringCount {
		m.DataType = DataTypeCount
		return nil
	}
	if dataType == DataTypeStringSize {
		m.DataType = DataTypeSize
		return nil
	}
	return errors.New("invalid data type")
}

func (m *LayerPolicyActionMetrics) ParseTimeType(timeType string) error {
	timeType = strings.ToLower(timeType)
	if timeType == ActionMetricsTimeTypeStringMinute {
		m.TimeType = ActionMetricsTimeTypeMinute
		return nil
	}
	if timeType == ActionMetricsTimeTypeStringHour {
		m.TimeType = ActionMetricsTimeTypeHour
		return nil
	}
	if timeType == ActionMetricsTimeTypeStringDay {
		m.TimeType = ActionMetricsTimeTypeDay
		return nil
	}
	return errors.New("invalid time type")
}

func (m *LayerPolicyActionMetrics) ParseThreshold(thresholdString string) error {
	threshold, err := strconv.ParseUint(thresholdString, 10, 64)
	if err != nil {
		return err
	}
	m.Threshold = threshold
	return nil
}

func (m *LayerPolicyActionMetrics) ParseLessCount(lessCountString string) error {
	lessCount, err := strconv.Atoi(lessCountString)
	if err != nil {
		return err
	}
	m.LessCount = lessCount
	return nil
}

func (m *LayerPolicyActionMetrics) ParseTargetMedium(targetMediumString string) error {
	mt, err := parseMediumType(targetMediumString)
	if err != nil {
		return err
	}
	m.TargetMedium = mt
	return nil
}

func (m *LayerPolicyActionMetrics) String() string {
	sb := strings.Builder{}
	sb.WriteString("ClusterId: ")
	sb.WriteString(m.ClusterId)
	sb.WriteString(", VolumeName: ")
	sb.WriteString(m.VolumeName)
	sb.WriteString(", ModuleType: ")
	sb.WriteString(m.ModuleType)
	sb.WriteString(", Action: ")
	sb.WriteString(strconv.Itoa(m.Action))
	sb.WriteString(", DataType: ")
	sb.WriteString(strconv.Itoa(int(m.DataType)))
	sb.WriteString(", TimeType: ")
	sb.WriteString(strconv.Itoa(int(m.TimeType)))
	sb.WriteString(", Threshold: ")
	sb.WriteString(strconv.FormatUint(m.Threshold, 10))
	sb.WriteString(", LessCount: ")
	sb.WriteString(strconv.Itoa(m.LessCount))
	sb.WriteString(", TargetMedium: ")
	sb.WriteString(strconv.Itoa(int(m.TargetMedium)))
	return sb.String()
}

type LayerPolicyDPCreateTime struct {
	ClusterId  string
	VolumeName string
	OriginRule string // dpCreateTime:timestamp:1653486964:HDD  dpCreateTime:days:30:HDD
	TimeType   int8   // 1: fixed timestamp(unit: second),  2: the days since dp be created(unit: days)
	// TimeType = 1, dp should be migrated if dp create time is earlier then this value
	// TimeType = 2, dp should be migrated if the days since dp be created is more then this value
	TimeValue    int64
	TargetMedium MediumType //targetMediumType, 1:SSD 2:HDD 3:EC
}

func NewLayerPolicyDPCreateTime(cluster, volName string) *LayerPolicyDPCreateTime {
	return &LayerPolicyDPCreateTime{
		ClusterId:  cluster,
		VolumeName: volName,
	}
}

func (c *LayerPolicyDPCreateTime) Parse(originRule string) (err error) {
	items := strings.Split(originRule, ":")
	c.OriginRule = originRule
	if len(items) != 4 {
		log.LogErrorf("[LayerPolicyDPCreateTime Parse] invalid layer origin rule, cluster(%v) ,volumeName(%v), originRule(%v)",
			c.ClusterId, c.VolumeName, originRule)
		return errors.New("smart rule items is not enough")
	}
	if err = c.ParseTimeType(items[1]); err != nil {
		log.LogErrorf("[LayerPolicyDPCreateTime Parse] invalid layer origin rule, invalid time type, cluster(%v), volumeName(%v), originRule(%v), timeType(%v), err(%v)",
			c.ClusterId, c.VolumeName, originRule, items[1], err)
		return err
	}
	if err = c.ParseTimeValue(items[2]); err != nil {
		log.LogErrorf("[LayerPolicyDPCreateTime Parse] invalid layer origin rule, invalid time value, cluster(%v), volumeName(%v), originRule(%v), timeValue(%v), err(%v)",
			c.ClusterId, c.VolumeName, originRule, items[2], err)
		return err
	}
	if err = c.ParseTargetMedium(items[3]); err != nil {
		log.LogErrorf("[LayerPolicyDPCreateTime Parse] invalid layer origin rule, invalid target medium, cluster(%v), volumeName(%v), originRule(%v), targetMedium(%v), err(%v)",
			c.ClusterId, c.VolumeName, originRule, items[3], err)
		return err
	}
	return nil
}

func (c *LayerPolicyDPCreateTime) ParseTimeType(timeType string) error {
	timeType = strings.ToLower(timeType)
	if timeType == DPCreateTimeTypeStringTimestamp {
		c.TimeType = DPCreateTimeTypeTimestamp
		return nil
	}
	if timeType == DPCreateTimeTypeStringDays {
		c.TimeType = DPCreateTimeTypeDays
		return nil
	}
	return errors.New("invalid time type")
}

func (c *LayerPolicyDPCreateTime) ParseTimeValue(timeValue string) error {
	tv, err := strconv.ParseInt(timeValue, 10, 64)
	if err != nil {
		return err
	}
	c.TimeValue = tv
	return nil
}

func (c *LayerPolicyDPCreateTime) String() string {
	sb := strings.Builder{}
	sb.WriteString("ClusterId: ")
	sb.WriteString(c.ClusterId)
	sb.WriteString(", VolumeName: ")
	sb.WriteString(c.VolumeName)
	sb.WriteString(", TimeType: ")
	sb.WriteString(strconv.Itoa(int(c.TimeType)))
	sb.WriteString(", TimeValue: ")
	sb.WriteString(strconv.FormatInt(c.TimeValue, 10))
	sb.WriteString(", TargetMedium: ")
	sb.WriteString(strconv.Itoa(int(c.TargetMedium)))
	return sb.String()
}

func (c *LayerPolicyDPCreateTime) CheckDPMigrated(dp *DataPartitionResponse, originRule interface{}) bool {
	log.LogDebugf("[LayerPolicyDPCreateTime CheckDPMigrated], cluster(%v), volName(%v), dpId(%v), dpCreateTime(%v), policy(%v)",
		c.ClusterId, c.VolumeName, dp.PartitionID, dp.CreateTime, c.String())
	dpMediumType, _ := StrToMediumType(dp.MediumType)
	if dpMediumType >= c.TargetMedium {
		return false
	}
	if c.TimeType == DPCreateTimeTypeDays {
		diffDays := (time.Now().Unix() - dp.CreateTime) / 60 / 60 / 24
		if diffDays > c.TimeValue {
			return true
		}
	}
	if c.TimeType == DPCreateTimeTypeTimestamp && c.TimeValue > dp.CreateTime {
		return true
	}
	return false
}

func (c *LayerPolicyDPCreateTime) ParseTargetMedium(targetMediumString string) error {
	mt, err := parseMediumType(targetMediumString)
	if err != nil {
		return err
	}
	c.TargetMedium = mt
	return nil
}

func parseMediumType(mediumString string) (mediumType MediumType, err error) {
	mediumString = strings.ToLower(mediumString)
	if mediumString == MediumSSDName {
		return MediumSSD, nil
	}
	if mediumString == MediumHDDName {
		return MediumHDD, nil
	}
	if mediumString == MediumECName {
		return MediumEC, nil
	}
	return 0, errors.New("invalid target medium type")
}
