package multirate

import (
	"strconv"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestLimiterManager(t *testing.T) {
	var (
		zone               = "default"
		vol                = "ltptest"
		ratio       uint64 = 80
		limit              = []int64{1, 2, 3, 4}
		concurrency uint64 = 10
	)

	getLimitInfo := func(volName string) (info *proto.LimitInfo, err error) {
		info = new(proto.LimitInfo)
		info.NetworkFlowRatio = map[string]uint64{
			ModuleDataNode: ratio,
		}
		info.RateLimit = map[string]map[string]map[int]proto.AllLimitGroup{
			ModuleDataNode: {
				ZonePrefix + zone: {
					int(proto.OpRead): {indexCountPerDisk: limit[0], indexOutBytesPerDisk: limit[1], indexConcurrency: int64(concurrency)},
				},
				VolPrefix + vol: {
					int(proto.OpRead):  {indexCountPerDisk: limit[2]},
					int(proto.OpWrite): {indexCountPerDisk: limit[3]},
				},
			},
		}
		return
	}
	m := NewLimiterManager(ModuleDataNode, zone, getLimitInfo)
	m.Stop()
	m.updateLimitInfo()
	ml := m.GetLimiter()
	property := []Properties{
		{{PropertyTypeFlow, FlowNetwork}},
		{{PropertyTypeOp, strconv.Itoa(int(proto.OpRead))}, {PropertyTypeDisk, ""}},
		{{PropertyTypeOp, strconv.Itoa(int(proto.OpWrite))}, {PropertyTypeDisk, ""}},
		{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(proto.OpRead))}, {PropertyTypeDisk, ""}},
		{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(proto.OpWrite))}, {PropertyTypeDisk, ""}},
	}
	speed := getSpeed()
	expect := []LimitGroup{
		{statTypeInBytes: rate.Limit(speed * int(ratio) / 100), statTypeOutBytes: rate.Limit(speed * int(ratio) / 100)},
		{statTypeCount: rate.Limit(limit[0]), statTypeOutBytes: rate.Limit(limit[1])},
		{},
		{statTypeCount: rate.Limit(limit[2])},
		{statTypeCount: rate.Limit(limit[3])},
	}
	check(t, ml, property, expect)
	assert.Equal(t, concurrency, m.GetTokenManager(int(proto.OpRead), "").GetConfCnt())

	ratio = 90
	concurrency = 5
	getLimitInfo1 := func(volName string) (info *proto.LimitInfo, err error) {
		info = new(proto.LimitInfo)
		info.NetworkFlowRatio = map[string]uint64{
			ModuleDataNode: ratio,
		}
		info.RateLimit = map[string]map[string]map[int]proto.AllLimitGroup{
			ModuleDataNode: {
				ZonePrefix + zone: {
					int(proto.OpRead):  {indexCountPerDisk: limit[1], indexConcurrency: int64(concurrency)},
					int(proto.OpWrite): {indexCountPerDisk: limit[0]},
				},
				VolPrefix + vol: {
					int(proto.OpWrite): {indexCountPerDisk: limit[2]},
				},
			},
		}
		return
	}
	expect = []LimitGroup{
		{statTypeInBytes: rate.Limit(speed * int(ratio) / 100), statTypeOutBytes: rate.Limit(speed * int(ratio) / 100)},
		{statTypeCount: rate.Limit(limit[1])},
		{statTypeCount: rate.Limit(limit[0])},
		{},
		{statTypeCount: rate.Limit(limit[2])},
	}
	m.setGetLimitInfoFunc(getLimitInfo1)
	m.updateLimitInfo()
	check(t, ml, property, expect)
	assert.Equal(t, concurrency, m.GetTokenManager(int(proto.OpRead), "").GetConfCnt())
}

func check(t *testing.T, ml *MultiLimiter, property []Properties, limit []LimitGroup) {
	for i, p := range property {
		val, ok := ml.rules.Load(p.name())
		if limit[i].haveLimit() {
			assert.True(t, ok)
			r := val.(*Rule)
			assert.Equal(t, limit[i], r.limit)
		} else {
			assert.False(t, ok)
		}
	}
}
