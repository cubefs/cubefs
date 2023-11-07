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
		zone          = "default"
		vol           = "ltptest"
		ratio  uint64 = 80
		limit1 uint64 = 1
		limit2 uint64 = 2
		limit3 uint64 = 3
		limit4 uint64 = 4
	)

	getLimitInfo := func(volName string) (info *proto.LimitInfo, err error) {
		info = new(proto.LimitInfo)
		info.DataNodeNetworkFlowRatio = ratio
		info.DataNodeReqZoneOpRateLimitMap = map[string]map[uint8]uint64{
			zone: {proto.OpRead: limit1},
		}
		info.DataNodeReqVolOpRateLimitMap = map[string]map[uint8]uint64{
			vol: {proto.OpRead: limit3, proto.OpWrite: limit4},
		}
		return
	}
	m := NewLimiterManager(ModulDataNode, zone, getLimitInfo)
	m.Stop()
	m.updateLimitInfo()
	ml := m.GetLimiter()
	property := []Properties{
		{{PropertyTypeNetwork, NetworkOut}},
		Properties{{PropertyTypeOp, strconv.Itoa(int(proto.OpRead))}, {PropertyTypeDisk, ""}},
		Properties{{PropertyTypeOp, strconv.Itoa(int(proto.OpWrite))}, {PropertyTypeDisk, ""}},
		Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(proto.OpRead))}, {PropertyTypeDisk, ""}},
		Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(proto.OpWrite))}, {PropertyTypeDisk, ""}},
	}
	speed := getSpeed()
	limit := []rate.Limit{
		rate.Limit(speed * int(ratio) / 100),
		rate.Limit(limit1),
		0,
		rate.Limit(limit3),
		rate.Limit(limit4),
	}
	check(t, ml, property, limit)

	ratio = 90
	getLimitInfo1 := func(volName string) (info *proto.LimitInfo, err error) {
		info = new(proto.LimitInfo)
		info.DataNodeNetworkFlowRatio = ratio
		info.DataNodeReqZoneOpRateLimitMap = map[string]map[uint8]uint64{
			zone: {proto.OpRead: limit2, proto.OpWrite: limit1},
		}
		info.DataNodeReqVolOpRateLimitMap = map[string]map[uint8]uint64{
			vol: {proto.OpWrite: limit3},
		}
		return
	}
	limit = []rate.Limit{
		rate.Limit(speed * int(ratio) / 100),
		rate.Limit(limit2),
		rate.Limit(limit1),
		0,
		rate.Limit(limit3),
	}
	m.setGetLimitInfoFunc(getLimitInfo1)
	m.updateLimitInfo()
	check(t, ml, property, limit)
}

func check(t *testing.T, ml *MultiLimiter, property []Properties, limit []rate.Limit) {
	for i, p := range property {
		val, ok := ml.rules.Load(p.RuleName())
		if limit[i] > 0 {
			assert.True(t, ok)
			r := val.(*Rule)
			assert.Equal(t, r.Limit, limit[i])
		} else {
			assert.False(t, ok)
		}
	}
}
