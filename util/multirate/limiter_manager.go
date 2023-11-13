package multirate

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

const (
	ModulMaster    = "master"
	ModulDataNode  = "datanode"
	ModulMetaNode  = "metanode"
	ModulFlashNode = "flashnode"

	ZonePrefix = "zone:"
	VolPrefix  = "vol:"

	updateConfigInterval    = 5 * time.Minute
	defaultNetworkFlowRatio = 90

	speedFile    = "/sys/class/net/%v/speed"
	bond0        = "bond0"
	eth0         = "eth0"
	defaultSpeed = 1250 // 10Gb/s
)

type indexType int

const (
	indexTypeTotal indexType = iota
	indexTypePerDisk
	indexTypePerPartition
	indexTypeMax // count of index types
)

const (
	indexTimeout = iota
	indexCount
	indexInBytes
	indexOutBytes
	indexCountPerDisk
	indexInBytesPerDisk
	indexOutBytesPerDisk
	indexCountPerPartition
	indexInBytesPerPartition
	indexOutBytesPerPartition
	IndexMax // count of indexes
)

type GetLimitInfoFunc func(volName string) (info *proto.LimitInfo, err error)

type LimiterManager struct {
	module       string
	zoneName     string
	getLimitInfo GetLimitInfoFunc
	ml           *MultiLimiter
	stopC        chan struct{}
	wg           sync.WaitGroup

	oldOpRateLimitMap    map[int]proto.AllLimitGroup
	oldVolOpRateLimitMap map[string]map[int]proto.AllLimitGroup
}

func HaveLimit(g proto.AllLimitGroup) bool {
	for _, l := range g {
		if l > 0 {
			return true
		}
	}
	return false
}

func (t indexType) String() string {
	if t == indexTypeTotal {
		return "total"
	} else if t == indexTypePerDisk {
		return "per disk"
	} else if t == indexTypePerPartition {
		return "per partition"
	}
	return ""
}

func GetLimitGroupDesc(g proto.AllLimitGroup) string {
	var sb strings.Builder
	if g[indexTimeout] > 0 {
		sb.WriteString(fmt.Sprintf("timeout:%v", time.Duration(g[indexTimeout])))
	}
	for t := indexTypeTotal; t < indexTypeMax; t++ {
		g := getLimitGroup(g, t)
		if !g.haveLimit() {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(fmt.Sprintf("%v:%v", t, g))
	}
	return sb.String()
}

func haveLimit(g proto.AllLimitGroup, t indexType) bool {
	for i := 1 + t*3; i < 1+(t+1)*3; i++ {
		if g[i] > 0 {
			return true
		}
	}
	return false
}

func getLimitGroup(g proto.AllLimitGroup, t indexType) (re LimitGroup) {
	for i := range re {
		re[i] = rate.Limit(g[1+t*3+indexType(i)])
	}
	return
}

func getBurstGroup(g proto.AllLimitGroup, t indexType) (re BurstGroup) {
	for i := range re {
		re[i] = int(g[1+t*3+indexType(i)])
	}
	return
}

func NewLimiterManager(module string, zoneName string, getLimitInfo GetLimitInfoFunc) *LimiterManager {
	m := new(LimiterManager)
	m.module = module
	m.zoneName = zoneName
	m.getLimitInfo = getLimitInfo
	m.ml = NewMultiLimiterWithHandler()
	m.stopC = make(chan struct{})
	m.oldOpRateLimitMap = make(map[int]proto.AllLimitGroup)
	m.oldVolOpRateLimitMap = make(map[string]map[int]proto.AllLimitGroup)
	if err := m.updateLimitInfo(); err != nil {
		return nil
	}
	m.wg.Add(1)
	go m.update()
	return m
}

func (m *LimiterManager) GetLimiter() *MultiLimiter {
	return m.ml
}

func (m *LimiterManager) Stop() {
	close(m.stopC)
	m.wg.Wait()
}

func (m *LimiterManager) setGetLimitInfoFunc(getLimitInfo GetLimitInfoFunc) {
	m.getLimitInfo = getLimitInfo
}

func (m *LimiterManager) update() {
	defer m.wg.Done()
	for {
		err := m.updateWithRecover()
		if err == nil {
			break
		}
		log.LogErrorf("LimiterManager.updateLimitInfo: err(%v) try next update", err)
		time.Sleep(updateConfigInterval)
	}
}

func (m *LimiterManager) updateWithRecover() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("LimiterManager.updateLimitInfo panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("LimiterManager.updateLimitInfo panic: err(%v)", r)
			err = errors.New(msg)
		}
	}()
	ticker := time.NewTicker(updateConfigInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopC:
			return
		case <-ticker.C:
			m.updateLimitInfo()
		}
	}
}

func (m *LimiterManager) updateLimitInfo() (err error) {
	limitInfo, err := m.getLimitInfo("")
	if err != nil {
		log.LogWarnf("updateLimitInfo, get limit info err: %s", err.Error())
		return
	}

	ratio := limitInfo.NetworkFlowRatio[m.module]
	speed := getSpeed() * int(ratio) / 100
	rule := NewRule(Properties{}, LimitGroup{statTypeInBytes: rate.Limit(speed), statTypeOutBytes: rate.Limit(speed)}, BurstGroup{statTypeInBytes: speed, statTypeOutBytes: speed})
	m.ml.AddRule(rule)

	m.updateOpLimit(limitInfo)
	m.updateVolOpLimit(limitInfo)
	return
}

// updateOpLimit update limit of each disk for opcode
func (m *LimiterManager) updateOpLimit(limitInfo *proto.LimitInfo) {
	opRateLimitMap := m.getOpRateLimitMap(limitInfo)
	if opRateLimitMap == nil {
		opRateLimitMap = make(map[int]proto.AllLimitGroup)
	}

	var p Properties
	for op, oldGroup := range m.oldOpRateLimitMap {
		group, ok := opRateLimitMap[op]
		for i := indexType(0); i < indexTypeMax; i++ {
			if !haveLimit(oldGroup, i) || (ok && haveLimit(group, i)) {
				continue
			}
			if i == indexTypeTotal {
				p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}}
			} else if i == indexTypePerDisk {
				p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypeDisk, ""}}
			} else if i == indexTypePerPartition {
				p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypePartition, ""}}
			}
			m.ml.ClearRule(p)
		}
	}

	for op, group := range opRateLimitMap {
		for i := indexType(0); i < indexTypeMax; i++ {
			if !haveLimit(group, i) {
				continue
			}
			if i == indexTypeTotal {
				p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}}
			} else if i == indexTypePerDisk {
				p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypeDisk, ""}}
			} else if i == indexTypePerPartition {
				p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypePartition, ""}}
			}
			rule := NewRuleWithTimeout(p, getLimitGroup(group, i), getBurstGroup(group, i), time.Duration(group[indexTimeout]))
			m.ml.AddRule(rule)
		}
	}
	m.oldOpRateLimitMap = opRateLimitMap
}

func (m *LimiterManager) getOpRateLimitMap(limitInfo *proto.LimitInfo) map[int]proto.AllLimitGroup {
	zoneOpRateLimitMap, ok := limitInfo.RateLimit[m.module]
	if !ok {
		return nil
	}
	opRateLimitMap, ok := zoneOpRateLimitMap[ZonePrefix+m.zoneName]
	defaultOpRateLimitMap, defaultOk := zoneOpRateLimitMap[ZonePrefix]
	if !ok {
		return defaultOpRateLimitMap
	}
	if !defaultOk {
		return opRateLimitMap
	}

	for op, limit := range defaultOpRateLimitMap {
		l, ok := opRateLimitMap[op]
		if !ok {
			opRateLimitMap[op] = limit
		}
		for i := 0; i < int(statTypeMax); i++ {
			if l[i] == 0 {
				l[i] = limit[i]
			}
		}
	}
	return opRateLimitMap
}

// updateVolOpLimit update limit of each disk for vol & opcode
func (m *LimiterManager) updateVolOpLimit(limitInfo *proto.LimitInfo) {
	volOpRateLimitMap := limitInfo.RateLimit[m.module]
	if volOpRateLimitMap == nil {
		volOpRateLimitMap = make(map[string]map[int]proto.AllLimitGroup)
	}

	var p Properties
	for vol, oldOpRateLimitMap := range m.oldVolOpRateLimitMap {
		opRateLimitMap, volOk := volOpRateLimitMap[vol]
		for op, oldGroup := range oldOpRateLimitMap {
			var group proto.AllLimitGroup
			var ok bool
			if volOk {
				group, ok = opRateLimitMap[op]
			}
			vol = strings.TrimPrefix(vol, VolPrefix)
			for i := indexType(0); i < indexTypeMax; i++ {
				if !haveLimit(oldGroup, i) || (ok && haveLimit(group, i)) {
					continue
				}
				if i == indexTypeTotal {
					p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}}
				} else if i == indexTypePerDisk {
					p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypeDisk, ""}}
				} else if i == indexTypePerPartition {
					p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypePartition, ""}}
				}
				m.ml.ClearRule(p)
			}
		}
	}

	for vol, opRateLimitMap := range volOpRateLimitMap {
		if !strings.HasPrefix(vol, VolPrefix) {
			continue
		}
		vol = strings.TrimPrefix(vol, VolPrefix)
		for op, group := range opRateLimitMap {
			for i := indexType(0); i < indexTypeMax; i++ {
				if !haveLimit(group, i) {
					continue
				}
				if i == indexTypeTotal {
					p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}}
				} else if i == indexTypePerDisk {
					p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypeDisk, ""}}
				} else if i == indexTypePerPartition {
					p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypePartition, ""}}
				}
				rule := NewRuleWithTimeout(p, getLimitGroup(group, i), getBurstGroup(group, i), time.Duration(group[indexTimeout]))
				m.ml.AddRule(rule)
			}
		}
	}
	m.oldVolOpRateLimitMap = volOpRateLimitMap
}

func getSpeed() (speed int) {
	all := []string{bond0, eth0}
	for _, item := range all {
		speed = getEthSpeed(item)
		if speed > 0 {
			return
		}
	}
	return defaultSpeed
}

func getEthSpeed(eth string) int {
	if data, err := os.ReadFile(fmt.Sprintf(speedFile, eth)); err == nil {
		speed, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if speed > 0 {
			return int(speed / 8)
		}
	}
	return 0
}
