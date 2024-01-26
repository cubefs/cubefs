package multirate

import (
	"context"
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
	ModuleMaster    = "master"
	ModuleDataNode  = "datanode"
	ModuleMetaNode  = "metanode"
	ModuleFlashNode = "flashnode"

	ZonePrefix = "zone:"
	VolPrefix  = "vol:"

	updateConfigInterval    = 5 * time.Minute
	defaultNetworkFlowRatio = 90

	speedFile    = "/sys/class/net/%v/speed"
	bond0        = "bond0"
	eth0         = "eth0"
	defaultSpeed = 1250 // 10000 Mb/s
	minSpeed     = 125  // 1000  Mb/s
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
	indexConcurrency
	IndexMax // count of indexes
)

var (
	limiterManager *LimiterManager
)

var indexNameMap = map[string]int{
	"timeout":              indexTimeout,
	"count":                indexCount,
	"inBytes":              indexInBytes,
	"outBytes":             indexOutBytes,
	"countPerDisk":         indexCountPerDisk,
	"inBytesPerDisk":       indexInBytesPerDisk,
	"outBytesPerDisk":      indexOutBytesPerDisk,
	"countPerPartition":    indexCountPerPartition,
	"inBytesPerPartition":  indexInBytesPerPartition,
	"outBytesPerPartition": indexOutBytesPerPartition,
	"concurrency":          indexConcurrency,
}

type GetLimitInfoFunc func(volName string) (info *proto.LimitInfo, err error)

type LimiterManager struct {
	module       string
	zoneName     string
	getLimitInfo GetLimitInfoFunc
	ml           *MultiLimiter
	mc           *MultiConcurrency
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
	if g[indexConcurrency] > 0 {
		if sb.Len() > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(fmt.Sprintf("concurrency:%v", g[indexConcurrency]))
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

func GetIndexByName(name string) int {
	index, ok := indexNameMap[name]
	if !ok {
		return -1
	}
	return index
}

func InitLimiterManager(module string, zoneName string, getLimitInfo GetLimitInfoFunc) (lm *LimiterManager, err error) {
	if lm, err = newLimiterManager(module, zoneName, getLimitInfo); err == nil {
		limiterManager = lm
	}
	return
}

func GetLimiterManager() *LimiterManager {
	return limiterManager
}

func Wait(ctx context.Context, ps Properties) error {
	if limiterManager == nil {
		return nil
	}
	return limiterManager.ml.Wait(ctx, ps)
}

func WaitN(ctx context.Context, ps Properties, stat Stat) error {
	if limiterManager == nil {
		return nil
	}
	return limiterManager.ml.WaitN(ctx, ps, stat)
}

func Allow(ps Properties) bool {
	if limiterManager == nil {
		return true
	}
	return limiterManager.ml.Allow(ps)
}

func AllowN(ps Properties, stat Stat) bool {
	if limiterManager == nil {
		return true
	}
	return limiterManager.ml.AllowN(ps, stat)
}

func WaitUseDefaultTimeout(ctx context.Context, ps Properties) error {
	if limiterManager == nil {
		return nil
	}
	return limiterManager.ml.WaitUseDefaultTimeout(ctx, ps)
}

func WaitNUseDefaultTimeout(ctx context.Context, ps Properties, stat Stat) error {
	if limiterManager == nil {
		return nil
	}
	return limiterManager.ml.WaitNUseDefaultTimeout(ctx, ps, stat)
}

func WaitConcurrency(ctx context.Context, op int, disk string) error {
	if limiterManager == nil {
		return nil
	}
	return limiterManager.mc.WaitUseDefaultTimeout(ctx, op, disk)
}

func DoneConcurrency(op int, disk string) {
	if limiterManager != nil {
		limiterManager.mc.Done(op, disk)
	}
}

func Stop() {
	if limiterManager != nil {
		close(limiterManager.stopC)
		limiterManager.wg.Wait()
	}
}

func newLimiterManager(module string, zoneName string, getLimitInfo GetLimitInfoFunc) (*LimiterManager, error) {
	m := new(LimiterManager)
	m.module = module
	m.zoneName = zoneName
	m.getLimitInfo = getLimitInfo
	m.ml = NewMultiLimiterWithHandler()
	m.mc = NewMultiConcurrencyWithHandler()
	m.stopC = make(chan struct{})
	m.oldOpRateLimitMap = make(map[int]proto.AllLimitGroup)
	m.oldVolOpRateLimitMap = make(map[string]map[int]proto.AllLimitGroup)
	if err := m.updateLimitInfo(); err != nil {
		return nil, err
	}
	m.wg.Add(1)
	go m.update()
	return m, nil
}

func (m *LimiterManager) GetLimiter() *MultiLimiter {
	return m.ml
}

func (m *LimiterManager) GetConcurrency() *MultiConcurrency {
	return m.mc
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
	speed := getSpeed() * 1024 * 1024 * int(ratio) / 100
	rule := NewRule(Properties{{PropertyTypeFlow, FlowNetwork}}, LimitGroup{statTypeInBytes: rate.Limit(speed), statTypeOutBytes: rate.Limit(speed)}, BurstGroup{statTypeInBytes: speed, statTypeOutBytes: speed})
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
			old := m.getProperties(i, oldGroup, op, "", false)
			new := m.getProperties(i, group, op, "", false)
			if old.name() != new.name() {
				m.ml.ClearRule(old)
			}
		}
		if oldGroup[indexConcurrency] > 0 && (!ok || group[indexConcurrency] <= 0) {
			m.mc.addRule(op, 0, 0)
		}
	}

	for op, group := range opRateLimitMap {
		for i := indexType(0); i < indexTypeMax; i++ {
			if !haveLimit(group, i) {
				continue
			}
			p = m.getProperties(i, group, op, "", false)
			rule := NewRuleWithTimeout(p, getLimitGroup(group, i), getBurstGroup(group, i), time.Duration(group[indexTimeout]))
			m.ml.AddRule(rule)
		}
		if group[indexConcurrency] > 0 {
			m.mc.addRule(op, uint64(group[indexConcurrency]), time.Duration(group[indexTimeout]))
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
			continue
		}
		for i := 0; i < proto.LimitGroupCount; i++ {
			if l[i] == 0 {
				l[i] = limit[i]
			}
		}
		opRateLimitMap[op] = l
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
			if volOk {
				group = opRateLimitMap[op]
			}
			vol = strings.TrimPrefix(vol, VolPrefix)
			for i := indexType(0); i < indexTypeMax; i++ {
				old := m.getProperties(i, oldGroup, op, vol, true)
				new := m.getProperties(i, group, op, vol, true)
				if old.name() != new.name() {
					m.ml.ClearRule(old)
				}
			}
		}
	}

	var deleteKeys []string
	for vol, opRateLimitMap := range volOpRateLimitMap {
		if !strings.HasPrefix(vol, VolPrefix) {
			deleteKeys = append(deleteKeys, vol)
			continue
		}
		vol = strings.TrimPrefix(vol, VolPrefix)
		for op, group := range opRateLimitMap {
			for i := indexType(0); i < indexTypeMax; i++ {
				if !haveLimit(group, i) {
					continue
				}
				p = m.getProperties(i, group, op, vol, true)
				rule := NewRuleWithTimeout(p, getLimitGroup(group, i), getBurstGroup(group, i), time.Duration(group[indexTimeout]))
				m.ml.AddRule(rule)
			}
		}
	}
	for _, key := range deleteKeys {
		delete(volOpRateLimitMap, key)
	}
	m.oldVolOpRateLimitMap = volOpRateLimitMap
}

func (m *LimiterManager) getProperties(i indexType, group proto.AllLimitGroup, op int, vol string, hasVol bool) (p Properties) {
	if !haveLimit(group, i) {
		return
	}
	p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}}
	if hasVol {
		p = append(p, Property{PropertyTypeVol, vol})
	}
	var flowType string
	if i == indexTypeTotal {
		flowType = FlowNetwork
	} else if i == indexTypePerDisk {
		p = append(p, Property{PropertyTypeDisk, ""})
		flowType = FlowDisk
	} else if i == indexTypePerPartition {
		p = append(p, Property{PropertyTypePartition, ""})
		if m.module == ModuleDataNode {
			flowType = FlowDisk
		} else {
			flowType = FlowNetwork
		}
	}
	if group[i*indexTypeMax+2] > 0 || group[i*indexTypeMax+3] > 0 {
		p = append(p, Property{PropertyTypeFlow, flowType})
	}
	return
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
	if data, err := os.ReadFile(fmt.Sprintf(speedFile, eth)); err != nil {
		speed, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if speed > 0 {
			if speed/8 < minSpeed {
				speed = minSpeed * 8
			}
			return int(speed / 8)
		}
	}
	return 0
}
