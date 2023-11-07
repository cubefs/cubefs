package multirate

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/unit"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	ModulDataNode = "datanode"
	ModulMetaNode = "metanode"

	updateConfigInterval    = 5 * time.Minute
	defaultNetworkFlowRatio = 90

	bond0        = "bond0"
	eth0         = "eth0"
	defaultSpeed = unit.GB // 10Gb/s
	minSpeed     = unit.MB * 128
)

type GetLimitInfoFunc func(volName string) (info *proto.LimitInfo, err error)

type LimiterManager struct {
	module       string
	zoneName     string
	getLimitInfo GetLimitInfoFunc
	ml           *MultiLimiter
	stopC        chan struct{}
	wg           sync.WaitGroup

	oldOpRateLimitMap    map[uint8]uint64
	oldVolOpRateLimitMap map[string]map[uint8]uint64
}

func NewLimiterManager(module string, zoneName string, getLimitInfo GetLimitInfoFunc) *LimiterManager {
	m := new(LimiterManager)
	m.module = module
	m.zoneName = zoneName
	m.getLimitInfo = getLimitInfo
	m.ml = NewMultiLimiter()
	m.stopC = make(chan struct{})
	m.oldOpRateLimitMap = make(map[uint8]uint64)
	m.oldVolOpRateLimitMap = make(map[string]map[uint8]uint64)
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
		log.LogErrorf("updateRateLimitConfig: err(%v) try next update", err)
		time.Sleep(updateConfigInterval)
	}
}

func (m *LimiterManager) updateWithRecover() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("updateRateLimitConfig panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("updateRateLimitConfig panic: err(%v)", r)
			err = errors.New(msg)
		}
	}()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-m.stopC:
			return
		case <-timer.C:
			m.updateLimitInfo()
			timer.Reset(updateConfigInterval)
		}
	}
}

func (m *LimiterManager) updateLimitInfo() {
	limitInfo, err := m.getLimitInfo("")
	if err != nil {
		log.LogWarnf("[updateRateLimitInfo] get limit info err: %s", err.Error())
		return
	}
	if m.module == ModulDataNode {
		ratio := limitInfo.DataNodeNetworkFlowRatio
		if ratio == 0 {
			ratio = defaultNetworkFlowRatio
		}
		speed := getSpeed() * int(ratio) / 100
		ruleIn := NewRule(PropertyTypeNetwork, NetworkIn, speed, speed)
		ruleOut := NewRule(PropertyTypeNetwork, NetworkOut, speed, speed)
		m.ml.AddRule(ruleIn).AddRule(ruleOut)
	}
	m.updateOpLimit(limitInfo)
	m.updateVolOpLimit(limitInfo)
}

// updateOpLimit update limit of each disk for opcode
func (m *LimiterManager) updateOpLimit(limitInfo *proto.LimitInfo) {
	var (
		opRateLimitMap map[uint8]uint64
		ok             bool
		p              Properties
	)
	if m.module == ModulDataNode {
		opRateLimitMap, ok = limitInfo.DataNodeReqZoneOpRateLimitMap[m.zoneName]
		if !ok {
			opRateLimitMap, ok = limitInfo.DataNodeReqZoneOpRateLimitMap[""]
		}
		if !ok {
			opRateLimitMap = make(map[uint8]uint64)
		}
	} else if m.module == ModulMetaNode {
		opRateLimitMap = limitInfo.MetaNodeReqOpRateLimitMap
	}

	for op := range m.oldOpRateLimitMap {
		if _, ok = opRateLimitMap[op]; !ok {
			if m.module == ModulDataNode {
				p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypeDisk, ""}}
			} else if m.module == ModulMetaNode {
				p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}}
			}
			m.ml.ClearRule(p)
		}
	}
	for op, limit := range opRateLimitMap {
		if m.module == ModulDataNode {
			p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypeDisk, ""}}
		} else if m.module == ModulMetaNode {
			p = Properties{{PropertyTypeOp, strconv.Itoa(int(op))}}
		}
		rule := NewMultiPropertyRule(p, int(limit), int(limit))
		m.ml.AddRule(rule)
	}
	m.oldOpRateLimitMap = opRateLimitMap
}

// updateVolOpLimit update limit of each disk for vol & opcode
func (m *LimiterManager) updateVolOpLimit(limitInfo *proto.LimitInfo) {
	var p Properties
	volOpRateLimitMap := limitInfo.DataNodeReqVolOpRateLimitMap
	for vol, oldOpRateLimitMap := range m.oldVolOpRateLimitMap {
		for op := range oldOpRateLimitMap {
			if opRateLimitMap, ok := volOpRateLimitMap[vol]; ok {
				if _, ok = opRateLimitMap[op]; ok {
					continue
				}
			}
			if m.module == ModulDataNode {
				p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypeDisk, ""}}
			} else if m.module == ModulMetaNode {
				p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}}
			}
			m.ml.ClearRule(p)
		}
	}
	for vol, opRateLimitMap := range volOpRateLimitMap {
		for op, limit := range opRateLimitMap {
			if m.module == ModulDataNode {
				p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}, {PropertyTypeDisk, ""}}
			} else if m.module == ModulMetaNode {
				p = Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, strconv.Itoa(int(op))}}
			}
			rule := NewMultiPropertyRule(p, int(limit), int(limit))
			m.ml.AddRule(rule)
		}
	}
	m.oldVolOpRateLimitMap = volOpRateLimitMap
}

func getSpeed() (speed int) {
	all := []string{bond0, eth0}
	for _, item := range all {
		speed = getEthSpeed(item)
		if speed > minSpeed {
			return
		}
	}
	return defaultSpeed
}

func getEthSpeed(eth string) int {
	if data, err := os.ReadFile(fmt.Sprintf("/sys/class/net/%v/speed", eth)); err == nil {
		speed, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if speed > 0 {
			return int(speed / 8)
		}
	}
	return 0
}
