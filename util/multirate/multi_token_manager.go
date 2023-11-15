package multirate

import (
	"strconv"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/util/tokenmanager"
)

const (
	tmSeparator = ","
)

type MultiTokenManager struct {
	rules sync.Map // map[op]concurrency
	tms   sync.Map // map[key]*tokenmanager.TokenManager
}

func NewMultiTokenManager() *MultiTokenManager {
	return new(MultiTokenManager)
}

func (m *MultiTokenManager) GetTokenManager(op int, disk string) *tokenmanager.TokenManager {
	var tm *tokenmanager.TokenManager
	key := getTokenManagerKey(op, disk)
	tmVal, ok := m.tms.Load(key)
	if ok {
		return tmVal.(*tokenmanager.TokenManager)
	}

	val, ok := m.rules.Load(op)
	var concurrency uint64 = 0
	if ok {
		concurrency = val.(uint64)
	}
	if concurrency > 0 {
		tm = tokenmanager.NewTokenManager(uint64(concurrency))
		m.tms.Store(key, tm)
	}
	return tm
}

func (m *MultiTokenManager) addRule(op int, concurrency uint64) {
	val, ok := m.rules.Load(op)
	var oldConcurrency uint64 = 0
	if ok {
		oldConcurrency = val.(uint64)
	}
	if concurrency == oldConcurrency {
		return
	}

	if concurrency == 0 {
		m.rules.Delete(op)
	} else {
		m.rules.Store(op, concurrency)
	}
	m.updateTokenManager(op, concurrency)
}

func (m *MultiTokenManager) updateTokenManager(op int, concurrency uint64) {
	prefix := strconv.Itoa(op) + tmSeparator
	m.tms.Range(func(k, v interface{}) bool {
		if !strings.HasPrefix(k.(string), prefix) {
			return true
		}
		v.(*tokenmanager.TokenManager).ResetRunCnt(concurrency)
		if concurrency == 0 {
			m.tms.Delete(k.(string))
		}
		return true
	})
}

func getTokenManagerKey(op int, disk string) string {
	var sb strings.Builder
	sb.WriteString(strconv.Itoa(op))
	sb.WriteString(tmSeparator)
	sb.WriteString(disk)
	return sb.String()
}
