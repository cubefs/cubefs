package master

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/timeutil"
)

const (
	defaultVer           = "0.0.0"
	defaultHost          = "defHost"
	defaultRole          = "defRole"
	clientExpireInterval = 3600
	maxClientCnt         = 1000000
)

type ClientMgr struct {
	sync.RWMutex
	clients map[string]int64
}

func newClientMgr() *ClientMgr {
	mgr := &ClientMgr{}
	mgr.clients = make(map[string]int64)
	go mgr.evict()
	return mgr
}

func (cm *ClientMgr) PutItem(ip, host, vol, version, role, enableBcache string) {
	cm.Lock()
	defer cm.Unlock()

	if version == "" {
		version = defaultVer
	}

	if host == "" {
		host = defaultHost
	}

	if role == "" {
		role = defaultRole
	}

	if enableBcache == "1" {
		enableBcache = "true"
	} else {
		enableBcache = "false"
	}

	key := fmt.Sprintf("_%s_%s_%s_%s_%s_enableBcache-%s", vol, version, role, ip, host, enableBcache)

	if len(cm.clients) > maxClientCnt {
		log.LogWarnf("PutItem: too many record in cluster, ignore, key %s", key)
		return
	}

	cm.clients[key] = timeutil.GetCurrentTimeUnix()
}

func (cm *ClientMgr) GetClients(name string) map[string]int64 {
	cm.RLock()
	defer cm.RUnlock()

	if name != "" {
		name = fmt.Sprintf("_%s_", name)
	}

	items := make(map[string]int64, len(cm.clients))
	for k, v := range cm.clients {
		if name != "" && !strings.Contains(k, name) {
			continue
		}
		items[k] = v
	}
	return items
}

func (cm *ClientMgr) deleteByKey(key string) {
	cm.Lock()
	defer cm.Unlock()

	delete(cm.clients, key)
}

func (cm *ClientMgr) evict() {
	ticker := time.NewTicker(time.Minute * 10)
	for range ticker.C {
		log.LogInfof("ClientMgr.Evict: start check client info expire info")
		m := cm.GetClients("")
		now := timeutil.GetCurrentTimeUnix()
		for k, v := range m {
			if now > v+clientExpireInterval {
				log.LogInfof("ClientMgr.Evict: client is expired. key %s, val %d", k, v)
				cm.deleteByKey(k)
			}
		}
	}
}
