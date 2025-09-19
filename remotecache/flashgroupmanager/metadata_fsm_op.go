package flashgroupmanager

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type RaftCmd struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

func (m *RaftCmd) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts the byte array to a RaftCmd.
func (m *RaftCmd) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

type clusterValue struct {
	Name                         string
	FlashNodeHandleReadTimeout   int
	FlashNodeReadDataNodeTimeout int
	RemoteCacheTTL               int64
	RemoteCacheReadTimeout       int64
	RemoteCacheMultiRead         bool
	FlashNodeTimeoutCount        int64
	RemoteCacheSameZoneTimeout   int64
	RemoteCacheSameRegionTimeout int64
	FlashHotKeyMissCount         int
	FlashReadFlowLimit           int64
	FlashWriteFlowLimit          int64
	RemoteClientFlowLimit        int64
}

func newClusterValue(c *Cluster) (cv *clusterValue) {
	cv = &clusterValue{
		Name:                         c.Name,
		FlashNodeHandleReadTimeout:   c.cfg.FlashNodeHandleReadTimeout,
		FlashNodeReadDataNodeTimeout: c.cfg.FlashNodeReadDataNodeTimeout,
		RemoteCacheTTL:               c.cfg.RemoteCacheTTL,
		RemoteCacheReadTimeout:       c.cfg.RemoteCacheReadTimeout,
		RemoteCacheMultiRead:         c.cfg.RemoteCacheMultiRead,
		FlashNodeTimeoutCount:        c.cfg.FlashNodeTimeoutCount,
		RemoteCacheSameZoneTimeout:   c.cfg.RemoteCacheSameZoneTimeout,
		RemoteCacheSameRegionTimeout: c.cfg.RemoteCacheSameRegionTimeout,
		FlashHotKeyMissCount:         c.cfg.FlashHotKeyMissCount,
		FlashReadFlowLimit:           c.cfg.FlashReadFlowLimit,
		FlashWriteFlowLimit:          c.cfg.FlashWriteFlowLimit,
		RemoteClientFlowLimit:        c.cfg.RemoteClientFlowLimit,
	}
	return cv
}

func (c *Cluster) loadClusterValue() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(clusterPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadClusterValue],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		cv := &clusterValue{}
		if err = json.Unmarshal(value, cv); err != nil {
			log.LogErrorf("action[loadClusterValue], unmarshal err:%v", err.Error())
			return err
		}

		if cv.Name != c.Name {
			log.LogErrorf("action[loadClusterValue] clusterName(%v) not match loaded clusterName(%v), n loaded cluster value: %+v",
				c.Name, cv.Name, cv)
			continue
		}

		log.LogDebugf("action[loadClusterValue] loaded cluster value: %+v", cv)

		if cv.FlashNodeHandleReadTimeout == 0 {
			cv.FlashNodeHandleReadTimeout = defaultFlashNodeHandleReadTimeout
		}
		c.cfg.FlashNodeHandleReadTimeout = cv.FlashNodeHandleReadTimeout
		if cv.FlashNodeReadDataNodeTimeout == 0 {
			cv.FlashNodeReadDataNodeTimeout = defaultFlashNodeReadDataNodeTimeout
		}
		if cv.FlashHotKeyMissCount == 0 {
			cv.FlashHotKeyMissCount = defaultFlashHotKeyMissCount
		}
		c.cfg.FlashHotKeyMissCount = cv.FlashHotKeyMissCount

		c.cfg.FlashReadFlowLimit = cv.FlashReadFlowLimit
		c.cfg.FlashWriteFlowLimit = cv.FlashWriteFlowLimit
		c.cfg.RemoteClientFlowLimit = cv.RemoteClientFlowLimit

		c.cfg.FlashNodeReadDataNodeTimeout = cv.FlashNodeReadDataNodeTimeout
		log.LogInfof("action[loadClusterValue] flashNodeHandleReadTimeout %v(ms), flashNodeReadDataNodeTimeout%v(ms), flashHotKeyMissCount(%v), flashReadFlowLimit(%v), flashWriteFlowLimit(%v), remoteClientFlowLimit(%v)",
			cv.FlashNodeHandleReadTimeout, cv.FlashNodeReadDataNodeTimeout, cv.FlashHotKeyMissCount, cv.FlashReadFlowLimit, cv.FlashWriteFlowLimit, cv.RemoteClientFlowLimit)

		if cv.RemoteCacheTTL == 0 {
			cv.RemoteCacheTTL = proto.DefaultRemoteCacheTTL
		}
		c.cfg.RemoteCacheTTL = cv.RemoteCacheTTL

		if cv.RemoteCacheReadTimeout == 0 {
			cv.RemoteCacheReadTimeout = proto.DefaultRemoteCacheClientReadTimeout
		}
		c.cfg.RemoteCacheReadTimeout = cv.RemoteCacheReadTimeout
		c.cfg.RemoteCacheMultiRead = cv.RemoteCacheMultiRead

		if cv.FlashNodeTimeoutCount == 0 {
			cv.FlashNodeTimeoutCount = proto.DefaultFlashNodeTimeoutCount
		}
		c.cfg.FlashNodeTimeoutCount = cv.FlashNodeTimeoutCount

		if cv.RemoteCacheSameZoneTimeout == 0 {
			cv.RemoteCacheSameZoneTimeout = proto.DefaultRemoteCacheSameZoneTimeout
		}
		c.cfg.RemoteCacheSameZoneTimeout = cv.RemoteCacheSameZoneTimeout

		if cv.RemoteCacheSameRegionTimeout == 0 {
			cv.RemoteCacheSameRegionTimeout = proto.DefaultRemoteCacheSameRegionTimeout
		}
		c.cfg.RemoteCacheSameRegionTimeout = cv.RemoteCacheSameRegionTimeout
		log.LogInfof("action[loadClusterValue] remoteCacheTTL(%v), remoteCacheReadTimeout(%v), remoteCacheMultiRead(%v), flashNodeTimeoutCount(%v), remoteCacheSameZoneTimeout(%v), remoteCacheSameRegionTimeout(%v)",
			cv.RemoteCacheTTL, cv.RemoteCacheReadTimeout, cv.RemoteCacheMultiRead, cv.FlashNodeTimeoutCount, cv.RemoteCacheSameZoneTimeout, cv.RemoteCacheSameRegionTimeout)
	}

	return
}

func (c *Cluster) loadFlashNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(flashNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadFlashNodes],err:%v", err.Error())
		return
	}

	for _, value := range result {
		fnv := &FlashNodeValue{}
		if err = json.Unmarshal(value, fnv); err != nil {
			err = fmt.Errorf("action[loadFlashNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashNode := NewFlashNode(fnv.Addr, fnv.ZoneName, c.Name, fnv.Version, fnv.IsEnable)
		flashNode.ID = fnv.ID
		// load later in loadFlashTopology
		flashNode.FlashGroupID = fnv.FlashGroupID

		_, err = c.flashNodeTopo.GetZone(flashNode.ZoneName)
		if err != nil {
			c.flashNodeTopo.PutZoneIfAbsent(NewFlashNodeZone(flashNode.ZoneName))
			err = nil
		}
		c.flashNodeTopo.PutFlashNode(flashNode)
		log.LogInfof("action[loadFlashNodes], flashNode[flashNodeId:%v addr:%s flashGroupId:%v]", flashNode.ID, flashNode.Addr, flashNode.FlashGroupID)
	}
	return
}

func (c *Cluster) loadFlashGroups() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(flashGroupPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadFlashGroups],err:%v", err.Error())
		return err
	}
	for _, value := range result {
		var fgv FlashGroupValue
		if err = json.Unmarshal(value, &fgv); err != nil {
			err = fmt.Errorf("action[loadFlashGroups],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		flashGroup := NewFlashGroupFromFgv(fgv)
		c.flashNodeTopo.SaveFlashGroup(flashGroup)
		log.LogInfof("action[loadFlashGroups],flashGroup[%v]", flashGroup.ID)
	}
	return
}

func (c *Cluster) loadFlashTopology() (err error) {
	return c.flashNodeTopo.Load()
}

func (m *RaftCmd) setOpType() {
	keyArr := strings.Split(m.K, keySeparator)
	if len(keyArr) < 2 {
		log.LogWarnf("action[setOpType] invalid length[%v]", keyArr)
		return
	}
	switch keyArr[1] {
	case clusterAcronym:
		m.Op = opSyncPutCluster
	case maxCommonIDKey:
		m.Op = opSyncAllocCommonID
	default:
		log.LogWarnf("action[setOpType] unknown opCode[%v]", keyArr[1])
	}
}

func (c *Cluster) submit(metadata *RaftCmd) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return errors.New(err.Error())
	}
	if _, err = c.partition.Submit(cmd); err != nil {
		msg := fmt.Sprintf("action[metadata_submit] err:%v", err.Error())
		return errors.New(msg)
	}
	return
}

func (c *Cluster) syncPutCluster() (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opSyncPutCluster
	metadata.K = clusterPrefix + c.Name
	cv := newClusterValue(c)
	log.LogInfof("action[syncPutCluster] cluster value:[%+v]", cv)
	metadata.V, err = json.Marshal(cv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}
