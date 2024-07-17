package cluster

import (
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type nodeItemInfo struct {
	clustermgr.NodeInfo
	// extraInfo interface{}
}

type nodeItem struct {
	nodeID   proto.NodeID
	info     nodeItemInfo
	disks    map[proto.DiskID]*diskItem
	dropping bool

	lock sync.RWMutex
}

func (n *nodeItem) isUsingStatus() bool {
	return n.info.Status != proto.NodeStatusDropped
}

func (n *nodeItem) genFilterKey() string {
	return n.info.Host + n.info.DiskType.String()
}

func (n *nodeItem) withRLocked(f func() error) error {
	n.lock.RLock()
	err := f()
	n.lock.RUnlock()
	return err
}

func (n *nodeItem) withLocked(f func() error) error {
	n.lock.Lock()
	err := f()
	n.lock.Unlock()
	return err
}

type diskItemInfo struct {
	clustermgr.DiskInfo
	extraInfo interface{}
}

type diskItem struct {
	diskID         proto.DiskID
	info           diskItemInfo
	expireTime     time.Time
	lastExpireTime time.Time
	dropping       bool
	weightGetter   func(extraInfo interface{}) int64
	weightDecrease func(extraInfo interface{}, num int64)

	lock sync.RWMutex
}

func (d *diskItem) weight() int64 {
	return d.weightGetter(d.info.extraInfo)
}

func (d *diskItem) decrWeight(num int64) {
	d.weightDecrease(d.info.extraInfo, num)
}

func (d *diskItem) isExpire() bool {
	if d.expireTime.IsZero() {
		return false
	}
	return time.Since(d.expireTime) > 0
}

func (d *diskItem) isAvailable() bool {
	if d.info.Readonly || d.info.Status != proto.DiskStatusNormal || d.dropping {
		return false
	}
	return true
}

// isWritable return false if disk heartbeat expire or disk status is not normal or disk is readonly or dropping
func (d *diskItem) isWritable() bool {
	if d.isExpire() || !d.isAvailable() {
		return false
	}
	return true
}

func (d *diskItem) needFilter() bool {
	return d.info.Status != proto.DiskStatusRepaired && d.info.Status != proto.DiskStatusDropped
}

func (d *diskItem) genFilterKey() string {
	return d.info.Host + d.info.Path
}

func (d *diskItem) withRLocked(f func() error) error {
	d.lock.RLock()
	err := f()
	d.lock.RUnlock()
	return err
}

func (d *diskItem) withLocked(f func() error) error {
	d.lock.Lock()
	err := f()
	d.lock.Unlock()
	return err
}
