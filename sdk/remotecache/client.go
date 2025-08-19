package remotecache

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
)

type ZoneRankType int

const (
	SameZoneRank ZoneRankType = iota
	SameRegionRank
	CrossRegionRank
	UnknownZoneRank
)

const (
	ConnIdelTimeout            = 30 //
	PingCount                  = 3
	SameZoneWeight             = 70
	RefreshFlashNodesInterval  = time.Minute
	RefreshHostLatencyInterval = 20 * time.Second
)

type AddressPingStats struct {
	sync.Mutex
	durations []time.Duration
	index     int
}

type RemoteCacheBase struct {
	flashGroups *btree.BTree
	mc          *master.MasterClient
	conns       *util.ConnectPool
	hostLatency sync.Map
	TTL         int64
	ReadTimeout int64 // ms
	stopC       chan struct{}
	wg          sync.WaitGroup

	blockSize             uint64
	clusterEnabled        bool
	RemoteCacheMultiRead  bool
	FlashNodeTimeoutCount int32
	SameZoneTimeout       int64 // microsecond
	SameRegionTimeout     int64 // ms
	AddressPingMap        sync.Map
}

func (as *AddressPingStats) Add(duration time.Duration) {
	as.Lock()
	defer as.Unlock()
	if as.index < 5 {
		as.durations = append(as.durations, duration)
	} else {
		as.durations[as.index%5] = duration
	}
	as.index++
}

func (as *AddressPingStats) Average() time.Duration {
	as.Lock()
	defer as.Unlock()
	if len(as.durations) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range as.durations {
		total += d
	}
	return total / time.Duration(len(as.durations))
}

func NewRemoteCacheBase(masters []string, blockSize uint64) (rc *RemoteCacheBase, err error) {
	rc = new(RemoteCacheBase)

	log.LogDebugf("NewRemoteCacheBase")
	rc.stopC = make(chan struct{})
	rc.flashGroups = btree.New(32)
	rc.ReadTimeout = proto.DefaultRemoteCacheClientReadTimeout

	rc.mc = master.NewMasterClient(masters, false)
	rc.conns = util.NewConnectPoolWithTimeoutAndCap(5, 500, ConnIdelTimeout, 1)
	rc.TTL = proto.DefaultRemoteCacheTTL
	rc.RemoteCacheMultiRead = true
	rc.FlashNodeTimeoutCount = proto.DefaultFlashNodeTimeoutCount
	rc.SameZoneTimeout = proto.DefaultRemoteCacheSameZoneTimeout
	rc.SameRegionTimeout = proto.DefaultRemoteCacheSameRegionTimeout
	rc.blockSize = blockSize

	err = rc.UpdateFlashGroups()
	if err != nil {
		log.LogDebugf("NewRemoteCacheBase: updateFlashGroups err %v", err)
		return
	}

	rc.wg.Add(1)
	go rc.refresh()

	// rc.Started = true

	log.LogDebugf("NewRemoteCacheBase sucess")

	return rc, err
}

func (rc *RemoteCacheBase) Stop() {
	// rc.Started = false
	close(rc.stopC)
	rc.conns.Close()
	rc.wg.Wait()
}

func (rc *RemoteCacheBase) refresh() {
	defer rc.wg.Done()
	for {
		err := rc.refreshWithRecover()
		if err == nil {
			log.LogInfof("refresh: exit")
			break
		}
		log.LogErrorf("refresh: err(%v) try next update", err)
	}
}

func (rc *RemoteCacheBase) refreshWithRecover() (panicErr error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("refreshFlashNode panic: err(%v) stack(%v)", r, string(debug.Stack()))
			msg := fmt.Sprintf("refreshFlashNode panic: err(%v)", r)
			panicErr = errors.New(msg)
		}
	}()

	refreshView := time.NewTicker(RefreshFlashNodesInterval)
	defer refreshView.Stop()

	refreshLatency := time.NewTimer(0)
	defer refreshLatency.Stop()

	var err error
	for {
		select {
		case <-rc.stopC:
			log.LogInfof("refreshWithRecover: remote stop")
			return
		case <-refreshView.C:
			if err = rc.UpdateFlashGroups(); err != nil {
				log.LogErrorf("updateFlashGroups err: %v", err)
			}
		case <-refreshLatency.C:
			rc.refreshHostLatency()
			refreshLatency.Reset(RefreshHostLatencyInterval)
		}
	}
}

func (rc *RemoteCacheBase) UpdateClusterEnable() error {
	if rc.mc != nil {
		if fgv, err := rc.mc.AdminAPI().ClientFlashGroups(); err != nil {
			log.LogWarnf("updateFlashGroups: err(%v)", err)
			return err
		} else {
			rc.clusterEnabled = fgv.Enable && len(fgv.FlashGroups) != 0
		}
	}
	return nil
}

func (rc *RemoteCacheBase) SetClusterEnable(enable bool) {
	rc.clusterEnabled = enable
}

func (rc *RemoteCacheBase) IsClusterEnable() bool {
	return rc.clusterEnabled
}

func (rc *RemoteCacheBase) UpdateFlashGroups() (err error) {
	var (
		fgv            proto.FlashGroupView
		newFlashGroups = btree.New(32)
	)
	if fgv, err = rc.mc.AdminAPI().ClientFlashGroups(); err != nil {
		log.LogWarnf("updateFlashGroups: err(%v)", err)
		return
	}
	log.LogDebugf("updateFlashGroups. get flashGroupView [%v]", fgv)
	rc.clusterEnabled = fgv.Enable && len(fgv.FlashGroups) != 0
	if !fgv.Enable {
		rc.flashGroups = newFlashGroups
		return
	}

	for _, fg := range fgv.FlashGroups {
		newAdded := make([]string, 0)
		for _, host := range fg.Hosts {
			if _, ok := rc.hostLatency.Load(host); !ok {
				newAdded = append(newAdded, host)
			}
		}
		log.LogDebugf("updateFlashGroups: fgID(%v) newAdded hosts: %v", fg.ID, newAdded)

		rc.updateHostLatency(newAdded)
		sortedHosts := rc.ClassifyHostsByAvgDelay(fg.ID, fg.Hosts)

		flashGroup := NewFlashGroup(fg, sortedHosts)
		for _, slot := range fg.Slot {
			slotItem := &SlotItem{
				slot:       slot,
				FlashGroup: flashGroup,
			}
			newFlashGroups.ReplaceOrInsert(slotItem)
		}
	}
	rc.flashGroups = newFlashGroups

	return
}

func (rc *RemoteCacheBase) ClassifyHostsByAvgDelay(fgID uint64, hosts []string) (sortedHosts map[ZoneRankType][]string) {
	sortedHosts = make(map[ZoneRankType][]string)
	sameZoneTimeout := time.Duration(rc.SameZoneTimeout) * time.Microsecond
	sameRegionTimeout := time.Duration(rc.SameRegionTimeout) * time.Millisecond
	for _, host := range hosts {
		avgTime := time.Duration(0)
		v, ok := rc.hostLatency.Load(host)
		if ok {
			avgTime = v.(time.Duration)
		}
		if avgTime <= time.Duration(0) {
			sortedHosts[UnknownZoneRank] = append(sortedHosts[UnknownZoneRank], host)
			auditlog.LogOpMsg("ClassifyHostsByAvgDelay", fmt.Sprintf("add host %v to unknown region", host), nil)
		} else if avgTime <= sameZoneTimeout {
			sortedHosts[SameZoneRank] = append(sortedHosts[SameZoneRank], host)
		} else if avgTime <= sameRegionTimeout {
			sortedHosts[SameRegionRank] = append(sortedHosts[SameRegionRank], host)
		} else {
			sortedHosts[CrossRegionRank] = append(sortedHosts[CrossRegionRank], host)
			auditlog.LogOpMsg("ClassifyHostsByAvgDelay", fmt.Sprintf("add host %v to cross region by time %v", host, avgTime.String()), nil)
		}
	}
	log.LogInfof("ClassifyHostsByAvgDelay: fgID(%v) sortedHost:%v", fgID, sortedHosts)
	return sortedHosts
}

func (rc *RemoteCacheBase) resetFlashNodeTimeoutCount() {
	fgSet := make([]uint64, 0)

	isInSet := func(fg uint64, fgSet []uint64) bool {
		for _, v := range fgSet {
			if v == fg {
				return true
			}
		}
		return false
	}

	rangeFunc := func(i btree.Item) bool {
		item := i.(*SlotItem)
		fg := item.FlashGroup

		if isInSet(fg.ID, fgSet) {
			return true
		}
		fgSet = append(fgSet, fg.ID)
		fg.hostLock.Lock()
		for addr := range fg.hostTimeoutCount {
			if fg.hostTimeoutCount[addr] == 0 {
				continue
			}
			// Check if the timeout host ping was successful
			// if it was successful and within the sameZone and sameRegion ranges, reset the host timeout count to 0
			v, ok := rc.hostLatency.Load(addr)
			if ok {
				avgTime := v.(time.Duration)
				if avgTime > 0 && avgTime < time.Millisecond*time.Duration(rc.SameRegionTimeout) {
					log.LogDebugf("resetFlashNodeTimeoutCount fgId(%v) flashnode(%v) from %v to 0",
						fg.ID, addr, fg.hostTimeoutCount[addr])
					fg.hostTimeoutCount[addr] = 0
				}
			}
		}
		fg.hostLock.Unlock()
		return true
	}
	rc.rangeFlashGroups(nil, rangeFunc)
}

func (rc *RemoteCacheBase) refreshHostLatency() {
	hosts := rc.getFlashHostsMap()

	needPings := make([]string, 0)
	rc.hostLatency.Range(func(key, value interface{}) bool {
		host := key.(string)
		if _, exist := hosts[host]; !exist {
			rc.hostLatency.Delete(host)
			log.LogInfof("remove flashNode(%v)", host)
		} else {
			needPings = append(needPings, host)
		}
		return true
	})
	rc.updateHostLatency(needPings)
	log.LogDebugf("updateHostLatencyByLatency: needPings(%v)", len(needPings))
}

func (rc *RemoteCacheBase) updateHostLatency(hosts []string) {
	for _, host := range hosts {
		avgRtt, err := iputil.PingWithTimeout(strings.Split(host, ":")[0], PingCount, time.Millisecond*time.Duration(rc.ReadTimeout))
		if err == nil {
			v, _ := rc.AddressPingMap.LoadOrStore(host, &AddressPingStats{})
			aps := v.(*AddressPingStats)
			aps.Add(avgRtt)
			rc.hostLatency.Store(host, aps.Average())
			log.LogInfof("updateHostLatency: host(%v) avgRtt(%v)", host, avgRtt.String())
		} else {
			rangeFunc := func(i btree.Item) bool {
				item := i.(*SlotItem)
				fg := item.FlashGroup
				fg.hostLock.Lock()
				defer fg.hostLock.Unlock()
				for _, h := range fg.Hosts {
					if h == host {
						fg.hostTimeoutCount[host]++
						if fg.hostTimeoutCount[host] >= rc.FlashNodeTimeoutCount {
							rc.hostLatency.Delete(host)
						}
						return false
					}
				}
				return true
			}
			rc.rangeFlashGroups(nil, rangeFunc)
			log.LogWarnf("updateHostLatency: host(%v) err(%v)", host, err)
		}
	}
	rc.resetFlashNodeTimeoutCount()
}

func (rc *RemoteCacheBase) GetFlashGroupBySlot(slot uint32) (*FlashGroup, uint32) {
	var item *SlotItem

	pivot := &SlotItem{slot: slot}
	rangeFunc := func(i btree.Item) bool {
		item = i.(*SlotItem)
		return false
	}
	rc.rangeFlashGroups(pivot, rangeFunc)

	if item == nil {
		return rc.getMinFlashGroup()
	}
	return item.FlashGroup, item.slot
}

func (rc *RemoteCacheBase) getFlashHostsMap() map[string]bool {
	allHosts := make(map[string]bool)

	rangeFunc := func(i btree.Item) bool {
		fgItem := i.(*SlotItem)
		for _, host := range fgItem.FlashGroup.Hosts {
			allHosts[host] = true
		}
		return true
	}
	rc.rangeFlashGroups(nil, rangeFunc)

	return allHosts
}

func (rc *RemoteCacheBase) rangeFlashGroups(pivot *SlotItem, rangeFunc func(item btree.Item) bool) {
	flashGroups := rc.flashGroups

	if pivot == nil {
		flashGroups.Ascend(rangeFunc)
	} else {
		flashGroups.AscendGreaterOrEqual(pivot, rangeFunc)
	}
}

func (rc *RemoteCacheBase) getMinFlashGroup() (*FlashGroup, uint32) {
	flashGroups := rc.flashGroups

	if flashGroups.Len() > 0 {
		item := flashGroups.Min().(*SlotItem)
		if item != nil {
			return item.FlashGroup, item.slot
		}
	}
	return nil, 0
}

func (rc *RemoteCacheBase) getFlashGroup(key string, fixedFileOffset uint64) (uint32, *FlashGroup, uint32) {
	slot := proto.ComputeCacheBlockSlot(key, 0, fixedFileOffset)
	fg, ownerSlot := rc.GetFlashGroupBySlot(slot)
	return slot, fg, ownerSlot
}

func (rc *RemoteCacheBase) readFromRemoteCache(ctx context.Context, key string, offset, size uint64, cReadRequests []*CacheReadRequest) (total int, err error) {
	metric := exporter.NewTPCnt("RemoteCacheBase:readFromRemoteCache")
	metricBytes := exporter.NewCounter("RemoteCacheBase:readFromRemoteCacheBytes")
	defer func() {
		metric.SetWithLabels(err, map[string]string{"key": key})
		metricBytes.AddWithLabels(int64(total), map[string]string{"key": key})
	}()

	var read int
	for _, req := range cReadRequests {
		if len(req.CacheRequest.Sources) == 0 {
			total += int(req.Size_)
			continue
		}
		slot, fg, ownerSlot := rc.getFlashGroup(req.CacheRequest.Volume, req.CacheRequest.FixedFileOffset)
		if fg == nil {
			err = fmt.Errorf("readFromRemoteCache failed: cannot find any flashGroups")
			return
		}
		req.CacheRequest.Slot = uint64(slot)<<32 | uint64(ownerSlot)
		// if read, err = s.client.RemoteCache.Read(ctx, fg, s.inode, req); err != nil {
		if read, err = rc.Read(ctx, fg, 0, req); err != nil {
			if !proto.IsFlashNodeLimitError(err) {
				log.LogWarnf("readFromRemoteCache: flashGroup read failed. offset(%v) size(%v) fg(%v) req(%v) err(%v)", offset, size, fg, req, err)
			}
			return
		} else {
			log.LogDebugf("readFromRemoteCache: inode(%d) cacheReadRequest version %v, source %v",
				req.CacheRequest.Inode, req.CacheRequest.Version, req.CacheRequest.Sources)
			total += read
		}
	}
	log.LogDebugf("readFromRemoteCache: cacheReadRequests(%v) offset(%v) size(%v) total(%v)", cReadRequests, offset, size, total)
	return total, nil
}

func (rc *RemoteCacheBase) Get(ctx context.Context, reqId, key string, from, to int64) (r io.ReadCloser, length int, shouldCache bool, err error) {
	size := to - from
	data := make([]byte, size)

	var cacheReadRequests []*CacheReadRequest
	cacheReadRequests, err = rc.prepareCacheRequests(key, uint64(from), uint64(size), data)

	if err == nil {
		var read int
		if read, err = rc.readFromRemoteCache(ctx, key, uint64(from), uint64(size), cacheReadRequests); err == nil {
			remoteCacheHitMetric := exporter.NewCounter("readRemoteCacheHit")
			remoteCacheHitMetric.AddWithLabels(1, map[string]string{"key": key})
			return io.NopCloser(bytes.NewReader(data[:read])), read, false, err
		} else {
			log.LogWarnf("RemoteCacheBase:Get failed, err(%v)", err)
		}
	}

	return nil, 0, false, nil
}

type CacheReadRequest struct {
	proto.CacheReadRequest
	Data []byte
}

func NewRemoteCachePacket(reqId int64, opcode uint8) *proto.Packet {
	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	if reqId == 0 {
		p.ReqID = proto.GenerateRequestID()
	} else {
		p.ReqID = reqId
	}
	p.Opcode = opcode
	return p
}

func NewFlashCacheReply() *proto.Packet {
	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	return p
}

func (rc *RemoteCacheBase) prepareCacheRequests(key string, offset, size uint64, data []byte) ([]*CacheReadRequest, error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("prepareCacheRequests", nil, bgTime, 1)
	}()

	var (
		cReadRequests []*CacheReadRequest
		cRequests     = make([]*proto.CacheRequest, 0)
	)
	for fixedOff := offset / rc.blockSize * rc.blockSize; fixedOff < offset+size; fixedOff += rc.blockSize {
		cReq := &proto.CacheRequest{
			Volume:          key,
			FixedFileOffset: fixedOff,
			TTL:             rc.TTL,
		}
		cRequests = append(cRequests, cReq)
	}
	cReadRequests = rc.GetCacheReadRequests(offset, size, data, cRequests)
	log.LogDebugf("prepareCacheRequests: extent[offset=%v,size=%v] cReadRequests %v ", offset, size, cReadRequests)
	return cReadRequests, nil
}

func (rc *RemoteCacheBase) GetCacheReadRequests(offset uint64, size uint64, data []byte, cRequests []*proto.CacheRequest) (cReadRequests []*CacheReadRequest) {
	cReadRequests = make([]*CacheReadRequest, 0, len(cRequests))
	startFixedOff := offset / rc.blockSize * rc.blockSize
	endFixedOff := (offset + size - 1) / rc.blockSize * rc.blockSize

	for _, cReq := range cRequests {
		cReadReq := new(CacheReadRequest)
		cReadReq.CacheRequest = cReq
		if cReq.FixedFileOffset == startFixedOff {
			cReadReq.Offset = offset - startFixedOff
		} else {
			cReadReq.Offset = 0
		}

		if cReq.FixedFileOffset == endFixedOff {
			cReadReq.Size_ = offset + size - cReq.FixedFileOffset - cReadReq.Offset
		} else {
			cReadReq.Size_ = rc.blockSize - cReadReq.Offset
		}

		dataStart := cReadReq.Offset + cReq.FixedFileOffset - offset
		cReadReq.Data = data[dataStart : dataStart+cReadReq.Size_]
		cReadRequests = append(cReadRequests, cReadReq)
	}
	return cReadRequests
}

func (rc *RemoteCacheBase) Read(ctx context.Context, fg *FlashGroup, reqId int64, req *CacheReadRequest) (read int, err error) {
	var (
		conn      *net.TCPConn
		moved     bool
		addr      string
		reqPacket *proto.Packet
	)
	bgTime := stat.BeginStat()
	defer func() {
		forceClose := err != nil && !proto.IsFlashNodeLimitError(err)
		rc.conns.PutConnect(conn, forceClose)
		if err != nil && strings.Contains(err.Error(), "timeout") {
			err = fmt.Errorf("read timeout")
		}
		parts := strings.Split(addr, ":")
		if len(parts) > 0 && addr != "" {
			stat.EndStat(fmt.Sprintf("flashNode:%v", parts[0]), err, bgTime, 1)
		}
		stat.EndStat("flashNode", err, bgTime, 1)
	}()
	for {
		addr = fg.getFlashHost()
		if addr == "" {
			err = fmt.Errorf("no available host")
			log.LogWarnf("FlashGroup Read failed: fg(%v) err(%v)", fg, err)
			return
		}
		reqPacket = NewRemoteCachePacket(reqId, proto.OpFlashNodeCacheRead)
		if err = reqPacket.MarshalDataPb(&req.CacheReadRequest); err != nil {
			log.LogWarnf("FlashGroup Read: failed to MarshalData (%+v). err(%v)", req, err)
			return
		}
		if conn, err = rc.conns.GetConnect(addr); err != nil {
			log.LogWarnf("FlashGroup Read: get connection failed, addr(%v) reqPacket(%v) err(%v) remoteCacheMultiRead(%v)", addr, req, err, rc.RemoteCacheMultiRead)
			moved = fg.moveToUnknownRank(addr, err, rc.FlashNodeTimeoutCount)
			if rc.RemoteCacheMultiRead {
				log.LogInfof("Retrying due to GetConnect of addr(%v) failure err(%v)", addr, err)
				continue
			}
			return
		}

		if err = reqPacket.WriteToConn(conn); err != nil {
			log.LogWarnf("FlashGroup Read: failed to write to addr(%v) err(%v) remoteCacheMultiRead(%v)", addr, err, rc.RemoteCacheMultiRead)
			rc.conns.PutConnect(conn, err != nil)
			moved = fg.moveToUnknownRank(addr, err, rc.FlashNodeTimeoutCount)
			if rc.RemoteCacheMultiRead {
				log.LogInfof("Retrying due to write to addr(%v) failure err(%v)", addr, err)
				continue
			}
			return
		}
		if read, err = rc.getReadReply(conn, reqPacket, req); err != nil {
			// TODO: may try other replica in future
			if proto.IsFlashNodeLimitError(err) {
				break
			}
			log.LogWarnf("FlashGroup Read: getReadReply from addr(%v) err(%v) remoteCacheMultiRead(%v)", addr, err, rc.RemoteCacheMultiRead)
			rc.conns.PutConnect(conn, err != nil)
			moved = fg.moveToUnknownRank(addr, err, rc.FlashNodeTimeoutCount)
			if rc.RemoteCacheMultiRead {
				log.LogInfof("Retrying due to getReadReply from addr(%v) failure  err(%v)", addr, err)
				continue
			}
		}
		break
	}

	log.LogDebugf("FlashGroup Read: flashGroup(%v) addr(%v) CacheReadRequest(%v) reqPacket(%v) err(%v) moved(%v) remoteCacheMultiRead(%v)",
		fg, addr, req, reqPacket, err, moved, rc.RemoteCacheMultiRead)
	return
}

func (rc *RemoteCacheBase) Prepare(ctx context.Context, fg *FlashGroup, req *proto.CachePrepareRequest) (err error) {
	var (
		conn  *net.TCPConn
		moved bool
	)
	addr := fg.getFlashHost()
	if addr == "" {
		err = fmt.Errorf("getFlashHost failed: can not find host")
		log.LogWarnf("FlashGroup prepare failed: err(%v)", err)
		return
	}
	reqPacket := NewRemoteCachePacket(0, proto.OpFlashNodeCachePrepare)
	if err = reqPacket.MarshalDataPb(req); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to MarshalData (%v), err(%v)", req, err)
		return
	}
	defer func() {
		if err != nil {
			moved = fg.moveToUnknownRank(addr, err, rc.FlashNodeTimeoutCount)
		}
	}()
	if conn, err = rc.conns.GetConnect(addr); err != nil {
		log.LogWarnf("FlashGroup prepare: get connection to curr addr failed, addr(%v) reqPacket(%v) err(%v)", addr, req, err)
		return
	}
	defer func() {
		rc.conns.PutConnect(conn, err != nil)
	}()

	if err = reqPacket.WriteToConn(conn); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to write to addr(%v) err(%v)", addr, err)
		return
	}

	replyPacket := NewFlashCacheReply()
	if err = replyPacket.ReadFromConnExt(conn, int(rc.ReadTimeout)); err != nil {
		log.LogWarnf("FlashGroup Prepare: failed to ReadFromConn, replyPacket(%v), fg host(%v) moved(%v), err(%v)", replyPacket, addr, moved, err)
		return
	}
	if replyPacket.ResultCode != proto.OpOk {
		log.LogWarnf("FlashGroup Prepare: ResultCode NOK, replyPacket(%v), fg host(%v), ResultCode(%v)", replyPacket, addr, replyPacket.ResultCode)
		err = fmt.Errorf("ResultCode NOK (%v)", replyPacket.ResultCode)
		return
	}
	log.LogDebugf("FlashGroup Prepare successful: flashGroup(%v) addr(%v) CachePrepareRequest(%v) reqPacket(%v) replyPacket(%v) err(%v) moved(%v)", fg, addr, req, reqPacket, replyPacket, err, moved)
	return
}

func (rc *RemoteCacheBase) getReadReply(conn *net.TCPConn, reqPacket *proto.Packet, req *CacheReadRequest) (readBytes int, err error) {
	for readBytes < int(req.Size_) {
		replyPacket := NewFlashCacheReply()
		start := time.Now()
		err = replyPacket.ReadFromConnExt(conn, int(rc.ReadTimeout))
		if err != nil {
			log.LogWarnf("getReadReply: failed to read from connect, req(%v) readBytes(%v) err(%v) cost %v ReadTimeout %v",
				reqPacket, readBytes, err, time.Since(start).String(), rc.ReadTimeout)
			return
		}
		if replyPacket.ResultCode != proto.OpOk {
			err = fmt.Errorf("%v", string(replyPacket.Data))
			if !proto.IsFlashNodeLimitError(err) {
				log.LogWarnf("getReadReply: ResultCode NOK, req(%v) reply(%v) ResultCode(%v)", reqPacket, replyPacket, replyPacket.ResultCode)
			}
			return
		}
		expectCrc := crc32.ChecksumIEEE(replyPacket.Data[:replyPacket.Size])
		if replyPacket.CRC != expectCrc {
			err = fmt.Errorf("inconsistent CRC, expect(%v) reply(%v)", expectCrc, replyPacket.CRC)
			log.LogWarnf("getReadReply: req(%v) err(%v)", req, err)
			return
		}
		copy(req.Data[readBytes:], replyPacket.Data)
		readBytes += int(replyPacket.Size)
	}
	return
}
