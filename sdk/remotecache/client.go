package remotecache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/auditlog"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/bytespool"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/google/uuid"
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
	SameZoneWeight             = 70
	RefreshFlashNodesInterval  = time.Minute
	RefreshHostLatencyInterval = 20 * time.Second
)

type AddressPingStats struct {
	sync.Mutex
	durations []time.Duration
	index     int
}

type RemoteCacheClient struct {
	flashGroups  *btree.BTree
	mc           *master.MasterClient
	conns        *util.ConnectPool
	hostLatency  sync.Map
	TTL          int64
	ReadTimeout  int64 // ms
	WriteTimeout int64
	stopC        chan struct{}
	wg           sync.WaitGroup

	blockSize             uint64
	clusterEnabled        bool
	RemoteCacheMultiRead  bool
	FlashNodeTimeoutCount int32
	SameZoneTimeout       int64 // microsecond
	SameRegionTimeout     int64 // ms
	AddressPingMap        sync.Map
	firstPacketTimeout    int64
	FromFuse              bool
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

type ClientConfig struct {
	Masters            []string
	BlockSize          uint64
	NeedInitLog        bool
	LogLevelStr        string
	LogDir             string
	ConnectTimeout     int64 // ms
	FirstPacketTimeout int64 // ms
	FromFuse           bool
}

func NewRemoteCacheClient(config *ClientConfig) (rc *RemoteCacheClient, err error) {
	if config.NeedInitLog {
		logLevel := log.ParseLogLevel(config.LogLevelStr)
		_, err = log.InitLog(config.LogDir, "remoteCacheClient", logLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
		if err != nil {
			return nil, errors.New("failed to init log")
		}
	}

	if proto.Buffers == nil {
		proto.InitBufferPool(32768)
	}

	rc = new(RemoteCacheClient)
	log.LogDebugf("NewRemoteCacheClient")
	rc.stopC = make(chan struct{})
	rc.flashGroups = btree.New(32)
	rc.ReadTimeout = proto.DefaultRemoteCacheClientReadTimeout
	rc.WriteTimeout = proto.DefaultRemoteCacheExtentReadTimeout
	rc.firstPacketTimeout = config.FirstPacketTimeout

	rc.mc = master.NewMasterClient(config.Masters, false)
	rc.conns = util.NewConnectPoolWithTimeoutAndCap(5, 500, ConnIdelTimeout, config.ConnectTimeout, true)
	rc.TTL = proto.DefaultRemoteCacheTTL
	rc.RemoteCacheMultiRead = false
	rc.FlashNodeTimeoutCount = proto.DefaultFlashNodeTimeoutCount
	rc.SameZoneTimeout = proto.DefaultRemoteCacheSameZoneTimeout
	rc.SameRegionTimeout = proto.DefaultRemoteCacheSameRegionTimeout
	rc.blockSize = config.BlockSize
	rc.FromFuse = config.FromFuse
	if !config.FromFuse {
		err = rc.updateRemoteCacheConfig()
		if err != nil {
			log.LogWarnf("NewRemoteCacheClient: updateRemoteCacheConfig err %v", err)
		}
	}
	err = rc.UpdateFlashGroups()
	if err != nil {
		log.LogWarnf("NewRemoteCacheClient: updateFlashGroups err %v", err)
	}
	rc.wg.Add(1)
	go rc.refresh()
	log.LogDebugf("NewRemoteCacheClient sucess")
	return rc, err
}

func (rc *RemoteCacheClient) Stop() {
	close(rc.stopC)
	rc.conns.Close()
	log.LogFlush()
	rc.wg.Wait()
}

func (rc *RemoteCacheClient) refresh() {
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

func (rc *RemoteCacheClient) refreshWithRecover() (panicErr error) {
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

	var (
		err    error
		notNew bool
	)
	for {
		select {
		case <-rc.stopC:
			log.LogInfof("refreshWithRecover: remote stop")
			return
		case <-refreshView.C:
			if !rc.FromFuse {
				if err = rc.updateRemoteCacheConfig(); err != nil {
					log.LogErrorf("updateRemoteCacheConfig err: %v", err)
				}
			}
		case <-refreshLatency.C:
			if notNew {
				if err = rc.UpdateFlashGroups(); err != nil {
					log.LogErrorf("updateFlashGroups err: %v", err)
				}
			} else {
				notNew = true
			}
			rc.refreshHostLatency()
			refreshLatency.Reset(RefreshHostLatencyInterval)
		}
	}
}

func (rc *RemoteCacheClient) UpdateClusterEnable() error {
	if rc.mc != nil {
		if fgv, err := rc.mc.AdminAPI().ClientFlashGroups(); err != nil {
			log.LogWarnf("updateFlashGroups: err(%v)", err)
			return err
		} else {
			rc.SetClusterEnable(fgv.Enable && len(fgv.FlashGroups) != 0)
		}
	}
	return nil
}

func (rc *RemoteCacheClient) SetClusterEnable(enable bool) {
	if rc.clusterEnabled != enable {
		log.LogInfof("enableRemoteCacheCluster: %v -> %v", rc.clusterEnabled, enable)
		rc.clusterEnabled = enable
	}
}

func (rc *RemoteCacheClient) IsClusterEnable() bool {
	return rc.clusterEnabled
}

func (rc *RemoteCacheClient) UpdateFlashGroups() (err error) {
	var (
		fgv            proto.FlashGroupView
		newFlashGroups = btree.New(32)
	)

	for i := 0; i < 3; i++ {
		if fgv, err = rc.mc.AdminAPI().ClientFlashGroups(); err == nil {
			break
		}
		log.LogWarnf("updateFlashGroups: attempt %d failed, err(%v)", i+1, err)
		if i == 2 { // Last attempt
			return
		}
	}
	log.LogDebugf("updateFlashGroups. get flashGroupView [%v]", fgv)
	rc.SetClusterEnable(fgv.Enable && len(fgv.FlashGroups) != 0)
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

func (rc *RemoteCacheClient) ClassifyHostsByAvgDelay(fgID uint64, hosts []string) (sortedHosts map[ZoneRankType][]string) {
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

func (rc *RemoteCacheClient) resetFlashNodeTimeoutCount() {
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

func (rc *RemoteCacheClient) refreshHostLatency() {
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

func (rc *RemoteCacheClient) HeartBeat(addr string) (duration time.Duration, err error) {
	var conn *net.TCPConn
	packet := proto.NewPacket()
	packet.Opcode = proto.OpFlashSDKHeartbeat

	defer func() {
		rc.conns.PutConnect(conn, err != nil)
	}()

	if conn, err = rc.conns.GetConnect(addr); err != nil {
		log.LogWarnf("HeartBeat get connection to addr failed, addr(%v) err(%v)", addr, err)
		return
	}
	start := time.Now()
	if err = packet.WriteToConn(conn); err != nil {
		log.LogWarnf("HeartBeat failed write to addr(%v) err(%v)", addr, err)
		return
	}
	if err = packet.ReadFromConnExt(conn, int(rc.SameRegionTimeout)); err != nil {
		log.LogWarnf("HeartBeat failed to ReadFromConn addr(%v) err(%v)", addr, err)
		return
	}

	duration = time.Since(start)
	log.LogDebugf("HeartBeat from addr(%v) cost(%v)", addr, duration)
	return
}

func (rc *RemoteCacheClient) updateHostLatency(hosts []string) {
	for _, host := range hosts {
		// avgRtt, err := iputil.PingWithTimeout(strings.Split(host, ":")[0], PingCount, time.Millisecond*time.Duration(rc.ReadTimeout))
		avgRtt, err := rc.HeartBeat(host)
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

func (rc *RemoteCacheClient) GetFlashGroupBySlot(slot uint32) (*FlashGroup, uint32) {
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

func (rc *RemoteCacheClient) getFlashHostsMap() map[string]bool {
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

func (rc *RemoteCacheClient) rangeFlashGroups(pivot *SlotItem, rangeFunc func(item btree.Item) bool) {
	flashGroups := rc.flashGroups

	if pivot == nil {
		flashGroups.Ascend(rangeFunc)
	} else {
		flashGroups.AscendGreaterOrEqual(pivot, rangeFunc)
	}
}

func (rc *RemoteCacheClient) getMinFlashGroup() (*FlashGroup, uint32) {
	flashGroups := rc.flashGroups

	if flashGroups.Len() > 0 {
		item := flashGroups.Min().(*SlotItem)
		if item != nil {
			return item.FlashGroup, item.slot
		}
	}
	return nil, 0
}

func (rc *RemoteCacheClient) getFlashGroupByKey(key string) (uint32, *FlashGroup, uint32) {
	slot := proto.ComputeCacheBlockSlot(key, 0, 0)
	fg, ownerSlot := rc.GetFlashGroupBySlot(slot)
	return slot, fg, ownerSlot
}

func (rc *RemoteCacheClient) Delete(ctx context.Context, key string) (err error) {
	if key == "" {
		log.LogWarnf("delete block key is nil")
		return proto.ErrorDeleteBlockKeyNil
	}
	slot := proto.ComputeCacheBlockSlot(key, 0, 0)
	fg, _ := rc.GetFlashGroupBySlot(slot)
	if fg == nil {
		err = proto.ErrorNoFlashGroup
		log.LogWarnf("FlashGroup delete failed: err(%v)", err)
		return
	}
	addr := fg.getFlashHost()
	if log.EnableDebug() {
		log.LogDebugf("FlashGroup addr(%v) delete key(%v)", addr, key)
	}
	if addr == "" {
		err = proto.ErrorNoAvailableHost
		log.LogWarnf("FlashGroup fg(%v) delete failed: err(%v)", fg, err)
		return
	}
	rc.deleteRemoteBlock(key, addr)
	return
}

func (rc *RemoteCacheClient) Put(ctx context.Context, reqId, key string, r io.Reader, length int64) (err error) {
	if length <= 0 || length > proto.CACHE_OBJECT_BLOCK_SIZE {
		log.LogWarnf("put data length %v is leq 0 or gt 4M", length)
		return fmt.Errorf(proto.ErrorPutDataLengthInvalidTpl, length)
	}
	slot := proto.ComputeCacheBlockSlot(key, 0, 0)
	fg, _ := rc.GetFlashGroupBySlot(slot)
	if fg == nil {
		err = proto.ErrorNoFlashGroup
		log.LogWarnf("FlashGroup reqId(%v) put failed: err(%v)", reqId, err)
		return
	}
	addr := fg.getFlashHost()
	if addr == "" {
		err = proto.ErrorNoAvailableHost
		log.LogWarnf("FlashGroup fg(%v) reqId(%v) put failed: err(%v)", fg, reqId, err)
		return
	}
	var conn *net.TCPConn
	if conn, err = rc.conns.GetConnect(addr); err != nil {
		moved := fg.moveToUnknownRank(addr, err, rc.FlashNodeTimeoutCount)
		log.LogWarnf("FlashGroup put: reqId(%v) get connection to curr addr failed, addr(%v) key(%v) moved(%v) err(%v)", reqId, addr, key, moved, err)
		return
	}
	bgTime := stat.BeginStat()
	defer func() {
		forceClose := err != nil && !proto.IsFlashNodeLimitError(err)
		rc.conns.PutConnect(conn, forceClose)
		parts := strings.Split(addr, ":")
		if len(parts) > 0 && addr != "" {
			stat.EndStat(fmt.Sprintf("flashPutBlock:%v", parts[0]), err, bgTime, 1)
		}
		stat.EndStat("flashPutBlock", err, bgTime, 1)
	}()
	req := &proto.PutBlockHead{
		UniKey:   key,
		BlockLen: length,
		TTL:      uint64(rc.TTL),
	}
	reqPacket := proto.NewPacketReqID()
	if len(reqId) == 0 {
		reqId = uuid.NewString()
	}
	reqPacket.Arg = []byte(reqId)
	reqPacket.ArgLen = uint32(len(reqId))
	_ = reqPacket.MarshalDataPb(req)
	reqPacket.Opcode = proto.OpFlashNodeCachePutBlock
	if err = reqPacket.WriteToConn(conn); err != nil {
		log.LogWarnf("FlashGroup put: reqId(%v) failed to write header to addr(%v) err(%v)", reqId, addr, err)
		return
	}
	replyPacket := NewFlashCacheReply()
	if err = replyPacket.ReadFromConnExt(conn, int(rc.firstPacketTimeout)); err != nil {
		log.LogWarnf("FlashGroup put: reqId(%v) failed to ReadFromConn, replyPacket(%v), fg host(%v) , err(%v)", reqId, replyPacket, addr, err)
		return
	}
	if replyPacket.ResultCode != proto.OpOk {
		err = fmt.Errorf(string(replyPacket.Data))
		if !proto.IsFlashNodeLimitError(err) {
			log.LogWarnf("getPutBlockReply: ResultCode NOK, req(%v) reply(%v) ResultCode(%v)", reqPacket, replyPacket, replyPacket.ResultCode)
		}
		return
	}
	defer func() {
		if err != nil {
			log.LogWarnf("FlashGroup put: reqId(%v) remove key %v by err %v", reqId, key, err)
			rc.deleteRemoteBlock(key, addr)
		}
	}()
	var (
		totalWritten int64
		n            int
	)
	writeLen := proto.CACHE_BLOCK_PACKET_SIZE + proto.CACHE_BLOCK_CRC_SIZE
	buf := bytespool.Alloc(writeLen)
	ch := &proto.CoonHandler{
		Completed:   make(chan struct{}),
		WaitAckChan: make(chan struct{}, 128),
	}
	defer func() {
		bytespool.Free(buf)
		if !ch.WaitAckChanClosed {
			ch.WaitAckChanClosed = true
			close(ch.WaitAckChan)
		}
	}()
	go rc.processPutReply(conn, ch, reqId)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if ch.RemoteError != nil {
			return ch.RemoteError
		}
		readSize := proto.CACHE_BLOCK_PACKET_SIZE
		if totalWritten+int64(readSize) > length {
			readSize = int(length - totalWritten)
		}
		bufferOffset := 0
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if ch.RemoteError != nil {
				return ch.RemoteError
			}
			currentReadSize := readSize - bufferOffset
			n, err = r.Read(buf[bufferOffset : bufferOffset+currentReadSize])
			bufferOffset += n
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			} else if bufferOffset == readSize {
				if log.EnableDebug() {
					log.LogDebugf(" total written %v write size %v to fg", totalWritten, readSize)
				}
				break
			}
		}
		if bufferOffset != readSize {
			log.LogWarnf("FlashGroup put: expected to read %d bytes, but only read %d", readSize, bufferOffset)
			return fmt.Errorf(proto.ErrorExpectedReadBytesMismatchTpl, readSize, bufferOffset)
		}
		if bufferOffset > 0 {
			if log.EnableDebug() {
				log.LogDebugf("FlashGroup put: write %d bytes total(%d) to fg", bufferOffset, totalWritten)
			}
			binary.BigEndian.PutUint32(buf[proto.CACHE_BLOCK_PACKET_SIZE:], crc32.ChecksumIEEE(buf[:proto.CACHE_BLOCK_PACKET_SIZE]))
			conn.SetWriteDeadline(time.Now().Add(proto.WriteDeadlineTime * time.Second))
			if _, err = conn.Write(buf[:writeLen]); err != nil {
				log.LogErrorf("wirte data and crc to flashnode get err %v", err)
				return fmt.Errorf(proto.ErrorWriteDataAndCRCToFlashNodeTpl, err)
			}
			ch.WaitAckChan <- struct{}{}
			totalWritten += int64(bufferOffset)
		}
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			return err
		} else if totalWritten == length {
			if log.EnableDebug() {
				log.LogDebugf("key(%v) total written(%v) equal to lengh(%v)", key, totalWritten, length)
			}
			break
		}
	}
	if !ch.WaitAckChanClosed {
		ch.WaitAckChanClosed = true
		close(ch.WaitAckChan)
	}
	<-ch.Completed
	if ch.RemoteError != nil {
		return ch.RemoteError
	}
	if totalWritten != length {
		return fmt.Errorf(proto.ErrorUnexpectedDataLengthTpl, length, totalWritten)
	}
	if log.EnableDebug() {
		log.LogDebugf("put data success key(%v) addr(%v) totalWriten(%v)", key, addr, totalWritten)
	}
	return nil
}

func (rc *RemoteCacheClient) processPutReply(conn *net.TCPConn, ch *proto.CoonHandler, reqId string) {
	defer close(ch.Completed)
	var err error
	replyPacket := NewFlashCacheReply()
	for range ch.WaitAckChan {
		if err = replyPacket.ReadFromConnExt(conn, int(rc.WriteTimeout)); err != nil {
			log.LogWarnf("FlashGroup put data: reqId(%v) failed to ReadFromConn, replyPacket(%v) , err(%v)", reqId, replyPacket, err)
			ch.RemoteError = err
			return
		}
		if replyPacket.ResultCode != proto.OpOk {
			err = fmt.Errorf(string(replyPacket.Data))
			if !proto.IsFlashNodeLimitError(err) {
				log.LogWarnf("getPutBlockReply: put data ResultCode NOK, reqId(%v) reply(%v) ResultCode(%v)", reqId, replyPacket, replyPacket.ResultCode)
			}
			ch.RemoteError = err
			return
		}
	}
}

func (rc *RemoteCacheClient) deleteRemoteBlock(key string, addr string) {
	var (
		err  error
		conn *net.TCPConn
	)
	if conn, err = rc.conns.GetConnect(addr); err != nil {
		log.LogWarnf("FlashGroup delete: get connection to curr addr failed, addr(%v) key(%v)  err(%v)", addr, key, err)
		return
	}
	defer func() {
		rc.conns.PutConnect(conn, err != nil)
	}()
	p := proto.NewPacketReqID()
	p.Data = ([]byte)(key)
	p.Opcode = proto.OpFlashNodeCacheDelete
	p.Size = uint32(len(p.Data))
	if err = p.WriteToConn(conn); err != nil {
		log.LogWarnf("FlashGroup delete: failed to write to addr(%v) err(%v)", conn.RemoteAddr().String(), err)
		return
	}
	replyPacket := NewFlashCacheReply()
	if err = replyPacket.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		log.LogWarnf("FlashGroup delete: failed to ReadFromConn, replyPacket(%v), fg host(%v) err(%v)", replyPacket, addr, err)
		return
	}
}

type CacheReadRequest struct {
	proto.CacheReadRequest
	Data []byte
}

func NewRemoteCachePacket(reqId string, opcode uint8) *proto.Packet {
	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.Opcode = opcode
	if len(reqId) != 0 {
		p.Arg = []byte(reqId)
		p.ArgLen = uint32(len(reqId))
	}
	return p
}

func NewFlashCacheReply() *proto.Packet {
	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	return p
}

func (rc *RemoteCacheClient) GetCacheReadRequests(offset uint64, size uint64, data []byte, cRequests []*proto.CacheRequest) (cReadRequests []*CacheReadRequest) {
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

func (rc *RemoteCacheClient) Read(ctx context.Context, fg *FlashGroup, reqId int64, req *CacheReadRequest) (read int, err error) {
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
			err = proto.ErrorReadTimeout
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
			err = proto.ErrorNoAvailableHost
			log.LogWarnf("FlashGroup Read failed: fg(%v) err(%v)", fg, err)
			return
		}
		reqPacket = NewRemoteCachePacket("", proto.OpFlashNodeCacheRead)
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

func (rc *RemoteCacheClient) Prepare(ctx context.Context, fg *FlashGroup, req *proto.CachePrepareRequest) (err error) {
	var (
		conn  *net.TCPConn
		moved bool
	)
	addr := fg.getFlashHost()
	if addr == "" {
		err = proto.ErrorNoAvailableHost
		log.LogWarnf("FlashGroup prepare failed: err(%v)", err)
		return
	}
	reqPacket := NewRemoteCachePacket("", proto.OpFlashNodeCachePrepare)
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
		err = fmt.Errorf(proto.ErrorResultCodeNOKTpl, replyPacket.ResultCode)
		return
	}
	log.LogDebugf("FlashGroup Prepare successful: flashGroup(%v) addr(%v) CachePrepareRequest(%v) reqPacket(%v) replyPacket(%v) err(%v) moved(%v)", fg, addr, req, reqPacket, replyPacket, err, moved)
	return
}

func (rc *RemoteCacheClient) getReadReply(conn *net.TCPConn, reqPacket *proto.Packet, req *CacheReadRequest) (readBytes int, err error) {
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
			err = fmt.Errorf(string(replyPacket.Data))
			if !proto.IsFlashNodeLimitError(err) {
				log.LogWarnf("getReadReply: ResultCode NOK, req(%v) reply(%v) ResultCode(%v)", reqPacket, replyPacket, replyPacket.ResultCode)
			}
			return
		}
		expectCrc := crc32.ChecksumIEEE(replyPacket.Data[:replyPacket.Size])
		if replyPacket.CRC != expectCrc {
			err = fmt.Errorf(proto.ErrorInconsistentCRCTpl, expectCrc, replyPacket.CRC)
			log.LogWarnf("getReadReply: req(%v) err(%v)", req, err)
			return
		}
		copy(req.Data[readBytes:], replyPacket.Data)
		readBytes += int(replyPacket.Size)
	}
	return
}

func (rc *RemoteCacheClient) Name() string {
	return "remoteCache"
}

func (rc *RemoteCacheClient) Get(ctx context.Context, reqId, key string, from, to int64) (r io.ReadCloser, length int64, shouldCache bool, err error) {
	log.LogDebugf("RemoteCacheClient Get: key(%v) reqId(%v) from(%v) to(%v)", key, reqId, from, to)
	if from < 0 || to-from <= 0 {
		return nil, 0, false, fmt.Errorf(proto.ErrorInvalidRangeTpl, from, to)
	}
	remoteCacheMetric := exporter.NewCounter("readRemoteCache")
	remoteCacheMetric.AddWithLabels(1, map[string]string{"key": key})
	var cr *RemoteCacheReader
	if cr, length, err = rc.readObjectFromRemoteCache(ctx, key, reqId, uint64(from), uint64(to-from)); err == nil {
		r = cr
		remoteCacheHitMetric := exporter.NewCounter("readRemoteCacheHit")
		remoteCacheHitMetric.AddWithLabels(1, map[string]string{"key": key})
	} else if strings.Contains(err.Error(), proto.ErrorNotExistShouldCache.Error()) {
		shouldCache = true
	}
	return
}

func (rc *RemoteCacheClient) updateRemoteCacheConfig() (err error) {
	var config *proto.RemoteCacheConfig

	for i := 0; i < 3; i++ {
		if config, err = rc.mc.AdminAPI().GetRemoteCacheConfig(); err == nil {
			break
		}
		log.LogWarnf("updateRemoteCacheConfig: attempt %d failed, GetRemoteCacheConfig fail err(%v)", i+1, err)
		if i == 2 { // Last attempt
			return
		}
	}

	log.LogInfof("updateRemoteCacheConfig: config(%v)", config)

	if rc.TTL != config.RemoteCacheTTL {
		log.LogInfof("updateRemoteCacheConfig RcTTL: %d -> %d", rc.TTL, config.RemoteCacheTTL)
		rc.TTL = config.RemoteCacheTTL
	}
	if rc.ReadTimeout != config.RemoteCacheReadTimeout {
		log.LogInfof("updateRemoteCacheConfig RcReadTimeoutSec: %d(ms) -> %d(ms)", rc.ReadTimeout, config.RemoteCacheReadTimeout)
		rc.ReadTimeout = config.RemoteCacheReadTimeout
	}
	if rc.RemoteCacheMultiRead != config.RemoteCacheMultiRead {
		log.LogInfof("updateRemoteCacheConfig RcMultiRead: %v -> %v", rc.RemoteCacheMultiRead, config.RemoteCacheMultiRead)
		rc.RemoteCacheMultiRead = config.RemoteCacheMultiRead
	}

	if rc.FlashNodeTimeoutCount != int32(config.FlashNodeTimeoutCount) {
		log.LogInfof("updateRemoteCacheConfig RcFlashNodeTimeoutCount: %d -> %d", rc.FlashNodeTimeoutCount, int32(config.FlashNodeTimeoutCount))
		rc.FlashNodeTimeoutCount = int32(config.FlashNodeTimeoutCount)
	}
	if rc.SameZoneTimeout != config.RemoteCacheSameZoneTimeout {
		log.LogInfof("updateRemoteCacheConfig RcSameZoneTimeout: %d -> %d", rc.SameZoneTimeout, config.RemoteCacheSameZoneTimeout)
		rc.SameZoneTimeout = config.RemoteCacheSameZoneTimeout
	}
	if rc.SameRegionTimeout != config.RemoteCacheSameRegionTimeout {
		log.LogInfof("updateRemoteCacheConfig RcSameRegionTimeout: %d -> %d", rc.SameRegionTimeout, config.RemoteCacheSameRegionTimeout)
		rc.SameRegionTimeout = config.RemoteCacheSameRegionTimeout
	}
	return
}

func (rc *RemoteCacheClient) readObjectFromRemoteCache(ctx context.Context, key string, reqId string, offset, size uint64) (reader *RemoteCacheReader, length int64, err error) {
	metric := exporter.NewTPCnt("RemoteCacheClient:readObjectFromRemoteCache")
	metricBytes := exporter.NewCounter("RemoteCacheClient:readFromRemoteCacheBytes")
	defer func() {
		metric.SetWithLabels(err, map[string]string{"key": key})
		metricBytes.AddWithLabels(length, map[string]string{"key": key})
	}()

	slot, fg, ownerSlot := rc.getFlashGroupByKey(key)
	if fg == nil {
		err = proto.ErrorNoFlashGroup
		log.LogWarnf("readObjectFromRemoteCache: flashGroup read failed. key(%v) offset(%v) size(%v) reqId(%v)"+
			" err(%v)", key, offset, size, reqId, err)
		return
	}

	var req proto.CacheReadRequestBase
	req.Key = key
	req.TTL = rc.TTL
	req.Slot = uint64(slot)<<32 | uint64(ownerSlot)
	req.Offset = offset
	req.Size_ = size
	if deadline, ok := ctx.Deadline(); ok {
		req.Deadline = uint64(deadline.UnixNano())
	} else {
		req.Deadline = uint64(time.Now().Add(time.Millisecond * time.Duration(rc.ReadTimeout)).UnixNano())
	}
	if reader, length, err = rc.ReadObject(ctx, fg, reqId, &req); err != nil {
		log.LogWarnf("readObjectFromRemoteCache: flashGroup read failed. key(%v) offset(%v) size(%v) fg(%v) req(%v) reqId(%v)"+
			" err(%v)", key, offset, size, fg, req, reqId, err)
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("readObjectFromRemoteCache success: cacheReadRequestBase(%v) key(%v) reqId(%v) offset(%v) size(%v)",
			req, key, reqId, offset, size)
	}
	return
}

func (reader *RemoteCacheReader) getReadObjectReply(p *proto.Packet) (length int64, err error) {
	header, err := proto.Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer proto.Buffers.Put(header)
	var n int
	if n, err = io.ReadFull(reader.conn, header); err != nil {
		return
	}
	if n != util.PacketHeaderSize {
		return 0, syscall.EBADMSG
	}

	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if err = p.TryReadExtraFieldsFromConn(reader.conn); err != nil {
		return
	}

	if p.ArgLen > 0 {
		p.Arg = make([]byte, int(p.ArgLen))
		if _, err = io.ReadFull(reader.conn, p.Arg[:int(p.ArgLen)]); err != nil {
			return 0, err
		}
	}

	if p.ResultCode != proto.OpOk {
		size := p.Size
		p.Data = make([]byte, size)
		if _, err = io.ReadFull(reader.conn, p.Data[:size]); err != nil {
			return 0, err
		}
		err = fmt.Errorf(string(p.Data))
		if !proto.IsFlashNodeLimitError(err) {
			log.LogWarnf("getReadObjectReply: ResultCode NOK, err(%v) ResultCode(%v)", err, p.ResultCode)
		}
		return
	}
	return int64(p.Size), nil
}

type RemoteCacheReader struct {
	conn           *net.TCPConn
	rc             *RemoteCacheClient
	needReadLen    int64
	alreadyReadLen int64
	reqID          string
	ctx            context.Context
	buffer         []byte
	localData      []byte
	offset         int
	size           int
	closed         bool
}

func (rc *RemoteCacheClient) NewRemoteCacheReader(ctx context.Context, conn *net.TCPConn, reqID string) *RemoteCacheReader {
	r := &RemoteCacheReader{
		conn:   conn,
		rc:     rc,
		reqID:  reqID,
		ctx:    ctx,
		buffer: bytespool.Alloc(proto.CACHE_BLOCK_PACKET_SIZE),
		offset: 0,
		size:   0,
	}
	return r
}

func (reader *RemoteCacheReader) read(p []byte) (n int, err error) {
	if reader.ctx.Err() != nil {
		log.LogErrorf("RemoteCacheReader:reqID(%v) err(%v)", reader.reqID, reader.ctx.Err())
		return 0, reader.ctx.Err()
	}
	reply := new(proto.Packet)
	var expectLen int64

	alreadyReadLen := atomic.LoadInt64(&reader.alreadyReadLen)
	if alreadyReadLen >= reader.needReadLen {
		return 0, io.EOF
	}
	reader.conn.SetReadDeadline(time.Now().Add(time.Second * proto.ReadDeadlineTime))
	expectLen, err = reader.getReadObjectReply(reply)
	if err != nil {
		if proto.IsFlashNodeLimitError(err) {
			log.LogInfof("RemoteCacheReader:reqID(%v) Read reply err(%v)", reader.reqID, err)
		} else {
			log.LogErrorf("RemoteCacheReader:reqID(%v) Read reply err(%v)", reader.reqID, err)
		}
		return
	}
	_, err = io.ReadFull(reader.conn, p)
	if err != nil {
		log.LogErrorf("RemoteCacheReader:reqID(%v) Read err(%v)", reader.reqID, err)
		return
	}

	// check crc
	actualCrc := crc32.ChecksumIEEE(p)
	if actualCrc != reply.CRC {
		err = fmt.Errorf(proto.ErrorInconsistentCRCObjectTpl, reply.KernelOffset, reply.ExtentOffset, reply.CRC, actualCrc)
		log.LogErrorf("RemoteCacheReader:Read check crc failed  reqID(%v) offset(%v) extentOffset(%v) expect(%v) actualCrc(%v)",
			reader.reqID, reply.KernelOffset, reply.ExtentOffset, reply.CRC, actualCrc)
		return
	}
	extentOffset := reply.ExtentOffset
	reader.localData = p[extentOffset : extentOffset+expectLen]
	log.LogDebugf("RemoteCacheReader:Read offset(%v) extentOffset(%v) expectLen(%v) p.len(%v)", reply.KernelOffset, reply.ExtentOffset, expectLen, len(p))
	atomic.StoreInt64(&reader.alreadyReadLen, reader.alreadyReadLen+expectLen)
	return int(expectLen), nil
}

func (reader *RemoteCacheReader) Read(p []byte) (n int, err error) {
	defer func() {
		if err != nil && err != io.EOF && !reader.closed {
			reader.closed = true
			bytespool.Free(reader.buffer)
			reader.rc.conns.PutConnect(reader.conn, true)
		}
	}()
	if len(p) == 0 {
		return 0, nil
	}
	if reader.closed {
		log.LogErrorf("read from close reader reqID(%v)", reader.reqID)
		return 0, fmt.Errorf(proto.ErrorReadFromCloseReaderTpl, reader.reqID)
	}
	if reader.ctx.Err() != nil {
		log.LogErrorf("RemoteCacheReader:reqID(%v) err(%v)", reader.reqID, err)
		return 0, reader.ctx.Err()
	}

	totalRead := 0

	for totalRead < len(p) {
		// if buffer is empty, read from flash reader
		if reader.offset >= reader.size {
			reader.size, err = reader.read(reader.buffer)
			if reader.size == 0 {
				if log.EnableDebug() {
					log.LogDebugf("RemoteCacheReader:reqID(%v), from connection size 0", reader.reqID)
				}
				if err != nil {
					return totalRead, err
				} else {
					return totalRead, io.EOF
				}
			}
			reader.offset = 0
		}

		// copy buffer data to target slice
		available := reader.size - reader.offset
		toRead := len(p) - totalRead
		if toRead > available {
			toRead = available
		}

		copy(p[totalRead:], reader.localData[reader.offset:reader.offset+toRead])
		totalRead += toRead
		reader.offset += toRead
	}
	if log.EnableDebug() {
		log.LogDebugf("RemoteCacheReader:reqID(%v), totalRead:%d", reader.reqID, totalRead)
	}
	return totalRead, nil
}

func (reader *RemoteCacheReader) Close() error {
	if !reader.closed {
		log.LogDebugf("RemoteCacheReader Close, reqId(%v)", reader.reqID)
		reader.closed = true
		bytespool.Free(reader.buffer)
		reader.rc.conns.PutConnect(reader.conn, false)
	}
	return nil
}

func (rc *RemoteCacheClient) ReadObjectFirstReply(conn *net.TCPConn, p *proto.Packet, reqId string) (length int64, err error) {
	header, err := proto.Buffers.Get(util.PacketHeaderSize)
	if err != nil {
		header = make([]byte, util.PacketHeaderSize)
	}
	defer proto.Buffers.Put(header)
	var n int
	if n, err = io.ReadFull(conn, header); err != nil {
		return
	}
	if n != util.PacketHeaderSize {
		return 0, syscall.EBADMSG
	}

	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if err = p.TryReadExtraFieldsFromConn(conn); err != nil {
		return
	}

	if p.ArgLen > 0 {
		p.Arg = make([]byte, int(p.ArgLen))
		if _, err = io.ReadFull(conn, p.Arg[:int(p.ArgLen)]); err != nil {
			return 0, err
		}
	}

	if p.ResultCode != proto.OpOk {
		size := p.Size
		p.Data = make([]byte, size)
		if _, err = io.ReadFull(conn, p.Data[:size]); err != nil {
			return 0, err
		}
		err = fmt.Errorf(string(p.Data))
		if !proto.IsFlashNodeLimitError(err) {
			log.LogWarnf("ReadObjectFirstReply: ResultCode NOK, err(%v) ResultCode(%v)", err, p.ResultCode)
		}
		return
	}
	return int64(p.Size), nil
}

func (rc *RemoteCacheClient) ReadObject(ctx context.Context, fg *FlashGroup, reqId string, req *proto.CacheReadRequestBase) (reader *RemoteCacheReader, length int64, err error) {
	var (
		conn      *net.TCPConn
		addr      string
		reqPacket *proto.Packet
		logPrefix = fmt.Sprintf("reqID[%v]", reqId)
	)
	bgTime := stat.BeginStat()
	defer func() {
		if err != nil && strings.Contains(err.Error(), "timeout") {
			err = proto.ErrorReadTimeout
		}
		parts := strings.Split(addr, ":")
		if len(parts) > 0 && addr != "" {
			stat.EndStat(fmt.Sprintf("flashNode:%v", parts[0]), err, bgTime, 1)
		}
		stat.EndStat("flashNode", err, bgTime, 1)
	}()
	for {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		addr = fg.getFlashHost()
		if addr == "" {
			err = proto.ErrorNoAvailableHost
			log.LogWarnf("%v FlashGroup Read failed: fg(%v) err(%v)", logPrefix, fg, err)
			return
		}
		reqPacket = NewRemoteCachePacket(reqId, proto.OpFlashNodeCacheReadObject)
		if err = reqPacket.MarshalDataPb(req); err != nil {
			log.LogWarnf("%v FlashGroup Read: failed to MarshalData (%+v). err(%v)", logPrefix, req, err)
			return
		}
		if conn, err = rc.conns.GetConnect(addr); err != nil {
			log.LogWarnf("%v FlashGroup Read: get connection failed, addr(%v) reqPacket(%v) err(%v) remoteCacheMultiRead(%v)",
				logPrefix, addr, req, err, rc.RemoteCacheMultiRead)
			fg.moveToUnknownRank(addr, err, rc.FlashNodeTimeoutCount)
			if rc.RemoteCacheMultiRead {
				log.LogInfof("Retrying due to GetConnect of addr(%v) failure err(%v)", addr, err)
				continue
			}
			return
		}

		if err = reqPacket.WriteToConn(conn); err != nil {
			log.LogWarnf("%v FlashGroup Read: failed to write to addr(%v) err(%v) remoteCacheMultiRead(%v)",
				logPrefix, addr, err, rc.RemoteCacheMultiRead)
			rc.conns.PutConnect(conn, err != nil)
			fg.moveToUnknownRank(addr, err, rc.FlashNodeTimeoutCount)
			if rc.RemoteCacheMultiRead {
				log.LogInfof("Retrying due to write to addr(%v) failure err(%v)", addr, err)
				continue
			}
			return
		}

		reply := new(proto.Packet)
		// set time out for first packet
		conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(rc.firstPacketTimeout)))
		length, err = rc.ReadObjectFirstReply(conn, reply, reqId)
		if err != nil {
			log.LogWarnf("%v ReadObject getReadObjectReply from(%v) failed, reply(%v) error(%v)",
				logPrefix, conn.RemoteAddr(), reply, err)
			rc.conns.PutConnect(conn, err != nil)
			fg.moveToUnknownRank(addr, err, rc.FlashNodeTimeoutCount)
			return nil, 0, err
		}
		reader = rc.NewRemoteCacheReader(ctx, conn, reqId)
		reader.needReadLen = int64(req.Size_)
		break
	}
	if log.EnableDebug() {
		log.LogDebugf("%v FlashGroup Read: flashGroup(%v) addr(%v) CacheReadRequest(%v) reqPacket(%v) err(%v)"+
			" remoteCacheMultiRead(%v)", logPrefix, fg, addr, req, reqPacket, err, rc.RemoteCacheMultiRead)
	}
	return
}
