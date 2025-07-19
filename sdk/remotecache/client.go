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
	"golang.org/x/time/rate"
)

type ZoneRankType int

const (
	SameZoneRank ZoneRankType = iota
	SameRegionRank
	CrossRegionRank
	UnknownZoneRank
)

const (
	ConnIdelTimeout               = 30 //
	SameZoneWeight                = 70
	SameRegionWeight              = 20
	RefreshFlashNodesInterval     = time.Minute
	RefreshHostLatencyInterval    = 20 * time.Second
	DefaultReadQueueWaitTime      = 10 * time.Millisecond
	DefaultActivateTime           = 200 * time.Microsecond
	DefaultConnWorkers            = 64
	DefaultConnPutConsumers       = 8
	DefaultConnPutChanSize        = 10000
	DefaultReadPoolSize           = 256
	DefaultMaxRetryCount          = 3
	DefaultFirstRetryPercent      = 80
	DefaultSubsequentRetryPercent = 10
	DefaultSecondRetryPercent     = 20
)

type AddressPingStats struct {
	sync.Mutex
	durations []time.Duration
	index     int
}

type ConnPutTask struct {
	conn       *net.TCPConn
	forceClose bool
}

// ReadOperation contains both request parameters and response result for executeReadOperation
type ReadOperation struct {
	// Request parameters
	Addr                 string
	ReqPacket            *proto.Packet
	ReqId                string
	Fg                   *FlashGroup
	FirstPacketStartTime time.Time
	LogPrefix            string
	EndLoop              int32
	flashIp              string

	// Response result
	Conn          *net.TCPConn
	BlockDataSize uint32
	Err           error
	HasResult     int32
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

	blockSize              uint64
	clusterEnabled         bool
	RemoteCacheMultiRead   bool
	FlashNodeTimeoutCount  int32
	SameZoneTimeout        int64 // microsecond
	SameRegionTimeout      int64 // ms
	AddressPingMap         sync.Map
	firstPacketTimeout     int64
	FromFuse               bool
	BatchReadReqId         uint64
	ReadQueueMap           sync.Map // string[*ReadTaskQueue]
	disableBatch           bool
	activateTime           time.Duration
	connWorkers            int
	connPutChan            chan *ConnPutTask
	readPool               *util.GTaskPool
	flowLimiter            *rate.Limiter
	disableFlowLimitUpdate bool
	WriteChunkSize         int64 // bytes
	UpdateWarmPath         func([]byte, string)
}

func (rc *RemoteCacheClient) EnqueueConnTask(task *ConnPutTask) {
	select {
	case <-rc.stopC:
		rc.processConnPut(task)
	default:
		rc.connPutChan <- task
	}
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
	Masters                []string
	BlockSize              uint64
	NeedInitLog            bool
	NeedInitStat           bool
	LogLevelStr            string
	LogDir                 string
	ConnectTimeout         int64 // ms
	FirstPacketTimeout     int64 // ms
	FromFuse               bool
	InitClientTime         int
	DisableBatch           bool
	ActivateTime           int64
	ConnWorkers            int
	FlowLimit              int64 // bytes per second, default 5GB
	DisableFlowLimitUpdate bool
	WriteChunkSize         int64 // bytes
}

func NewRemoteCacheClient(config *ClientConfig) (rc *RemoteCacheClient, err error) {
	if config.NeedInitLog {
		logLevel := log.ParseLogLevel(config.LogLevelStr)
		_, err = log.InitLog(config.LogDir, "remoteCacheClient", logLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
		if err != nil {
			return nil, errors.New("failed to init log")
		}
		if config.NeedInitStat {
			_, err = stat.NewStatistic(config.LogDir, "remoteCacheClient", int64(stat.DefaultStatLogSize), stat.DefaultTimeOutUs, true)
			if err != nil {
				return nil, errors.New("failed to stat log")
			}
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
	rc.conns = util.NewConnectPoolWithTimeoutAndCap(5, 1000, ConnIdelTimeout, config.ConnectTimeout, true)
	rc.TTL = proto.DefaultRemoteCacheTTL
	rc.RemoteCacheMultiRead = false
	rc.FlashNodeTimeoutCount = proto.DefaultFlashNodeTimeoutCount
	rc.SameZoneTimeout = proto.DefaultRemoteCacheSameZoneTimeout
	rc.SameRegionTimeout = proto.DefaultRemoteCacheSameRegionTimeout
	rc.blockSize = config.BlockSize
	rc.FromFuse = config.FromFuse
	rc.disableBatch = config.DisableBatch
	rc.disableFlowLimitUpdate = config.DisableFlowLimitUpdate
	if config.ActivateTime == 0 {
		rc.activateTime = DefaultActivateTime
	} else {
		rc.activateTime = time.Duration(config.ActivateTime) * time.Microsecond
	}
	if config.ConnWorkers == 0 {
		rc.connWorkers = DefaultConnWorkers
	} else {
		rc.connWorkers = config.ConnWorkers
	}
	if config.FlowLimit > 0 {
		rc.flowLimiter = rate.NewLimiter(rate.Limit(config.FlowLimit), int(config.FlowLimit/2))
	} else {
		rc.flowLimiter = rate.NewLimiter(rate.Inf, 0)
	}
	if config.WriteChunkSize <= 0 {
		rc.WriteChunkSize = proto.CACHE_WRITE_CHUCK_SIZE
	} else if config.WriteChunkSize%(16*1024) != 0 || proto.CACHE_BLOCK_PACKET_SIZE%config.WriteChunkSize != 0 {
		rc.WriteChunkSize = proto.CACHE_WRITE_CHUCK_SIZE
	} else {
		rc.WriteChunkSize = config.WriteChunkSize
	}
	rc.connPutChan = make(chan *ConnPutTask, DefaultConnPutChanSize)
	rc.readPool = util.NewGTaskPool(DefaultReadPoolSize)
	rc.readPool.SetMaxDeltaRunning(512)
	for i := 0; i < DefaultConnPutConsumers; i++ {
		rc.wg.Add(1)
		go rc.consumeConnPut()
	}
	if !config.FromFuse {
		changeFromRemote := make(chan struct{})
		startTime := time.Now()
		go func() {
			err = rc.updateRemoteCacheConfig()
			if err != nil {
				log.LogWarnf("NewRemoteCacheClient: updateRemoteCacheConfig err %v", err)
			}
			err = rc.UpdateFlashGroups()
			if err != nil {
				log.LogWarnf("NewRemoteCacheClient: updateFlashGroups err %v", err)
			}
			log.LogInfof("NewRemoteCacheClient: initialization completed in %v", time.Since(startTime))
			close(changeFromRemote)
		}()
		if config.InitClientTime == 0 {
			config.InitClientTime = 5
		}
		select {
		case <-changeFromRemote:
		case <-time.After(time.Duration(config.InitClientTime) * time.Second):
			log.LogWarnf("NewRemoteCacheClient: init remote cache timeout for remote client %v", startTime)
			err = proto.ErrorInitRemoteTimeout
		}
	} else {
		err = rc.UpdateFlashGroups()
		if err != nil {
			log.LogWarnf("NewRemoteCacheClient: updateFlashGroups err %v", err)
		}
	}
	rc.wg.Add(1)
	go rc.refresh()
	log.LogDebugf("NewRemoteCacheClient sucess")
	return rc, err
}

func (rc *RemoteCacheClient) consumeConnPut() {
	defer rc.wg.Done()

	for {
		select {
		case <-rc.stopC:
			log.LogDebugf("consumeConnPut: stopC received, draining connPutChan for up to 10s")
			func() {
				timer := time.NewTimer(10 * time.Second)
				defer timer.Stop()
				for {
					select {
					case connTask := <-rc.connPutChan:
						if connTask != nil {
							rc.processConnPut(connTask)
						}
					case <-timer.C:
						log.LogDebugf("consumeConnPut: drain window elapsed, exiting")
						return
					}
				}
			}()
			return
		case connTask := <-rc.connPutChan:
			if connTask != nil {
				rc.processConnPut(connTask)
			}
		}
	}
}

func (rc *RemoteCacheClient) processConnPut(connTask *ConnPutTask) {
	if connTask == nil {
		return
	}
	rc.conns.PutConnect(connTask.conn, connTask.forceClose)
	if log.EnableDebug() {
		log.LogDebugf("processConnPut: processed connection, forceClose=%v", connTask.forceClose)
	}
}

func (rc *RemoteCacheClient) getBatchReqId() uint64 {
	return atomic.AddUint64(&rc.BatchReadReqId, 1)
}

func (rc *RemoteCacheClient) calculateConnTimeout(retryIndex int) int {
	if retryIndex == 0 {
		return int(rc.firstPacketTimeout)
	} else if retryIndex == 1 {
		return int(rc.firstPacketTimeout * DefaultSecondRetryPercent / 100)
	} else {
		return int(rc.firstPacketTimeout * DefaultSubsequentRetryPercent / 100)
	}
}

func (rc *RemoteCacheClient) calculateRetryTimeout(retryIndex int) int {
	if retryIndex == 0 {
		return int(rc.firstPacketTimeout * DefaultFirstRetryPercent / 100)
	} else {
		return int(rc.firstPacketTimeout * DefaultSubsequentRetryPercent / 100)
	}
}

// NewReadTaskItem creates a new ReadTaskItem for batch read operations
func (rc *RemoteCacheClient) NewReadTaskItem(req *proto.CacheReadRequestBase, reqId string) *ReadTaskItem {
	batchReqId := rc.getBatchReqId()
	batchReadItem := &proto.BatchReadItem{
		Key:    req.Key,
		Offset: req.Offset,
		Size_:  req.Size_,
		Slot:   req.Slot,
		ReqId:  batchReqId,
		Tid:    reqId,
	}

	return &ReadTaskItem{
		req:      batchReadItem,
		res:      nil,
		done:     make(chan struct{}),
		deadline: int64(req.Deadline),
	}
}

func (rc *RemoteCacheClient) Stop() {
	close(rc.stopC)
	rc.conns.Close()
	rc.readPool.Close()
	rc.ReadQueueMap.Range(func(key, value interface{}) bool {
		if readQueue, ok := value.(*ReadTaskQueue); ok {
			readQueue.Stop()
		}
		return true
	})

	log.LogFlush()
	stat.WriteStat()
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
	startTime := time.Now()
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
	if log.EnableDebug() {
		log.LogDebugf("updateFlashGroups. get flashGroupView [%v]", fgv)
	}
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
			if !rc.FromFuse {
				if _, ok := rc.ReadQueueMap.Load(host); !ok {
					readQueue := NewReadTaskQueue(host, DefaultReadQueueWaitTime, rc)
					_, loaded := rc.ReadQueueMap.LoadOrStore(host, readQueue)
					if loaded {
						readQueue.Stop()
					}
					if log.EnableDebug() {
						log.LogDebugf("updateFlashGroups: created ReadQueueMap for host(%v)", host)
					}
				}
			}
		}
		if log.EnableDebug() {
			log.LogDebugf("updateFlashGroups: fgID(%v) newAdded hosts: %v cost(%v)", fg.ID, newAdded, time.Since(startTime))
		}
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
	if log.EnableInfo() {
		log.LogInfof("updateFlashGroups: completed in %v", time.Since(startTime))
	}
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
		rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: err != nil})
	}()

	if conn, err = rc.conns.GetConnect(addr); err != nil {
		log.LogWarnf("HeartBeat get connection to addr failed, addr(%v) err(%v)", addr, err)
		return
	}
	start := time.Now()
	if err = packet.WriteToConn(conn); err != nil {
		log.LogWarnf("HeartBeat failed write to addr(%v) err(%v) start(%v)", addr, err, start)
		return
	}
	if err = packet.ReadFromConnExt(conn, int(rc.ReadTimeout)); err != nil {
		log.LogWarnf("HeartBeat failed to ReadFromConn addr(%v) err(%v) start(%v)", addr, err, start)
		return
	}
	if len(packet.Data) != 0 && rc.UpdateWarmPath != nil {
		go rc.UpdateWarmPath(packet.Data, addr)
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
	addr := fg.getFlashHostWithCrossRegion()
	if addr == "" {
		err = proto.ErrorNoAvailableHost
		log.LogWarnf("FlashGroup fg(%v) reqId(%v) put failed: err(%v)", fg, reqId, err)
		return
	}
	var conn *net.TCPConn
	if conn, err = rc.conns.GetConnect(addr); err != nil {
		log.LogWarnf("FlashGroup put: reqId(%v) get connection to curr addr failed, addr(%v) key(%v) err(%v)", reqId, addr, key, err)
		return
	}
	bgTime := stat.BeginStat()
	forceClose := false
	defer func() {
		rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: forceClose})
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
	conn.SetWriteDeadline(time.Now().Add(time.Millisecond * time.Duration(rc.firstPacketTimeout)))
	if err = reqPacket.WriteToNoDeadLineConn(conn); err != nil {
		forceClose = true
		log.LogWarnf("FlashGroup put: reqId(%v) failed to write header to addr(%v) err(%v)", reqId, addr, err)
		return
	}
	replyPacket := NewFlashCacheReply()
	if err = replyPacket.ReadFromConnExt(conn, int(rc.firstPacketTimeout)); err != nil {
		forceClose = true
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
	var (
		totalWritten int64
		n            int
		readCost     int64
		writeCost    int64
		readErr      error
	)
	forceClose = true
	defer func() {
		if err != nil {
			log.LogWarnf("FlashGroup put: reqId(%v) remove key %v by err %v readCost(%v)ns writeCost(%v)ns", reqId, key, err, readCost, writeCost)
			rc.deleteRemoteBlock(key, addr)
		}
	}()
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
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetWriteDeadline(deadline)
	} else {
		conn.SetWriteDeadline(time.Now().Add(proto.WriteDeadlineTime * time.Second))
	}
	go rc.processPutReply(conn, ch, reqId)
	var readOffset int
	for {
		if ctx.Err() != nil {
			return fmt.Errorf(proto.ErrorContextErrorTpl, ctx.Err())
		}
		if ch.RemoteError != nil {
			return fmt.Errorf(proto.ErrorRemoteErrorTpl, ch.RemoteError)
		}
		readSize := int(rc.WriteChunkSize)
		if totalWritten+int64(readSize) > length {
			readSize = int(length - totalWritten)
		}
		if err = rc.flowLimiter.WaitN(ctx, readSize); err != nil {
			log.LogWarnf("FlashGroup put: reqId(%v) flow limit wait failed, err(%v)", reqId, err)
			return fmt.Errorf(proto.ErrorFlowLimitErrorTpl, err)
		}
		bufferOffset := 0
		for {
			if ctx.Err() != nil {
				return fmt.Errorf(proto.ErrorContextErrorTpl, ctx.Err())
			}
			if ch.RemoteError != nil {
				return fmt.Errorf(proto.ErrorRemoteErrorTpl, ch.RemoteError)
			}
			currentReadSize := readSize - bufferOffset
			readStart := time.Now()
			bs := readOffset + bufferOffset
			n, readErr = r.Read(buf[bs : bs+currentReadSize])
			readCost += time.Since(readStart).Nanoseconds()
			bufferOffset += n
			if readErr == io.EOF {
				err = io.EOF
				break
			} else if readErr != nil {
				err = fmt.Errorf(proto.ErrorReadErrorTpl, readErr)
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
			sIndex := readOffset
			endIndex := bufferOffset + sIndex
			wcc := false
			if endIndex == proto.CACHE_BLOCK_PACKET_SIZE || totalWritten+int64(bufferOffset) == length {
				binary.BigEndian.PutUint32(buf[endIndex:], crc32.ChecksumIEEE(buf[:endIndex]))
				readOffset = 0
				endIndex += proto.CACHE_BLOCK_CRC_SIZE
				wcc = true
			} else {
				readOffset = endIndex
			}
			writeStart := time.Now()
			if _, err = conn.Write(buf[sIndex:endIndex]); err != nil {
				writeCost += time.Since(writeStart).Nanoseconds()
				log.LogErrorf("wirte data and crc to flashnode get err %v", err)
				return fmt.Errorf(proto.ErrorWriteDataAndCRCToFlashNodeTpl, err)
			}
			writeCost += time.Since(writeStart).Nanoseconds()
			if wcc {
				ch.WaitAckChan <- struct{}{}
			}
			totalWritten += int64(bufferOffset)
		}
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			return fmt.Errorf(proto.ErrorIOErrorTpl, err)
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
		return fmt.Errorf(proto.ErrorRemoteErrorTpl, ch.RemoteError)
	}
	if totalWritten != length {
		return fmt.Errorf(proto.ErrorUnexpectedDataLengthTpl, length, totalWritten)
	}
	forceClose = false
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
		if err = replyPacket.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
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
		rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: err != nil})
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
		rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: forceClose})
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
			rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: err != nil})
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
			rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: err != nil})
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
		rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: err != nil})
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

func (rc *RemoteCacheClient) GetReadQueueMap(host string) *ReadTaskQueue {
	if value, ok := rc.ReadQueueMap.Load(host); ok {
		return value.(*ReadTaskQueue)
	}
	return nil
}

func (rc *RemoteCacheClient) Get(ctx context.Context, reqId, key string, from, to int64) (r io.ReadCloser, length int64, shouldCache bool, err error) {
	if log.EnableDebug() {
		log.LogDebugf("RemoteCacheClient Get: key(%v) reqId(%v) from(%v) to(%v)", key, reqId, from, to)
	}
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

	if !rc.disableFlowLimitUpdate && config.RemoteClientFlowLimit >= 0 {
		rc.updateFlowLimiter(config.RemoteClientFlowLimit)
	}
	return
}

// updateFlowLimiter updates the flowLimiter with the given flow limit value
func (rc *RemoteCacheClient) updateFlowLimiter(flowLimit int64) {
	newFlowLimit := rate.Limit(flowLimit)
	// Compare with current limit to avoid unnecessary updates
	if rc.flowLimiter.Limit() == newFlowLimit {
		return
	}
	if log.EnableInfo() {
		log.LogInfof("updateFlowLimiter: updated from %v to %d bytes per second", rc.flowLimiter.Limit(), flowLimit)
	}
	if flowLimit <= 0 {
		rc.flowLimiter.SetLimit(rate.Inf)
		rc.flowLimiter.SetBurst(0)
	} else {
		rc.flowLimiter.SetLimit(newFlowLimit)
		rc.flowLimiter.SetBurst(int(flowLimit / 2))
	}
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
		if !proto.IsCacheMissError(err) {
			log.LogWarnf("readObjectFromRemoteCache: flashGroup read failed. key(%v) offset(%v) size(%v) fg(%v) req(%v) reqId(%v)"+
				" err(%v)", key, offset, size, fg, req, reqId, err)
		}
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("readObjectFromRemoteCache success: cacheReadRequestBase(%v) key(%v) reqId(%v) offset(%v) size(%v) deadline(%v)",
			req, key, reqId, offset, size, req.Deadline)
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
			log.LogWarnf("getReadObjectReply:reqID(%v) ResultCode NOK, err(%v) ResultCode(%v)", reader.reqID, err, p.ResultCode)
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
	flashIp        string
	currOffset     uint32
	endOffset      uint32
	blockDataSize  uint32
	ctx            context.Context
	buffer         []byte
	localData      []byte
	offset         int
	size           int
	closed         bool
	directRead     bool
	loadAll        bool
}

func (rc *RemoteCacheClient) NewRemoteCacheReader(ctx context.Context, conn *net.TCPConn, reqID, flashIp string, curr, end uint32) *RemoteCacheReader {
	r := &RemoteCacheReader{
		conn:       conn,
		rc:         rc,
		reqID:      reqID,
		ctx:        ctx,
		offset:     0,
		size:       0,
		flashIp:    flashIp,
		currOffset: curr,
		endOffset:  end,
	}
	return r
}

func (reader *RemoteCacheReader) read(p []byte) (n int, err error) {
	bg := stat.BeginStat()
	defer func() {
		err1 := err
		if err1 == io.EOF {
			err1 = nil
		}
		stat.EndStat(fmt.Sprintf("readCache:%v", reader.flashIp), err1, bg, 1)
	}()
	if !reader.directRead && !reader.loadAll && reader.ctx.Err() != nil {
		log.LogErrorf("RemoteCacheReader:reqID(%v) err(%v)", reader.reqID, reader.ctx.Err())
		return 0, reader.ctx.Err()
	}
	alreadyReadLen := atomic.LoadInt64(&reader.alreadyReadLen)
	if alreadyReadLen >= reader.needReadLen {
		if log.EnableDebug() {
			log.LogDebugf("RemoteCacheReader:reqID(%v) alreadyReadLen(%v) needReadLen(%v) get EOF", reader.reqID, alreadyReadLen, reader.needReadLen)
		}
		return 0, io.EOF
	}
	var expectLen int64
	if reader.directRead {
		expectLen = reader.needReadLen
		reader.localData = p[:expectLen]
		atomic.AddInt64(&reader.alreadyReadLen, expectLen)
		return int(expectLen), nil
	}
	alignedOffset := reader.currOffset / proto.CACHE_BLOCK_PACKET_SIZE * proto.CACHE_BLOCK_PACKET_SIZE
	readPackageSize := uint32(util.Min(proto.CACHE_BLOCK_PACKET_SIZE, int(reader.blockDataSize-alignedOffset)))
	if err = reader.rc.flowLimiter.WaitN(reader.ctx, int(readPackageSize)); err != nil {
		log.LogWarnf("RemoteCacheReader:reqID(%v) flow limit wait failed, err(%v)", reader.reqID, err)
		return 0, err
	}
	reply := new(proto.Packet)
	reader.conn.SetReadDeadline(time.Now().Add(time.Second * proto.ReadDeadlineTime))
	expectLen, err = reader.getReadObjectReply(reply)
	if err != nil {
		if proto.IsFlashNodeLimitError(err) {
			log.LogInfof("RemoteCacheReader:reqID(%v) Read reply err(%v)", reader.reqID, err)
		} else {
			log.LogErrorf("RemoteCacheReader:reqID(%v) Read reply err(%v)", reader.reqID, err)
		}
		err = fmt.Errorf("failed to read packet reply from flash node: %v", err)
		return
	}
	_, err = io.ReadFull(reader.conn, p[:readPackageSize])
	if err != nil {
		log.LogErrorf("RemoteCacheReader:reqID(%v) Read err(%v)", reader.reqID, err)
		err = fmt.Errorf("failed to read data payload from flash node connection: %v", err)
		return
	}

	// check crc
	actualCrc := crc32.ChecksumIEEE(p[:readPackageSize])
	if actualCrc != reply.CRC {
		err = fmt.Errorf(proto.ErrorInconsistentCRCObjectTpl, reply.KernelOffset, reply.ExtentOffset, reply.CRC, actualCrc)
		log.LogErrorf("RemoteCacheReader:Read check crc failed  reqID(%v) offset(%v) extentOffset(%v) expect(%v) actualCrc(%v)",
			reader.reqID, reply.KernelOffset, reply.ExtentOffset, reply.CRC, actualCrc)
		return
	}
	extentOffset := reply.ExtentOffset
	reader.localData = p[extentOffset : extentOffset+expectLen]
	if log.EnableDebug() {
		log.LogDebugf("RemoteCacheReader:Read reqID(%v) offset(%v) extentOffset(%v) expectLen(%v) p.len(%v) cost(%v)", reader.reqID, reply.KernelOffset, reply.ExtentOffset, expectLen, len(p), time.Since(*bg))
	}
	atomic.AddInt64(&reader.alreadyReadLen, expectLen)
	if atomic.LoadInt64(&reader.alreadyReadLen) >= reader.needReadLen {
		reader.loadAll = true
		reader.rc.EnqueueConnTask(&ConnPutTask{conn: reader.conn, forceClose: false})
		reader.conn = nil
	}
	return int(expectLen), nil
}

func (reader *RemoteCacheReader) Read(p []byte) (n int, err error) {
	defer func() {
		if err != nil && err != io.EOF && !reader.closed {
			reader.closed = true
			reader.rc.EnqueueConnTask(&ConnPutTask{conn: reader.conn, forceClose: true})
			if !reader.directRead {
				bytespool.Free(reader.buffer)
			}
		}
	}()
	if atomic.LoadUint32(&reader.currOffset) >= reader.endOffset {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if log.EnableDebug() {
		log.LogDebugf("RemoteCacheReader:reqID(%v) start(%v) readsize(%v)", reader.reqID, reader.currOffset, len(p))
	}
	if reader.closed {
		log.LogErrorf("read from close reader reqID(%v)", reader.reqID)
		return 0, fmt.Errorf(proto.ErrorReadFromCloseReaderTpl, reader.reqID)
	}
	if !reader.directRead && !reader.loadAll && reader.ctx.Err() != nil {
		err = reader.ctx.Err()
		log.LogErrorf("RemoteCacheReader:reqID(%v) err(%v)", reader.reqID, err)
		return 0, err
	}

	totalRead := 0

	for totalRead < len(p) {
		// if buffer is empty, read from flash reader
		if reader.offset >= reader.size {
			reader.size, err = reader.read(reader.buffer)
			if reader.size == 0 {
				if log.EnableDebug() {
					log.LogDebugf("RemoteCacheReader:reqID(%v), totalRead(%v) from connection size 0 err(%v)", reader.reqID, totalRead, err)
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
		atomic.AddUint32(&reader.currOffset, uint32(toRead))
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
		reader.rc.EnqueueConnTask(&ConnPutTask{conn: reader.conn, forceClose: reader.currOffset < reader.endOffset})
		if !reader.directRead {
			bytespool.Free(reader.buffer)
		}
	}
	return nil
}

func (rc *RemoteCacheClient) executeReadOperation(op *ReadOperation, isLast bool, connTimeOut int) {
	var (
		conn          *net.TCPConn
		blockDataSize uint32
		err           error
	)
	bg := stat.BeginStat()
	defer func() {
		stat.EndStat(fmt.Sprintf("readFirst:%v", op.flashIp), err, bg, 1)
	}()
	if conn, err = rc.conns.GetConnect(op.Addr); err != nil {
		log.LogWarnf("%v FlashGroup Read: get connection failed, addr(%v) reqPacket(%v) err(%v) remoteCacheMultiRead(%v)",
			op.LogPrefix, op.Addr, op.ReqPacket, err, rc.RemoteCacheMultiRead)
		if atomic.CompareAndSwapInt32(&op.HasResult, 0, 1) {
			op.Conn = nil
			op.BlockDataSize = 0
			op.Err = err
			atomic.StoreInt32(&op.EndLoop, 1)
		}
		return
	}

	conn.SetWriteDeadline(time.Now().Add(time.Duration(connTimeOut) * time.Millisecond))
	if err = op.ReqPacket.WriteToNoDeadLineConn(conn); err != nil {
		log.LogWarnf("%v FlashGroup Read: failed to write to addr(%v) err(%v)",
			op.LogPrefix, op.Addr, err)
		rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: err != nil})
		if isLast && atomic.CompareAndSwapInt32(&op.HasResult, 0, 1) {
			op.Conn = nil
			op.BlockDataSize = 0
			op.Err = err
			atomic.StoreInt32(&op.EndLoop, 1)
		}
		return
	}

	reply := new(proto.Packet)
	conn.SetReadDeadline(time.Now().Add(time.Duration(connTimeOut) * time.Millisecond))
	var openConn bool
	blockDataSize, err, openConn = rc.ReadObjectFirstReply(conn, reply, op.ReqId)
	if err != nil {
		if !openConn {
			log.LogWarnf("%v ReadObject getReadObjectReply from(%v) failed, reply(%v) error(%v)",
				op.LogPrefix, conn.RemoteAddr(), reply, err)
		} else if log.EnableDebug() {
			log.LogDebugf("%v ReadObject getReadObjectReply from(%v) failed, reply(%v) error(%v)",
				op.LogPrefix, conn.RemoteAddr(), reply, err)
		}
		rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: !openConn})
		if openConn && atomic.CompareAndSwapInt32(&op.HasResult, 0, 1) {
			op.Conn = nil
			op.BlockDataSize = 0
			op.Err = err
			atomic.StoreInt32(&op.EndLoop, 1)
		}
		return
	}
	if atomic.CompareAndSwapInt32(&op.HasResult, 0, 1) {
		op.Conn = conn
		op.BlockDataSize = blockDataSize
		op.Err = nil
		atomic.StoreInt32(&op.EndLoop, 1)
	} else {
		rc.EnqueueConnTask(&ConnPutTask{conn: conn, forceClose: true})
	}
}

func (rc *RemoteCacheClient) ReadObjectFirstReply(conn *net.TCPConn, p *proto.Packet, reqId string) (length uint32, err error, openConn bool) {
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
		return 0, syscall.EBADMSG, false
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
			return 0, err, false
		}
	}

	if p.ResultCode != proto.OpOk {
		size := p.Size
		p.Data = make([]byte, size)
		if _, err = io.ReadFull(conn, p.Data[:size]); err != nil {
			return 0, err, false
		}
		openConn = true
		err = fmt.Errorf(string(p.Data))
		if !proto.IsFlashNodeLimitError(err) && !proto.IsCacheMissError(err) {
			log.LogWarnf("ReadObjectFirstReply: ResultCode NOK, err(%v) ResultCode(%v)", err, p.ResultCode)
		}
		return
	}
	return p.Size, nil, true
}

func (rc *RemoteCacheClient) ReadObject(ctx context.Context, fg *FlashGroup, reqId string, req *proto.CacheReadRequestBase) (reader *RemoteCacheReader, length int64, err error) {
	var (
		conn      *net.TCPConn
		addr      string
		flashIp   string
		reqPacket *proto.Packet
		logPrefix = fmt.Sprintf("reqID[%v]", reqId)
	)
	bgTime := stat.BeginStat()
	defer func() {
		if err != nil && strings.Contains(err.Error(), "timeout") {
			err = proto.ErrorReadTimeout
		}
		if len(flashIp) > 0 {
			stat.EndStat(fmt.Sprintf("flashNode:%v", flashIp), err, bgTime, 1)
		}
		stat.EndStat("flashNode", err, bgTime, 1)
	}()
	var stcTime, rcvTime int64
	firstPacketStartTime := time.Now()
	addr = fg.getFlashHostWithCrossRegion()
	if addr == "" {
		err = proto.ErrorNoAvailableHost
		log.LogWarnf("%v FlashGroup Read failed: fg(%v) err(%v)", logPrefix, fg, err)
		return
	}
	flashIp = strings.Split(addr, ":")[0]
	stcTime = time.Now().UnixNano()
	var blockDataSize uint32
	var directRead bool
	var resultData []byte
	if !rc.disableBatch && req.Offset%proto.CACHE_BLOCK_PACKET_SIZE == 0 && req.Size_ <= proto.SMALL_OBJECT_BLOCK_SIZE {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		readQueue := rc.GetReadQueueMap(addr)
		if readQueue != nil {
			task := rc.NewReadTaskItem(req, reqId)
			readQueue.Enqueue(task)
			select {
			case <-task.done:
				if task.res == nil || len(task.res.Data) == 0 {
					err = fmt.Errorf("batch read failed: no result")
					log.LogWarnf("%v ReadQueueMap: failed to read small object, reqId(%v) batchReqId(%v) err(%v)",
						logPrefix, reqId, task.req.ReqId, err)
					return
				}
				if task.res.ResultCode != uint32(proto.OpOk) {
					err = fmt.Errorf("batch read failed: %s", string(task.res.Data))
					if !proto.IsCacheMissError(err) {
						log.LogWarnf("%v ReadQueueMap: failed to read small object, reqId(%v) batchReqId(%v) err(%v)",
							logPrefix, reqId, task.req.ReqId, err)
					}
					return
				}
				resultData = task.res.Data
				actualCrc := crc32.ChecksumIEEE(resultData[:req.Size_])
				if actualCrc != task.res.CRC {
					err = fmt.Errorf(proto.ErrorInconsistentCRCObjectTpl, req.Offset, req.Size_, task.res.CRC, actualCrc)
					log.LogErrorf("RemoteCacheReader: first reply read check crc failed key(%v) reqID(%v) offset(%v) extentOffset(%v) expect(%v) actualCrc(%v)",
						task.req.Key, task.req.ReqId, task.req.Offset, task.req.Size_, task.res.CRC, actualCrc)
					return
				}
				length = int64(req.Size_)
				directRead = true
				if log.EnableDebug() {
					log.LogDebugf("%v ReadQueueMap: successfully read small object, reqId(%v) batchReqId(%v) size(%v)",
						logPrefix, reqId, task.req.ReqId, len(resultData))
				}
			case <-ctx.Done():
				err = ctx.Err()
				return
			}
		} else {
			err = proto.ErrorNoAvailableHost
			return
		}
	} else {
		reqPacket = NewRemoteCachePacket(reqId, proto.OpFlashNodeCacheReadObject)
		if err = reqPacket.MarshalDataPb(req); err != nil {
			log.LogWarnf("%v FlashGroup Read: failed to MarshalData (%+v). err(%v)", logPrefix, req, err)
			return
		}
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		readOp := &ReadOperation{
			Addr:                 addr,
			ReqPacket:            reqPacket,
			ReqId:                reqId,
			flashIp:              flashIp,
			Fg:                   fg,
			FirstPacketStartTime: firstPacketStartTime,
			LogPrefix:            logPrefix,
		}
		timeOut := rc.calculateRetryTimeout(0)
		for retryCount := 0; retryCount < DefaultMaxRetryCount && atomic.LoadInt32(&readOp.EndLoop) == 0; retryCount++ {
			readDone := make(chan struct{})
			isLast := retryCount == DefaultMaxRetryCount-1
			connTimeOut := rc.calculateConnTimeout(retryCount)
			_ = rc.readPool.AsyncRunNoHang(func() {
				rc.executeReadOperation(readOp, isLast, connTimeOut)
				close(readDone)
			})
			select {
			case <-readDone:
			case <-time.After(time.Duration(timeOut) * time.Millisecond):
				timeOut = rc.calculateRetryTimeout(retryCount + 1)
				if log.EnableDebug() {
					log.LogDebugf("ReadObject: timeout on retry (%d/%d), addr(%v)", retryCount+1, DefaultMaxRetryCount, addr)
				}
			}
		}
		conn = readOp.Conn
		blockDataSize = readOp.BlockDataSize
		if atomic.CompareAndSwapInt32(&readOp.HasResult, 1, 2) {
			err = readOp.Err
		} else {
			err = fmt.Errorf("first packet timeout after all retries")
		}
		if err != nil {
			if !proto.IsCacheMissError(err) {
				log.LogWarnf("Read operation failed after %d retries, addr(%v) err(%v)", DefaultMaxRetryCount, addr, err)
			}
			return nil, 0, err
		}
		length = int64(req.Size_)
	}
	rcvTime = time.Now().UnixNano()
	reader = rc.NewRemoteCacheReader(ctx, conn, reqId, flashIp, uint32(req.Offset), uint32(req.Offset+req.Size_))
	reader.needReadLen = int64(req.Size_)
	reader.blockDataSize = blockDataSize
	if directRead {
		reader.buffer = resultData
		reader.directRead = true
	} else {
		reader.buffer = bytespool.Alloc(proto.CACHE_BLOCK_PACKET_SIZE)
	}
	if log.EnableDebug() {
		log.LogDebugf("%v FlashGroup Read: flashGroup(%v) addr(%v) CacheReadRequest(%v) reqPacket(%v) err(%v)"+
			" remoteCacheMultiRead(%v) fisrtPackage cost(%v)", logPrefix, fg, addr, req, reqPacket, err, rc.RemoteCacheMultiRead, rcvTime-stcTime)
	}
	return
}

func (rc *RemoteCacheClient) ApplyWarmupMetaToken(flashNodeAddr string, clientId string, requestType uint8) (bool, error) {
	var conn *net.TCPConn
	var err error

	defer func() {
		rc.conns.PutConnect(conn, err != nil)
	}()

	// Create packet with OpApplyWarmupMetaToken opcode
	packet := proto.NewPacket()
	packet.Opcode = proto.OpApplyWarmupMetaToken

	// Set request data (requestType)
	packet.Data = []byte{requestType}
	packet.Size = uint32(len(packet.Data))

	// Set client ID in Arg field
	if clientId != "" {
		packet.Arg = []byte(clientId)
		packet.ArgLen = uint32(len(packet.Arg))
	}

	// Get connection to flashnode
	if conn, err = rc.conns.GetConnect(flashNodeAddr); err != nil {
		log.LogWarnf("ApplyWarmupMetaToken: get connection to flashnode failed, addr(%v) err(%v)", flashNodeAddr, err)
		return false, err
	}

	// Write request to flashnode
	if err = packet.WriteToConn(conn); err != nil {
		log.LogWarnf("ApplyWarmupMetaToken: failed to write to flashnode(%v) err(%v)", flashNodeAddr, err)
		return false, err
	}

	// Read response from flashnode
	replyPacket := proto.NewPacket()
	if err = replyPacket.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		log.LogWarnf("ApplyWarmupMetaToken: failed to read response from flashnode(%v) err(%v)", flashNodeAddr, err)
		return false, err
	}

	// Check if operation was successful
	if replyPacket.ResultCode != proto.OpOk {
		err = fmt.Errorf("flashnode returned error: %s", string(replyPacket.Data))
		log.LogWarnf("ApplyWarmupMetaToken: flashnode(%v) returned error ResultCode(%v) err(%v)",
			flashNodeAddr, replyPacket.ResultCode, err)
		return false, err
	}

	// Check response data to determine if token operation was successful
	if replyPacket.Size > 0 && len(replyPacket.Data) > 0 {
		if replyPacket.Data[0] == 1 {
			log.LogDebugf("ApplyWarmupMetaToken: flashnode(%v) clientId(%v) requestType(%v) success",
				flashNodeAddr, clientId, requestType)
			return true, nil
		} else {
			log.LogDebugf("ApplyWarmupMetaToken: flashnode(%v) clientId(%v) requestType(%v) failed (token limit reached or client not found)",
				flashNodeAddr, clientId, requestType)
			return false, nil
		}
	}

	log.LogWarnf("ApplyWarmupMetaToken: flashnode(%v) returned empty response data", flashNodeAddr)
	return false, fmt.Errorf("empty response data from flashnode")
}
