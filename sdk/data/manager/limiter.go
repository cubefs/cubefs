package manager

import (
	"container/list"
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

const (
	runNow               = 1
	runLater             = 2
	gridHitLimitCnt      = 1
	girdCntOneSecond     = 3
	gridWindowTimeScope  = 10
	qosExpireTime        = 20
	qosReportMinGap      = uint32(time.Second) / 2
	defaultMagnifyFactor = 100
)

type UploadFlowInfoFunc func(clientInfo wrapper.SimpleClientInfo) error

type GridElement struct {
	time     time.Time
	used     uint64
	limit    uint64
	buffer   uint64
	hitLimit bool
	ID       uint64
	sync.RWMutex
}

type AllocElement struct {
	used    uint32
	magnify uint32
	future  *util.Future
}

type LimitFactor struct {
	factorType         uint32
	gridList           *list.List
	waitList           *list.List
	gidHitLimitCnt     uint8
	mgr                *LimitManager
	gridId             uint64
	magnify            uint32
	winBuffer          uint64
	lock               sync.RWMutex
	valAllocApply      uint64
	valAllocCommit     uint64
	valAllocLastApply  uint64
	valAllocLastCommit uint64
	isSetLimitZero     bool
}

func (factor *LimitFactor) getNeedByMagnify(allocCnt uint32, magnify uint32) uint64 {
	if magnify == 0 {
		return 0
	}
	if allocCnt > 1000 {
		log.LogDebugf("action[getNeedByMagnify] allocCnt %v", allocCnt)
		magnify = defaultMagnifyFactor
	}

	need := uint64(allocCnt * magnify)
	if factor.factorType == proto.FlowWriteType || factor.factorType == proto.FlowReadType {
		if need > util.GB/8 {
			need = util.GB / 8
		}
	}
	return need
}

func (factor *LimitFactor) alloc(allocCnt uint32) (ret uint8, future *util.Future) {
	log.LogDebugf("action[alloc] type [%v] alloc [%v], tmp factor waitlist [%v] hitlimtcnt [%v] len [%v]", proto.QosTypeString(factor.factorType),
		allocCnt, factor.waitList.Len(), factor.gidHitLimitCnt, factor.gridList.Len())
	atomic.AddUint64(&factor.valAllocApply, uint64(allocCnt))
	if !factor.mgr.enable {
		// used not accurate also fine, the purpose is get master's info
		// without lock can better performance just the used value large than 0
		gridEnd := factor.gridList.Back()
		if gridEnd != nil {
			grid := gridEnd.Value.(*GridElement)
			// grid.used = grid.used+uint64(allocCnt)
			atomic.AddUint64(&grid.used, uint64(allocCnt))
			//atomic.CompareAndSwapUint64(&factor.valAllocApply, factor.valAllocApply, factor.valAllocApply+uint64(allocCnt))

		}
		return runNow, nil
	}

	type activeSt struct {
		activeUpdate bool
		needWait     bool
	}
	activeState := &activeSt{}
	defer func(active *activeSt) {
		if !active.needWait {
			factor.lock.RUnlock()
		} else if !active.activeUpdate {
			factor.lock.Unlock()
		}

	}(activeState)

	factor.lock.RLock()
	grid := factor.gridList.Back().Value.(*GridElement)

	if factor.mgr.enable && (factor.waitList.Len() > 0 || atomic.LoadUint64(&grid.used)+uint64(allocCnt) > grid.limit+grid.buffer) {
		factor.lock.RUnlock()
		factor.lock.Lock()
		activeState.needWait = true
		future = util.NewFuture()

		factor.waitList.PushBack(&AllocElement{
			used:    allocCnt,
			future:  future,
			magnify: factor.magnify,
		})

		if grid.hitLimit == false {
			factor.gidHitLimitCnt++
			// 1s have several gird, gidHitLimitCnt is the count that gird count hit limit in latest 1s,
			// if gidHitLimitCnt large than limit then request for enlarge factor limit
			// GetSimpleVolView will call back simpleClient function to get factor info and send to master
			if factor.gidHitLimitCnt >= factor.mgr.HitTriggerCnt {
				tmpTime := time.Now()
				if factor.mgr.lastReqTime.Add(time.Duration(factor.mgr.ReqPeriod) * time.Second).Before(tmpTime) {
					factor.mgr.lastReqTime = tmpTime
					log.LogDebugf("CheckGrid factor [%v] unlock before active update simple vol view,gird id[%v] limit[%v] buffer [%v] used [%v]",
						proto.QosTypeString(factor.factorType), grid.ID, grid.limit, grid.buffer, grid.used)
					// unlock need call here,UpdateSimpleVolView will lock again
					factor.lock.Unlock()
					activeState.activeUpdate = true
					go factor.mgr.WrapperUpdate(factor.mgr.simpleClient)
				}
			}
		}
		grid.hitLimit = true
		return runLater, future
	}
	atomic.AddUint64(&grid.used, uint64(allocCnt))
	//atomic.CompareAndSwapUint64(&grid.used, grid.used, grid.used+uint64(allocCnt))
	return runNow, future
}

func (factor *LimitFactor) SetLimit(limitVal uint64, bufferVal uint64) {
	log.LogDebugf("acton[SetLimit] factor type [%v] limitVal [%v] bufferVal [%v]", proto.QosTypeString(factor.factorType), limitVal, bufferVal)
	var grid *GridElement
	factor.mgr.lastTimeOfSetLimit = time.Now()
	factor.lock.Lock()

	defer func() {
		factor.TryReleaseWaitList()
		factor.lock.Unlock()
	}()

	if factor.gridList.Len() == 0 {
		grid = &GridElement{
			time:   time.Now(),
			limit:  limitVal / girdCntOneSecond,
			buffer: bufferVal / girdCntOneSecond,
			ID:     factor.gridId,
		}
		factor.gridId++
		factor.gridList.PushBack(grid)
	} else {
		grid = factor.gridList.Back().Value.(*GridElement)
		grid.buffer = bufferVal / girdCntOneSecond
		grid.limit = limitVal / girdCntOneSecond
	}

	// should not enter in,do more protection for enhance client usage
	if grid.limit == 0 {
		factor.isSetLimitZero = true
		switch factor.factorType {
		case proto.IopsReadType, proto.IopsWriteType:
			grid.limit = proto.MinIopsLimit / girdCntOneSecond
			if grid.limit == 0 {
				grid.limit = 1
			}
		case proto.FlowReadType, proto.FlowWriteType:
			grid.limit = proto.MinFLowLimit / girdCntOneSecond
			if grid.limit == 0 {
				grid.limit = 10 * util.KB
			}
		}
	} else {
		factor.isSetLimitZero = false
	}

	grid = factor.gridList.Back().Value.(*GridElement)
	log.LogDebugf("action[SetLimit] factor type [%v] gird id %v limit %v buffer %v",
		proto.QosTypeString(factor.factorType), grid.ID, grid.limit, grid.buffer)

}

// clean wait list if limit be enlrarged by master
// no lock need for parallel,caller own the lock and will release it
func (factor *LimitFactor) TryReleaseWaitList() {

	gridIter := factor.gridList.Back()
	tGrid := gridIter.Value.(*GridElement)
	cnt := 0

	for factor.waitList.Len() > 0 {
		value := factor.waitList.Front()
		ele := value.Value.(*AllocElement)

		log.LogDebugf("action[TryReleaseWaitList] type [%v] ele used [%v]", proto.QosTypeString(factor.factorType), ele.used)
		for atomic.LoadUint64(&tGrid.used)+uint64(ele.used) > tGrid.limit+tGrid.buffer {

			log.LogWarnf("action[TryReleaseWaitList] type [%v] new gird be used up.alloc in waitlist left cnt [%v],"+
				"grid be allocated [%v] grid limit [%v] and buffer[%v], gird id:[%v], use pregrid size[%v]",
				proto.QosTypeString(factor.factorType), factor.waitList.Len(), tGrid.used, tGrid.limit, tGrid.buffer,
				tGrid.ID, uint32(tGrid.limit+tGrid.buffer-tGrid.used))

			tUsed := atomic.LoadUint64(&tGrid.used)
			val := tGrid.limit + tGrid.buffer - tUsed                        // uint may out range
			if tGrid.limit+tGrid.buffer > tUsed && ele.used >= uint32(val) { // not atomic pretect,grid used may larger than limit and buffer
				ele.used -= uint32(val)
				log.LogDebugf("action[TryReleaseWaitList] type [%v] ele used reduce [%v] and left [%v]", proto.QosTypeString(factor.factorType), val, ele.used)
				// atomic.AddUint64(&curGrid.used, tGrid.limit+ tGrid.buffer)
				atomic.AddUint64(&tGrid.used, val)
			}
			cnt++
			if gridIter.Prev() == nil || cnt >= girdCntOneSecond {
				return
			}
			gridIter = gridIter.Prev()
			tGrid = gridIter.Value.(*GridElement)
		}
		atomic.AddUint64(&tGrid.used, uint64(ele.used))
		log.LogDebugf("action[TryReleaseWaitList] type [%v] ele used [%v] consumed!", proto.QosTypeString(factor.factorType), ele.used)
		ele.future.Respond(true, nil)

		value = value.Next()
		factor.waitList.Remove(factor.waitList.Front())
	}

}

func (factor *LimitFactor) CheckGrid() {
	defer func() {
		factor.lock.Unlock()
	}()
	factor.lock.Lock()

	grid := factor.gridList.Back().Value.(*GridElement)
	newGrid := &GridElement{
		time:   time.Now(),
		limit:  grid.limit,
		used:   0,
		buffer: grid.buffer,
		ID:     factor.gridId,
	}
	factor.gridId++

	if factor.mgr.enable == true && factor.mgr.lastTimeOfSetLimit.Add(time.Second*qosExpireTime).Before(newGrid.time) {
		log.LogWarnf("action[CheckGrid]. qos recv no command from master in long time, last time %v, grid time %v",
			factor.mgr.lastTimeOfSetLimit, newGrid.time)
	}
	log.LogDebugf("action[CheckGrid] factor type:[%v] gridlistLen:[%v] waitlistLen:[%v] hitlimitcnt:[%v] "+
		"add new grid info girdid[%v] used:[%v] limit:[%v] buffer:[%v] time:[%v]",
		proto.QosTypeString(factor.factorType), factor.gridList.Len(), factor.waitList.Len(), factor.gidHitLimitCnt,
		newGrid.ID, newGrid.used, newGrid.limit, newGrid.buffer, newGrid.time)

	factor.gridList.PushBack(newGrid)
	for factor.gridList.Len() > gridWindowTimeScope*girdCntOneSecond {
		firstGrid := factor.gridList.Front().Value.(*GridElement)
		if firstGrid.hitLimit {
			factor.gidHitLimitCnt--
			log.LogDebugf("action[CheckGrid] factor [%v] after minus gidHitLimitCnt:[%v]",
				proto.QosTypeString(factor.factorType), factor.gidHitLimitCnt)
		}
		log.LogDebugf("action[CheckGrid] type:[%v] remove oldest grid id[%v] info buffer:[%v] limit:[%v] used[%v] from gridlist",
			proto.QosTypeString(factor.factorType), firstGrid.ID, firstGrid.buffer, firstGrid.limit, firstGrid.used)
		factor.gridList.Remove(factor.gridList.Front())
	}
	factor.TryReleaseWaitList()

}

func newLimitFactor(mgr *LimitManager, factorType uint32) *LimitFactor {
	limit := &LimitFactor{
		mgr:        mgr,
		factorType: factorType,
		waitList:   list.New(),
		gridList:   list.New(),
		magnify:    defaultMagnifyFactor,
	}

	limit.SetLimit(0, 0)
	return limit
}

type LimitManager struct {
	ID                 uint64
	limitMap           map[uint32]*LimitFactor
	enable             bool
	simpleClient       wrapper.SimpleClientInfo
	exitCh             chan struct{}
	WrapperUpdate      UploadFlowInfoFunc
	ReqPeriod          uint32
	HitTriggerCnt      uint8
	lastReqTime        time.Time
	lastTimeOfSetLimit time.Time
	isLastReqValid     bool
	once               sync.Once
}

func NewLimitManager(client wrapper.SimpleClientInfo) *LimitManager {
	mgr := &LimitManager{
		limitMap:      make(map[uint32]*LimitFactor, 0),
		enable:        false, // assign from master
		simpleClient:  client,
		HitTriggerCnt: gridHitLimitCnt,
		ReqPeriod:     1,
	}
	mgr.limitMap[proto.IopsReadType] = newLimitFactor(mgr, proto.IopsReadType)
	mgr.limitMap[proto.IopsWriteType] = newLimitFactor(mgr, proto.IopsWriteType)
	mgr.limitMap[proto.FlowWriteType] = newLimitFactor(mgr, proto.FlowWriteType)
	mgr.limitMap[proto.FlowReadType] = newLimitFactor(mgr, proto.FlowReadType)

	mgr.ScheduleCheckGrid()
	return mgr
}

func (factor *LimitFactor) GetWaitTotalSize() (waitSize uint64) {
	value := factor.waitList.Front()
	for {
		if value == nil {
			break
		}
		ele := value.Value.(*AllocElement)
		waitSize += uint64(ele.used)
		value = value.Next()
	}
	return
}

func (limitManager *LimitManager) CalcNeed(limitFactor *LimitFactor, used uint64) (need uint64) {
	if limitFactor.waitList.Len() == 0 {
		return 0
	}
	if limitFactor.factorType == proto.FlowReadType || limitFactor.factorType == proto.FlowWriteType {
		used += limitFactor.GetWaitTotalSize()
		if used < 128*util.KB {
			used = 128 * util.KB
		}
		if used < util.MB {
			need = 5 * used
		} else if used < 5*util.MB {
			need = 2 * used
		} else if used < 10*util.MB {
			need = uint64(1.5 * float64(used))
		} else if used < 50*util.MB {
			need = uint64(1.2 * float64(used))
		} else if used < 100*util.MB {
			need = uint64(1.1 * float64(used))
		} else if used < 300*util.MB {
			need = used
		} else {
			need = 300 * util.MB
		}
	} else {
		if used == 0 {
			used = uint64(limitFactor.waitList.Len())
		}
		if used < 10 {
			need = 3 * used
		} else if used < 50 {
			need = uint64(1.5 * float64(used))
		} else if used < 100 {
			need = uint64(1.2 * float64(used))
		} else if used < 1000 {
			need = uint64(1.1 * float64(used))
		} else if used < 3000 {
			need = used
		} else {
			need = 3000
		}
	}
	return
}

func (limitManager *LimitManager) GetFlowInfo() (*proto.ClientReportLimitInfo, bool) {
	log.LogDebugf("action[LimitManager.GetFlowInfo]")
	info := &proto.ClientReportLimitInfo{
		FactorMap: make(map[uint32]*proto.ClientLimitInfo, 0),
	}
	var (
		validCliInfo bool
		griCnt       int
		limit        uint64
		buffer       uint64
	)
	for factorType, limitFactor := range limitManager.limitMap {
		limitFactor.lock.RLock()

		var reqUsed uint64
		griCnt = 0
		grid := limitFactor.gridList.Back()
		grid = grid.Prev()
		//reqUsed := limitFactor.valAllocLastCommit

		for griCnt < limitFactor.gridList.Len()-1 {
			reqUsed += atomic.LoadUint64(&grid.Value.(*GridElement).used)
			limit += grid.Value.(*GridElement).limit
			buffer += grid.Value.(*GridElement).buffer
			griCnt++

			log.LogDebugf("action[GetFlowInfo] type [%v] grid id[%v] used %v limit %v buffer %v time %v sum_used %v sum_limit %v,len %v",
				proto.QosTypeString(factorType),
				grid.Value.(*GridElement).ID,
				grid.Value.(*GridElement).used,
				grid.Value.(*GridElement).limit,
				grid.Value.(*GridElement).buffer,
				grid.Value.(*GridElement).time,
				reqUsed,
				limit, limitFactor.gridList.Len())
			if grid.Prev() == nil || griCnt >= girdCntOneSecond {
				log.LogDebugf("action[[GetFlowInfo] type [%v] grid count %v reqused %v list len %v",
					proto.QosTypeString(factorType), griCnt, reqUsed, limitFactor.gridList.Len())
				break
			}
			grid = grid.Prev()
		}

		if griCnt > 0 {
			timeElapse := uint64(time.Second) * uint64(griCnt) / girdCntOneSecond
			if timeElapse < uint64(qosReportMinGap) {
				log.LogWarnf("action[GetFlowInfo] type [%v] timeElapse [%v] since last report",
					proto.QosTypeString(limitFactor.factorType), timeElapse)
				timeElapse = uint64(qosReportMinGap) // time of interval get vol view from master todo:change to config time
			}
			reqUsed = uint64(float64(reqUsed) / (float64(timeElapse) / float64(time.Second)))
		}

		factor := &proto.ClientLimitInfo{
			Used:       reqUsed,
			Need:       limitManager.CalcNeed(limitFactor, reqUsed),
			UsedLimit:  limitFactor.gridList.Back().Value.(*GridElement).limit * girdCntOneSecond,
			UsedBuffer: limitFactor.gridList.Back().Value.(*GridElement).buffer * girdCntOneSecond,
		}

		limitFactor.lock.RUnlock()

		info.FactorMap[factorType] = factor
		info.Host = wrapper.LocalIP
		info.Status = proto.QosStateNormal
		info.ID = limitManager.ID
		if limitFactor.waitList.Len() > 0 ||
			!limitFactor.isSetLimitZero ||
			factor.Used|factor.Need > 0 {
			log.LogDebugf("action[GetFlowInfo] type [%v]  len [%v] isSetLimitZero [%v] used [%v] need [%v]", proto.QosTypeString(limitFactor.factorType),
				limitFactor.waitList.Len(), limitFactor.isSetLimitZero, factor.Used, factor.Need)
			validCliInfo = true
		}

		if griCnt > 0 {
			log.LogDebugf("action[GetFlowInfo] type [%v] last commit[%v] report to master "+
				"with simpleClient limit info [%v,%v,%v,%v],host [%v], "+
				"status [%v] grid [%v, %v, %v]",
				proto.QosTypeString(limitFactor.factorType), limitFactor.valAllocLastCommit,
				factor.Used, factor.Need, factor.UsedBuffer, factor.UsedLimit, info.Host,
				info.Status, grid.Value.(*GridElement).ID, grid.Value.(*GridElement).limit, grid.Value.(*GridElement).buffer)
		}
	}

	lastValid := limitManager.isLastReqValid
	limitManager.isLastReqValid = validCliInfo

	limitManager.once.Do(func() {
		validCliInfo = true
	})
	// client has no user request then don't report to master
	if !lastValid && !validCliInfo {
		return info, false
	}
	return info, true
}

func (limitManager *LimitManager) ScheduleCheckGrid() {
	go func() {
		ticker := time.NewTicker(1000 / girdCntOneSecond * time.Millisecond)
		defer func() {
			ticker.Stop()
		}()
		var cnt uint64
		for {
			select {
			case <-limitManager.exitCh:
				return
			case <-ticker.C:
				cnt++
				for factorType, limitFactor := range limitManager.limitMap {
					limitFactor.CheckGrid()
					if cnt%girdCntOneSecond == 0 {
						log.LogDebugf("action[ScheduleCheckGrid] type [%v] factor apply val:[%v] commit val:[%v]",
							proto.QosTypeString(factorType), atomic.LoadUint64(&limitFactor.valAllocApply), atomic.LoadUint64(&limitFactor.valAllocCommit))
						limitFactor.valAllocLastApply = atomic.LoadUint64(&limitFactor.valAllocLastApply)
						limitFactor.valAllocLastCommit = atomic.LoadUint64(&limitFactor.valAllocCommit)
						atomic.StoreUint64(&limitFactor.valAllocApply, 0)
						atomic.StoreUint64(&limitFactor.valAllocCommit, 0)
					}
				}
			}
		}
	}()
}

func (limitManager *LimitManager) SetClientLimit(limit *proto.LimitRsp2Client) {
	if limit == nil {
		log.LogErrorf("action[SetClientLimit] limit info is nil")
		return
	}
	log.LogDebugf("action[SetClientLimit] limit enable %v", limit.Enable)
	if limitManager.enable != limit.Enable {
		log.LogWarnf("action[SetClientLimit] enable [%v]", limit.Enable)
	}
	limitManager.enable = limit.Enable
	if limit.HitTriggerCnt > 0 {
		log.LogWarnf("action[SetClientLimit] update to HitTriggerCnt [%v] from [%v]", limitManager.HitTriggerCnt, limit.HitTriggerCnt)
		limitManager.HitTriggerCnt = limit.HitTriggerCnt
	}
	if limit.ReqPeriod > 0 {
		log.LogWarnf("action[SetClientLimit] update to ReqPeriod [%v] from [%v]", limitManager.ReqPeriod, limit.ReqPeriod)
		limitManager.ReqPeriod = limit.ReqPeriod
	}

	for factorType, clientLimitInfo := range limit.FactorMap {
		limitManager.limitMap[factorType].SetLimit(clientLimitInfo.UsedLimit, clientLimitInfo.UsedBuffer)
	}
	for factorType, magnify := range limit.Magnify {
		if magnify > 0 && magnify != limitManager.limitMap[factorType].magnify {
			log.LogDebugf("action[SetClientLimit] type [%v] update magnify [%v] to [%v]",
				proto.QosTypeString(factorType), limitManager.limitMap[factorType].magnify, magnify)
			limitManager.limitMap[factorType].magnify = magnify
		}
	}
}

func (limitManager *LimitManager) ReadAlloc(ctx context.Context, size int) {
	limitManager.WaitN(ctx, limitManager.limitMap[proto.IopsReadType], 1)
	limitManager.WaitN(ctx, limitManager.limitMap[proto.FlowReadType], size)
}

func (limitManager *LimitManager) WriteAlloc(ctx context.Context, size int) {
	limitManager.WaitN(ctx, limitManager.limitMap[proto.IopsWriteType], 1)
	limitManager.WaitN(ctx, limitManager.limitMap[proto.FlowWriteType], size)
}

// WaitN blocks until alloc success
func (limitManager *LimitManager) WaitN(ctx context.Context, lim *LimitFactor, n int) (err error) {
	var fut *util.Future
	var ret uint8
	if ret, fut = lim.alloc(uint32(n)); ret == runNow {
		atomic.AddUint64(&lim.valAllocCommit, uint64(n))
		log.LogDebugf("action[WaitN] type [%v] return now waitlistlen [%v]", proto.QosTypeString(lim.factorType), lim.waitList.Len())
		return nil
	}

	respCh, errCh := fut.AsyncResponse()

	select {
	case <-ctx.Done():
		log.LogWarnf("action[WaitN] type [%v] ctx done return waitlistlen [%v]", proto.QosTypeString(lim.factorType), lim.waitList.Len())
		return ctx.Err()
	case err = <-errCh:
		log.LogWarnf("action[WaitN] type [%v] err return waitlistlen [%v]", proto.QosTypeString(lim.factorType), lim.waitList.Len())
		return
	case <-respCh:
		atomic.AddUint64(&lim.valAllocCommit, uint64(n))
		log.LogDebugf("action[WaitN] type [%v] return waitlistlen [%v]", proto.QosTypeString(lim.factorType), lim.waitList.Len())
		return nil
		//default:
	}
}

func (limitManager *LimitManager) UpdateFlowInfo(limit *proto.LimitRsp2Client) {
	log.LogDebugf("action[LimitManager.UpdateFlowInfo]")
	limitManager.SetClientLimit(limit)
	return
}

func (limitManager *LimitManager) SetClientID(id uint64) (err error) {
	limitManager.ID = id
	return
}
