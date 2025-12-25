package flashgroupmanager

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

const (
	UnusedFlashNodeFlashGroupID      = 0
	DefaultWaitClientUpdateFgTimeSec = 65
	WaitForRecoverCount              = 20
)

type FlashGroupValue struct {
	ID               uint64
	Slots            []uint32 // FlashGroup's position in hasher ring, set by cli. value is range of crc32.
	ReservedSlots    []uint32
	SlotStatus       proto.SlotStatus
	PendingSlots     []uint32
	Step             uint32
	Weight           uint32
	Status           proto.FlashGroupStatus
	LostAllFlashNode int32
	ReducingSlots    int32
	IncreasingSlots  int32
	ReduceAllTime    int64 // unix second
}

type FlashGroup struct {
	FlashGroupValue
	lock       sync.RWMutex
	flashNodes map[string]*FlashNode // key: FlashNodeAddr
}

func (fg *FlashGroup) GetAdminView() (view proto.FlashGroupAdminView) {
	fg.lock.RLock()
	view = proto.FlashGroupAdminView{
		ID:              fg.ID,
		Slots:           fg.Slots,
		ReservedSlots:   fg.ReservedSlots,
		IsReducingSlots: fg.ReducingSlots != 0,
		Weight:          fg.Weight,
		Status:          fg.Status,
		SlotStatus:      fg.SlotStatus,
		PendingSlots:    fg.PendingSlots,
		Step:            fg.Step,
	}
	view.ZoneFlashNodes = make(map[string][]*proto.FlashNodeViewInfo)
	view.FlashNodeCount = len(fg.flashNodes)
	for _, flashNode := range fg.flashNodes {
		view.ZoneFlashNodes[flashNode.ZoneName] = append(view.ZoneFlashNodes[flashNode.ZoneName], flashNode.GetFlashNodeViewInfo())
	}
	fg.lock.RUnlock()
	return
}

func newFlashGroup(id uint64, slots []uint32, slotStatus proto.SlotStatus, pendingSlots []uint32, step uint32, status proto.FlashGroupStatus, weight uint32) *FlashGroup {
	fg := new(FlashGroup)
	fg.ID = id
	fg.Slots = slots
	fg.SlotStatus = slotStatus
	fg.PendingSlots = pendingSlots
	fg.ReservedSlots = make([]uint32, 0)
	fg.Step = step
	fg.Weight = weight
	fg.Status = status
	fg.flashNodes = make(map[string]*FlashNode)
	return fg
}

func (fg *FlashGroup) GetSlots() (slots []uint32) {
	fg.lock.RLock()
	slots = make([]uint32, 0, len(fg.Slots))
	slots = append(slots, fg.Slots...)
	fg.lock.RUnlock()
	return
}

func argConvertFlashGroupStatus(active bool) proto.FlashGroupStatus {
	if active {
		return proto.FlashGroupStatus_Active
	}
	return proto.FlashGroupStatus_Inactive
}

func (fg *FlashGroup) IsLostAllFlashNode() bool {
	return atomic.LoadInt32(&fg.LostAllFlashNode) != 0
}

func (fg *FlashGroup) ReduceSlot(syncFlashGroupFunc SyncUpdateFlashGroupFunc) {
	if !atomic.CompareAndSwapInt32(&fg.ReducingSlots, 0, 1) {
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("flashgroup %v is reducing slots", fg)
	}
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer func() {
			atomic.StoreInt32(&fg.ReducingSlots, 0)
			ticker.Stop()
		}()
		var i int
		numToSelect := (len(fg.Slots) + 4 - 1) / 4
		for {
			if !fg.IsLostAllFlashNode() || len(fg.Slots) == 0 {
				return
			}
			<-ticker.C
			i++
			if i <= WaitForRecoverCount {
				continue
			}
			fg.executeReduceSlot(numToSelect, syncFlashGroupFunc)
		}
	}()
}

func (fg *FlashGroup) IncreaseSlot(syncFlashGroupFunc SyncUpdateFlashGroupFunc) {
	if !atomic.CompareAndSwapInt32(&fg.IncreasingSlots, 0, 1) {
		return
	}
	if log.EnableDebug() {
		log.LogDebugf("flashgroup %v is increasing slots", fg)
	}
	totalSlots := len(fg.Slots) + len(fg.ReservedSlots)
	numToSelect := (totalSlots + 8 - 1) / 8
	fg.executeIncreaseSlot(numToSelect, syncFlashGroupFunc)
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer func() {
			atomic.StoreInt32(&fg.IncreasingSlots, 0)
			ticker.Stop()
		}()
		var i int
		for {
			if len(fg.ReservedSlots) == 0 {
				return
			}
			<-ticker.C
			i++
			fg.executeIncreaseSlot(numToSelect, syncFlashGroupFunc)
		}
	}()
}

func (fg *FlashGroup) executeReduceSlot(numToReduce int, syncFlashGroupFunc SyncUpdateFlashGroupFunc) {
	if len(fg.Slots) == 0 {
		return
	}
	fg.lock.Lock()
	defer fg.lock.Unlock()
	numToReduce = util.Min(numToReduce, len(fg.Slots))
	if numToReduce == 0 {
		return
	}
	var oldSlots, oldReservedSlots []uint32
	oldSlots = append(oldSlots, fg.Slots...)
	oldReservedSlots = append(oldReservedSlots, fg.ReservedSlots...)
	if log.EnableInfo() {
		log.LogInfof("executeReduceSlot fg(%v) oldSlots(%v) oldReservedSlots(%v)", fg.ID, oldSlots, oldReservedSlots)
	}
	fg.ReservedSlots = append(fg.ReservedSlots, fg.Slots[:numToReduce]...)
	fg.Slots = fg.Slots[numToReduce:]
	if len(fg.Slots) == 0 {
		fg.ReduceAllTime = time.Now().Unix()
	}
	if err := syncFlashGroupFunc(fg); err != nil {
		fg.Slots = oldSlots
		fg.ReservedSlots = oldReservedSlots
		fg.ReduceAllTime = 0
	}
}

func (fg *FlashGroup) executeIncreaseSlot(numToIncrease int, syncFlashGroupFunc SyncUpdateFlashGroupFunc) {
	if len(fg.ReservedSlots) == 0 {
		return
	}
	fg.lock.Lock()
	defer fg.lock.Unlock()
	numToIncrease = util.Min(numToIncrease, len(fg.ReservedSlots))
	if numToIncrease == 0 {
		return
	}
	var oldSlots, oldReservedSlots []uint32
	oldSlots = append(oldSlots, fg.Slots...)
	oldReservedSlots = append(oldReservedSlots, fg.ReservedSlots...)
	if log.EnableInfo() {
		log.LogInfof("executeIncreaseSlot fg(%v) oldSlots(%v) oldReservedSlots(%v)", fg.ID, oldSlots, oldReservedSlots)
	}
	fg.Slots = append(fg.Slots, fg.ReservedSlots[:numToIncrease]...)
	fg.ReservedSlots = fg.ReservedSlots[numToIncrease:]
	if len(fg.ReservedSlots) == 0 {
		fg.ReduceAllTime = 0
	}
	if err := syncFlashGroupFunc(fg); err != nil {
		fg.Slots = oldSlots
		fg.ReservedSlots = oldReservedSlots
		if len(fg.ReservedSlots) == 0 {
			fg.ReduceAllTime = 0
		}
	}
}

func (fg *FlashGroup) GetStatus() (st proto.FlashGroupStatus) {
	fg.lock.RLock()
	st = fg.Status
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getFlashNodeHostsEnableAndActive() (hosts []string) {
	hosts = make([]string, 0, len(fg.flashNodes))
	fg.lock.RLock()
	for host, flashNode := range fg.flashNodes {
		if !flashNode.isActiveAndEnable() {
			continue
		}
		hosts = append(hosts, host)
	}
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) GetSlotStatus() (status proto.SlotStatus) {
	fg.lock.RLock()
	status = fg.SlotStatus
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) GetFlashNodesCount() (count int) {
	fg.lock.RLock()
	count = len(fg.flashNodes)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) GetSlotsCount() (count int) {
	fg.lock.RLock()
	count = len(fg.Slots)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) GetFlashNodeHosts(checkStatus bool) (hosts []string) {
	hosts = make([]string, 0, len(fg.flashNodes))
	fg.lock.RLock()
	for host, flashNode := range fg.flashNodes {
		if checkStatus && !flashNode.isActiveAndEnable() {
			continue
		}
		hosts = append(hosts, host)
	}
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) RemoveFlashNode(addr string) {
	fg.lock.Lock()
	delete(fg.flashNodes, addr)
	fg.lock.Unlock()
}

func (fg *FlashGroup) putFlashNode(fn *FlashNode) {
	fg.lock.Lock()
	fg.flashNodes[fn.Addr] = fn
	fg.lock.Unlock()
}

func (fg *FlashGroup) getTargetZoneFlashNodeHosts(targetZone string) (hosts []string) {
	fg.lock.RLock()
	for _, flashNode := range fg.flashNodes {
		if flashNode.ZoneName == targetZone {
			hosts = append(hosts, flashNode.Addr)
		}
	}
	fg.lock.RUnlock()
	return
}

func NewFlashGroupFromFgv(fgv FlashGroupValue) *FlashGroup {
	fg := new(FlashGroup)
	fg.ID = fgv.ID
	fg.Slots = fgv.Slots
	fg.SlotStatus = fgv.SlotStatus
	fg.PendingSlots = fgv.PendingSlots
	fg.ReservedSlots = fgv.ReservedSlots
	fg.Step = fgv.Step
	fg.Weight = fgv.Weight
	fg.Status = fgv.Status
	fg.flashNodes = make(map[string]*FlashNode)
	return fg
}

func (fg *FlashGroup) GetPendingSlotsCount() (count int) {
	fg.lock.RLock()
	count = len(fg.PendingSlots)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) UpdateStatus(status proto.FlashGroupStatus,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc, flashNodeTopo *FlashNodeTopology,
) (err error) {
	fg.lock.Lock()
	defer fg.lock.Unlock()
	oldStatus := fg.Status
	fg.Status = status
	if oldStatus != status {
		if err = syncUpdateFlashGroupFunc(fg); err != nil {
			fg.Status = oldStatus
			return
		}
		flashNodeTopo.updateClientCache()
	}
	return nil
}

func (fg *FlashGroup) UpdateSlots(topology *FlashNodeTopology, needDeleteFgFlag bool, syncDeleteFlashGroupFunc SyncDeleteFlashGroupFunc,
	syncUpdateFlashGroupFunc SyncUpdateFlashGroupFunc,
) (err error) {
	fg.lock.Lock()
	var updatedSlotsNum uint32
	var newSlotStatus proto.SlotStatus
	var newPendingSlots []uint32

	leftPendingSlotsNum := uint32(len(fg.PendingSlots)) - fg.Step
	oldSlots := fg.Slots
	oldPendingSlots := fg.PendingSlots
	oldSlotStatus := fg.SlotStatus
	oldStatus := fg.Status
	if leftPendingSlotsNum > 0 { // previous steps
		updatedSlotsNum = fg.Step
		newPendingSlots = oldPendingSlots[updatedSlotsNum:]
		newSlotStatus = oldSlotStatus
	} else { // final step
		updatedSlotsNum = uint32(len(fg.PendingSlots))
		newPendingSlots = nil
		newSlotStatus = proto.SlotStatus_Completed
	}
	newSlots := getNewSlots(fg.Slots, fg.PendingSlots[:updatedSlotsNum], oldSlotStatus)
	fg.Slots = newSlots
	fg.PendingSlots = newPendingSlots
	fg.SlotStatus = newSlotStatus
	if needDeleteFgFlag {
		fg.Status = proto.FlashGroupStatus_Inactive
		err = syncDeleteFlashGroupFunc(fg)
	} else {
		err = syncUpdateFlashGroupFunc(fg)
	}

	if err != nil {
		fg.Slots = oldSlots
		fg.PendingSlots = oldPendingSlots
		fg.SlotStatus = oldSlotStatus
		if needDeleteFgFlag {
			fg.Status = oldStatus
		}
		fg.lock.Unlock()
		return
	}
	fg.lock.Unlock()
	if oldSlotStatus == proto.SlotStatus_Creating {
		for _, slot := range oldPendingSlots[:updatedSlotsNum] {
			topology.slotsMap[slot] = fg.ID
		}
	} else {
		topology.removeSlots(oldPendingSlots[:updatedSlotsNum])
		if needDeleteFgFlag {
			topology.flashGroupMap.Delete(fg.ID)
		}
	}
	return
}
