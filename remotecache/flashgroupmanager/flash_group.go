package flashgroupmanager

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	UnusedFlashNodeFlashGroupID      = 0
	DefaultWaitClientUpdateFgTimeSec = 65
)

type flashGroupValue struct {
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
	SlotChanged      int32
}

type FlashGroup struct {
	flashGroupValue
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
	fg.Step = step
	fg.Weight = weight
	fg.Status = status
	fg.flashNodes = make(map[string]*FlashNode)
	return fg
}

func (fg *FlashGroup) getSlots() (slots []uint32) {
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

func (fg *FlashGroup) ReduceSlot() {
	reducingSlots := atomic.LoadInt32(&fg.ReducingSlots) != 0
	if reducingSlots {
		return
	}
	atomic.StoreInt32(&fg.ReducingSlots, 1)

	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	go func() {
		for {
			<-ticker.C
			if fg.IsLostAllFlashNode() {
				fg.executeReduceSlot()
				atomic.StoreInt32(&fg.SlotChanged, 1)
			} else {
				atomic.StoreInt32(&fg.ReducingSlots, 0)
				return
			}
		}
	}()
}

func (fg *FlashGroup) executeReduceSlot() {
	rand.Seed(time.Now().UnixNano())
	numToSelect := len(fg.Slots) / 4
	if numToSelect == 0 {
		return
	}
	selectedIndexes := rand.Perm(len(fg.Slots))[:numToSelect]
	for i := len(selectedIndexes) - 1; i >= 0; i-- {
		index := selectedIndexes[i]
		// add selected slot to ReservedSlots
		fg.ReservedSlots = append(fg.ReservedSlots, fg.Slots[index])
		// remove selected slot from Slots
		fg.Slots = append(fg.Slots[:index], fg.Slots[index+1:]...)
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

func (fg *FlashGroup) getSlotStatus() (status proto.SlotStatus) {
	fg.lock.RLock()
	status = fg.SlotStatus
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getFlashNodesCount() (count int) {
	fg.lock.RLock()
	count = len(fg.flashNodes)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getSlotsCount() (count int) {
	fg.lock.RLock()
	count = len(fg.Slots)
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getFlashNodeHosts(checkStatus bool) (hosts []string) {
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

func (fg *FlashGroup) removeFlashNode(addr string) {
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

func NewFlashGroupFromFgv(fgv flashGroupValue) *FlashGroup {
	fg := new(FlashGroup)
	fg.ID = fgv.ID
	fg.Slots = fgv.Slots
	fg.SlotStatus = fgv.SlotStatus
	fg.PendingSlots = fgv.PendingSlots
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
