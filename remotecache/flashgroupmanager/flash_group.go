package flashgroupmanager

import (
	"sync"

	"github.com/cubefs/cubefs/proto"
)

const (
	UnusedFlashNodeFlashGroupID      = 0
	DefaultWaitClientUpdateFgTimeSec = 65
)

type flashGroupValue struct {
	ID           uint64
	Slots        []uint32 // FlashGroup's position in hasher ring, set by cli. value is range of crc32.
	SlotStatus   proto.SlotStatus
	PendingSlots []uint32
	Step         uint32
	Weight       uint32
	Status       proto.FlashGroupStatus
}

type FlashGroup struct {
	flashGroupValue
	lock       sync.RWMutex
	flashNodes map[string]*FlashNode // key: FlashNodeAddr
}

func (fg *FlashGroup) GetAdminView() (view proto.FlashGroupAdminView) {
	fg.lock.RLock()
	view = proto.FlashGroupAdminView{
		ID:           fg.ID,
		Slots:        fg.Slots,
		Weight:       fg.Weight,
		Status:       fg.Status,
		SlotStatus:   fg.SlotStatus,
		PendingSlots: fg.PendingSlots,
		Step:         fg.Step,
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

func (fg *FlashGroup) GetStatus() (st proto.FlashGroupStatus) {
	fg.lock.RLock()
	st = fg.Status
	fg.lock.RUnlock()
	return
}

func (fg *FlashGroup) getFlashNodeHostsEnabled() (hosts []string) {
	hosts = make([]string, 0, len(fg.flashNodes))
	fg.lock.RLock()
	for host, flashNode := range fg.flashNodes {
		if !flashNode.isEnable() {
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
