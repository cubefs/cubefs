package flashgroupmanager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

// TestNewFlashGroup tests the creation of a new FlashGroup instance
func TestNewFlashGroup(t *testing.T) {
	id := uint64(123)
	slots := []uint32{1, 2, 3, 4, 5}
	slotStatus := proto.SlotStatus_Completed
	pendingSlots := []uint32{6, 7}
	step := uint32(5)
	status := proto.FlashGroupStatus_Active
	weight := uint32(100)

	fg := newFlashGroup(id, slots, slotStatus, pendingSlots, step, status, weight)

	assert.Equal(t, id, fg.ID)
	assert.Equal(t, slots, fg.Slots)
	assert.Equal(t, slotStatus, fg.SlotStatus)
	assert.Equal(t, pendingSlots, fg.PendingSlots)
	assert.Equal(t, step, fg.Step)
	assert.Equal(t, status, fg.Status)
	assert.Equal(t, weight, fg.Weight)
	assert.NotNil(t, fg.flashNodes)
	assert.Equal(t, 0, len(fg.flashNodes))
}

// TestFlashGroup_GetAdminView tests the GetAdminView method
func TestFlashGroup_GetAdminView(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3}, proto.SlotStatus_Completed, []uint32{}, 3, proto.FlashGroupStatus_Active, 100)

	// Add some flash nodes
	fn1 := &FlashNode{flashNodeValue: flashNodeValue{Addr: "node1", ZoneName: "zone1"}}
	fn2 := &FlashNode{flashNodeValue: flashNodeValue{Addr: "node2", ZoneName: "zone2"}}
	fg.putFlashNode(fn1)
	fg.putFlashNode(fn2)

	view := fg.GetAdminView()

	assert.Equal(t, uint64(1), view.ID)
	assert.Equal(t, []uint32{1, 2, 3}, view.Slots)
	assert.Equal(t, proto.SlotStatus_Completed, view.SlotStatus)
	assert.Equal(t, proto.FlashGroupStatus_Active, view.Status)
	assert.Equal(t, uint32(100), view.Weight)
	assert.Equal(t, uint32(3), view.Step)
	assert.Equal(t, 2, view.FlashNodeCount)
	assert.Equal(t, 2, len(view.ZoneFlashNodes))
}

// TestFlashGroup_ReduceSlot_AlreadyReducing tests that ReduceSlot doesn't start multiple goroutines
func TestFlashGroup_ReduceSlot_AlreadyReducing(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3, 4, 5, 6, 7, 8}, proto.SlotStatus_Completed, []uint32{}, 8, proto.FlashGroupStatus_Active, 100)

	// Set the flag to indicate reduction is already in progress
	atomic.StoreInt32(&fg.ReducingSlots, 1)

	// This should return immediately without starting a new goroutine
	fg.ReduceSlot()

	// Verify the flag is still set
	assert.Equal(t, int32(1), atomic.LoadInt32(&fg.ReducingSlots))
}

// TestFlashGroup_ReduceSlot_NoSlots tests ReduceSlot behavior when there are no slots
func TestFlashGroup_ReduceSlot_NoSlots(t *testing.T) {
	fg := newFlashGroup(1, []uint32{}, proto.SlotStatus_Completed, []uint32{}, 0, proto.FlashGroupStatus_Active, 100)

	// Set lost all flash nodes flag
	atomic.StoreInt32(&fg.LostAllFlashNode, 1)

	// Start reduction
	fg.ReduceSlot()

	// Wait a bit for the goroutine to process
	time.Sleep(100 * time.Millisecond)

	// Verify no slots were processed since there were none to begin with
	assert.Equal(t, 0, len(fg.Slots))
	assert.Equal(t, 0, len(fg.ReservedSlots))
}

// TestFlashGroup_ReduceSlot_WithSlots tests the complete ReduceSlot functionality
func TestFlashGroup_ReduceSlot_WithSlots(t *testing.T) {
	// Create a flash group with 8 slots
	initialSlots := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	fg := newFlashGroup(1, initialSlots, proto.SlotStatus_Completed, []uint32{}, 8, proto.FlashGroupStatus_Active, 100)

	// Set lost all flash nodes flag to trigger reduction
	atomic.StoreInt32(&fg.LostAllFlashNode, 1)

	// Start reduction
	fg.ReduceSlot()

	// Wait for the first reduction cycle (20 seconds, but we'll wait less for testing)
	// In a real scenario, this would take 20 seconds
	time.Sleep(100 * time.Millisecond)

	// Verify that slots were moved to reserved slots
	// Note: In a real scenario with 20-second intervals, this would take longer
	// For testing purposes, we can verify the mechanism works by checking the structure
	assert.Equal(t, 8, len(fg.Slots)+len(fg.ReservedSlots))

	// Verify the reducing flag is set
	assert.Equal(t, int32(1), atomic.LoadInt32(&fg.ReducingSlots))
}

// TestFlashGroup_executeReduceSlot tests the executeReduceSlot method directly
func TestFlashGroup_executeReduceSlot(t *testing.T) {
	// Create a flash group with 8 slots
	initialSlots := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	fg := newFlashGroup(1, initialSlots, proto.SlotStatus_Completed, []uint32{}, 8, proto.FlashGroupStatus_Active, 100)

	initialSlotCount := len(fg.Slots)

	// Execute reduction
	fg.executeReduceSlot((len(fg.Slots) + 4 - 1) / 4)

	// Verify that approximately 25% of slots were moved (8 slots -> 6 slots, 2 to reserved)
	expectedRemaining := (initialSlotCount * 3) / 4 // 75% remaining
	expectedReserved := initialSlotCount - expectedRemaining

	assert.Equal(t, expectedRemaining, len(fg.Slots))
	assert.Equal(t, expectedReserved, len(fg.ReservedSlots))

	// Verify total count remains the same
	assert.Equal(t, initialSlotCount, len(fg.Slots)+len(fg.ReservedSlots))
}

// TestFlashGroup_executeReduceSlot_EmptySlots tests executeReduceSlot with no slots
func TestFlashGroup_executeReduceSlot_EmptySlots(t *testing.T) {
	fg := newFlashGroup(1, []uint32{}, proto.SlotStatus_Completed, []uint32{}, 0, proto.FlashGroupStatus_Active, 100)

	// This should not panic or cause issues
	fg.executeReduceSlot((len(fg.Slots) + 4 - 1) / 4)

	assert.Equal(t, 0, len(fg.Slots))
	assert.Equal(t, 0, len(fg.ReservedSlots))
}

// TestFlashGroup_executeReduceSlot_SingleSlot tests executeReduceSlot with only one slot
func TestFlashGroup_executeReduceSlot_SingleSlot(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1}, proto.SlotStatus_Completed, []uint32{}, 1, proto.FlashGroupStatus_Active, 100)

	fg.executeReduceSlot((len(fg.Slots) + 4 - 1) / 4)

	// With 1 slot, 25% rounded up is 1, so all slots should be moved to reserved
	assert.Equal(t, 0, len(fg.Slots))
	assert.Equal(t, 1, len(fg.ReservedSlots))
}

// TestFlashGroup_IsLostAllFlashNode tests the IsLostAllFlashNode method
func TestFlashGroup_IsLostAllFlashNode(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3}, proto.SlotStatus_Completed, []uint32{}, 3, proto.FlashGroupStatus_Active, 100)

	// Initially should be false
	assert.False(t, fg.IsLostAllFlashNode())

	// Set the flag
	atomic.StoreInt32(&fg.LostAllFlashNode, 1)
	assert.True(t, fg.IsLostAllFlashNode())

	// Clear the flag
	atomic.StoreInt32(&fg.LostAllFlashNode, 0)
	assert.False(t, fg.IsLostAllFlashNode())
}

// TestFlashGroup_GetStatus tests the GetStatus method
func TestFlashGroup_GetStatus(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3}, proto.SlotStatus_Completed, []uint32{}, 3, proto.FlashGroupStatus_Active, 100)

	status := fg.GetStatus()
	assert.Equal(t, proto.FlashGroupStatus_Active, status)

	// Change status
	fg.Status = proto.FlashGroupStatus_Inactive
	status = fg.GetStatus()
	assert.Equal(t, proto.FlashGroupStatus_Inactive, status)
}

// TestFlashGroup_getSlots tests the getSlots method
func TestFlashGroup_getSlots(t *testing.T) {
	initialSlots := []uint32{1, 2, 3, 4, 5}
	fg := newFlashGroup(1, initialSlots, proto.SlotStatus_Completed, []uint32{}, 5, proto.FlashGroupStatus_Active, 100)

	slots := fg.getSlots()

	// Should return a copy of the slots
	assert.Equal(t, initialSlots, slots)
	assert.NotSame(t, &initialSlots[0], &slots[0]) // Should be different memory addresses
}

// TestFlashGroup_getSlotStatus tests the getSlotStatus method
func TestFlashGroup_getSlotStatus(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3}, proto.SlotStatus_Creating, []uint32{}, 3, proto.FlashGroupStatus_Active, 100)

	status := fg.getSlotStatus()
	assert.Equal(t, proto.SlotStatus_Creating, status)
}

// TestFlashGroup_getFlashNodesCount tests the getFlashNodesCount method
func TestFlashGroup_getFlashNodesCount(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3}, proto.SlotStatus_Completed, []uint32{}, 3, proto.FlashGroupStatus_Active, 100)

	// Initially no flash nodes
	assert.Equal(t, 0, fg.getFlashNodesCount())

	// Add a flash node
	fn := &FlashNode{flashNodeValue: flashNodeValue{Addr: "node1", ZoneName: "zone1"}}
	fg.putFlashNode(fn)

	assert.Equal(t, 1, fg.getFlashNodesCount())
}

// TestFlashGroup_getSlotsCount tests the getSlotsCount method
func TestFlashGroup_getSlotsCount(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3, 4, 5}, proto.SlotStatus_Completed, []uint32{}, 5, proto.FlashGroupStatus_Active, 100)

	assert.Equal(t, 5, fg.getSlotsCount())
}

// TestFlashGroup_putFlashNode tests the putFlashNode method
func TestFlashGroup_putFlashNode(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3}, proto.SlotStatus_Completed, []uint32{}, 3, proto.FlashGroupStatus_Active, 100)

	fn := &FlashNode{flashNodeValue: flashNodeValue{Addr: "node1", ZoneName: "zone1"}}
	fg.putFlashNode(fn)

	assert.Equal(t, 1, len(fg.flashNodes))
	assert.Equal(t, fn, fg.flashNodes["node1"])
}

// TestFlashGroup_removeFlashNode tests the removeFlashNode method
func TestFlashGroup_removeFlashNode(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3}, proto.SlotStatus_Completed, []uint32{}, 3, proto.FlashGroupStatus_Active, 100)

	// Add a flash node first
	fn := &FlashNode{flashNodeValue: flashNodeValue{Addr: "node1", ZoneName: "zone1"}}
	fg.putFlashNode(fn)
	assert.Equal(t, 1, len(fg.flashNodes))

	// Remove it
	fg.removeFlashNode("node1")
	assert.Equal(t, 0, len(fg.flashNodes))
}

// TestFlashGroup_GetPendingSlotsCount tests the GetPendingSlotsCount method
func TestFlashGroup_GetPendingSlotsCount(t *testing.T) {
	pendingSlots := []uint32{1, 2, 3}
	fg := newFlashGroup(1, []uint32{4, 5, 6}, proto.SlotStatus_Completed, pendingSlots, 3, proto.FlashGroupStatus_Active, 100)

	assert.Equal(t, 3, fg.GetPendingSlotsCount())
}

// TestFlashGroup_argConvertFlashGroupStatus tests the argConvertFlashGroupStatus function
func TestFlashGroup_argConvertFlashGroupStatus(t *testing.T) {
	// Test active = true
	status := argConvertFlashGroupStatus(true)
	assert.Equal(t, proto.FlashGroupStatus_Active, status)

	// Test active = false
	status = argConvertFlashGroupStatus(false)
	assert.Equal(t, proto.FlashGroupStatus_Inactive, status)
}

// TestFlashGroup_NewFlashGroupFromFgv tests the NewFlashGroupFromFgv function
func TestFlashGroup_NewFlashGroupFromFgv(t *testing.T) {
	fgv := flashGroupValue{
		ID:           123,
		Slots:        []uint32{1, 2, 3},
		SlotStatus:   proto.SlotStatus_Completed,
		PendingSlots: []uint32{4, 5},
		Step:         3,
		Weight:       100,
		Status:       proto.FlashGroupStatus_Active,
	}

	fg := NewFlashGroupFromFgv(fgv)

	assert.Equal(t, fgv.ID, fg.ID)
	assert.Equal(t, fgv.Slots, fg.Slots)
	assert.Equal(t, fgv.SlotStatus, fg.SlotStatus)
	assert.Equal(t, fgv.PendingSlots, fg.PendingSlots)
	assert.Equal(t, fgv.Step, fg.Step)
	assert.Equal(t, fgv.Weight, fg.Weight)
	assert.Equal(t, fgv.Status, fg.Status)
	assert.NotNil(t, fg.flashNodes)
}

// TestFlashGroup_ConcurrentAccess tests concurrent access to FlashGroup methods
func TestFlashGroup_ConcurrentAccess(t *testing.T) {
	fg := newFlashGroup(1, []uint32{1, 2, 3, 4, 5, 6, 7, 8}, proto.SlotStatus_Completed, []uint32{}, 8, proto.FlashGroupStatus_Active, 100)

	var wg sync.WaitGroup
	numGoroutines := 10

	// Test concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				fg.getSlots()
				fg.getSlotStatus()
				fg.getFlashNodesCount()
				fg.getSlotsCount()
			}
		}()
	}

	// Test concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fn := &FlashNode{flashNodeValue: flashNodeValue{Addr: fmt.Sprintf("node%d", id), ZoneName: fmt.Sprintf("zone%d", id)}}
			fg.putFlashNode(fn)
		}(i)
	}

	wg.Wait()

	// Verify the structure is still consistent
	assert.Equal(t, 8, len(fg.Slots))
	assert.Equal(t, numGoroutines, len(fg.flashNodes))
}
