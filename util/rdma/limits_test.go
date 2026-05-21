package rdma

import (
	"testing"

	"github.com/cubefs/cubefs/util"
)

// TestMinValidSlotSize_Formula sanity-checks the assumption underlying
// MinValidSlotSize: it must be strictly greater than util.BlockSize, otherwise
// a typical single-block write would not fit in one slot.
func TestMinValidSlotSize_Formula(t *testing.T) {
	if MinValidSlotSize <= util.BlockSize {
		t.Fatalf("MinValidSlotSize=%d must exceed BlockSize=%d", MinValidSlotSize, util.BlockSize)
	}
	expected := slotHeaderBytes + maxPacketHeaderBytes + util.BlockSize
	if MinValidSlotSize != expected {
		t.Fatalf("MinValidSlotSize=%d, expected %d", MinValidSlotSize, expected)
	}
}

func TestValidateSlotSize_Accepts(t *testing.T) {
	cases := []int{MinValidSlotSize, MinValidSlotSize + 1, DefaultSlotSize, 256 * 1024}
	for _, size := range cases {
		if err := ValidateSlotSize(size); err != nil {
			t.Errorf("size=%d: unexpected error: %v", size, err)
		}
	}
}

func TestValidateSlotSize_Rejects(t *testing.T) {
	cases := []int{0, -1, 1024, util.BlockSize, MinValidSlotSize - 1}
	for _, size := range cases {
		if err := ValidateSlotSize(size); err == nil {
			t.Errorf("size=%d: expected error, got nil", size)
		}
	}
}

// TestDefaultSlotSize_AboveMin ensures the recommended default is strictly
// above the minimum so out-of-the-box configurations validate.
func TestDefaultSlotSize_AboveMin(t *testing.T) {
	if DefaultSlotSize < MinValidSlotSize {
		t.Fatalf("DefaultSlotSize=%d must be >= MinValidSlotSize=%d",
			DefaultSlotSize, MinValidSlotSize)
	}
}

func TestCreditAckMode_String(t *testing.T) {
	tests := map[CreditAckMode]string{
		CreditAckSync:  "sync",
		CreditAckAsync: "async",
	}
	for mode, want := range tests {
		if got := mode.String(); got != want {
			t.Errorf("CreditAckMode(%d).String()=%q, want %q", mode, got, want)
		}
	}
	// Unknown modes are reported with their numeric value.
	if got := CreditAckMode(99).String(); got == "" {
		t.Error("unknown mode should produce non-empty string")
	}
}
