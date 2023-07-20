package node

import (
	"math"
	"strconv"
)

const (
	Active      = "Active"
	InActive    = "Inactive"
	UnAvailable = "Unavailable"
)

func FormatNodeStatus(isActive bool, badDisks ...string) string {
	if len(badDisks) > 0 {
		return UnAvailable
	}
	if isActive {
		return Active
	}
	return InActive
}

func FormatAvailable(available, used, total uint64) uint64 {
	if used > total {
		return  0
	}
	return available
}

func FormatWritableStr(isWritable bool) string {
	if isWritable {
		return "writable"
	}
	return "readonly"
}

func FormatDiskAndPartitionStatus(status int8) string {
	switch status {
	case 1:
		return "ReadOnly"
	case 2:
		return "ReadWrite"
	case -1:
		return "Unavailable"
	default:
		return "Unknown"
	}
}

func FormatUint64(num uint64) string {
	if num >= math.MaxInt64 {
		return "unlimited"
	}
	return strconv.FormatUint(num, 10)
}