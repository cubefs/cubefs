package errors

const (
	CodeNoAvaliableVolume = 801
	CodeAllocBidFromCm    = 802
)

var (
	ErrNoAvaliableVolume = Error(CodeNoAvaliableVolume)
	ErrAllocBidFromCm    = Error(CodeAllocBidFromCm)
)
