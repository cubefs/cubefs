package util

import "regexp"

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	DefaultDataPartitionSize = 120 * GB
	TaskWorkerInterval       = 1
)

const (
	BlockHeaderInoSize     = 8
	BlockHeaderCrcSize     = PerBlockCrcSize * BlockCount
	BlockHeaderCrcIndex    = BlockHeaderInoSize
	BlockHeaderDelMarkSize = 1
	BlockHeaderSize        = BlockHeaderInoSize + BlockHeaderCrcSize + BlockHeaderDelMarkSize
	BlockCount             = 1024
	MarkDelete             = 'D'
	UnMarkDelete           = 'U'
	MarkDeleteIndex        = BlockHeaderSize - 1
	BlockSize              = 65536 * 4
	ReadBlockSize          = 65536
	PerBlockCrcSize        = 4
	DeleteIndexFileName    = "delete.index"
	ExtentSize             = BlockCount * BlockSize
	ExtentFileSizeLimit    = BlockHeaderSize + ExtentSize
	PacketHeaderSize       = 45
)

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func IP(val interface{}) bool {
	ip4Pattern := `((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)`
	ip4 := regexpCompile(ip4Pattern)
	return isMatch(ip4, val)
}

func regexpCompile(str string) *regexp.Regexp {
	return regexp.MustCompile("^" + str + "$")
}

func isMatch(exp *regexp.Regexp, val interface{}) bool {
	switch v := val.(type) {
	case []rune:
		return exp.MatchString(string(v))
	case []byte:
		return exp.Match(v)
	case string:
		return exp.MatchString(v)
	default:
		return false
	}
}
