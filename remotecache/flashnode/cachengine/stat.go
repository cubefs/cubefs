package cachengine

import "sync/atomic"

var StatMap = make(map[string]*MetricStat)

type MetricStat struct {
	ReadBytes  uint64
	ReadCount  uint64
	WriteBytes uint64
	WriteCount uint64
}

func UpdateWriteBytesMetric(size uint64, d string) {
	if stat, ok := StatMap[d]; ok {
		atomic.AddUint64(&stat.WriteBytes, size)
	}
}

func UpdateWriteCountMetric(d string) {
	if stat, ok := StatMap[d]; ok {
		atomic.AddUint64(&stat.WriteCount, 1)
	}
}
