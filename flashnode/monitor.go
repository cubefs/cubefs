package flashnode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/statistics"
	"sync/atomic"
)

func (f *FlashNode) recordMonitorAction(volume string, action int, size uint64) {
	val, found := f.statistics.Load(volume)
	if !found {
		val, _ = f.statistics.LoadOrStore(volume, statistics.InitMonitorData(statistics.ModelFlashNode))
	}
	datas, is := val.([]*statistics.MonitorData)
	if !is {
		f.statistics.Delete(volume)
		return
	}
	datas[action].UpdateData(size)
}

func (f *FlashNode) reportSummary(reportTime int64) []*statistics.MonitorData {
	var results = make([]*statistics.MonitorData, 0)
	f.statistics.Range(func(key, value interface{}) (re bool) {
		re = true
		var is bool
		var volume string
		if volume, is = key.(string); !is {
			f.statistics.Delete(key)
			return
		}
		var datas []*statistics.MonitorData
		if datas, is = value.([]*statistics.MonitorData); !is {
			f.statistics.Delete(key)
			return
		}
		for i := 0; i < len(datas); i++ {
			var data = datas[i]
			if data.Count == 0 {
				continue
			}
			results = append(results, &statistics.MonitorData{
				VolName:     volume,
				PartitionID: 0,
				Action:      i,
				ActionStr:   proto.ActionFlashMap[i],
				Size:        atomic.SwapUint64(&data.Size, 0),
				Count:       atomic.SwapUint64(&data.Count, 0),
				ReportTime:  reportTime,
			})
		}
		return
	})
	return results
}
