package flashnode

import (
	"github.com/cubefs/cubefs/util/statistics"
)

func (f *FlashNode) BeforeTp(volume string, action int) *statistics.TpObject {
	val, found := f.statistics.Load(volume)
	if !found {
		val, _ = f.statistics.LoadOrStore(volume, statistics.InitMonitorData(statistics.ModelFlashNode))
	}
	datas, is := val.([]*statistics.MonitorData)
	if !is {
		f.statistics.Delete(volume)
		return nil
	}
	return datas[action].BeforeTp()
}

func (f *FlashNode) rangeMonitorData(deal func(data *statistics.MonitorData, vol, path string, pid uint64)) {
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

		for _, data := range datas {
			deal(data, volume, "", 0)
		}
		return
	})
}

