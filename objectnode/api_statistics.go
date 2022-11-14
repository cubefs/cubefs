package objectnode

import (
	"sync/atomic"

	"github.com/chubaofs/chubaofs/util/statistics"
)

type StatisticsAction int

const (
	StatisticsActionHeadObject              StatisticsAction = statistics.ActionS3HeadObject
	StatisticsActionGetObject               StatisticsAction = statistics.ActionS3GetObject
	StatisticsActionPutObject               StatisticsAction = statistics.ActionS3PutObject
	StatisticsActionListObjects             StatisticsAction = statistics.ActionS3ListObjects
	StatisticsActionDeleteObject            StatisticsAction = statistics.ActionS3DeleteObject
	StatisticsActionCopyObject              StatisticsAction = statistics.ActionS3CopyObject
	StatisticsActionCreateMultipartUpload   StatisticsAction = statistics.ActionS3CreateMultipartUpload
	StatisticsActionUploadPart              StatisticsAction = statistics.ActionS3UploadPart
	StatisticsActionCompleteMultipartUpload StatisticsAction = statistics.ActionS3CompleteMultipartUpload
	StatisticsActionAbortMultipartUpload    StatisticsAction = statistics.ActionS3AbortMultipartUpload
	StatisticsActionListMultipartUploads    StatisticsAction = statistics.ActionS3ListMultipartUploads
	StatisticsActionListParts               StatisticsAction = statistics.ActionS3ListParts
)

func (o *ObjectNode) recordAction(volume string, action StatisticsAction, size uint64) {
	val, found := o.statistics.Load(volume)
	if !found {
		val, _ = o.statistics.LoadOrStore(volume, statistics.InitMonitorData(statistics.ModelObjectNode))
	}
	datas, is := val.([]*statistics.MonitorData)
	if !is {
		o.statistics.Delete(volume)
		return
	}
	datas[action].UpdateData(size)
}

func (o *ObjectNode) reportSummary(reportTime int64) []*statistics.MonitorData {
	var results = make([]*statistics.MonitorData, 0)
	o.statistics.Range(func(key, value interface{}) (re bool) {
		re = true
		var is bool
		var volume string
		if volume, is = key.(string); !is {
			o.statistics.Delete(key)
			return
		}
		var datas []*statistics.MonitorData
		if datas, is = value.([]*statistics.MonitorData); !is {
			o.statistics.Delete(key)
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
				ActionStr:   statistics.ActionObjectMap[i],
				Size:        atomic.SwapUint64(&data.Size, 0),
				Count:       atomic.SwapUint64(&data.Count, 0),
				ReportTime:  reportTime,
			})
		}
		return
	})
	return results
}
