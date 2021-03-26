package statistics

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
	"time"
)

var lock sync.RWMutex
var infoMap = new(sync.Map)
var once sync.Once
var address string
var module string

type reportInfo struct {
	Addr       string
	Model      string
	Infos      []*info
	ReportTime int64
}

type info struct {
	lock        sync.Mutex
	VolID       uint64
	PartitionID uint64
	Action      string
	MaxTime     int64
	MinTime     int64
	Count       uint64
}

func Report(action string, volID, partitionID uint64, useTimeD time.Duration) {
	var (
		load interface{}
		ok   bool
	)
	key := fmt.Sprintf("%d-%d-%s", volID, partitionID, action)
	useTime := int64(useTimeD / 1e6)

	lock.RLock()
	load, ok = infoMap.Load(key)
	lock.RUnlock()
	if !ok {
		lock.Lock()
		if load, ok = infoMap.Load(key); !ok {
			infoMap.Store(key, &info{
				VolID:       volID,
				PartitionID: partitionID,
				Action:      action,
				MaxTime:     useTime,
				MinTime:     useTime,
				Count:       1,
			})
			lock.Unlock()
			return
		}
		lock.Unlock()
	}

	info := load.(*info)

	info.lock.Lock()
	defer info.lock.Unlock()
	info.Count += 1
	if info.MaxTime < useTime {
		info.MaxTime = useTime
	}
	if info.MinTime > useTime {
		info.MinTime = useTime
	}
}

func currentMap() *sync.Map {
	lock.Lock()
	defer lock.Unlock()

	reportMap := infoMap
	infoMap = new(sync.Map)
	return reportMap
}

func StartReportJob(ctx context.Context, addr, moduleName string) {
	once.Do(func() {
		address = addr
		module = moduleName
		reportJob(ctx)
	})
}

var TickerTime = 1 * time.Second

func reportJob(ctx context.Context) {
	ticker := time.NewTicker(TickerTime)
	for {
		select {
		case <-ticker.C:
			reportMap := currentMap()
			go report(reportMap)
			//do some thing
		case <-ctx.Done():
			log.LogWarnf("stop report statistics")
			return
		}
	}
}

func report(reportMap *sync.Map) {
	report := reportInfo{
		Model:      module,
		Addr:       address,
		ReportTime: time.Now().UnixNano() / 1e6,
	}

	reportMap.Range(func(_key, value interface{}) bool {
		report.Infos = append(report.Infos, value.(*info))
		return true
	})

	//post ....
	marshal, _ := json.Marshal(report)
	fmt.Println(string(marshal))
}
