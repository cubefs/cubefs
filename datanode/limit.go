package datanode

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

var (
	deleteLimiteRater            = rate.NewLimiter(rate.Inf, defaultMarkDeleteLimitBurst)
	MaxExtentRepairLimit   int32 = 100
	MinExtentRepairLimit   int32 = 5
	CurExtentRepairLimit   int32 = MaxExtentRepairLimit
	RaterReporterInterval        = 10 * time.Second
	extentRepairLimitRater sync.Map
)

func initRepairLimit(s *DataNode, cfg *config.Config) {
	for _, d := range cfg.GetSlice(ConfigKeyDisks) {
		var val int32
		// format "PATH:RESET_SIZE"
		disk := strings.Split(d.(string), ":")[0]
		extentRepairLimitRater.Store(disk, &val)
	}

	go func() {
		tick := time.NewTicker(RaterReporterInterval)
		for {
			select {
			case <-tick.C:
				extentRepairLimitRater.Range(func(key, value interface{}) bool {
					disk := key.(string)
					rater := value.(*int32)
					used := float64(atomic.LoadInt32(rater))
					s.metrics.Routines.SetWithLabelValues(used, disk)
					return true
				})
			}

		}
	}()
}

func requestDoExtentRepair(p *DataPartition) error {
	var rater *int32

	disk := p.disk.Path
	data, found := extentRepairLimitRater.Load(disk)
	if !found {
		var val int32
		log.LogErrorf("New disk[%v] on partition[%v]", disk, p.partitionID)
		extentRepairLimitRater.Store(disk, &val)
		rater = &val
	}
	rater = data.(*int32)

	if atomic.AddInt32(rater, 1) > atomic.LoadInt32(&CurExtentRepairLimit) {
		atomic.AddInt32(rater, -1)
		return fmt.Errorf("repair limit, cannot do extentRepair")
	}

	return nil
}

func finishDoExtentRepair(p *DataPartition) {
	var rater *int32

	disk := p.disk.Path
	data, found := extentRepairLimitRater.Load(disk)
	if !found {
		var val int32
		log.LogErrorf("New disk[%v] on partition[%v]", disk, p.partitionID)
		extentRepairLimitRater.Store(disk, &val)
		rater = &val
	}
	rater = data.(*int32)

	if atomic.AddInt32(rater, -1) < 0 {
		atomic.AddInt32(rater, 1)
	}
}

func setDoExtentRepair(s *DataNode, value int32) {
	if value <= 0 {
		value = MaxExtentRepairLimit
	}

	if value > MaxExtentRepairLimit {
		value = MaxExtentRepairLimit
	}

	if value < MinExtentRepairLimit {
		value = MinExtentRepairLimit
	}

	atomic.StoreInt32(&CurExtentRepairLimit, value)
}

func DeleteLimiterWait() {
	ctx := context.Background()
	deleteLimiteRater.Wait(ctx)
}

func setLimiter(limiter *rate.Limiter, limitValue uint64) {
	r := limitValue
	l := rate.Limit(r)
	if r == 0 {
		l = rate.Inf
	}
	limiter.SetLimit(l)
}
