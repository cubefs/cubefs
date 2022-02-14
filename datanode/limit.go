package datanode

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/config"
	"golang.org/x/time/rate"
)

var (
	deleteLimiteRater      = rate.NewLimiter(rate.Inf, defaultMarkDeleteLimitBurst)
	MaxExtentRepairLimit   = 100
	MinExtentRepairLimit   = 5
	CurExtentRepairLimit   = MaxExtentRepairLimit
	RaterReporterInterval  = 10 * time.Second
	extentRepairLimitRater map[string]chan struct{}
	lock                   sync.Mutex
)

func initRepairLimit(s *DataNode, cfg *config.Config) {
	extentRepairLimitRater = make(map[string]chan struct{})
	for _, d := range cfg.GetSlice(ConfigKeyDisks) {
		// format "PATH:RESET_SIZE"
		disk := strings.Split(d.(string), ":")[0]
		ch := make(chan struct{}, MaxExtentRepairLimit)
		for i := 0; i < MaxExtentRepairLimit; i++ {
			ch <- struct{}{}
		}
		extentRepairLimitRater[disk] = ch
	}

	go func() {
		tick := time.NewTicker(RaterReporterInterval)
		for {
			select {
			case <-tick.C:
				for disk, rater := range extentRepairLimitRater {
					cnt := float64(CurExtentRepairLimit - len(rater))
					lock.Lock()
					s.metrics.Routines.SetWithLabelValues(cnt, disk)
					lock.Unlock()
				}
			}
		}
	}()
}

func requestDoExtentRepair(p *DataPartition) (err error) {
	err = fmt.Errorf("repair limit, cannot do extentRepair")

	disk := p.disk.Path
	select {
	case <-extentRepairLimitRater[disk]:
		return nil
	default:
		return
	}

	return
}

func finishDoExtentRepair(p *DataPartition) {

	disk := p.disk.Path
	select {
	case extentRepairLimitRater[disk] <- struct{}{}:
		return
	default:
		return
	}
}

func setDoExtentRepair(s *DataNode, value int) {
	if value <= 0 {
		value = MaxExtentRepairLimit
	}

	if value > MaxExtentRepairLimit {
		value = MaxExtentRepairLimit
	}

	if value < MinExtentRepairLimit {
		value = MinExtentRepairLimit
	}

	// FIXME: need lock?
	if CurExtentRepairLimit != value {
		CurExtentRepairLimit = value
		for disk, rater := range extentRepairLimitRater {
			close(rater)

			ch := make(chan struct{}, CurExtentRepairLimit)
			for i := 0; i < CurExtentRepairLimit; i++ {
				ch <- struct{}{}
			}
			extentRepairLimitRater[disk] = ch
		}
	}
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
