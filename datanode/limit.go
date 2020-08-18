package datanode

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
)

var (
	deleteLimiteRater       = rate.NewLimiter(rate.Inf, defaultMarkDeleteLimitBurst)
	MaxExtentRepairLimit    = 20000
	MinExtentRepairLimit    = 5
	extentRepairLimiteRater = make(chan struct{}, MaxExtentRepairLimit)
)

func requestDoExtentRepair() (err error) {
	err = fmt.Errorf("cannot do extentRepair")
	select {
	case <-extentRepairLimiteRater:
		return nil
	default:
		return
	}

	return
}

func fininshDoExtentRepair() {
	select {
	case extentRepairLimiteRater <- struct{}{}:
		return
	default:
		return
	}
}

func setDoExtentRepair(value int) {
	close(extentRepairLimiteRater)
	if value > MaxExtentRepairLimit {
		value = MaxExtentRepairLimit
	}
	if value < MinExtentRepairLimit {
		value = MinExtentRepairLimit
	}
	extentRepairLimiteRater = make(chan struct{}, value)
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
