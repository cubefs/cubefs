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
	CurExtentRepairLimit    = MaxExtentRepairLimit
	extentRepairLimitRater chan struct{}
)

func initRepairLimit() {
	extentRepairLimitRater = make(chan struct{}, MaxExtentRepairLimit)
	for i := 0; i < MaxExtentRepairLimit; i++ {
		extentRepairLimitRater <- struct{}{}
	}
}

func requestDoExtentRepair() (err error) {
	err = fmt.Errorf("repair limit, cannot do extentRepair")

	select {
	case <-extentRepairLimitRater:
		return nil
	default:
		return
	}

	return
}

func fininshDoExtentRepair() {
	select {
	case extentRepairLimitRater <- struct{}{}:
		return
	default:
		return
	}
}

func setDoExtentRepair(value int) {
	if value <= 0 {
		value = MaxExtentRepairLimit
	}

	if value > MaxExtentRepairLimit {
		value = MaxExtentRepairLimit
	}

	if value < MinExtentRepairLimit {
		value = MinExtentRepairLimit
	}

	if CurExtentRepairLimit != value {
		CurExtentRepairLimit = value
		close(extentRepairLimitRater)
		extentRepairLimitRater = make(chan struct{}, CurExtentRepairLimit)
		for i := 0; i < CurExtentRepairLimit; i++ {
			extentRepairLimitRater <- struct{}{}
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
