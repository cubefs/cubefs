package datanode

import (
	"context"
	"golang.org/x/time/rate"
)

var (
	deleteLimiteRater     = rate.NewLimiter(rate.Inf, defaultMarkDeleteLimitBurst)
	autoRepairLimiteRater = rate.NewLimiter(rate.Inf, 512)
)

func AutoRepairLimiterWait() (err error) {
	ctx := context.Background()
	autoRepairLimiteRater.Wait(ctx)
	return
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
