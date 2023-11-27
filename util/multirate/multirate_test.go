package multirate

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestBaseRate(t *testing.T) {
	limit := rate.Limit(10)
	burst := 10
	vol := "vol1"
	ml := NewMultiLimiter()
	r1 := NewRule(Properties{{PropertyTypeVol, ""}}, LimitGroup{statTypeCount: limit}, BurstGroup{statTypeCount: burst})
	ml.AddRule(r1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	var err error
	err = ml.WaitN(ctx, Properties{{PropertyTypeVol, vol}}, Stat{Count: 9})
	assert.Nil(t, err)
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, vol}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, vol}})
	assert.NotNil(t, err)
	time.Sleep(100 * time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, vol}})
	assert.Nil(t, err)
}

func TestCancelRate(t *testing.T) {
	ml := NewMultiLimiter()
	ml.AddRule(NewRule(Properties{{PropertyTypeVol, ""}}, LimitGroup{statTypeCount: 1}, BurstGroup{statTypeCount: 1}))
	ctx, cancel := context.WithCancel(context.Background())

	err := ml.Wait(ctx, Properties{{PropertyTypeVol, "vol1"}})
	assert.Nil(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err = ml.WaitUseDefaultTimeout(ctx, Properties{{PropertyTypeVol, "vol1"}})
		assert.Contains(t, err.Error(), "canceled")
		wg.Done()
	}()
	cancel()
	wg.Wait()
}

func TestMultiRate(t *testing.T) {
	restrictedVol := "restrictedVol"
	ml := NewMultiLimiter()
	r1 := NewRule(Properties{{PropertyTypeVol, ""}}, LimitGroup{statTypeCount: 10}, BurstGroup{statTypeCount: 10})
	r2 := NewRule(Properties{{PropertyTypeVol, restrictedVol}}, LimitGroup{statTypeCount: 1}, BurstGroup{statTypeCount: 1})
	r3 := NewRule(Properties{{PropertyTypeOp, ""}}, LimitGroup{statTypeCount: 10}, BurstGroup{statTypeCount: 10})
	ml.AddRule(r1).AddRule(r2).AddRule(r3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	var (
		err error
		ok  bool
	)
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, restrictedVol}})
	assert.Nil(t, err)
	ok = ml.Allow(Properties{{PropertyTypeVol, restrictedVol}})
	assert.False(t, ok)

	err = ml.WaitN(ctx, Properties{{PropertyTypeVol, "vol1"}, {PropertyTypeOp, "read"}}, Stat{Count: 9})
	assert.Nil(t, err)
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, "vol1"}, {PropertyTypeOp, "read"}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, "vol2"}, {PropertyTypeOp, "write"}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, "vol1"}})
	assert.NotNil(t, err)
	err = ml.Wait(ctx, Properties{{PropertyTypeOp, "read"}})
	assert.NotNil(t, err)
	time.Sleep(100 * time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, "vol1"}, {PropertyTypeOp, "read"}})
	assert.Nil(t, err)
}

func TestCompoundRate(t *testing.T) {
	vol := "vol1"
	ml := NewMultiLimiter()
	// rate 10 ops/s for every vol
	r1 := NewRule(Properties{{PropertyTypeVol, ""}}, LimitGroup{statTypeCount: 10}, BurstGroup{statTypeCount: 10})
	// rate 5 ops/s for every op of every vol
	r2 := NewRule(Properties{{PropertyTypeVol, ""}, {PropertyTypeOp, ""}}, LimitGroup{statTypeCount: 5}, BurstGroup{statTypeCount: 5})
	// rate 2 ops/s for read of every vol
	r3 := NewRule(Properties{{PropertyTypeVol, ""}, {PropertyTypeOp, "read"}}, LimitGroup{statTypeCount: 2}, BurstGroup{statTypeCount: 2})
	ml.AddRule(r1).AddRule(r2).AddRule(r3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	var (
		err error
		ok  bool
	)
	err = ml.WaitN(ctx, Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, "write"}}, Stat{Count: 2})
	assert.Nil(t, err)
	err = ml.WaitN(ctx, Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, "read"}}, Stat{Count: 2})
	assert.Nil(t, err)
	ok = ml.Allow(Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, "read"}})
	assert.False(t, ok)
	err = ml.WaitN(ctx, Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, "write"}}, Stat{Count: 3})
	assert.Nil(t, err)
	ok = ml.Allow(Properties{{PropertyTypeVol, vol}, {PropertyTypeOp, "write"}})
	assert.False(t, ok)
	err = ml.Wait(ctx, Properties{{PropertyTypeVol, vol}})
	assert.Nil(t, err)
	// the previous two failed events may have not executed r1
	err = ml.WaitN(ctx, Properties{{PropertyTypeVol, vol}}, Stat{Count: 3})
	assert.NotNil(t, err)
}

func TestBaseRule(t *testing.T) {
	ml := NewMultiLimiter()
	p := Properties{{PropertyTypeVol, "vol"}, {PropertyTypeOp, "read"}, {PropertyTypeDisk, ""}}
	r := NewRule(p, LimitGroup{statTypeCount: 1}, BurstGroup{statTypeCount: 1})
	p1 := Properties{{PropertyTypeVol, "vol"}, {PropertyTypeOp, "read"}, {PropertyTypeDisk, "disk1"}}
	r1 := NewRule(p1, LimitGroup{statTypeCount: 2}, BurstGroup{statTypeCount: 2})
	ml.AddRule(r).AddRule(r1)

	p2 := Properties{{PropertyTypeVol, "vol"}, {PropertyTypeOp, "read"}, {PropertyTypeDisk, "disk2"}}
	limiters := ml.getLimiters(p1)
	assert.True(t, len(limiters) == 1 && limiters[0][statTypeCount].limiter.Limit() == rate.Limit(2))
	limiters = ml.getLimiters(p2)
	assert.True(t, len(limiters) == 1 && limiters[0][statTypeCount].limiter.Limit() == rate.Limit(1))

	ml.ClearRule(p)
	_, ok := ml.limiters.Load(p1.name())
	assert.False(t, ok)
	_, ok = ml.limiters.Load(p2.name())
	assert.False(t, ok)
	ml.ClearRule(p1)
	limiters = ml.getLimiters(p1)
	assert.True(t, len(limiters) == 0)
}

func TestComplexRule1(t *testing.T) {
	ml := NewMultiLimiter()
	ml.AddRule(NewRule(Properties{{PropertyTypeVol, ""}, {PropertyTypeOp, "read"}}, LimitGroup{statTypeCount: 1}, BurstGroup{statTypeCount: 1}))
	ml.AddRule(NewRule(Properties{{PropertyTypeVol, ""}, {PropertyTypeOp, "write"}}, LimitGroup{statTypeCount: 2}, BurstGroup{statTypeCount: 2}))
	ml.AddRule(NewRule(Properties{{PropertyTypeVol, "vola"}, {PropertyTypeOp, ""}}, LimitGroup{statTypeInBytes: 3}, BurstGroup{statTypeInBytes: 3}))
	limiters := ml.getLimiters(Properties{{PropertyTypeVol, "vola"}, {PropertyTypeOp, "read"}})
	assert.True(t, len(limiters) == 1 && limiters[0][statTypeCount].limiter.Limit() == rate.Limit(1) && limiters[0][statTypeInBytes].limiter.Limit() == rate.Limit(3))
}

func TestComplexRule2(t *testing.T) {
	property := []Properties{
		{{PropertyTypeVol, ""}, {PropertyTypeOp, ""}},
		{{PropertyTypeVol, "vola"}, {PropertyTypeOp, ""}},
		{{PropertyTypeVol, ""}, {PropertyTypeOp, "read"}},
		{{PropertyTypeVol, "volb"}, {PropertyTypeOp, "write"}},
	}
	limit := []rate.Limit{4, 3, 2, 1}
	ml := NewMultiLimiter()
	for i := range property {
		rule := NewRule(property[i], LimitGroup{statTypeCount: limit[i]}, BurstGroup{statTypeCount: int(limit[i])})
		ml.AddRule(rule)
	}

	eventProperty := []Properties{
		{{PropertyTypeVol, "vola"}, {PropertyTypeOp, "read"}},
		{{PropertyTypeVol, "vola"}, {PropertyTypeOp, "write"}},
		{{PropertyTypeVol, "volb"}, {PropertyTypeOp, "read"}},
		{{PropertyTypeVol, "volb"}, {PropertyTypeOp, "write"}},
		{{PropertyTypeVol, "volc"}, {PropertyTypeOp, "write"}},
	}
	expectLimit := []rate.Limit{
		rate.Limit(math.Min(math.Min(float64(limit[0]), float64(limit[1])), float64(limit[2]))),
		rate.Limit(math.Min(float64(limit[0]), float64(limit[1]))),
		rate.Limit(math.Min(float64(limit[0]), float64(limit[2]))),
		limit[3],
		limit[0],
	}
	checkEvent(t, ml, eventProperty, expectLimit)

	ml.ClearRule(property[1])
	expectLimit = []rate.Limit{
		rate.Limit(math.Min(float64(limit[0]), float64(limit[2]))),
		limit[0],
		rate.Limit(math.Min(float64(limit[0]), float64(limit[2]))),
		limit[3],
		limit[0],
	}
	checkEvent(t, ml, eventProperty, expectLimit)

	ml.ClearRule(property[2]).ClearRule(property[3])
	expectLimit = []rate.Limit{limit[0], limit[0], limit[0], limit[0], limit[0]}
	checkEvent(t, ml, eventProperty, expectLimit)

	ml.ClearRule(property[0])
	expectLimit = []rate.Limit{0, 0, 0, 0, 0}
	checkEvent(t, ml, eventProperty, expectLimit)
}

func checkEvent(t *testing.T, ml *MultiLimiter, property []Properties, expectLimit []rate.Limit) {
	for i := range property {
		limiters := ml.getLimiters(property[i])
		if expectLimit[i] > 0 {
			assert.True(t, len(limiters) == 1 && limiters[0][statTypeCount].limiter.Limit() == expectLimit[i])
		} else {
			assert.True(t, len(limiters) == 0)
		}
	}
}

func BenchmarkWait(b *testing.B) {
	ml := NewMultiLimiter()
	ml.AddRule(NewRule(Properties{{PropertyTypeVol, ""}, {PropertyTypeOp, "read"}, {PropertyTypeDisk, ""}}, LimitGroup{statTypeCount: 1000 * 1000}, BurstGroup{statTypeCount: 1000 * 1000}))
	ml.AddRule(NewRule(Properties{{PropertyTypeVol, "vol1"}, {PropertyTypeOp, "read"}, {PropertyTypeDisk, ""}}, LimitGroup{statTypeCount: 900 * 1000}, BurstGroup{statTypeCount: 900 * 1000}))
	ml.AddRule(NewRule(Properties{{PropertyTypeVol, "vol2"}, {PropertyTypeOp, "write"}, {PropertyTypeDisk, ""}}, LimitGroup{statTypeCount: 800 * 1000}, BurstGroup{statTypeCount: 800 * 1000}))
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		ml.Wait(ctx, Properties{{PropertyTypeVol, "vol1"}, {PropertyTypeOp, "read"}, {PropertyTypeDisk, "disk1"}})
	}
}
