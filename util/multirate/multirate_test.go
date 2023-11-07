package multirate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBaseRate(t *testing.T) {
	limit := 10
	burst := 10
	vol := "vol1"
	ml := NewMultiLimiter()
	r1 := NewRule(PropertyTypeVol, "", limit, burst)
	ml.AddRule(r1)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()

	var err error
	err = ml.WaitN(ctx, 9, []Property{{PropertyTypeVol, vol}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, vol}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, vol}})
	assert.NotNil(t, err)
	time.Sleep(100 * time.Millisecond)
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, vol}})
	assert.Nil(t, err)
}

func TestMultiRate(t *testing.T) {
	restrictedVol := "restrictedVol"
	ml := NewMultiLimiter()
	r1 := NewRule(PropertyTypeVol, "", 10, 10)
	r2 := NewRule(PropertyTypeVol, restrictedVol, 1, 1)
	r3 := NewRule(PropertyTypeOp, "", 10, 10)
	ml.AddRule(r1).AddRule(r2).AddRule(r3)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()

	var err error
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, restrictedVol}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, restrictedVol}})
	assert.NotNil(t, err)

	err = ml.WaitN(ctx, 9, []Property{{PropertyTypeVol, "vol1"}, {PropertyTypeOp, "read"}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, "vol1"}, {PropertyTypeOp, "read"}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, "vol2"}, {PropertyTypeOp, "write"}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, "vol1"}})
	assert.NotNil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeOp, "read"}})
	assert.NotNil(t, err)
	time.Sleep(100 * time.Millisecond)
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, "vol1"}, {PropertyTypeOp, "read"}})
	assert.Nil(t, err)
}

func TestCompounddRate(t *testing.T) {
	vol := "vol1"
	ml := NewMultiLimiter()
	// rate 10 ops/s for every vol
	r1 := NewRule(PropertyTypeVol, "", 10, 10)
	// rate 5 ops/s for every op of every vol
	r2 := NewMultiPropertyRule([]Property{{PropertyTypeVol, ""}, {PropertyTypeOp, ""}}, 5, 5)
	// rate 3 ops/s for read of every vol
	r3 := NewMultiPropertyRule([]Property{{PropertyTypeVol, ""}, {PropertyTypeOp, "read"}}, 2, 2)
	ml.AddRule(r1).AddRule(r2).AddRule(r3)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()

	var err error
	err = ml.WaitN(ctx, 2, []Property{{PropertyTypeVol, vol}, {PropertyTypeOp, "write"}})
	assert.Nil(t, err)
	err = ml.WaitN(ctx, 2, []Property{{PropertyTypeVol, vol}, {PropertyTypeOp, "read"}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, vol}, {PropertyTypeOp, "read"}})
	assert.NotNil(t, err)
	err = ml.WaitN(ctx, 3, []Property{{PropertyTypeVol, vol}, {PropertyTypeOp, "write"}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, vol}, {PropertyTypeOp, "write"}})
	assert.NotNil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, vol}})
	assert.Nil(t, err)
	err = ml.Wait(ctx, []Property{{PropertyTypeVol, vol}})
	assert.NotNil(t, err)
}

func TestRule(t *testing.T) {
	ml := NewMultiLimiter()
	p := Properties{{PropertyTypeVol, ""}}
	r := NewMultiPropertyRule(p, 1, 1)
	p1 := Properties{{PropertyTypeVol, "vol1"}}
	r1 := NewMultiPropertyRule(p1, 1, 1)
	ml.AddRule(r).AddRule(r1)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()

	var (
		ok  bool
		err error
	)
	p2 := Properties{{PropertyTypeVol, "vol2"}}
	_, ok = ml.limiters.Load(p1.RuleName())
	assert.True(t, ok)
	_, ok = ml.limiters.Load(p2.RuleName())
	assert.False(t, ok)
	err = ml.Wait(ctx, p2)
	assert.Nil(t, err)
	_, ok = ml.limiters.Load(p2.RuleName())
	assert.True(t, ok)

	ml.ClearRule(p)
	_, ok = ml.limiters.Load(p1.RuleName())
	assert.True(t, ok)
	_, ok = ml.limiters.Load(p2.RuleName())
	assert.False(t, ok)
	ml.ClearRule(p1)
	_, ok = ml.limiters.Load(p1.RuleName())
	assert.False(t, ok)
}

func BenchmarkWait(b *testing.B) {
	ml := NewMultiLimiter()
	r := NewRule(PropertyTypeVol, "", 1000*1000, 1000*1000)
	ml.AddRule(r)
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		ml.Wait(ctx, []Property{{PropertyTypeVol, "vol1"}})
	}
}
