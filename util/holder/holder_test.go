package holder

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type __extentAction struct {
	ExtentID uint64
	Offset   uint64
	Size     uint64
}

func (a *__extentAction) Overlap(o Action) bool {
	other, is := o.(*__extentAction)
	return is &&
		a.ExtentID == other.ExtentID &&
		a.Offset < other.Offset+other.Size &&
		other.Offset < a.Offset+a.Size
}

func TestActionHolder_Wait_WithoutAnyOverlap(t *testing.T) {
	var holder = NewActionHolder()
	holder.Register(100, &__extentAction{
		ExtentID: 10,
		Offset:   1024,
		Size:     1024,
	})
	holder.Register(101, &__extentAction{
		ExtentID: 11,
		Offset:   0,
		Size:     512,
	})
	var ctx, _ = context.WithTimeout(context.Background(), time.Second)
	assert.Nil(t, holder.Wait(ctx, &__extentAction{
		ExtentID: 10,
		Offset:   0,
		Size:     512,
	}))
}

func TestActionHolder_Wait_WithOverlap(t *testing.T) {
	var holder = NewActionHolder()
	holder.Register(100, &__extentAction{
		ExtentID: 10,
		Offset:   1024,
		Size:     1024,
	})
	holder.Register(101, &__extentAction{
		ExtentID: 11,
		Offset:   0,
		Size:     512,
	})
	var ctx, _ = context.WithTimeout(context.Background(), time.Second)
	assert.NotNil(t, holder.Wait(ctx, &__extentAction{
		ExtentID: 10,
		Offset:   512,
		Size:     1024,
	}))
}

func TestActionHolder_Wait_WithOverlapThenUnregister(t *testing.T) {
	var holder = NewActionHolder()
	holder.Register(100, &__extentAction{
		ExtentID: 10,
		Offset:   1024,
		Size:     1024,
	})
	holder.Register(101, &__extentAction{
		ExtentID: 11,
		Offset:   0,
		Size:     512,
	})
	go func() {
		time.Sleep(time.Second)
		holder.Unregister(100)
	}()
	var ctx, _ = context.WithTimeout(context.Background(), time.Second*5)
	assert.Nil(t, holder.Wait(ctx, &__extentAction{
		ExtentID: 10,
		Offset:   512,
		Size:     1024,
	}))
}

func TestActionHolder_Register_Twice(t *testing.T) {
	var holder = NewActionHolder()
	holder.Register(100, &__extentAction{
		ExtentID: 10,
		Offset:   1024,
		Size:     1024,
	})
	holder.Register(101, &__extentAction{
		ExtentID: 11,
		Offset:   0,
		Size:     512,
	})
	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), time.Second)
	assert.NotNil(t, holder.Wait(ctx, &__extentAction{
		ExtentID: 11,
		Offset:   128,
		Size:     1024,
	}))
	go func() {
		time.Sleep(time.Second)
		holder.Register(101, &__extentAction{
			ExtentID: 12,
			Offset:   0,
			Size:     512,
		})
	}()
	ctx, _ = context.WithTimeout(context.Background(), time.Second*5)
	assert.Nil(t, holder.Wait(ctx, &__extentAction{
		ExtentID: 11,
		Offset:   128,
		Size:     1024,
	}))
}
