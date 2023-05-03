package datanode

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestInitRepairLimit(t *testing.T) {
	initRepairLimit()
	assert.Equal(t, MaxExtentRepairLimit, cap(extentRepairLimitRater))
}

func TestRequestDoExtentRepair(t *testing.T) {
	initRepairLimit()

	go func() {
		<-extentRepairLimitRater
	}()

	err := requestDoExtentRepair()
	require.NoError(t, err)

	for {
		select {
		case <-extentRepairLimitRater:
		default:
			return
		}
	}

	close(extentRepairLimitRater)

	err = requestDoExtentRepair()
	require.NoError(t, err)
}

func TestFininshDoExtentRepair(t *testing.T) {
	initRepairLimit()
	setDoExtentRepair(2)

	err := requestDoExtentRepair()
	require.NoError(t, err)

	for {
		select {
		case <-extentRepairLimitRater:
		default:
			return
		}
	}

	fininshDoExtentRepair()

	select {
	case <-extentRepairLimitRater:
		t.Fatalf("extentRepairLimitRater should be empty")
	default:
	}
}

func TestSetDoExtentRepair(t *testing.T) {
	initRepairLimit()

	setDoExtentRepair(15)
	assert.Equal(t, 15, CurExtentRepairLimit)

	setDoExtentRepair(-10)
	assert.Equal(t, MaxExtentRepairLimit, CurExtentRepairLimit)

	setDoExtentRepair(0)
	assert.Equal(t, MaxExtentRepairLimit, CurExtentRepairLimit)

	setDoExtentRepair(25000)
	assert.Equal(t, MaxExtentRepairLimit, CurExtentRepairLimit)

	setDoExtentRepair(1)
	assert.Equal(t, MinExtentRepairLimit, CurExtentRepairLimit)
}

func TestDeleteLimiterWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		DeleteLimiterWait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("DeleteLimiterWait took too long")
	}
}

func TestSetLimiter(t *testing.T) {
	limiter := rate.NewLimiter(1, 1)
	setLimiter(limiter, 10)
	assert.Equal(t, rate.Limit(10), limiter.Limit())
	setLimiter(limiter, 0)
	assert.Equal(t, rate.Inf, limiter.Limit())
}
