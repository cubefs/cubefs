package tpmonitor

import (
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestTpMonitor(t *testing.T) {
	tp := NewTpMonitor()
	for i := 0; i < 100; i++ {
		tp.Accumulate(i)
	}
	tpResult := tp.TpReset()
	assert.Equal(t, 98, tpResult.Tp99)
	assert.Equal(t, 99, tpResult.Max)
	assert.Equal(t, 99/2, tpResult.Avg)
	assert.Equal(t, 0, tp.TpReset().Tp99)

	for i := 0; i < 1000; i++ {
		tp.Accumulate(i)
	}
	tpResult = tp.TpReset()
	assert.Equal(t, 989, tpResult.Tp99)
	assert.Equal(t, 999, tpResult.Max)
	assert.Equal(t, 999/2, tpResult.Avg)

	for i := 0; i < 990; i++ {
		tp.Accumulate(0)
	}
	for i := 990; i < 1000; i++ {
		tp.Accumulate(120 * 1000)
	}
	tpResult = tp.TpReset()
	assert.Equal(t, 0, tpResult.Tp99)
	assert.Equal(t, 120*1000, tpResult.Max)
	assert.Equal(t, 120*1000*10/1000, tpResult.Avg)

	for i := 0; i < 989; i++ {
		tp.Accumulate(0)
	}
	for i := 989; i < 1000; i++ {
		tp.Accumulate(120 * 1000)
	}
	tpResult = tp.TpReset()
	assert.Equal(t, 60*1000, tpResult.Tp99)
	assert.Equal(t, 120*1000, tpResult.Max)
	assert.Equal(t, 120*1000*11/1000, tpResult.Avg)
}

func TestBinarySearch(t *testing.T) {
	sortedArray := []int{0, 1, 2, 3, 4, 5, 10, 100, 1000, 10000}
	assert.Equal(t, 3, sort.SearchInts(sortedArray, 3))
	assert.Equal(t, 8, sort.SearchInts(sortedArray, 1000))
	assert.Equal(t, 7, sort.SearchInts(sortedArray, 50))
	assert.Equal(t, 1, sort.SearchInts(sortedArray, 1))
	assert.Equal(t, 0, sort.SearchInts(sortedArray, 0))
	assert.Equal(t, 10, sort.SearchInts(sortedArray, 100000))
}
