package data_check

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMerge(t *testing.T) {
	arrs := [][]uint64{
		{0, 3},
		{2, 4},
		{6, 9},
		{9, 10},
	}
	//expect: {0, 4}, {6, 10}
	arrsNew := merge(arrs)
	if !assert.Equal(t, len(arrsNew), 2) {
		return
	}
	if !assert.True(t, isHole(2, 4, arrsNew)) {
		return
	}
	if !assert.False(t, isHole(5, 6, arrsNew)) {
		return
	}
	if !assert.False(t, isHole(5, 5, arrsNew)) {
		return
	}
}
