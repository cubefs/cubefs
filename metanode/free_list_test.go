package metanode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreeList(t *testing.T) {
	fl := newFreeList()

	require.Equal(t, 0, fl.Len())

	// Test Push
	fl.Push(1)
	require.Equal(t, 1, fl.Len())

	fl.Push(2)
	require.Equal(t, 2, fl.Len())

	// Test Pop
	require.Equal(t, uint64(1), fl.Pop())
	require.Equal(t, 1, fl.Len())

	require.Equal(t, uint64(2), fl.Pop())
	require.Equal(t, 0, fl.Len())

	// Test Remove
	fl.Push(3)
	fl.Push(4)
	require.Equal(t, 2, fl.Len())

	fl.Remove(3)
	require.Equal(t, 1, fl.Len())

	require.Equal(t, uint64(4), fl.Pop())
	require.Equal(t, 0, fl.Len())
}
