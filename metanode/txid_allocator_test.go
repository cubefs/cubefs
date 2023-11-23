package metanode

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTxIDAllocator(t *testing.T) {
	alloc := newTxIDAllocator()

	// Test setTransactionID
	alloc.setTransactionID(10)
	require.Equal(t, uint64(10), alloc.getTransactionID())

	// Test allocateTransactionID
	require.Equal(t, uint64(11), alloc.allocateTransactionID())
	require.Equal(t, uint64(11), alloc.getTransactionID())

	// Test Reset
	alloc.Reset()
	require.Equal(t, uint64(0), alloc.getTransactionID())
}
