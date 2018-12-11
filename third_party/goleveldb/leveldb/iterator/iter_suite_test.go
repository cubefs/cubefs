package iterator_test

import (
	"testing"

	"github.com/tiglabs/containerfs/third_party/goleveldb/leveldb/testutil"
)

func TestIterator(t *testing.T) {
	testutil.RunSuite(t, "Iterator Suite")
}
