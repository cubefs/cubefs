package memdb

import (
	"testing"

	"github.com/tiglabs/containerfs/third_party/goleveldb/leveldb/testutil"
)

func TestMemDB(t *testing.T) {
	testutil.RunSuite(t, "MemDB Suite")
}
