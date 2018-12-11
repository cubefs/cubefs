package table

import (
	"testing"

	"github.com/tiglabs/containerfs/third_party/goleveldb/leveldb/testutil"
)

func TestTable(t *testing.T) {
	testutil.RunSuite(t, "Table Suite")
}
