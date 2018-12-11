package leveldb

import (
	"testing"

	"github.com/tiglabs/containerfs/third_party/goleveldb/leveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
