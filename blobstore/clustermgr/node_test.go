package clustermgr

import (
	"testing"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/stretchr/testify/require"
)

func TestAllocNodeID(t *testing.T) {
	testService, clean := initTestService(t)
	defer clean()
	testClusterClient := initTestClusterClient(testService)
	ctx := newCtx()

	// test node id alloc
	{
		for i := 1; i <= 10; i++ {
			ret, err := testClusterClient.AllocNodeID(ctx)
			require.NoError(t, err)
			require.Equal(t, proto.NodeID(i), ret)
		}
	}
}
