package master

import (
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestMetaNode(t *testing.T) {
	// /metaNode/add and /metaNode/response processed by mock meta server
	addr := mms7Addr
	func() {
		mockServerLock.Lock()
		defer mockServerLock.Unlock()
		mockMetaServers = append(mockMetaServers, addMetaServer(addr, testZone3))
	}()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getMetaNodeInfo(addr, t)
	decommissionMetaNode(addr, t)
}

func getMetaNodeInfo(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.GetMetaNode, addr)
	process(reqURL, t)
}

func decommissionMetaNode(addr string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?addr=%v", hostAddr, proto.DecommissionMetaNode, addr)
	process(reqURL, t)
}

func TestMetaNodePartitionCntLimitedEx(t *testing.T) {
	const limit = uint64(100)

	t.Run("online under limit", func(t *testing.T) {
		mn := &MetaNode{
			MetaPartitionCount: 10,
			MpCntLimit:         limit,
		}
		require.True(t, mn.PartitionCntLimitedEx(1))
		require.True(t, mn.PartitionCntLimited())
	})

	t.Run("online over limit", func(t *testing.T) {
		mn := &MetaNode{
			MetaPartitionCount: 101,
			MpCntLimit:         limit,
		}
		require.False(t, mn.PartitionCntLimitedEx(1))
	})

	t.Run("ToBeOffline under limit", func(t *testing.T) {
		mn := &MetaNode{
			MetaPartitionCount: 10,
			MpCntLimit:         limit,
			ToBeOffline:        true,
		}
		require.False(t, mn.PartitionCntLimitedEx(1))
		require.True(t, mn.PartitionCntLimited())
	})
}
