package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	apierrors "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func TestTopoMgr_AllocSetID(t *testing.T) {
	testTopoMgr := newTopoMgr()
	ni, di := new(nodeItem), new(diskItem)
	startID, endID := 2, 10
	for i := startID; i < endID+1; i++ {
		ni = &nodeItem{
			nodeID: proto.NodeID(i),
			info: nodeItemInfo{
				NodeInfo: clustermgr.NodeInfo{
					NodeID:    proto.NodeID(i),
					Role:      proto.NodeRoleBlobNode,
					DiskType:  proto.DiskTypeHDD,
					NodeSetID: proto.NodeSetID(i),
					Status:    proto.NodeStatusNormal,
					Idc:       "z0",
					Rack:      "rack0",
				},
			},
		}
		testTopoMgr.AddNodeToNodeSet(ni)
		di = &diskItem{
			diskID: proto.DiskID(i),
			info: diskItemInfo{
				DiskInfo: clustermgr.DiskInfo{
					NodeID:    proto.NodeID(startID),
					DiskSetID: proto.DiskSetID(i),
					Status:    proto.DiskStatusNormal,
				},
				extraInfo: &clustermgr.DiskHeartBeatInfo{
					DiskID: proto.DiskID(i),
				},
			},
		}
		testTopoMgr.AddDiskToDiskSet(ni.info.DiskType, proto.NodeSetID(startID), di)
	}
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	ni.info.NodeID = proto.NodeID(startID)
	ni.info.NodeSetID = proto.NodeSetID(startID)
	heartbeatInfo := di.info.extraInfo.(*clustermgr.DiskHeartBeatInfo)
	heartbeatInfo.DiskID = proto.DiskID(100)
	conf := CopySetConfig{
		NodeSetCap:                6,
		NodeSetIdcCap:             2,
		NodeSetRackCap:            3,
		DiskSetCap:                6,
		DiskCountPerNodeInDiskSet: 3,
	}
	for i := 0; i < 99; i++ {
		nodeSetID := testTopoMgr.AllocNodeSetID(ctx, &ni.info.NodeInfo, conf, false)
		diskSetID := testTopoMgr.AllocDiskSetID(ctx, &di.info.DiskInfo, &ni.info.NodeInfo, conf)
		require.Equal(t, proto.NodeSetID(startID), nodeSetID)
		require.Equal(t, proto.DiskSetID(startID), diskSetID)
	}
}

// TestTopoMgr_ValidateNodeSetID covers all error branches of ValidateNodeSetID
// and the happy path.
func TestTopoMgr_ValidateNodeSetID(t *testing.T) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	tm := newTopoMgr()

	// register a nodeSet via AllocNodeSetID so curNodeSetID moves forward
	ni := &nodeItem{
		nodeID: proto.NodeID(1),
		info: nodeItemInfo{
			NodeInfo: clustermgr.NodeInfo{
				NodeID:   proto.NodeID(1),
				Role:     proto.NodeRoleBlobNode,
				DiskType: proto.DiskTypeHDD,
				Status:   proto.NodeStatusNormal,
				Idc:      "z0",
				Rack:     "rack0",
			},
		},
	}
	// bump curNodeSetID past the reserved ecNodeSetID, allocate a new id and
	// register the node so the nodeSet actually exists in allNodeSets
	tm.SetNodeSetID(ecNodeSetID)
	conf := CopySetConfig{NodeSetCap: 6, NodeSetIdcCap: 2, NodeSetRackCap: 3, DiskSetCap: 6, DiskCountPerNodeInDiskSet: 3}
	nodeSetID := tm.AllocNodeSetID(ctx, &ni.info.NodeInfo, conf, false)
	require.NotEqual(t, ecNodeSetID, nodeSetID)
	ni.info.NodeSetID = nodeSetID
	tm.AddNodeToNodeSet(ni)

	// happy path
	require.NoError(t, tm.ValidateNodeSetID(ctx, proto.DiskTypeHDD, nodeSetID))

	// node set id larger than curNodeSetID => ErrIllegalArguments
	err := tm.ValidateNodeSetID(ctx, proto.DiskTypeHDD, nodeSetID+100)
	require.ErrorIs(t, err, apierrors.ErrIllegalArguments)

	// ecNodeSetID is reserved and should be rejected
	err = tm.ValidateNodeSetID(ctx, proto.DiskTypeHDD, ecNodeSetID)
	require.ErrorIs(t, err, apierrors.ErrIllegalArguments)

	// disk type not exist (SSD was never registered) => ErrCMNodeSetNotFound
	err = tm.ValidateNodeSetID(ctx, proto.DiskTypeSSD, nodeSetID)
	require.ErrorIs(t, err, apierrors.ErrCMNodeSetNotFound)

	// disk type exists but node set id not in map => ErrCMNodeSetNotFound
	// construct a valid id that is < curNodeSetID but not registered
	orphanID := nodeSetID - 1
	if orphanID > ecNodeSetID {
		err = tm.ValidateNodeSetID(ctx, proto.DiskTypeHDD, orphanID)
		require.ErrorIs(t, err, apierrors.ErrCMNodeSetNotFound)
	}
}
