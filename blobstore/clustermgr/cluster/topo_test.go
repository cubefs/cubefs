package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
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
