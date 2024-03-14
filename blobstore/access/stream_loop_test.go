package access

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

type mockVolumeMgr struct{}

func (m *mockVolumeMgr) Alloc(ctx context.Context, args *cmapi.AllocVolsArgs) (allocVols []cmapi.AllocRet, err error) {
	return nil, nil
}

func (m *mockVolumeMgr) List(ctx context.Context, codeMode codemode.CodeMode) (vids []proto.Vid, volumes []cmapi.AllocVolumeInfo, err error) {
	return nil, nil, nil
}

func (m *mockVolumeMgr) Release(ctx context.Context, args *cmapi.ReleaseVolumes) error {
	return nil
}

func (m *mockVolumeMgr) Discard(ctx context.Context, args *cmapi.DiscardVolsArgs) error {
	return nil
}

func (m *mockVolumeMgr) Close() {
}

func TestAccessReleaseClusterVids(t *testing.T) {
	stopCh := make(chan struct{})
	ctr := gomock.NewController(&testing.T{})
	cid := proto.ClusterID(9)
	cluster := &cmapi.ClusterInfo{
		Region:    "test-region",
		ClusterID: cid,
		Nodes:     []string{"node-1", "node-2", "node-3"},
	}
	cc := NewMockClusterController(ctr)
	cc.EXPECT().All().Times(2).Return([]*cmapi.ClusterInfo{cluster})
	cc.EXPECT().GetVolumeAllocator(gomock.Any()).Times(2).Return(nil, nil)
	streamer := &Handler{
		StreamConfig: StreamConfig{
			ClusterConfig: controller.ClusterConfig{
				VolumeReleaseSecs: 1,
			},
		},
		clusterController: cc,
		stopCh:            stopCh,
	}
	{
		// test loopReleaseVids
		go streamer.loopReleaseVids()
		time.Sleep(time.Second * time.Duration(streamer.ClusterConfig.VolumeReleaseSecs+1))
		close(stopCh)
	}

	{
		// test initReleaseVids
		allocGroup := streamer.initReleaseVids(ctx)
		require.Equal(t, 1, len(allocGroup))
		_, ok := allocGroup[cid]
		require.True(t, ok)
	}

	{
		// test releaseClusterVids
		sealedVid := newVolumeMap()
		sealedVid.addVid(9009)
		streamer.releaseVids.Store(cid, &releaseVids{
			normalVids: newVolumeMap(),
			sealedVids: sealedVid,
		})
		allocMgr := &mockVolumeMgr{}
		allocGroup := make(map[proto.ClusterID]controller.VolumeMgr)
		allocGroup[cid] = allocMgr
		cid2 := proto.ClusterID(22)
		streamer.releaseVids.Store(cid2, &releaseVids{
			normalVids: newVolumeMap(),
			sealedVids: newVolumeMap(),
		})
		allocGroup[cid2] = nil
		allocGroup[1234] = nil

		streamer.releaseClusterVids(ctx, allocGroup)
		v, ok := streamer.releaseVids.Load(cid)
		require.True(t, ok)
		vol := v.(*releaseVids)
		require.Equal(t, 0, vol.normalVids.len())
		require.Equal(t, 0, vol.sealedVids.len())
	}
}
