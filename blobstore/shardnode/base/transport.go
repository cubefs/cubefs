package base

import (
	"context"
	"strconv"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func NewTransport(cmClient *clustermgr.Client, myself *clustermgr.ShardNodeInfo) *Transport {
	return &Transport{
		cmClient: cmClient,
		myself:   myself,
	}
}

type Transport struct {
	myself   *clustermgr.ShardNodeInfo
	allNodes sync.Map
	allDisks sync.Map
	cmClient *clustermgr.Client

	singleRun singleflight.Group
}

func (t *Transport) GetNode(ctx context.Context, nodeID proto.NodeID) (*clustermgr.ShardNodeInfo, error) {
	v, ok := t.allNodes.Load(nodeID)
	if ok {
		return v.(*clustermgr.ShardNodeInfo), nil
	}

	v, err, _ := t.singleRun.Do(strconv.Itoa(int(nodeID)), func() (interface{}, error) {
		nodeInfo, err := t.cmClient.NodeInfo(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		t.allNodes.Store(nodeID, nodeInfo)
		return nodeInfo, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*clustermgr.ShardNodeInfo), err
}

func (t *Transport) GetDisk(ctx context.Context, diskID proto.DiskID) (*clustermgr.ShardNodeDiskInfo, error) {
	v, ok := t.allDisks.Load(diskID)
	if ok {
		return v.(*clustermgr.ShardNodeDiskInfo), nil
	}

	v, err, _ := t.singleRun.Do(strconv.Itoa(int(diskID)), func() (interface{}, error) {
		diskInfo, err := t.cmClient.DiskInfo(ctx, diskID)
		if err != nil {
			return nil, err
		}
		t.allNodes.Store(diskID, diskInfo)
		return diskInfo, nil
	})
	if err != nil {
		return nil, err
	}

	return v.(*clustermgr.ShardNodeDiskInfo), err
}

func (t *Transport) AllocDiskID(ctx context.Context) (proto.DiskID, error) {
	return t.cmClient.AllocDiskID(ctx)
}

func (t *Transport) RegisterDisk(ctx context.Context, disk *clustermgr.ShardNodeDiskInfo) error {
	return nil
	// return t.cmClient.AddDisk(ctx, Disk)
}

func (t *Transport) SetDiskBroken(ctx context.Context, diskID proto.DiskID) error {
	return t.cmClient.SetDisk(ctx, diskID, proto.DiskStatusBroken)
}

func (t *Transport) Register(ctx context.Context) error {
	return nil
	/*resp, err := t.cmClient.AddNode(ctx, t.myself)
	if err != nil {
		return err
	}

	t.myself.NodeID = resp.NodeID
	return nil*/
}

func (t *Transport) GetMyself() *clustermgr.ShardNodeInfo {
	node := *t.myself
	return &node
}

func (t *Transport) GetSpace(ctx context.Context, sid proto.SpaceID) (*clustermgr.Space, error) {
	// todo: add singleflight group to avoid too much get space request go through to master
	/*resp, err := t.cmClient.GetSpace(ctx, &proto.GetSpaceRequest{
		SpaceID: sid,
	})
	if err != nil {
		return proto.SpaceMeta{}, err
	}

	return resp.Info, nil*/
	return nil, nil
}

func (t *Transport) GetAllSpaces(ctx context.Context) ([]clustermgr.Space, error) {
	return nil, nil
}

/*func (t *Transport) GetRouteUpdate(ctx context.Context, routeVersion uint64) (uint64, []proto.CatalogChangeItem, error) {
	resp, err := t.cmClient.GetCatalogChanges(ctx, &proto.GetCatalogChangesRequest{RouteVersion: routeVersion, NodeID: t.myself.ID})
	if err != nil {
		return 0, nil, err
	}

	return resp.RouteVersion, resp.Items, nil
}*/

func (t *Transport) ShardReport(ctx context.Context, reports []clustermgr.ShardReport) ([]clustermgr.ShardTask, error) {
	/*resp, err := t.cmClient.Report(ctx, &proto.ReportRequest{
		NodeID: t.myself.ID,
		Infos:  infos,
	})
	if err != nil {
		return nil, err
	}

	return resp.Tasks, err*/
	return nil, nil
}

func (t *Transport) ListDisks(ctx context.Context) ([]clustermgr.ShardNodeDiskInfo, error) {
	// todo: change api to shard node api
	/*resp, err := t.cmClient.ListDisk(ctx, &clustermgr.ListOptionArgs{
		Host:  t.myself.Host,
		Count: 10000,
	})
	if err != nil {
		return nil, err
	}

	return resp.Disks, nil*/
	return nil, nil
}

func (t *Transport) HeartbeatDisks(ctx context.Context, disks []clustermgr.ShardNodeDiskHeartbeatInfo) error {
	//_, err := t.cmClient.HeartbeatDisk(ctx, disks)
	//return err
	return nil
}

func (t *Transport) NodeID() proto.NodeID {
	return t.myself.NodeID
}
