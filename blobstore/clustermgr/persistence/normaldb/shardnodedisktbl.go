package normaldb

import (
	"encoding/json"
	"strings"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

type ShardNodeInfoRecord struct {
	NodeInfoRecord
	RaftHost string `json:"raft_host"`
}

type ShardNodeDiskInfoRecord struct {
	DiskInfoRecord
	MaxShardCnt  int32 `json:"max_shard_cnt"`
	FreeShardCnt int32 `json:"free_shard_cnt"`
	UsedShardCnt int32 `json:"used_shard_cnt"`
	Size         int64 `json:"size"`
	Used         int64 `json:"used"`
	Free         int64 `json:"free"`
}

func OpenShardNodeDiskTable(db kvstore.KVStore, ensureIndex bool) (*ShardNodeDiskTable, error) {
	if db == nil {
		return nil, errors.New("OpenShardNodeDiskTable failed: db is nil")
	}
	table := &ShardNodeDiskTable{
		nodeDiskTable: &nodeDiskTable{
			nodeTbl:        db.Table(shardNodeCF),
			diskTbl:        db.Table(shardNodeDiskCF),
			droppedDiskTbl: db.Table(shardNodeDiskDropCF),
			indexes: map[string]indexItem{
				diskStatusIndex:  {indexNames: []string{diskStatusIndex}, tbl: db.Table(shardNodeDiskStatusIndexCF)},
				diskHostIndex:    {indexNames: []string{diskHostIndex}, tbl: db.Table(shardNodeDiskHostIndexCF)},
				diskIDCIndex:     {indexNames: []string{diskIDCIndex}, tbl: db.Table(shardNodeDiskIDCIndexCF)},
				diskIDCRACKIndex: {indexNames: strings.Split(diskIDCRACKIndex, "-"), tbl: db.Table(shardNodeDiskIDCRackIndexCF)},
			},
		},
	}
	table.nodeDiskTable.rd = table

	// ensure index
	if ensureIndex {
		list, err := table.GetAllDisks()
		if err != nil {
			return nil, errors.Info(err, "get all disk failed").Detail(err)
		}
		for i := range list {
			if err = table.AddDisk(list[i]); err != nil {
				return nil, errors.Info(err, "add disk failed").Detail(err)
			}
		}
	}

	return table, nil
}

type ShardNodeDiskTable struct {
	nodeDiskTable *nodeDiskTable
}

func (b *ShardNodeDiskTable) GetDisk(diskID proto.DiskID) (info *ShardNodeDiskInfoRecord, err error) {
	ret, err := b.nodeDiskTable.GetDisk(diskID)
	if err != nil {
		return nil, err
	}
	return ret.(*ShardNodeDiskInfoRecord), nil
}

func (b *ShardNodeDiskTable) GetAllDisks() ([]*ShardNodeDiskInfoRecord, error) {
	ret := make([]*ShardNodeDiskInfoRecord, 0)
	err := b.nodeDiskTable.ListDisksByDiskTbl(0, 0, func(i interface{}) {
		ret = append(ret, i.(*ShardNodeDiskInfoRecord))
	})
	return ret, err
}

func (b *ShardNodeDiskTable) ListDisk(opt *clustermgr.ListOptionArgs) ([]*ShardNodeDiskInfoRecord, error) {
	ret := make([]*ShardNodeDiskInfoRecord, 0)
	err := b.nodeDiskTable.ListDisk(opt, func(i interface{}) {
		ret = append(ret, i.(*ShardNodeDiskInfoRecord))
	})
	return ret, err
}

func (b *ShardNodeDiskTable) AddDisk(disk *ShardNodeDiskInfoRecord) error {
	return b.nodeDiskTable.AddDisk(disk.DiskID, disk)
}

func (b *ShardNodeDiskTable) UpdateDisk(diskID proto.DiskID, disk *ShardNodeDiskInfoRecord) error {
	return b.nodeDiskTable.UpdateDisk(diskID, disk)
}

func (b *ShardNodeDiskTable) UpdateDiskStatus(diskID proto.DiskID, status proto.DiskStatus) error {
	return b.nodeDiskTable.UpdateDiskStatus(diskID, status)
}

// GetAllDroppingDisk return all drop disk in memory
func (b *ShardNodeDiskTable) GetAllDroppingDisk() ([]proto.DiskID, error) {
	return b.nodeDiskTable.GetAllDroppingDisk()
}

// AddDroppingDisk add a dropping disk
func (b *ShardNodeDiskTable) AddDroppingDisk(diskID proto.DiskID) error {
	return b.nodeDiskTable.AddDroppingDisk(diskID)
}

// DroppedDisk finish dropping in a disk
func (b *ShardNodeDiskTable) DroppedDisk(diskID proto.DiskID) error {
	return b.nodeDiskTable.DroppedDisk(diskID)
}

// IsDroppingDisk find a dropping disk if exist
func (b *ShardNodeDiskTable) IsDroppingDisk(diskID proto.DiskID) (exist bool, err error) {
	return b.nodeDiskTable.IsDroppingDisk(diskID)
}

func (b *ShardNodeDiskTable) GetAllNodes() ([]*ShardNodeInfoRecord, error) {
	ret := make([]*ShardNodeInfoRecord, 0)
	err := b.nodeDiskTable.GetAllNodes(func(i interface{}) {
		ret = append(ret, i.(*ShardNodeInfoRecord))
	})
	return ret, err
}

func (b *ShardNodeDiskTable) UpdateNode(info *ShardNodeInfoRecord) error {
	return b.nodeDiskTable.UpdateNode(info.NodeID, info)
}

func (b *ShardNodeDiskTable) unmarshalDiskRecord(data []byte) (interface{}, error) {
	version := data[0]
	if version == DiskInfoVersionNormal {
		ret := &ShardNodeDiskInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		if err != nil {
			return nil, err
		}
		n, err := b.nodeDiskTable.GetNode(ret.NodeID)
		if err != nil {
			return nil, err
		}
		nodeInfo := n.(*ShardNodeInfoRecord)
		ret.Idc = nodeInfo.Idc
		ret.Rack = nodeInfo.Rack
		ret.Host = nodeInfo.Host
		ret.Version = version
		return ret, nil
	}
	return nil, errors.New("invalid disk info version")
}

func (b *ShardNodeDiskTable) marshalDiskRecord(v interface{}) ([]byte, error) {
	info := *v.(*ShardNodeDiskInfoRecord)
	info.Idc, info.Rack, info.Host = "", "", ""
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func (b *ShardNodeDiskTable) diskID(i interface{}) proto.DiskID {
	return i.(*ShardNodeDiskInfoRecord).DiskID
}

func (b *ShardNodeDiskTable) diskInfo(i interface{}) *DiskInfoRecord {
	return &i.(*ShardNodeDiskInfoRecord).DiskInfoRecord
}

func (b *ShardNodeDiskTable) unmarshalNodeRecord(data []byte) (interface{}, error) {
	version := data[0]
	if version == NodeInfoVersionNormal {
		ret := &ShardNodeInfoRecord{}
		err := json.Unmarshal(data[1:], ret)
		ret.Version = version
		return ret, err
	}
	return nil, errors.New("invalid node info version")
}

func (b *ShardNodeDiskTable) marshalNodeRecord(v interface{}) ([]byte, error) {
	info := v.(*ShardNodeInfoRecord)
	data, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	data = append([]byte{info.Version}, data...)
	return data, nil
}

func (b *ShardNodeDiskTable) nodeInfo(i interface{}) *NodeInfoRecord {
	return &i.(*ShardNodeInfoRecord).NodeInfoRecord
}
