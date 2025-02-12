package proto

import "time"

type MetaReplicaRec struct {
	Source       string `json:"source" bson:"source"`
	SrcMemSize   uint64 `json:"srcMemSize" bson:"srcmemsize"`
	SrcNodeSetId uint64 `json:"srcNodeSetId" bson:"srcnodesetid"`
	SrcZoneName  string `json:"srcZoneName" bson:"srczonename"`
	Destination  string `json:"destination" bson:"destination"`
	DstId        uint64 `json:"dstID" bson:"dstid"`
	DstNodeSetId uint64 `json:"dstNodeSetId" bson:"dstnodesetid"`
	DstZoneName  string `json:"dstZoneName" bson:"dstzonename"`
	Status       string `json:"status" bson:"status"`
}

type MetaPartitionPlan struct {
	ID         uint64            `json:"id" bson:"id"`
	CrossZone  bool              `json:"crossZone" bson:"crosszone"`
	Original   []*MetaReplicaRec `json:"original" bson:"original"`
	OverLoad   []*MetaReplicaRec `json:"overLoad" bson:"overload"`
	Plan       []*MetaReplicaRec `json:"plan" bson:"plan"`
	PlanNum    int               `json:"planNum" bson:"plannum"`
	InodeCount uint64            `json:"inodeCount" bson:"inodecount"`
}

type MetaNodeRec struct {
	ID             uint64   `json:"id"`
	Addr           string   `json:"address"`
	DomainAddr     string   `json:"domainAddress"`
	ZoneName       string   `json:"zone"`
	NodeSetID      uint64   `json:"nodeSetId"`
	Total          uint64   `json:"totalMem"`
	Used           uint64   `json:"usedMem"`
	Free           uint64   `json:"freeMem"`
	Ratio          float64  `json:"ratio"`
	MpCount        int      `json:"mpCount"`
	MetaPartitions []uint64 `json:"metaPartition"`
	InodeCount     uint64   `json:"inodeCount"`
	Estimate       int      `json:"estimate"`
	PlanCnt        int      `json:"planCount"`
}

type NodeSetPressureView struct {
	NodeSetID uint64                  `json:"nodeSetId"`
	Number    int                     `json:"number"`
	MetaNodes map[uint64]*MetaNodeRec `json:"metaNodes"`
}

type ZonePressureView struct {
	ZoneName string                          `json:"zone"`
	Status   string                          `json:"status"`
	NodeSet  map[uint64]*NodeSetPressureView `json:"nodeSet"`
}

type ClusterPlan struct {
	Low     map[string]*ZonePressureView `json:"-" bson:"-"`
	Plan    []*MetaPartitionPlan         `json:"plan" bson:"plan"`
	DoneNum int                          `json:"doneCount" bson:"donenum"`
	Total   int                          `json:"total" bson:"total"`
	Status  string                       `json:"status" bson:"status"`
	Expire  time.Time                    `json:"expire" bson:"expire"`
}
