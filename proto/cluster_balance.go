package proto

import "time"

type MrBalanceInfo struct {
	Source       string `json:"source" bson:"source"`
	SrcMemSize   uint64 `json:"srcMemSize" bson:"srcmemsize"`
	SrcNodeSetId uint64 `json:"srcNodeSetId" bson:"srcnodesetid"`
	SrcZoneName  string `json:"srcZoneName" bson:"srczonename"`
	Destination  string `json:"destination" bson:"destination"`
	DstId        uint64 `json:"dstID" bson:"dstid"`
	DstNodeSetId uint64 `json:"dstNodeSetId" bson:"dstnodesetid"`
	DstZoneName  string `json:"dstZoneName" bson:"dstzonename"`
	Status       string `json:"status" bson:"status"`
	Msg          string `json:"msg" bson:"msg"`
}

type MetaBalancePlan struct {
	ID         uint64           `json:"id" bson:"id"`
	CrossZone  bool             `json:"crossZone" bson:"crosszone"`
	Original   []*MrBalanceInfo `json:"original" bson:"original"`
	OverLoad   []*MrBalanceInfo `json:"overLoad" bson:"overload"`
	Plan       []*MrBalanceInfo `json:"plan" bson:"plan"`
	PlanNum    int              `json:"planNum" bson:"plannum"`
	InodeCount uint64           `json:"inodeCount" bson:"inodecount"`
	Msg        string           `json:"msg" bson:"msg"`
}

type MetaNodeBalanceInfo struct {
	ID             uint64   `json:"id"`
	Addr           string   `json:"address"`
	DomainAddr     string   `json:"domainAddress"`
	ZoneName       string   `json:"zone"`
	NodeSetID      uint64   `json:"nodeSetId"`
	Total          uint64   `json:"totalMem"`
	Used           uint64   `json:"usedMem"`
	Free           uint64   `json:"freeMem"`
	Ratio          float64  `json:"ratio"`
	NodeMemTotal   uint64   `json:"nodeMemTotal"`
	NodeMemUsed    uint64   `json:"nodeMemUsed"`
	NodeMemFree    uint64   `json:"nodeMemFree"`
	NodeMemRatio   float64  `json:"nodeMemRatio"`
	MpCount        int      `json:"mpCount"`
	MetaPartitions []uint64 `json:"metaPartition"`
	InodeCount     uint64   `json:"inodeCount"`
	Estimate       int      `json:"estimate"`
	PlanCnt        int      `json:"planCount"`
}

type NodeSetPressureView struct {
	NodeSetID uint64                          `json:"nodeSetId"`
	Number    int                             `json:"number"`
	MetaNodes map[uint64]*MetaNodeBalanceInfo `json:"metaNodes"`
}

type ZonePressureView struct {
	ZoneName string                          `json:"zone"`
	Status   string                          `json:"status"`
	NodeSet  map[uint64]*NodeSetPressureView `json:"nodeSet"`
}

type ClusterPlan struct {
	Low     map[string]*ZonePressureView `json:"-" bson:"-"`
	Plan    []*MetaBalancePlan           `json:"plan" bson:"plan"`
	DoneNum int                          `json:"doneCount" bson:"donenum"`
	Total   int                          `json:"total" bson:"total"`
	Status  string                       `json:"status" bson:"status"`
	Expire  time.Time                    `json:"expire" bson:"expire"`
	Type    string                       `json:"type" bson:"type"`
	Msg     string                       `json:"msg" bson:"msg"`
}
