// Copyright 2025 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"fmt"
	"sort"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

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
	HighPres   []*MetaReplicaRec `json:"overLoad" bson:"overload"`
	Plan       []*MetaReplicaRec `json:"plan" bson:"plan"`
	PlanNum    int               `json:"totalPlanCount"`
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
}

type GetMigrateAddrParam struct {
	Topo       map[string]*ZonePressureView
	ZoneName   string
	NodeSetID  uint64
	Excludes   []string
	RequestNum int
	LeastSize  uint64
}

func (c *Cluster) MetaNodeRecord(metaNode *MetaNode) *MetaNodeRec {
	mnView := &MetaNodeRec{
		ID:             metaNode.ID,
		Addr:           metaNode.Addr,
		DomainAddr:     metaNode.DomainAddr,
		ZoneName:       metaNode.ZoneName,
		NodeSetID:      metaNode.NodeSetID,
		Total:          metaNode.Total,
		Used:           metaNode.Used,
		Free:           metaNode.Total - metaNode.Used,
		Ratio:          metaNode.Ratio,
		MpCount:        metaNode.MetaPartitionCount,
		MetaPartitions: metaNode.PersistenceMetaPartitions,
		InodeCount:     0,
		PlanCnt:        0,
	}
	for _, mpid := range mnView.MetaPartitions {
		mp, err := c.getMetaPartitionByID(mpid)
		if err != nil {
			log.LogErrorf("Error to get meta partition by ID(%d), err: %s", mpid, err.Error())
			continue
		}
		mnView.InodeCount += mp.InodeCount
	}

	return mnView
}

func (c *Cluster) GetMetaNodePressureView() (*ClusterPlan, error) {
	cView := &ClusterPlan{
		Low:    make(map[string]*ZonePressureView),
		Plan:   make([]*MetaPartitionPlan, 0),
		Status: PlanTaskInit,
	}

	err := c.GetLowMemPressureTopology(cView)
	if err != nil {
		log.LogErrorf("GetLowPressureTopology error: %s", err.Error())
		return cView, err
	}

	err = c.CreateMetaPartitionMigratePlan(cView)
	if err != nil {
		log.LogErrorf("CreateMetaPartitionMigratePlan error: %s", err.Error())
		return cView, err
	}

	err = c.FindMigrateDestination(cView)
	if err != nil {
		log.LogErrorf("FindMigrateDestination error: %s", err.Error())
		return cView, err
	}

	return cView, nil
}

func (c *Cluster) CreateMetaPartitionMigratePlan(migratePlan *ClusterPlan) error {
	// Get the meta node list that memory usage percent larger than metaNodeMemHighThresPer
	HighPresNodes := make([]*MetaNodeRec, 0)
	c.metaNodes.Range(func(key, value interface{}) bool {
		metanode, ok := value.(*MetaNode)
		if !ok {
			return true
		}

		if metanode.Ratio >= gConfig.metaNodeMemHighPer {
			metaRecord := c.MetaNodeRecord(metanode)
			HighPresNodes = append(HighPresNodes, metaRecord)
		}

		return true
	})

	err := CalculateMetaNodeEstimate(HighPresNodes)
	if err != nil {
		log.LogErrorf("CalculateMetaNodeEstimate err: %s", err.Error())
		return err
	}

	for _, metaNode := range HighPresNodes {
		err = c.AddMetaPartitionIntoPlan(metaNode, migratePlan, HighPresNodes)
		if err != nil {
			log.LogErrorf("Error to add meta partition into plan: %s", err.Error())
			continue
		}
	}

	return nil
}

func CalculateMetaNodeEstimate(HighPresNodes []*MetaNodeRec) error {
	for _, metaNode := range HighPresNodes {
		if metaNode.Ratio <= 0 {
			err := fmt.Errorf("The meta node ratio (%f) is <= 0", metaNode.Ratio)
			log.LogErrorf(err.Error())
			return err
		}
		metaNode.Estimate = int((metaNode.Ratio - gConfig.metaNodeMemHighPer) / metaNode.Ratio * float64(metaNode.MpCount))
		if metaNode.Estimate <= 0 {
			log.LogWarnf("the calculate estimate(%d) is forced to 1", metaNode.Estimate)
			metaNode.Estimate = 1
		}
	}

	return nil
}

func (c *Cluster) AddMetaPartitionIntoPlan(metaNode *MetaNodeRec, migratePlan *ClusterPlan, HighPresNodes []*MetaNodeRec) error {
	if metaNode.PlanCnt >= metaNode.Estimate {
		return nil
	}

	safeVols := c.allVols()

	// get the copied meta partition list.
	mps := c.getAllMetaPartitionsByMetaNode(metaNode.Addr)
	// Sort the meta partitions by inode count.
	sort.Slice(mps, func(i, j int) bool { return mps[i].InodeCount >= mps[j].InodeCount })

	for _, mp := range mps {
		// The meta partition is in plan list already.
		if CheckMetaPartitionInPlan(mp, migratePlan) {
			continue
		}

		mpPlan := &MetaPartitionPlan{
			ID:         mp.PartitionID,
			Original:   make([]*MetaReplicaRec, 0, len(mp.Replicas)),
			HighPres:   make([]*MetaReplicaRec, 0, len(mp.Replicas)),
			Plan:       make([]*MetaReplicaRec, 0, len(mp.Replicas)),
			InodeCount: mp.InodeCount,
			PlanNum:    0,
		}

		for _, mr := range mp.Replicas {
			mn, err := c.metaNode(mr.Addr)
			if err != nil {
				log.LogErrorf("Failed to get meta node(%s), err: %s", mr.Addr, err.Error())
				return err
			}
			org := c.GetMetaReplicaRecord(mn)
			mpPlan.Original = append(mpPlan.Original, org)

			if !CheckMetaReplicaIsHighPressure(mr, HighPresNodes) {
				continue
			}

			highPressure := c.GetMetaReplicaRecord(mn)
			mpPlan.HighPres = append(mpPlan.HighPres, highPressure)
			mpPlan.PlanNum += 1
		}
		if mpPlan.PlanNum <= 0 {
			continue
		}
		// Update the meta node plan count.
		err := UpdateMetaReplicaPlanCount(mpPlan, HighPresNodes)
		if err != nil {
			log.LogErrorf("Error to update meta node plan count: %s", err.Error())
			return err
		}

		// Get the CrossZone value.
		mpPlan.CrossZone = GetVolumeCrossZone(safeVols, mpPlan)

		migratePlan.Plan = append(migratePlan.Plan, mpPlan)

		if metaNode.PlanCnt >= metaNode.Estimate {
			break
		}
	}

	return nil
}

func (c *Cluster) GetMetaReplicaRecord(metaNode *MetaNode) *MetaReplicaRec {
	ret := &MetaReplicaRec{
		Source:       metaNode.Addr,
		SrcNodeSetId: metaNode.NodeSetID,
		SrcZoneName:  metaNode.ZoneName,
		Status:       PlanTaskInit,
	}
	return ret
}

func CheckMetaPartitionInPlan(mp *MetaPartition, migratePlan *ClusterPlan) bool {
	for _, planItem := range migratePlan.Plan {
		if mp.PartitionID == planItem.ID {
			return true
		}
	}

	return false
}

func CheckMetaReplicaIsHighPressure(mr *MetaReplica, HighPresNodes []*MetaNodeRec) bool {
	for _, highPres := range HighPresNodes {
		if mr.Addr == highPres.Addr {
			return true
		}
	}

	return false
}

func GetVolumeCrossZone(vols map[string]*Vol, mpPlan *MetaPartitionPlan) bool {
	for _, vol := range vols {
		for _, entry := range vol.MetaPartitions {
			if entry.PartitionID == mpPlan.ID {
				return vol.crossZone
			}
		}
	}

	return false
}

func UpdateMetaReplicaPlanCount(mpPlan *MetaPartitionPlan, HighPresNodes []*MetaNodeRec) error {
	for _, mpReplica := range mpPlan.HighPres {
		for _, metaNodeRec := range HighPresNodes {
			if mpReplica.Source == metaNodeRec.Addr {
				// Add one into each meta replica plan count.
				metaNodeRec.PlanCnt += 1

				if metaNodeRec.InodeCount > 0 {
					// Update the meta partition replica source memory size at the same time.
					mpReplica.SrcMemSize = uint64(float64(mpPlan.InodeCount) / float64(metaNodeRec.InodeCount) * float64(metaNodeRec.Total))
				}
				break
			}
		}
	}

	return nil
}

func (c *Cluster) GetLowMemPressureTopology(migratePlan *ClusterPlan) error {
	if migratePlan == nil || migratePlan.Low == nil {
		err := fmt.Errorf("The migratePlan parameter is nil")
		log.LogErrorf(err.Error())
		return err
	}

	zones := c.t.getAllZones()
	for _, zone := range zones {
		zoneView := &ZonePressureView{
			ZoneName: zone.name,
			Status:   zone.getStatusToString(),
			NodeSet:  make(map[uint64]*NodeSetPressureView),
		}
		nsc := zone.getAllNodeSet()
		for _, ns := range nsc {
			nsView := &NodeSetPressureView{
				NodeSetID: ns.ID,
				MetaNodes: make(map[uint64]*MetaNodeRec),
			}
			zoneView.NodeSet[ns.ID] = nsView
			ns.metaNodes.Range(func(key, value interface{}) bool {
				metaNode := value.(*MetaNode)
				if metaNode.Ratio <= gConfig.metaNodeMemLowPer {
					mnView := c.MetaNodeRecord(metaNode)
					nsView.MetaNodes[metaNode.ID] = mnView
				}
				return true
			})
			nsView.Number = len(nsView.MetaNodes)
		}
		migratePlan.Low[zone.name] = zoneView
	}

	return nil
}

func (c *Cluster) FindMigrateDestination(migratePlan *ClusterPlan) error {
	for _, mp := range migratePlan.Plan {
		if mp.CrossZone {
			err := c.FindMigrateDestRetainZone(migratePlan, mp)
			if err != nil {
				log.LogErrorf("FindMigrateDestRetainZone error: %s", err.Error())
				return err
			}
		} else {
			err := c.FindMigrateDestInOneNodeSet(migratePlan, mp)
			if err != nil {
				log.LogErrorf("FindMigrateDestInOneNodeSet error: %s", err.Error())
				return err
			}
		}
	}

	return nil
}

func (c *Cluster) FindMigrateDestRetainZone(migratePlan *ClusterPlan, mpPlan *MetaPartitionPlan) error {
	// clean all the planed value.
	mpPlan.Plan = []*MetaReplicaRec{}

	for _, highPressure := range mpPlan.HighPres {
		// If it in the plan array. Skip it.
		done := false
		for _, item := range mpPlan.Plan {
			if item.Source == highPressure.Source {
				done = true
				break
			}
		}
		if done {
			continue
		}

		srcNode := GetHighPressureNodeArray(mpPlan, highPressure)
		err := CreateMigratePlanInNodeSet(migratePlan, mpPlan, srcNode)
		if err != nil {
			log.LogErrorf("CreateMigratePlanInNodeSet error: %s", err.Error())
			return err
		}
		srcNode = GetSameNodeSetArray(mpPlan, highPressure)
		err = CreateMigratePlanExcludeNodeSet(migratePlan, mpPlan, srcNode)
		if err != nil {
			log.LogErrorf("CreateMigratePlanExcludeNodeSet error: %s", err.Error())
			return err
		}
	}

	return nil
}

func CreateMigratePlanInNodeSet(migratePlan *ClusterPlan, mpPlan *MetaPartitionPlan, srcNode []*MetaReplicaRec) error {
	if len(srcNode) <= 0 {
		return nil
	}

	var maxMemSize uint64
	for _, node := range srcNode {
		if maxMemSize < node.SrcMemSize {
			maxMemSize = node.SrcMemSize
		}
	}

	getParam := &GetMigrateAddrParam{
		Topo:       migratePlan.Low,
		ZoneName:   srcNode[0].SrcZoneName,
		NodeSetID:  srcNode[0].SrcNodeSetId,
		Excludes:   make([]string, 0),
		RequestNum: len(srcNode),
		LeastSize:  maxMemSize,
	}
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// try to find one meta node from the same node set.
	find, dests := GetMigrateDestAddr(getParam)
	if !find {
		return fmt.Errorf("Can't find request num (%d) free nodes from the nodeset(%d)", getParam.RequestNum, getParam.NodeSetID)
	}

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray error: %s", err.Error())
		return err
	}

	return nil
}

func CreateMigratePlanExcludeNodeSet(migratePlan *ClusterPlan, mpPlan *MetaPartitionPlan, srcNode []*MetaReplicaRec) error {
	if len(srcNode) <= 0 {
		return nil
	}

	var maxMemSize uint64
	for _, node := range srcNode {
		if maxMemSize < node.SrcMemSize {
			maxMemSize = node.SrcMemSize
		}
	}

	getParam := &GetMigrateAddrParam{
		Topo:       migratePlan.Low,
		ZoneName:   srcNode[0].SrcZoneName,
		NodeSetID:  srcNode[0].SrcNodeSetId,
		Excludes:   make([]string, 0),
		RequestNum: len(srcNode),
		LeastSize:  maxMemSize,
	}
	FillExcludeAddrIntoGetParam(mpPlan, getParam)

	// try to find new meta node from the same zone.
	find, dests := GetMigrateAddrExcludeNodeSet(getParam)
	if !find {
		return fmt.Errorf("Can't find %d free nodes from the zone(%s)", getParam.RequestNum, getParam.ZoneName)
	}

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray error: %s", err.Error())
		return err
	}

	return nil
}

func FillMigratePlanArray(migratePlan *ClusterPlan, mpPlan *MetaPartitionPlan, srcNode []*MetaReplicaRec, dests []*MetaReplicaRec) error {
	for i := 0; i < len(srcNode); i++ {
		item := &MetaReplicaRec{
			Source:       srcNode[i].Source,
			SrcMemSize:   srcNode[i].SrcMemSize,
			SrcNodeSetId: srcNode[i].SrcNodeSetId,
			SrcZoneName:  srcNode[i].SrcZoneName,
			Destination:  dests[i].Destination,
			DstId:        dests[i].DstId,
			DstNodeSetId: dests[i].DstNodeSetId,
			DstZoneName:  dests[i].DstZoneName,
			Status:       PlanTaskInit,
		}
		mpPlan.Plan = append(mpPlan.Plan, item)
		// Update the low pressure topology
		err := UpdateLowPressureNodeTopo(migratePlan, item)
		if err != nil {
			log.LogErrorf("UpdateLowPressureNodeTopo error: %s", err.Error())
			return err
		}
	}

	return nil
}

func UpdateLowPressureNodeTopo(migratePlan *ClusterPlan, newPlan *MetaReplicaRec) error {
	zone, ok := migratePlan.Low[newPlan.DstZoneName]
	if !ok {
		return fmt.Errorf("Error to get destination zone: %s", newPlan.DstZoneName)
	}

	nodeSet, ok := zone.NodeSet[newPlan.DstNodeSetId]
	if !ok {
		return fmt.Errorf("Error to get node set %d", newPlan.DstNodeSetId)
	}

	metaNode, ok := nodeSet.MetaNodes[newPlan.DstId]
	if !ok {
		return fmt.Errorf("Error to get meta node %d", newPlan.DstId)
	}

	metaNode.Used += newPlan.SrcMemSize * metaNodeMemoryRatio
	metaNode.Free = metaNode.Total - metaNode.Used
	if metaNode.Total > 0 {
		metaNode.Ratio = float64(metaNode.Used) / float64(metaNode.Total)
	}

	if metaNode.Ratio >= gConfig.metaNodeMemMidPer {
		delete(nodeSet.MetaNodes, metaNode.ID)
		nodeSet.Number -= 1
	}

	return nil
}

func GetHighPressureNodeArray(mpPlan *MetaPartitionPlan, mrRec *MetaReplicaRec) []*MetaReplicaRec {
	ret := make([]*MetaReplicaRec, 0, len(mpPlan.HighPres))
	for _, entry := range mpPlan.HighPres {
		if entry.SrcNodeSetId == mrRec.SrcNodeSetId {
			ret = append(ret, entry)
		}
	}

	return ret
}

func GetSameNodeSetArray(mpPlan *MetaPartitionPlan, mrRec *MetaReplicaRec) []*MetaReplicaRec {
	ret := make([]*MetaReplicaRec, 0, len(mpPlan.Original))
	for _, entry := range mpPlan.Original {
		if entry.SrcNodeSetId == mrRec.SrcNodeSetId {
			ret = append(ret, entry)
		}
	}

	return ret
}

func FillExcludeAddrIntoGetParam(mpPlan *MetaPartitionPlan, getParam *GetMigrateAddrParam) {
	for _, mrRec := range mpPlan.Original {
		getParam.Excludes = append(getParam.Excludes, mrRec.Source)
	}
	for _, mrRec := range mpPlan.Plan {
		getParam.Excludes = append(getParam.Excludes, mrRec.Destination)
	}
}

func (c *Cluster) FindMigrateDestInOneNodeSet(migratePlan *ClusterPlan, mpPlan *MetaPartitionPlan) error {
	requestNum := len(mpPlan.HighPres)
	if requestNum <= 0 {
		return fmt.Errorf("The high memory pressure meta node list is null")
	}
	var maxMemSize uint64
	for _, item := range mpPlan.HighPres {
		if item.SrcMemSize > maxMemSize {
			maxMemSize = item.SrcMemSize
		}
	}
	mpPlan.Plan = []*MetaReplicaRec{}
	getParam := &GetMigrateAddrParam{
		Topo:       migratePlan.Low,
		ZoneName:   mpPlan.HighPres[0].SrcZoneName,
		NodeSetID:  mpPlan.HighPres[0].SrcNodeSetId,
		Excludes:   make([]string, 0),
		RequestNum: requestNum,
		LeastSize:  maxMemSize,
	}
	FillExcludeAddrIntoGetParam(mpPlan, getParam)
	find, dests := GetMigrateDestAddr(getParam)
	if find {
		err := MigratePlanHighPresToDest(migratePlan, mpPlan, dests)
		if err != nil {
			log.LogErrorf("MigratePlanHighPresToDest error: %s", err.Error())
			return err
		}

		return nil
	}

	// try the others node set under the same zone.
	getParam.RequestNum = 3
	find, dests = GetMigrateAddrExcludeNodeSet(getParam)
	if !find {
		find, dests = GetMigrateAddrExcludeZone(getParam)
	}
	if !find {
		return fmt.Errorf("can't find the request low pressure nodes (%d)", getParam.RequestNum)
	}

	err := MigratePlanOriginalToDest(migratePlan, mpPlan, dests)
	if err != nil {
		log.LogErrorf("MigratePlanOriginalToDest error: %s", err.Error())
		return err
	}

	return nil
}

func MigratePlanHighPresToDest(migratePlan *ClusterPlan, mpPlan *MetaPartitionPlan, dests []*MetaReplicaRec) error {
	srcNode := make([]*MetaReplicaRec, 0, len(mpPlan.HighPres))
	for _, item := range mpPlan.HighPres {
		srcNode = append(srcNode, item)
	}

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray error: %s", err.Error())
		return err
	}

	return nil
}

func MigratePlanOriginalToDest(migratePlan *ClusterPlan, mpPlan *MetaPartitionPlan, dests []*MetaReplicaRec) error {
	srcNode := make([]*MetaReplicaRec, 0, len(mpPlan.Original))
	for _, item := range mpPlan.Original {
		srcNode = append(srcNode, item)
	}

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray returns err: %s", err.Error())
		return err
	}

	return nil
}

func GetMigrateDestAddr(param *GetMigrateAddrParam) (find bool, address []*MetaReplicaRec) {
	find = false
	zone, ok := param.Topo[param.ZoneName]
	if !ok {
		log.LogErrorf("Can't find zone: %s", param.ZoneName)
		return
	}

	nodeSet, ok := zone.NodeSet[param.NodeSetID]
	if !ok {
		log.LogErrorf("Can't find node set: %d", param.NodeSetID)
		return
	}

	if nodeSet.Number < param.RequestNum {
		log.LogErrorf("RequestNum: %d, but node set: %d only has %d free nodes", param.RequestNum, param.NodeSetID, nodeSet.Number)
		return
	}

	address = make([]*MetaReplicaRec, 0, param.RequestNum)
	for _, entry := range nodeSet.MetaNodes {
		bExcluded := false
		for _, item := range param.Excludes {
			if item == entry.Addr {
				bExcluded = true
			}
		}
		if bExcluded {
			continue
		}
		// check the free memory.
		if entry.Free <= metaNodeReserveMemorySize {
			continue
		}
		// the free memory size is larger than 2 * source meta partition's used.
		if entry.Free <= metaNodeMemoryRatio*param.LeastSize {
			continue
		}

		dstVal := &MetaReplicaRec{
			Destination:  entry.Addr,
			DstId:        entry.ID,
			DstNodeSetId: entry.NodeSetID,
			DstZoneName:  entry.ZoneName,
		}
		address = append(address, dstVal)
		if len(address) >= param.RequestNum {
			find = true
			break
		}
	}

	return
}

func GetMigrateAddrExcludeNodeSet(param *GetMigrateAddrParam) (find bool, address []*MetaReplicaRec) {
	zone, ok := param.Topo[param.ZoneName]
	if !ok {
		log.LogErrorf("Can't find zone: %s", param.ZoneName)
		return
	}

	for _, nodeSet := range zone.NodeSet {
		if nodeSet.NodeSetID == param.NodeSetID {
			continue
		}
		newParam := &GetMigrateAddrParam{
			Topo:       param.Topo,
			ZoneName:   param.ZoneName,
			NodeSetID:  nodeSet.NodeSetID,
			Excludes:   param.Excludes,
			RequestNum: param.RequestNum,
			LeastSize:  param.LeastSize,
		}
		find, address = GetMigrateDestAddr(newParam)
		if find {
			return
		}
	}

	log.LogErrorf("Failed to get (%d) free nodes from zone: %s", param.RequestNum, param.ZoneName)
	find = false
	return
}

func GetMigrateAddrExcludeZone(param *GetMigrateAddrParam) (find bool, address []*MetaReplicaRec) {
	for _, zone := range param.Topo {
		if zone.ZoneName == param.ZoneName {
			continue
		}

		for _, nodeSet := range zone.NodeSet {
			newParam := &GetMigrateAddrParam{
				Topo:       param.Topo,
				ZoneName:   zone.ZoneName,
				NodeSetID:  nodeSet.NodeSetID,
				Excludes:   param.Excludes,
				RequestNum: param.RequestNum,
				LeastSize:  param.LeastSize,
			}
			find, address = GetMigrateDestAddr(newParam)
			if find {
				return
			}
		}
	}

	log.LogErrorf("Failed to get (%d) free nodes from cluster", param.RequestNum)
	find = false
	return
}

func (c *Cluster) UpdateMigrateDestination(migratePlan *ClusterPlan, mpPlan *MetaPartitionPlan) error {
	// Renew the low pressure memory topology.
	migratePlan.Low = make(map[string]*ZonePressureView)

	err := c.GetLowMemPressureTopology(migratePlan)
	if err != nil {
		log.LogErrorf("GetLowMemPressureTopology error: %s", err.Error())
		return err
	}

	// renew the planed destination meta node.
	if mpPlan.CrossZone {
		err = c.FindMigrateDestRetainZone(migratePlan, mpPlan)
		if err != nil {
			log.LogErrorf("FindMigrateDestRetainZone error: %s", err.Error())
			return err
		}
	} else {
		err = c.FindMigrateDestInOneNodeSet(migratePlan, mpPlan)
		if err != nil {
			log.LogErrorf("FindMigrateDestInOneNodeSet error: %s", err.Error())
			return err
		}
	}

	return nil
}

func (c *Cluster) RunMetaPartitionBalanceTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.PlanRun {
		return nil
	}

	// Get the planed balance task.
	plan, err := c.loadBalanceTask()
	if err != nil {
		log.LogErrorf("loadBalanceTask err: %s", err.Error())
		return fmt.Errorf("can't find meta partition balance task in raft storage: %s", err.Error())
	}

	plan.Status = PlanTaskRun
	err = c.UpdateBalanceTaskStatus(plan)
	if err != nil {
		log.LogErrorf("UpdateBalanceTaskStatus err: %s", err.Error())
		return err
	}

	c.PlanRun = true
	go c.DoMetaPartitionBalanceTask(plan)

	return nil
}

func (c *Cluster) DoMetaPartitionBalanceTask(plan *ClusterPlan) {
	var (
		mp  *MetaPartition
		err error
	)

	for _, mpPlan := range plan.Plan {
		if VerifyMetaReplicaPlanNotAllInit(mpPlan) {
			continue
		}
		err = c.VerifyAllDestinationsIsLowLoad(plan, mpPlan)
		if err != nil {
			log.LogErrorf("VerifyAllDestinationsIsLow err: %s", err.Error())
			plan.Status = PlanTaskError
			c.PlanRun = false
			c.UpdateBalanceTaskStatus(plan)
			return
		}

		for _, mrPlan := range mpPlan.Plan {
			// Update raft storage.
			mrPlan.Status = PlanTaskRun
			err = c.syncUpdateBalanceTask(plan)
			if err != nil {
				log.LogErrorf("syncUpdateBalanceTask error: %s", err.Error())
				return
			}

			if !c.PlanRun {
				plan.Status = PlanTaskStop
				c.PlanRun = false
				c.UpdateBalanceTaskStatus(plan)
				return
			}
			if c.partition == nil || !c.partition.IsRaftLeader() {
				c.PlanRun = false
				return
			}

			log.LogDebugf("Start to migrate meta partition(%d) from %s to %s", mpPlan.ID, mrPlan.Source, mrPlan.Destination)
			mp, err = c.getMetaPartitionByID(mpPlan.ID)
			if err != nil {
				log.LogErrorf("getMetaPartitionByID(%d) error: %s", mpPlan.ID, err.Error())
				c.SetMetaReplicaPlanStatusError(plan, mrPlan)
				return
			}
			err = c.migrateMetaPartition(mrPlan.Source, mrPlan.Destination, mp)
			if err != nil {
				log.LogErrorf("migrateMetaPartition(%d) from %s to %s error: %s", mpPlan.ID, mrPlan.Source, mrPlan.Destination, err.Error())
				c.SetMetaReplicaPlanStatusError(plan, mrPlan)
				return
			}
			// Wait for migrating done.
			err = c.WaitForMetaPartitionMigrateDone(mp, mrPlan.Destination)
			if err != nil {
				log.LogErrorf("WaitForMetaPartitionMigrateDone mpid(%d) meta replica(%s) error: %s", mpPlan.ID, mrPlan.Destination, err.Error())
				c.SetMetaReplicaPlanStatusError(plan, mrPlan)
				return
			}

			// Update raft storage.
			mrPlan.Status = PlanTaskDone
			err = c.syncUpdateBalanceTask(plan)
			if err != nil {
				log.LogErrorf("syncUpdateBalanceTask error: %s", err.Error())
				return
			}
			log.LogDebugf("Migrate meta partition(%d) from %s to %s done", mpPlan.ID, mrPlan.Source, mrPlan.Destination)
		}
	}

	// clear the run flag.
	c.PlanRun = false
}

func (c *Cluster) SetMetaReplicaPlanStatusError(plan *ClusterPlan, mrPlan *MetaReplicaRec) {
	c.PlanRun = false
	plan.Status = PlanTaskError
	mrPlan.Status = PlanTaskError
	c.UpdateBalanceTaskStatus(plan)
	return
}

func (c *Cluster) UpdateBalanceTaskStatus(plan *ClusterPlan) error {
	err := c.syncUpdateBalanceTask(plan)
	if err != nil {
		log.LogErrorf("syncUpdateBalanceTask error: %s", err.Error())
		return err
	}
	return nil
}

func (c *Cluster) WaitForMetaPartitionMigrateDone(mp *MetaPartition, addr string) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for i := 0; i < 600; i++ {
		select {
		case <-ticker.C:
			if mp.IsRecover {
				continue
			}
			metaNode, err := c.metaNode(addr)
			if err != nil {
				log.LogErrorf("Failed to get meta node(%s): err: %s", addr, err.Error())
				return err
			}
			for _, mpInfo := range metaNode.metaPartitionInfos {
				if mpInfo.PartitionID != mp.PartitionID {
					continue
				}
				if mpInfo.MaxInodeID != mp.MaxInodeID {
					continue
				}
				if mpInfo.InodeCnt != mp.InodeCount {
					continue
				}
				if mpInfo.DentryCnt != mp.DentryCount {
					continue
				}
				// Migrating meta partition is done.
				return nil
			}
		case <-c.stopc:
			c.PlanRun = false
			return nil
		}
	}

	return fmt.Errorf("Waiting for meta partition(%d) replicat(%s) timeout", mp.PartitionID, addr)
}

func (c *Cluster) VerifyMetaNodeMemoryOverLoad(addr string) (bool, error) {
	metaNode, err := c.metaNode(addr)
	if err != nil {
		log.LogErrorf("Failed to get meta node(%s): err: %s", addr, err.Error())
		return false, err
	}

	if metaNode.Ratio >= gConfig.metaNodeMemMidPer {
		return true, nil
	}

	return false, nil
}

func VerifyMetaReplicaPlanNotAllInit(mpPlan *MetaPartitionPlan) bool {
	for _, mrPlan := range mpPlan.Plan {
		if mrPlan.Status != PlanTaskInit {
			return true
		}
	}

	return false
}

func (c *Cluster) StopMetaPartitionBalanceTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.PlanRun {
		return fmt.Errorf("Balance task is not running")
	}

	c.PlanRun = false
	return nil
}

func (c *Cluster) VerifyAllDestinationsIsLowLoad(plan *ClusterPlan, mpPlan *MetaPartitionPlan) (err error) {
	overLoad := false
	// Verify the destination node memory pressure is low.
	for _, mrPlan := range mpPlan.Plan {
		overLoad, err = c.VerifyMetaNodeMemoryOverLoad(mrPlan.Destination)
		if err != nil {
			log.LogErrorf("VerifyMetaNodeMemoryOverLoad err: %s", err.Error())
			return
		}
		if overLoad {
			break
		}
	}
	// If the memory is over load, update the meta replica plans.
	if overLoad {
		err = c.UpdateMigrateDestination(plan, mpPlan)
		if err != nil {
			log.LogErrorf("UpdateMigrateDestination err: %s", err.Error())
			return
		}
	}

	return nil
}

func (c *Cluster) scheduleStartBalanceTask() {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if c.partition == nil || !c.partition.IsRaftLeader() || c.PlanRun {
					continue
				}

				err := c.RestartMetaPartitionBalanceTask()
				if err != nil {
					log.LogErrorf("RestartMetaPartitionBalanceTask err: %s", err.Error())
				}
			case <-c.stopc:
				return
			}
		}
	}()
}

func (c *Cluster) RestartMetaPartitionBalanceTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.PlanRun {
		return nil
	}

	// Get the planed balance task.
	plan, err := c.loadBalanceTask()
	if err != nil {
		// If the raft stoarge is null, just return nil.
		return nil
	}

	if plan.Status != PlanTaskRun {
		// No start the plan task if the status is not running.
		return nil
	}

	c.PlanRun = true
	go c.DoMetaPartitionBalanceTask(plan)

	return nil
}
