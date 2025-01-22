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

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

type GetMigrateAddrParam struct {
	Topo       map[string]*proto.ZonePressureView
	ZoneName   string
	NodeSetID  uint64
	Excludes   []string
	RequestNum int
	LeastSize  uint64
}

func (c *Cluster) FreezeEmptyMetaPartitionJob(name string, freezeList []*MetaPartition) error {
	c.mu.Lock()
	task, ok := c.cleanTask[name]
	if ok {
		if task.Status == CleanTaskFreezing || task.Status == CleanTaskBackuping {
			c.mu.Unlock()
			return fmt.Errorf("The clean task for volume(%s) is %s", name, task.Status)
		}
		task.Status = CleanTaskFreezing
	} else {
		task = &CleanTask{
			Name:    name,
			Status:  CleanTaskFreezing,
			TaskCnt: len(freezeList),
		}
		c.cleanTask[name] = task
	}
	c.mu.Unlock()

	go func() {
		// waiting for client to update meta partition 10 minutes.
		time.Sleep(WaitForClientUpdateTimeMin * time.Minute)

		for _, mp := range freezeList {
			// freeze meta partition.
			err := c.FreezeEmptyMetaPartition(mp, true)
			if err != nil {
				log.LogErrorf("Failed to freeze volume(%s) meta partition(%d), error: %s", name, mp.PartitionID, err.Error())
				continue
			}
			task.FreezeCnt += 1
		}

		task.Status = CleanTaskFreezed
		task.Timeout = time.Now().Add(WaitForTaskDeleteByHour * time.Hour)
		c.mu.Lock()
		if !c.Cleaning {
			go c.DeleteCleanTasks()
			c.Cleaning = true
		}
		c.mu.Unlock()
	}()

	return nil
}

func (c *Cluster) FreezeEmptyMetaPartition(mp *MetaPartition, freeze bool) error {
	mr, err := mp.getMetaReplicaLeader()
	if err != nil {
		log.LogErrorf("get meta replica leader error: %s", err.Error())
		return err
	}
	task := mr.createTaskToFreezeReplica(mp.PartitionID, freeze)
	metaNode, err := c.metaNode(task.OperatorAddr)
	if err != nil {
		log.LogErrorf("failed to get metanode(%s), error: %s", task.OperatorAddr, err.Error())
		return err
	}
	_, err = metaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[FreezeEmptyMetaPartition] meta partition(%d), err: %s", mp.PartitionID, err.Error())
		return err
	}

	return nil
}

func (c *Cluster) StartCleanEmptyMetaPartition(name string) error {
	c.mu.Lock()
	task, ok := c.cleanTask[name]
	if ok {
		if task.Status == CleanTaskFreezing || task.Status == CleanTaskBackuping {
			c.mu.Unlock()
			return fmt.Errorf("The clean task for volume(%s) is %s", name, task.Status)
		}
		task.Status = CleanTaskBackuping
	} else {
		task = &CleanTask{
			Name:   name,
			Status: CleanTaskBackuping,
		}
		c.cleanTask[name] = task
	}
	c.mu.Unlock()

	go func() {
		err := c.DoCleanEmptyMetaPartition(name)
		if err != nil {
			log.LogErrorf("Failed to clean volume(%s) empty meta partition, error: %s", name, err.Error())
		}

		task.Status = CleanTaskBackuped
		task.Timeout = time.Now().Add(WaitForTaskDeleteByHour * time.Hour)
		c.mu.Lock()
		if !c.Cleaning {
			go c.DeleteCleanTasks()
			c.Cleaning = true
		}
		c.mu.Unlock()
	}()

	return nil
}

func (c *Cluster) DoCleanEmptyMetaPartition(name string) error {
	c.mu.Lock()
	task, ok := c.cleanTask[name]
	if !ok {
		log.LogErrorf("Can't find clean task for volume(%s)", name)
		return fmt.Errorf("Can't find clean task for volume(%s)", name)
	}
	c.mu.Unlock()

	vol, err := c.getVol(name)
	if err != nil {
		log.LogErrorf("DoCleanEmptyMetaPartition get volume(%s) error: %s", name, err.Error())
		return err
	}

	if vol.Status == proto.VolStatusMarkDelete {
		log.LogInfof("volume(%s) is deleted before cleaned empty meta partitions.", name)
		return nil
	}

	deleteMaps := make(map[uint64]*MetaPartition)
	mps := vol.cloneMetaPartitionMap()
	for key, mp := range mps {
		if !mp.IsFreeze {
			continue
		}

		// restore back the mp status if it is written.
		if mp.InodeCount != 0 || mp.DentryCount != 0 {
			// freeze meta partition.
			err = c.FreezeEmptyMetaPartition(mp, false)
			if err != nil {
				log.LogErrorf("Failed to unfreeze volume(%s) meta partition(%d), error: %s", name, mp.PartitionID, err.Error())
				continue
			}

			mp.IsFreeze = false
			// store the meta partition status.
			err = c.syncUpdateMetaPartition(mp)
			if err != nil {
				log.LogErrorf("volume(%s) meta partition(%d) update failed: %s", name, mp.PartitionID, err.Error())
				continue
			}
			task.ResetCnt += 1
		} else {
			err = c.CleanEmptyMetaPartition(mp)
			if err != nil {
				log.LogErrorf("action[DoCleanEmptyMetaPartition] clean meta partition(%d) error: %s", mp.PartitionID, err.Error())
				continue
			}

			deleteMaps[key] = mp
			task.CleanCnt += 1
		}
	}

	vol.mpsLock.Lock()
	for key, val := range deleteMaps {
		c.syncDeleteMetaPartition(val)
		delete(vol.MetaPartitions, key)
	}
	vol.mpsLock.UnLock()

	return nil
}

func (c *Cluster) CleanEmptyMetaPartition(mp *MetaPartition) error {
	for _, replica := range mp.Replicas {
		task := replica.createTaskToBackupReplica(mp.PartitionID)
		metaNode, err := c.metaNode(task.OperatorAddr)
		if err != nil {
			log.LogErrorf("failed to get metanode(%s), error: %s", task.OperatorAddr, err.Error())
			return err
		}
		_, err = metaNode.Sender.syncSendAdminTask(task)
		if err != nil {
			log.LogErrorf("action[FreezeEmptyMetaPartition] meta partition(%d), err: %s", mp.PartitionID, err.Error())
			return err
		}
	}

	return nil
}

func (c *Cluster) DeleteCleanTasks() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			keysToDelete := make([]string, 0, len(c.cleanTask))
			for key, task := range c.cleanTask {
				if task.Status == CleanTaskFreezing || task.Status == CleanTaskBackuping {
					continue
				}
				if task.Timeout.Before(now) {
					keysToDelete = append(keysToDelete, key)
				}
			}
			for _, key := range keysToDelete {
				delete(c.cleanTask, key)
			}
			if len(c.cleanTask) == 0 {
				c.Cleaning = false
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()
		case <-c.stopc:
			return
		}
	}
}

func (c *Cluster) MetaNodeRecord(metaNode *MetaNode) *proto.MetaNodeRec {
	mnView := &proto.MetaNodeRec{
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

func (c *Cluster) GetMetaNodePressureView() (*proto.ClusterPlan, error) {
	cView := &proto.ClusterPlan{
		Low:    make(map[string]*proto.ZonePressureView),
		Plan:   make([]*proto.MetaPartitionPlan, 0),
		Status: PlanTaskInit,
	}

	err := c.GetLowMemPressureTopology(cView)
	if err != nil {
		log.LogErrorf("GetLowMemPressureTopology error: %s", err.Error())
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
	cView.Total = len(cView.Plan)

	return cView, nil
}

func (c *Cluster) GetLowMemPressureTopology(migratePlan *proto.ClusterPlan) error {
	if migratePlan == nil || migratePlan.Low == nil {
		err := fmt.Errorf("The migratePlan parameter is nil")
		log.LogErrorf(err.Error())
		return err
	}

	zones := c.t.getAllZones()
	for _, zone := range zones {
		zoneView := &proto.ZonePressureView{
			ZoneName: zone.name,
			Status:   zone.getStatusToString(),
			NodeSet:  make(map[uint64]*proto.NodeSetPressureView),
		}
		nsc := zone.getAllNodeSet()
		for _, ns := range nsc {
			nsView := &proto.NodeSetPressureView{
				NodeSetID: ns.ID,
				MetaNodes: make(map[uint64]*proto.MetaNodeRec),
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

func (c *Cluster) CreateMetaPartitionMigratePlan(migratePlan *proto.ClusterPlan) error {
	// Get the meta node list that memory usage percent larger than metaNodeMemHighThresPer
	overLoadNodes := make([]*proto.MetaNodeRec, 0)
	c.metaNodes.Range(func(key, value interface{}) bool {
		metanode, ok := value.(*MetaNode)
		if !ok {
			return true
		}

		if metanode.Ratio >= gConfig.metaNodeMemHighPer {
			metaRecord := c.MetaNodeRecord(metanode)
			overLoadNodes = append(overLoadNodes, metaRecord)
		}

		return true
	})

	err := CalculateMetaNodeEstimate(overLoadNodes)
	if err != nil {
		log.LogErrorf("CalculateMetaNodeEstimate err: %s", err.Error())
		return err
	}

	for _, metaNode := range overLoadNodes {
		err = c.AddMetaPartitionIntoPlan(metaNode, migratePlan, overLoadNodes)
		if err != nil {
			log.LogErrorf("Error to add meta partition into plan: %s", err.Error())
			continue
		}
	}

	return nil
}

func CalculateMetaNodeEstimate(overLoadNodes []*proto.MetaNodeRec) error {
	for _, metaNode := range overLoadNodes {
		if metaNode.Ratio <= 0 {
			err := fmt.Errorf("The meta node ratio (%f) is <= 0", metaNode.Ratio)
			log.LogErrorf(err.Error())
			return err
		}
		metaNode.Estimate = int((metaNode.Ratio - gConfig.metaNodeMemHighPer) / metaNode.Ratio * float64(metaNode.MpCount))
		metaNode.Estimate += 1
		if metaNode.Estimate <= 0 {
			log.LogWarnf("the calculate estimate(%d) is forced to 1", metaNode.Estimate)
			metaNode.Estimate = 1
		}
	}

	return nil
}

func (c *Cluster) AddMetaPartitionIntoPlan(metaNode *proto.MetaNodeRec, migratePlan *proto.ClusterPlan, overLoadNodes []*proto.MetaNodeRec) error {
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

		mpPlan := &proto.MetaPartitionPlan{
			ID:         mp.PartitionID,
			Original:   make([]*proto.MetaReplicaRec, 0, len(mp.Replicas)),
			OverLoad:   make([]*proto.MetaReplicaRec, 0, len(mp.Replicas)),
			Plan:       make([]*proto.MetaReplicaRec, 0, len(mp.Replicas)),
			InodeCount: mp.InodeCount,
			PlanNum:    0,
		}

		for _, mr := range mp.Replicas {
			mn, err := c.metaNode(mr.Addr)
			if err != nil {
				log.LogErrorf("Failed to get meta node(%s), err: %s", mr.Addr, err.Error())
				return err
			}
			mrRec := c.GetMetaReplicaRecord(mn)
			mpPlan.Original = append(mpPlan.Original, mrRec)

			if !CheckMetaReplicaIsOverLoad(mr, overLoadNodes) {
				continue
			}

			mpPlan.OverLoad = append(mpPlan.OverLoad, mrRec)
			mpPlan.PlanNum += 1
		}
		if mpPlan.PlanNum <= 0 {
			continue
		}
		// Update the meta node plan count.
		err := UpdateMetaReplicaPlanCount(mpPlan, overLoadNodes)
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

func (c *Cluster) GetMetaReplicaRecord(metaNode *MetaNode) *proto.MetaReplicaRec {
	ret := &proto.MetaReplicaRec{
		Source:       metaNode.Addr,
		SrcNodeSetId: metaNode.NodeSetID,
		SrcZoneName:  metaNode.ZoneName,
		Status:       PlanTaskInit,
	}
	return ret
}

func CheckMetaPartitionInPlan(mp *MetaPartition, migratePlan *proto.ClusterPlan) bool {
	for _, planItem := range migratePlan.Plan {
		if mp.PartitionID == planItem.ID {
			return true
		}
	}

	return false
}

func CheckMetaReplicaIsOverLoad(mr *MetaReplica, overLoadNodes []*proto.MetaNodeRec) bool {
	for _, node := range overLoadNodes {
		if mr.Addr == node.Addr {
			return true
		}
	}

	return false
}

func GetVolumeCrossZone(vols map[string]*Vol, mpPlan *proto.MetaPartitionPlan) bool {
	for _, vol := range vols {
		for _, entry := range vol.MetaPartitions {
			if entry.PartitionID == mpPlan.ID {
				return vol.crossZone
			}
		}
	}

	return false
}

func UpdateMetaReplicaPlanCount(mpPlan *proto.MetaPartitionPlan, overLoadNodes []*proto.MetaNodeRec) error {
	for _, mrRec := range mpPlan.OverLoad {
		for _, node := range overLoadNodes {
			if mrRec.Source == node.Addr {
				// Add the meta node plan count by 1.
				node.PlanCnt += 1

				if node.InodeCount > 0 {
					// Update the meta replica source memory size at the same time.
					mrRec.SrcMemSize = uint64(float64(mpPlan.InodeCount) / float64(node.InodeCount) * float64(node.Total))
				}
				break
			}
		}
	}

	return nil
}

func (c *Cluster) FindMigrateDestination(migratePlan *proto.ClusterPlan) error {
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

func (c *Cluster) FindMigrateDestRetainZone(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan) error {
	// clean all the planed value.
	mpPlan.Plan = []*proto.MetaReplicaRec{}

	for _, highPressure := range mpPlan.OverLoad {
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

		srcNode := GetOverLoadNodeArray(mpPlan, highPressure)
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

func GetOverLoadNodeArray(mpPlan *proto.MetaPartitionPlan, mrRec *proto.MetaReplicaRec) []*proto.MetaReplicaRec {
	ret := make([]*proto.MetaReplicaRec, 0, len(mpPlan.OverLoad))
	for _, entry := range mpPlan.OverLoad {
		if entry.SrcNodeSetId == mrRec.SrcNodeSetId {
			ret = append(ret, entry)
		}
	}

	return ret
}

func CreateMigratePlanInNodeSet(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan, srcNode []*proto.MetaReplicaRec) error {
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

func GetSameNodeSetArray(mpPlan *proto.MetaPartitionPlan, mrRec *proto.MetaReplicaRec) []*proto.MetaReplicaRec {
	ret := make([]*proto.MetaReplicaRec, 0, len(mpPlan.Original))
	for _, entry := range mpPlan.Original {
		if entry.SrcNodeSetId == mrRec.SrcNodeSetId {
			ret = append(ret, entry)
		}
	}

	return ret
}

func CreateMigratePlanExcludeNodeSet(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan, srcNode []*proto.MetaReplicaRec) error {
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

func FillMigratePlanArray(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan, srcNode []*proto.MetaReplicaRec, dests []*proto.MetaReplicaRec) error {
	for i := 0; i < len(srcNode); i++ {
		item := &proto.MetaReplicaRec{
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

func UpdateLowPressureNodeTopo(migratePlan *proto.ClusterPlan, newPlan *proto.MetaReplicaRec) error {
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

func FillExcludeAddrIntoGetParam(mpPlan *proto.MetaPartitionPlan, getParam *GetMigrateAddrParam) {
	for _, mrRec := range mpPlan.Original {
		getParam.Excludes = append(getParam.Excludes, mrRec.Source)
	}
	for _, mrRec := range mpPlan.Plan {
		getParam.Excludes = append(getParam.Excludes, mrRec.Destination)
	}
}

func (c *Cluster) FindMigrateDestInOneNodeSet(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan) error {
	requestNum := len(mpPlan.OverLoad)
	if requestNum <= 0 {
		return fmt.Errorf("The high memory pressure meta node list is null")
	}
	var maxMemSize uint64
	for _, item := range mpPlan.OverLoad {
		if item.SrcMemSize > maxMemSize {
			maxMemSize = item.SrcMemSize
		}
	}
	mpPlan.Plan = []*proto.MetaReplicaRec{}
	getParam := &GetMigrateAddrParam{
		Topo:       migratePlan.Low,
		ZoneName:   mpPlan.OverLoad[0].SrcZoneName,
		NodeSetID:  mpPlan.OverLoad[0].SrcNodeSetId,
		Excludes:   make([]string, 0),
		RequestNum: requestNum,
		LeastSize:  maxMemSize,
	}
	FillExcludeAddrIntoGetParam(mpPlan, getParam)
	find, dests := GetMigrateDestAddr(getParam)
	if find {
		err := MigratePlanOverLoadToDest(migratePlan, mpPlan, dests)
		if err != nil {
			log.LogErrorf("MigratePlanOverLoadToDest error: %s", err.Error())
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

func MigratePlanOverLoadToDest(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan, dests []*proto.MetaReplicaRec) error {
	srcNode := make([]*proto.MetaReplicaRec, 0, len(mpPlan.OverLoad))
	for _, item := range mpPlan.OverLoad {
		srcNode = append(srcNode, item)
	}

	err := FillMigratePlanArray(migratePlan, mpPlan, srcNode, dests)
	if err != nil {
		log.LogErrorf("FillMigratePlanArray error: %s", err.Error())
		return err
	}

	return nil
}

func MigratePlanOriginalToDest(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan, dests []*proto.MetaReplicaRec) error {
	srcNode := make([]*proto.MetaReplicaRec, 0, len(mpPlan.Original))
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

func GetMigrateDestAddr(param *GetMigrateAddrParam) (find bool, address []*proto.MetaReplicaRec) {
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

	address = make([]*proto.MetaReplicaRec, 0, param.RequestNum)
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

		dstVal := &proto.MetaReplicaRec{
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

func GetMigrateAddrExcludeNodeSet(param *GetMigrateAddrParam) (find bool, address []*proto.MetaReplicaRec) {
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

func GetMigrateAddrExcludeZone(param *GetMigrateAddrParam) (find bool, address []*proto.MetaReplicaRec) {
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

func (c *Cluster) UpdateMigrateDestination(migratePlan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan) error {
	// Renew the low pressure memory topology.
	migratePlan.Low = make(map[string]*proto.ZonePressureView)

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

func (c *Cluster) DoMetaPartitionBalanceTask(plan *proto.ClusterPlan) {
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
			log.LogErrorf("VerifyAllDestinationsIsLowLoad err: %s", err.Error())
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
		plan.DoneNum += 1
	}

	// clear the run flag.
	c.PlanRun = false

	plan.Status = PlanTaskDone
	err = c.UpdateBalanceTaskStatus(plan)
	if err != nil {
		log.LogErrorf("UpdateBalanceTaskStatus err: %s", err.Error())
	}
}

func (c *Cluster) SetMetaReplicaPlanStatusError(plan *proto.ClusterPlan, mrPlan *proto.MetaReplicaRec) {
	c.PlanRun = false
	plan.Status = PlanTaskError
	mrPlan.Status = PlanTaskError
	c.UpdateBalanceTaskStatus(plan)
	return
}

func (c *Cluster) UpdateBalanceTaskStatus(plan *proto.ClusterPlan) error {
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

	return fmt.Errorf("Waiting for meta partition(%d) destination(%s) timeout", mp.PartitionID, addr)
}

func (c *Cluster) VerifyMetaNodeExceedMemMid(addr string) (bool, error) {
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

func VerifyMetaReplicaPlanNotAllInit(mpPlan *proto.MetaPartitionPlan) bool {
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

func (c *Cluster) VerifyAllDestinationsIsLowLoad(plan *proto.ClusterPlan, mpPlan *proto.MetaPartitionPlan) (err error) {
	overLoad := false
	// Verify the destination node memory pressure is low.
	for _, mrPlan := range mpPlan.Plan {
		overLoad, err = c.VerifyMetaNodeExceedMemMid(mrPlan.Destination)
		if err != nil {
			log.LogErrorf("VerifyMetaNodeExceedMemMid err: %s", err.Error())
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

func (c *Cluster) DeleteMetaPartitionBalanceTask() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.PlanRun {
		return fmt.Errorf("Please stop the running task before deleting it.")
	}

	// search the raft storage. Only store one plan
	err := c.syncDeleteBalanceTask()
	if err != nil {
		log.LogErrorf("syncDeleteBalanceTask err: %s", err.Error())
	}

	return err
}
