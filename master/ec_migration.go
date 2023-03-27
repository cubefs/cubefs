// Copyright 2018 The CubeFS Authors.
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
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	EcTaskMigrating               = 0
	EcTaskRetry                   = 1
	EcTaskFail                    = 2
	EcMaxRetryTimes               = 3
	EcMaxMigrateGoroutineSize     = 10
	EcTimeMinute                  = 60
	EcMigrateInterval             = time.Minute * 3
	EcMigrateTimeOutInterval      = time.Minute * 5
	EcRetryInterval               = time.Minute * 5
)

var (
	migrateTaskList      *migrateList
	migrateGoroutineArgs chan *MigrateArgs
)

var StatusMap = map[uint8]string{
	EcTaskMigrating: "TaskMigrating",
	EcTaskRetry:     "TaskRetry",
	EcTaskFail:      "TaskFail",
}

type MigrateArgs struct {
	volName     string
	partitionID uint64
}

func (c *Cluster) scheduleToMigrationEc() {
	for index := 0; index < EcMaxMigrateGoroutineSize; index++ {
		go c.startMigrate()
	}

	migrateGoroutineArgs = make(chan *MigrateArgs, EcMaxMigrateGoroutineSize)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.LogErrorf("startMigrate err:%v", r)
				c.doScheduleMigrationEc()
			}
		}()

		c.doScheduleMigrationEc()
	}()
}

func (c *Cluster) doScheduleMigrationEc() {
	migrateTimer := time.NewTimer(EcMigrateInterval)
	statusDetectTimer := time.NewTimer(EcMigrateTimeOutInterval)
	retryTimer := time.NewTimer(EcRetryInterval)
	if migrateTaskList == nil {
		migrateTaskList = NewMigrateList()
	}

	for {
		select {
		case <-migrateTimer.C:
			if c.partition.IsRaftLeader() {
				c.detectAllDataPartition()
			}
			migrateTimer.Reset(EcMigrateInterval)
		case <-statusDetectTimer.C:
			if c.partition.IsRaftLeader() {
				c.checkMigrateTaskTimeout()
			}
			statusDetectTimer.Reset(EcMigrateTimeOutInterval)
		case <-retryTimer.C:
			if c.partition.IsRaftLeader() {
				c.retryFailTask()
			}
			retryTimer.Reset(EcRetryInterval)
		}
	}
}

func (c *Cluster) detectAllDataPartition() {
	maxMigrateTaskSize := c.MaxCodecConcurrent * len(c.allActiveCodecNodes())


	vols := c.allVols()
	for _, vol := range vols {
		if vol.EcEnable {
			for i, dp := range vol.getCanMigrateDp() {
				if !c.partition.IsRaftLeader() {
					return
				}

				if i+migrateTaskList.Len() >= maxMigrateTaskSize {
					log.LogWarnf("Task num already maximum，Len:%v, maxSize:%v", migrateTaskList.Len()+i, maxMigrateTaskSize)
					return
				}

				select {
				case migrateGoroutineArgs <- &MigrateArgs{vol.Name, dp.PartitionID}:
				default:
					return
				}
			}
		}
	}
}

func isValidAddr(hosts []string, addr string) bool {
	for _, host := range hosts {
		if host == addr {
			return true
		}
	}
	return false
}

func getDpLastUpdateTime(dp *DataPartition) (lastUpdateTime int64) {
	dp.RLock()
	defer dp.RUnlock()
	for addr, updateTime := range dp.lastUpdateTimeMap {
		if updateTime > lastUpdateTime && isValidAddr(dp.Hosts, addr) {
			lastUpdateTime = updateTime
		}
	}
	if lastUpdateTime == 0 {
		lastUpdateTime = time.Now().Unix()
	}
	return
}

func (vol *Vol) getCanMigrateDp() []*DataPartition {
	dataPartitions := make([]*DataPartition, 0)
	migrationWaitTime := vol.EcMigrationWaitTime * EcTimeMinute

	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.partitionMap {
		if dp.EcMigrateStatus == proto.NotEcMigrate && time.Now().Unix()-getDpLastUpdateTime(dp) > migrationWaitTime && dp.used > 0 && dp.Status == proto.ReadOnly && !dp.isRecover {
			if _, exist := migrateTaskList.Exists(dp.PartitionID); exist {
				continue
			}

			dataPartitions = append(dataPartitions, dp)
		}
	}
	return dataPartitions
}

func (c *Cluster) startMigrate() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("startMigrate err:%v", r)
			c.startMigrate()
		}
	}()

	for {
		select {
		case args := <-migrateGoroutineArgs:
			c.dealMigrateRequest(args)
		}
	}
}

func (c *Cluster) needMigrateEc(vol *Vol, dp *DataPartition) (needMigrate bool, err error) {
	needMigrate = true
	if proto.IsEcFinished(dp.EcMigrateStatus) {
		needMigrate = false
		return
	}
	if dp.EcMigrateStatus != proto.RollBack {
		return
	}
	ecdp, err := vol.getEcPartitionByID(dp.PartitionID)
	if err == nil && ecdp.LastUpdateTime == getDpLastUpdateTime(dp) {
		needMigrate = false
		dp.EcMigrateStatus = proto.FinishEC
		if err = c.syncUpdateDataPartition(dp); err != nil {
			dp.EcMigrateStatus = proto.RollBack
			return
		}
		ecdp.FinishEcTime = time.Now().Unix()
		ecdp.EcMigrateStatus = proto.FinishEC
		if err = c.syncUpdateEcDataPartition(ecdp); err != nil {
			dp.EcMigrateStatus = proto.RollBack
			return
		}
	}
	return
}

func (c *Cluster) dealMigrateRequest(args *MigrateArgs) {
	if !c.partition.IsRaftLeader() {
		return
	}

	var (
		err       error
		vol       *Vol
		dp        *DataPartition
		ecdp      *EcDataPartition
		codEcNode *CodecNode
	)

	if dp, err = c.getDataPartitionByID(args.partitionID); err != nil {
		return
	}

	vol, err = c.getVol(args.volName)
	if err != nil {
		return
	}

	if ecdp, err = c.createEcDataPartition(vol, dp); err != nil {
		log.LogErrorf("migrate createEcPartition failed:%+v", err)
		return
	}

	if codEcNode = c.getAvailCodEcnode(); codEcNode == nil {
		err = errors.New("get codecnode fail")
		return
	}
	task := &MigrateTask{EcTaskMigrating, 0, args.volName, ecdp.PartitionID, 0, time.Now().Unix(), codEcNode.Addr}
	log.LogDebugf("start migrate partition[%v] status[%v]", ecdp.PartitionID, ecdp.Status)
	if err = c.sendMigrateTask(ecdp, dp, codEcNode); err != nil {
		log.LogErrorf("migrate sendTask failed:%+v", err)
		return
	}

	migrateTaskList.Push(task)
	if err = c.addMigrateTask(task); err != nil {
		log.LogWarnf("startMigrate addMigrateTask err:%+v", err)
	}

	log.LogInfof("start migration partition: %d\n", ecdp.PartitionID)
}

func (c *Cluster) sendMigrateTask(ecdp *EcDataPartition, dp *DataPartition, codEcNode *CodecNode) (err error) {
	var dataNode *DataNode
	profHosts := make([]string, 0)
	leaderAddr := dp.getLeaderAddr()
	if !contains(dp.Hosts, leaderAddr) {
		err = errors.NewErrorf("dataPartition(%v) no leader", dp.PartitionID)
		return
	}

	if dataNode, err = c.dataNode(leaderAddr); err != nil {
		return
	}

	profIp := fmt.Sprintf("%s:%s", strings.Split(dataNode.Addr, ":")[0], dataNode.HttpPort)
	profHosts = append(profHosts, profIp)

	for _, host := range dp.Hosts {
		if host == leaderAddr {
			continue
		}
		if dataNode, err = c.dataNode(host); err != nil {
			continue
		}
		profIp = fmt.Sprintf("%s:%s", strings.Split(dataNode.Addr, ":")[0], dataNode.HttpPort)
		profHosts = append(profHosts, profIp)
	}

	request := &proto.IssueMigrationTaskRequest{
		VolName:       ecdp.VolName,
		PartitionId:   ecdp.PartitionID,
		ProfHosts:     profHosts,
		EcDataNum:     ecdp.DataUnitsNum,
		EcParityNum:   ecdp.ParityUnitsNum,
		Hosts:         ecdp.Hosts,
		EcMaxUnitSize: ecdp.MaxSripeUintSize,
	}

	if migrateTask, exist := migrateTaskList.Exists(ecdp.PartitionID); exist {
		request.CurrentExtentID = migrateTask.CurrentExtentID
	}
	dp.EcMigrateStatus = proto.Migrating
	if err = c.syncUpdateDataPartition(dp); err != nil {
		return
	}
	task := proto.NewAdminTask(proto.OpIssueMigrationTask, codEcNode.Addr, request)
	if _, err = codEcNode.TaskManager.syncSendAdminTask(task); err != nil {
		log.LogErrorf("syncSendAdminTask error:%v", err)
	}
	return
}

func (c *Cluster) getAvailCodEcnode() *CodecNode {
	allCodEcNode := c.allActiveCodecNodes()
	if len(allCodEcNode) == 0 {
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	return allCodEcNode[rand.Intn(len(allCodEcNode))]
}

func (c *Cluster) checkMigrateTaskTimeout() {
	for _, task := range migrateTaskList.GetMigrateTimeoutTask() {
		vol, err := c.getVol(task.VolName)
		if err != nil {
			log.LogWarnf("get vol(%v) err(%v)", task.VolName, err)
			continue
		}
		migrateTimeOut := vol.EcMigrationTimeOut * EcTimeMinute
		log.LogInfof("checkMigrateTaskTimeout partition(%v) diffTime(%v) timeout(%v)", task.PartitionID, time.Now().Unix()-task.ModifyTime, migrateTimeOut)
		if time.Now().Unix()-task.ModifyTime > migrateTimeOut {
			task.Status = EcTaskRetry
			task.ModifyTime = time.Now().Unix()
			if err := c.updateMigrateTask(task); err != nil {
				log.LogWarnf("checkTaskTimeout partition(%v) updateMigrateTask err:%+v", task.PartitionID, err)
			}
		}
	}
}

func (c *Cluster) initMigrateTask(vol *Vol, ep *EcDataPartition, dp *DataPartition) {
	dp.EcMigrateStatus = proto.NotEcMigrate
	c.syncUpdateDataPartition(dp)
	migrateTaskList.Remove(dp.PartitionID)
	if ep != nil {
		c.delEcPartition(vol, ep)
	}
}

func (c *Cluster) retryFailTask() {
	var (
		needUpdateTask *MigrateTask
	    dataPartition *DataPartition
	    ecPartition *EcDataPartition
		codEcNode *CodecNode
		vol       *Vol
		err error
	)
	for _, task := range migrateTaskList.GetRetryTask() {
		if needUpdateTask != nil {
			if err = c.updateMigrateTask(needUpdateTask); err != nil {
				log.LogWarnf("retryFailTask updateMigrateTask err:Task:%+v err:%+v", needUpdateTask, err)
			}
			needUpdateTask = nil
		}
		vol, err = c.getVol(task.VolName)
		if err != nil {
			log.LogWarnf("get vol(%v) err(%v)", task.VolName, err)
			continue
		}
		retryWaitTime := EcTimeMinute * vol.EcMigrationRetryWait
		if time.Now().Unix()-task.ModifyTime > retryWaitTime {
			needUpdateTask = task
			task.RetryTimes++
			if task.RetryTimes+1 > EcMaxRetryTimes {
				task.Status = EcTaskFail
				err = c.updateEcMigrateStatus(task.PartitionID, proto.MigrateFailed)
				log.LogWarnf("partition(%v) updateStatus err(%v)", task.PartitionID, err)
				msg := fmt.Sprintf(" datapartition(%v) migrateEc failed", task.PartitionID)
				Warn(c.Name, msg)
				continue
			}
			task.ModifyTime = time.Now().Unix()
			if dataPartition, err = c.getDataPartitionByID(task.PartitionID); err != nil {
				log.LogWarnf("get dp(%v) err(%v)", task.PartitionID, err)
				migrateTaskList.Remove(task.PartitionID)
				continue
			}

			if proto.IsEcFinished(dataPartition.EcMigrateStatus) {
				log.LogDebugf("dataPartition(%v) already migrateEc Finished, don't need retry", task.PartitionID)
				if err = c.updateEcMigrateStatus(task.PartitionID, proto.FinishEC); err != nil {
					continue
				}
				migrateTaskList.Remove(task.PartitionID)
				c.delMigrateTask(task)
				continue
			}

			if ecPartition, err = c.getEcPartitionByID(task.PartitionID); err != nil {
				log.LogWarnf("get ep(%v) err(%v)", task.PartitionID, err)
				c.initMigrateTask(vol, ecPartition, dataPartition)
				continue
			}

			if !vol.dpIsCanEc(dataPartition) {
				log.LogWarnf("dataPartition(%v) can't migrateEc", dataPartition.PartitionID)
				c.initMigrateTask(vol, ecPartition, dataPartition)
				continue
			}

			if !ecPartition.canWrite() {
				log.LogWarnf("ecPartition(%v) can't write", ecPartition.PartitionID)
				c.initMigrateTask(vol, ecPartition, dataPartition)
				continue
			}

			log.LogDebugf("start migrate retry partition[%v] status[%v]", dataPartition.PartitionID, ecPartition.Status)
			if codEcNode = c.getAvailCodEcnode(); codEcNode == nil {
				err = errors.New("get codecnode fail")
				continue
			}
			needUpdateTask.CodecNode = codEcNode.Addr
			if err = c.sendMigrateTask(ecPartition, dataPartition, codEcNode); err != nil {
				log.LogWarnf("migrate retryTask failed: task:%+v, err:%+v", task, err)
			}
		}
	}

	if needUpdateTask != nil {
		if err = c.updateMigrateTask(needUpdateTask); err != nil {
			log.LogWarnf("retryFailTask updateMigrateTask err:Task:%+v err:%+v", needUpdateTask, err)
		}
	}
	return
}

func (c *Cluster) addMigrateTask(task *MigrateTask) error {
	return c.persistMigrateTask(opSyncAddMigrateTask, task)
}

func (c *Cluster) updateMigrateTask(task *MigrateTask) error {
	return c.persistMigrateTask(opSyncUpdateMigrateTask, task)
}

func (c *Cluster) delMigrateTask(task *MigrateTask) error {
	return c.persistMigrateTask(opSyncDeleteMigrateTask, task)
}

func (c *Cluster) persistMigrateTask(opType uint32, task *MigrateTask) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = migratePrefix + strconv.FormatUint(task.PartitionID, 10)
	metadata.V, err = json.Marshal(task)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) updateEcMigrateStatus(partitionId uint64, status uint8) (err error) {
	var (
		dp *DataPartition
		ep *EcDataPartition
	)
	dp, err = c.getDataPartitionByID(partitionId)
	if err != nil {
		return
	}
	dp.EcMigrateStatus = status
	if err = c.syncUpdateDataPartition(dp); err != nil {
		return
	}

	ep, err = c.getEcPartitionByID(partitionId)
	if err != nil {
		return
	}

	ep.EcMigrateStatus = status
	if status == proto.FinishEC {
		ep.FinishEcTime = time.Now().Unix()
	}
	err = c.syncUpdateEcDataPartition(ep)
	return
}

func (c *Cluster) finishEcMigrate(response *proto.CodecNodeMigrationResponse) (err error) {
	if !c.partition.IsRaftLeader() {
		err = errors.New("ec finishMigrate deal fail: current node is not leader")
		return
	}
	var migrateTask *MigrateTask
	migrateTask, exist := migrateTaskList.Exists(response.PartitionId)
	if !exist {
		return
	}


	if response.Status == proto.TaskSucceeds {
		if err = c.updateEcMigrateStatus(migrateTask.PartitionID, proto.FinishEC); err != nil {
			return
		}
		migrateTaskList.Remove(response.PartitionId)
		err = c.delMigrateTask(migrateTask)
		log.LogDebugf("partitionID[%d] migrate task sucess", response.PartitionId)
	} else if response.Status == proto.TaskRunning {
		migrateTask.ModifyTime = time.Now().Unix()
		migrateTask.CurrentExtentID = response.CurrentExtentID
		err = c.updateMigrateTask(migrateTask)
	} else {
		if migrateTask.Status == EcTaskMigrating {
			migrateTask.Status = EcTaskRetry
			migrateTask.ModifyTime = time.Now().Unix()
			migrateTask.CurrentExtentID = response.CurrentExtentID
			err = c.updateMigrateTask(migrateTask)
		}
		log.LogErrorf("partitionID[%d] migrate task fail; reason: %+v", response.PartitionId, response.Result)
	}
	return
}

func (c *Cluster) clearMigrateTask() {
	if migrateTaskList == nil {
		return
	}
	migrateTaskList.Clear()
}

func (c *Cluster) loadMigrateTask() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(migratePrefix))
	if err != nil {
		log.LogErrorf("action[loadMigrateTask],err:%v", err.Error())
		return
	}

	if migrateTaskList == nil {
		migrateTaskList = NewMigrateList()
	}
	for _, value := range result {
		migrate := &MigrateTask{}
		if err = json.Unmarshal(value, migrate); err != nil {
			log.LogWarnf("action[loadMigrateTask],value:%v,unmarshal err:%v", string(value), err)
			continue
		}
		migrateTaskList.Push(&MigrateTask{migrate.Status, migrate.RetryTimes,
			 migrate.VolName, migrate.PartitionID, migrate.CurrentExtentID, migrate.ModifyTime, migrate.CodecNode})
	}
	return
}

func (c *Cluster) ecMigrateById(partitionID uint64, test bool) (err error) {
	var (
		dp    *DataPartition
		exist bool
	)
	defer func() {
		if err != nil {
			log.LogErrorf("ecMigrateById err(%v)", err)
		}
	}()
	if dp, err = c.getDataPartitionByID(partitionID); err != nil {
		return
	}
	var vol *Vol
	if vol, err = c.getVol(dp.VolName); err != nil {
		return
	}

	if !vol.dpIsCanEc(dp) && test == false {
		err = errors.NewErrorf("dp can't migrate ec, please check dp status")
		return
	}

	needMigrate, err := c.needMigrateEc(vol, dp)
	if !needMigrate {
		log.LogDebugf("ecMigrateById didn't need Ec")
		return
	}

	var migrateTask *MigrateTask


	var ecdp *EcDataPartition
	if ecdp, err = c.createEcDataPartition(vol, dp); err != nil {
		log.LogErrorf("migrate createEcPartition failed:%+v", err)
		return
	}

	if !ecdp.canWrite() {
		err = errors.NewErrorf("partition[%v] can't write", ecdp.PartitionID)
		return
	}

	if dp.EcMigrateStatus != proto.NotEcMigrate {
		dp.EcMigrateStatus = proto.NotEcMigrate
		err = c.syncUpdateDataPartition(dp)
	}

	var codEcNode *CodecNode
	if codEcNode = c.getAvailCodEcnode(); codEcNode == nil {
		err = errors.New("get codecnode fail")
		return
	}

	migrateTask, exist = migrateTaskList.Exists(partitionID)
	if exist {
		if migrateTask.Status != EcTaskFail {
			err = errors.New("dp already running ec migration")
			return
		}
		migrateTask.Status = EcTaskMigrating
		migrateTask.RetryTimes = 0
		/*	migrateTaskList.Remove(partitionID)
			if err = c.delMigrateTask(migrateTask); err != nil {
				return
			} */
	}else {
		migrateTask = &MigrateTask{
			Status:        EcTaskMigrating,
			VolName:       dp.VolName,
			PartitionID:   partitionID,
			ModifyTime:    time.Now().Unix(),
			CodecNode:     codEcNode.Addr,
		}
	}

	if err = c.sendMigrateTask(ecdp, dp, codEcNode); err != nil {
		log.LogErrorf("migrate sendTask failed:%+v", err)
		return
	}

	if !exist {
		migrateTaskList.Push(migrateTask)

		if err = c.addMigrateTask(migrateTask); err != nil {
			log.LogWarnf("startMigrate addMigrateTask err:%+v", err)
		}
	}

	log.LogInfof("start migration partition: partition[%d] by ecMigrateById", ecdp.PartitionID)

	return
}

func (c *Cluster) delEcPartition(vol *Vol, ecDp *EcDataPartition) (err error){
	log.LogInfof("delEcPartition(%v)", ecDp.PartitionID)
	for _, host := range ecDp.Hosts {
		ecNode, err := c.ecNode(host)
		if err != nil {
			continue
		}
		task := ecDp.createTaskToDeleteEcPartition(host)
		_, err = ecNode.TaskManager.syncSendAdminTask(task)
		if err != nil {
			log.LogErrorf("dp(%v) rollback err(%v)", ecDp.PartitionID, err)
			return err
		}
	}
	vol.ecDataPartitions.delete(ecDp.PartitionID)
	c.syncDelEcDataPartition(ecDp)
	return
}

func (c *Cluster) ecRollBack(partitionID uint64, needDelEc bool) (err error) {
	var (
		ecDp *EcDataPartition
		dp   *DataPartition
		vol  *Vol
	)
	log.LogDebugf("start EcRollback partition(%v)", partitionID)
	if ecDp, err = c.getEcPartitionByID(partitionID); err != nil {
		return
	}

	if dp, err = c.getDataPartitionByID(partitionID); err != nil {
		return
	}

	if vol, err = c.getVol(dp.VolName); err != nil {
		return
	}

	if ecDp.EcMigrateStatus == proto.OnlyEcExist {
		err = errors.New("ec roll back to dp fail: dp already delete")
		return
	}

	dp.EcMigrateStatus = proto.RollBack
	err = c.syncUpdateDataPartition(dp)
	if err != nil {
		return
	}
	if ecDp.EcMigrateStatus != proto.FinishEC || needDelEc {
		migrateTaskList.Remove(ecDp.PartitionID)//stop migrating
		err = c.delEcPartition(vol, ecDp)
	}
	log.LogDebugf("end EcRollback partition(%v)", partitionID)
	return
}

func (c *Cluster) delDpAlreadyEc(partitionID uint64) (err error) {
	var (
		dp *DataPartition
	)
	if dp, err = c.getDataPartitionByID(partitionID); err != nil {
		return
	}

	if dp.EcMigrateStatus != proto.FinishEC {
		return errors.NewErrorf("EcMigrateStatus != FinishEC")
	}
	dp.doDelDpAlreadyEcTime = time.Now().Unix()
	//just record curTime, after 5 min, checkReplicaSaveTime handle dp delete
	var ecdp *EcDataPartition
	if ecdp, err = c.getEcPartitionByID(partitionID); err != nil {
		return
	}
	ecdp.EcMigrateStatus = proto.OnlyEcExist
	err = c.syncUpdateEcDataPartition(ecdp)
	return
}

func (c *Cluster) getAllTaskStatus() (taskView []*proto.MigrateTaskView, err error) {
	taskView = make([]*proto.MigrateTaskView, 0)
	for _, task := range migrateTaskList.GetAllTask() {
		taskview := &proto.MigrateTaskView{
			RetryTimes:      task.RetryTimes,
			VolName:         task.VolName,
			Status:          StatusMap[task.Status],
			PartitionID:     task.PartitionID,
			CurrentExtentID: task.CurrentExtentID,
			ModifyTime:      task.ModifyTime,
		}
		taskView = append(taskView, taskview)
	}

	return
}

func (c *Cluster) dealStopMigratingTaskById(partitionId uint64) (err error) {
	var (
		codec *CodecNode
	)
	migrateTask, exist := migrateTaskList.Exists(partitionId)
	if !exist {
		err = errors.NewErrorf("not fount MigrateTask")
		return
	}

	if codec, err = c.codecNode(migrateTask.CodecNode); err != nil {
		return
	}

	task := proto.NewAdminTask(proto.OpStopMigratingByDatapartitionTask, codec.Addr, nil)
	task.PartitionID = partitionId
	if _, err = codec.TaskManager.syncSendAdminTask(task); err != nil {
		log.LogErrorf("syncSendAdminTask error:%v", err)
	}
	return
}

func (c *Cluster) dealStopMigratingTaskByNode(addr string) (err error) {
	var (
		codecNodeCount int
		stopTasks      = make(map[string][]uint64)
		codec          *CodecNode
	)

	stopTasks = c.getStopTasksByNode(addr)
	codecNodeCount = len(stopTasks)
	if codecNodeCount == 0 {
		return fmt.Errorf("Node(%v) not fount Migrating task", addr)
	}
	log.LogInfof("getStopTasksByNode stopTasks(%v)", stopTasks)
	for codecAddr, stopTask := range stopTasks {
		if codec, err = c.codecNode(codecAddr); err != nil {
			return
		}
		task := proto.NewAdminTask(proto.OpStopMigratingByNodeTask, codec.Addr, stopTask)
		if _, err = codec.TaskManager.syncSendAdminTask(task); err != nil {
			log.LogErrorf("syncSendAdminTask error:%v", err)
		}

	}

	return
}

func (c *Cluster) getStopTasksByNode(addr string) (stopTasks map[string][]uint64) {
	stopTasks = make(map[string][]uint64)
	migrateTasks := migrateTaskList.GetAllTask()
	for _, task := range migrateTasks {
		dp, err := c.getDataPartitionByID(task.PartitionID)
		if err != nil {
			continue
		}
		if !contains(dp.Hosts, addr) {
			continue
		}
		stopTasks[task.CodecNode] = append(stopTasks[task.CodecNode], task.PartitionID)
	}
	return
}
