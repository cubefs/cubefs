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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

type Ver2PhaseCommit struct {
	op            uint8
	prepareInfo   *proto.VolVersionInfo
	commitCnt     uint32
	nodeCnt       uint32
	dataNodeArray *sync.Map
	metaNodeArray *sync.Map
}

func (commit *Ver2PhaseCommit) String() string {
	return fmt.Sprintf("prepareCommit:(op[%v] commitCnt[%v],nodeCnt[%v] info[%v])",
		commit.op, commit.commitCnt, commit.nodeCnt, commit.prepareInfo)
}

func (commit *Ver2PhaseCommit) reset() {
	commit.op = 0
	commit.commitCnt = 0
	commit.nodeCnt = 0
	// datanode and metanode will not allow change member during make snapshot
	commit.dataNodeArray = new(sync.Map)
	commit.metaNodeArray = new(sync.Map)
	log.LogDebugf("action[Ver2PhaseCommit.reset]")
}

type VolVersionManager struct {
	// ALL snapshots not include deleted one,deleted one should write in error log
	multiVersionList []*proto.VolVersionInfo
	vol              *Vol
	prepareCommit    *Ver2PhaseCommit
	status           uint32
	wait             chan error
	cancel           chan bool
	verSeq           uint64
	enabled          bool
	c                *Cluster
	sync.RWMutex
}

func newVersionMgr(vol *Vol) *VolVersionManager {
	return &VolVersionManager{
		vol:    vol,
		wait:   make(chan error, 1),
		cancel: make(chan bool, 1),
		prepareCommit: &Ver2PhaseCommit{
			dataNodeArray: new(sync.Map),
			metaNodeArray: new(sync.Map),
		},
	}
}
func (verMgr *VolVersionManager) String() string {
	return fmt.Sprintf("mgr:{vol[%v],status[%v] verSeq [%v], prepareinfo [%v]}",
		verMgr.vol.Name, verMgr.status, verMgr.verSeq, verMgr.prepareCommit)
}
func (verMgr *VolVersionManager) Persist() (err error) {
	var val []byte
	if val, err = json.Marshal(verMgr.multiVersionList); err != nil {
		return
	}
	if err = verMgr.c.syncMultiVersion(verMgr.vol, val); err != nil {
		return
	}
	return
}

func (verMgr *VolVersionManager) CommitVer() (ver *proto.VolVersionInfo) {
	log.LogInfof("action[CommitVer] %v", verMgr)
	if verMgr.prepareCommit.op == proto.CreateVersionPrepare {
		ver = verMgr.prepareCommit.prepareInfo
		commitVer := &proto.VolVersionInfo{
			Ver:    ver.Ver,
			Ctime:  ver.Ctime,
			Status: proto.VersionNormal,
		}
		verMgr.multiVersionList = append(verMgr.multiVersionList, commitVer)
		verMgr.verSeq = ver.Ver
		log.LogInfof("action[CommitVer] ask mgr do commit in next step version %v", ver)
		verMgr.wait <- nil
	} else if verMgr.prepareCommit.op == proto.DeleteVersion {
		idx, found := verMgr.getLayInfo(verMgr.prepareCommit.prepareInfo.Ver)
		if !found {
			log.LogErrorf("action[CommitVer] vol %v not found seq %v in list but commit", verMgr.vol.Name, verMgr.prepareCommit.prepareInfo.Ver)
			return
		}
		verMgr.multiVersionList[idx].Status = proto.VersionDeleting
		verMgr.multiVersionList[idx].DelTime = time.Now()
		if err := verMgr.Persist(); err != nil {
			log.LogErrorf("action[CommitVer] vol %v del seq %v persist error %v", verMgr.vol.Name, verMgr.prepareCommit.prepareInfo.Ver, err)
		}
	} else {
		log.LogErrorf("action[CommitVer] vol %v with seq %v wrong step", verMgr.vol.Name, verMgr.prepareCommit.prepareInfo.Ver)
	}
	log.LogInfof("action[CommitVer] verseq %v exit", verMgr.verSeq)
	return
}

func (verMgr *VolVersionManager) GenerateVer(verSeq uint64, op uint8) (err error) {
	log.LogInfof("action[GenerateVer] vol %v  enter verseq %v", verMgr.vol.Name, verSeq)
	verMgr.Lock()
	defer verMgr.Unlock()
	tm := time.Now()
	verMgr.enabled = true
	if len(verMgr.multiVersionList) > MaxSnapshotCount {
		err = fmt.Errorf("too much version exceed %v in list", MaxSnapshotCount)
		log.LogErrorf("action[GenerateVer] err %v", err)
		return
	}

	verMgr.prepareCommit.reset()
	verMgr.prepareCommit.prepareInfo = &proto.VolVersionInfo{
		Ver:    verSeq,
		Ctime:  tm,
		Status: proto.VersionNormal,
	}

	verMgr.prepareCommit.op = op
	size := len(verMgr.multiVersionList)
	if size > 0 && tm.Before(verMgr.multiVersionList[size-1].Ctime) {
		verMgr.prepareCommit.prepareInfo.Ctime = verMgr.multiVersionList[size-1].Ctime.Add(1)
		verMgr.prepareCommit.prepareInfo.Ver = uint64(verMgr.multiVersionList[size-1].Ctime.Unix() + 1)
		log.LogDebugf("action[GenerateVer] use ver %v", verMgr.prepareCommit.prepareInfo.Ver)
	}
	log.LogInfof("action[GenerateVer] exit")
	return
}

func (verMgr *VolVersionManager) DelVer(verSeq uint64) (err error) {
	verMgr.Lock()
	defer verMgr.Unlock()

	for i, ver := range verMgr.multiVersionList {
		if ver.Ver == verSeq {
			if ver.Status != proto.VersionDeleting && ver.Status != proto.VersionDeleteAbnormal {
				err = fmt.Errorf("with seq %v but it's status is %v", verSeq, ver.Status)
				log.LogErrorf("action[VolVersionManager.DelVer] err %v", err)
				return
			}
			verMgr.multiVersionList = append(verMgr.multiVersionList[:i], verMgr.multiVersionList[i+1:]...)
			break
		}
	}
	return
}

func (verMgr *VolVersionManager) UpdateVerStatus(verSeq uint64, status uint8) (err error) {
	verMgr.Lock()
	defer verMgr.Unlock()

	for _, ver := range verMgr.multiVersionList {
		if ver.Ver == verSeq {
			ver.Status = status
		}
		if ver.Ver > verSeq {
			return fmt.Errorf("not found")
		}
	}
	return
}

const (
	TypeNoReply      = 0
	TypeReply        = 1
	MaxSnapshotCount = 30
)

func (verMgr *VolVersionManager) handleTaskRsp(resp *proto.MultiVersionOpResponse, partitionType uint32) {

	verMgr.RLock()
	defer verMgr.RUnlock()
	log.LogInfof("action[handleTaskRsp] node %v partitionType %v,op %v, inner op %v",
		resp.Addr, partitionType, resp.Op, verMgr.prepareCommit.op)

	if resp.Op != verMgr.prepareCommit.op {
		log.LogErrorf("action[handleTaskRsp] op %v, inner op %v", resp.Op, verMgr.prepareCommit.op)
		return
	}

	if resp.Op != proto.DeleteVersion && resp.VerSeq != verMgr.prepareCommit.prepareInfo.Ver {
		log.LogErrorf("action[handleTaskRsp] op %v, inner verseq %v commit verseq %v",
			resp.Op, resp.VerSeq, verMgr.prepareCommit.prepareInfo.Ver)
		return
	}
	var needCommit bool
	dFunc := func(pType uint32, array *sync.Map) {
		if val, ok := array.Load(resp.Addr); ok {
			if rType, rok := val.(int); rok && rType == TypeNoReply {
				log.LogInfof("action[handleTaskRsp] node %v partitionType %v,op %v, inner op %v",
					resp.Addr, partitionType, resp.Op, verMgr.prepareCommit.op)
				array.Store(resp.Addr, TypeReply)

				if resp.Status != proto.TaskSucceeds || resp.Result != "" {
					log.LogErrorf("action[handleTaskRsp] type %v node %v rsp sucess. op %v, verseq %v,commit cnt %v, rsp status %v mgr status %v result %v",
						pType, resp.Addr, resp.Op, resp.VerSeq, atomic.LoadUint32(&verMgr.prepareCommit.commitCnt), resp.Status, verMgr.status, resp.Result)

					if verMgr.prepareCommit.prepareInfo.Status == proto.VersionWorking {
						verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
						verMgr.wait <- fmt.Errorf("pType %v node %v error %v", pType, resp.Addr, resp.Status)
						log.LogErrorf("action[handleTaskRsp] type %v commit cnt %v, rsp status %v mgr status %v result %v",
							pType, atomic.LoadUint32(&verMgr.prepareCommit.commitCnt), resp.Status, verMgr.status, resp.Result)
						return
					}
					return
				}
				if verMgr.prepareCommit.nodeCnt == atomic.AddUint32(&verMgr.prepareCommit.commitCnt, 1) {
					needCommit = true
				}
				log.LogInfof("action[handleTaskRsp] type %v node %v rsp sucess. op %v, verseq %v,commit cnt %v",
					pType, resp.Addr, resp.Op, resp.VerSeq, atomic.LoadUint32(&verMgr.prepareCommit.commitCnt))
			} else {
				log.LogWarnf("action[handleTaskRsp] type %v node %v op %v, inner verseq %v commit verseq %v status %v",
					pType, resp.Addr, resp.Op, resp.VerSeq, verMgr.prepareCommit.prepareInfo.Ver, val.(int))
			}
		} else {
			log.LogErrorf("action[handleTaskRsp] type %v node %v not found. op %v, inner verseq %v commit verseq %v",
				pType, resp.Addr, resp.Op, resp.VerSeq, verMgr.prepareCommit.prepareInfo.Ver)
		}
	}

	if partitionType == TypeDataPartition {
		dFunc(partitionType, verMgr.prepareCommit.dataNodeArray)
	} else {
		dFunc(partitionType, verMgr.prepareCommit.metaNodeArray)
	}

	log.LogInfof("action[handleTaskRsp] commit cnt %v, node cnt %v, operation %v", atomic.LoadUint32(&verMgr.prepareCommit.commitCnt),
		atomic.LoadUint32(&verMgr.prepareCommit.nodeCnt), verMgr.prepareCommit.op)

	if atomic.LoadUint32(&verMgr.prepareCommit.commitCnt) == verMgr.prepareCommit.nodeCnt && needCommit {
		if verMgr.prepareCommit.op == proto.DeleteVersion {
			verMgr.CommitVer()
			verMgr.prepareCommit.reset()
			verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingFinished
			log.LogWarnf("action[handleTaskRsp] do Del version finished, verMgr %v", verMgr)
		} else if verMgr.prepareCommit.op == proto.CreateVersionPrepare {
			log.LogInfof("action[handleTaskRsp] ver update prepare sucess. op %v, verseq %v,commit cnt %v",
				resp.Op, resp.VerSeq, atomic.LoadUint32(&verMgr.prepareCommit.commitCnt))
			verMgr.CommitVer()
		} else if verMgr.prepareCommit.op == proto.CreateVersionCommit {
			log.LogWarnf("action[handleTaskRsp] ver already update all node now! op %v, verseq %v,commit cnt %v",
				resp.Op, resp.VerSeq, atomic.LoadUint32(&verMgr.prepareCommit.commitCnt))
			verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingFinished
			verMgr.wait <- nil
		}
	}
}

func (verMgr *VolVersionManager) createVer2PhaseTask(cluster *Cluster, verSeq uint64, op uint8, force bool) (ver *proto.VolVersionInfo, err error) {
	if err = verMgr.startWork(); err != nil {
		return
	}
	defer func() {
		if err != nil {
			log.LogWarnf("action[createVer2PhaseTask] close lock due to err %v", err)
			verMgr.finishWork()
		}
	}()
	log.LogWarnf("action[createVer2PhaseTask] verMgr.status %v", verMgr.status)
	if op == proto.CreateVersion {
		if err = verMgr.GenerateVer(verSeq, op); err != nil {
			log.LogInfof("action[createVer2PhaseTask] exit")
			return
		}
	}

	log.LogInfof("action[createVer2PhaseTask] CreateVersionPrepare")
	if _, err = verMgr.createTask(cluster, verSeq, proto.CreateVersionPrepare, force); err != nil {
		log.LogInfof("action[createVer2PhaseTask] CreateVersionPrepare err %v", err)
		return
	}
	verMgr.prepareCommit.op = proto.CreateVersionPrepare
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() (ver *proto.VolVersionInfo, err error) {
		log.LogInfof("action[createVer2PhaseTask] verseq %v op %v enter wait schedule", verSeq, verMgr.prepareCommit.op)
		defer func() {
			log.LogDebugf("action[createVer2PhaseTask] status %v", verMgr.status)
			log.LogInfof("action[createVer2PhaseTask] verseq %v op %v exit wait schedule", verSeq, verMgr.prepareCommit.op)
			if err != nil {
				wg.Done()
				log.LogInfof("action[createVer2PhaseTask] verseq %v op %v exit schedule with err %v", verSeq, verMgr.prepareCommit.op, err)
			}
		}()
		ticker := time.NewTicker(time.Second)
		cnt := 0
		for {
			select {
			case err = <-verMgr.wait:
				log.LogInfof("action[createVer2PhaseTask] go routine verseq %v op %v get err %v", verSeq, verMgr.prepareCommit.op, err)
				if verMgr.prepareCommit.op == proto.CreateVersionPrepare {
					if err == nil {
						verMgr.verSeq = verSeq
						verMgr.prepareCommit.reset()
						verMgr.prepareCommit.op = proto.CreateVersionCommit
						if err = verMgr.Persist(); err != nil {
							log.LogErrorf("action[createVer2PhaseTask] err %v", err)
							return
						}
						log.LogInfof("action[createVer2PhaseTask] prepare fin.start commit")
						if ver, err = verMgr.createTask(cluster, verSeq, verMgr.prepareCommit.op, force); err != nil {
							log.LogInfof("action[createVer2PhaseTask] prepare error %v", err)
							return
						}
						wg.Done()
					} else {
						verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
						log.LogInfof("action[createVer2PhaseTask] prepare error %v", err)
						return
					}
				} else if verMgr.prepareCommit.op == proto.CreateVersionCommit {
					log.LogInfof("action[createVer2PhaseTask] create ver task commit, create 2phase finished")
					verMgr.prepareCommit.reset()
					verMgr.finishWork()
					return
				} else {
					log.LogErrorf("action[createVer2PhaseTask] op %v", verMgr.prepareCommit.op)
					return
				}
			case <-verMgr.cancel:
				log.LogInfof("action[createVer2PhaseTask.cancel] verseq %v op %v be canceled", verSeq, verMgr.prepareCommit.op)
				return
			case <-ticker.C:
				log.LogInfof("action[createVer2PhaseTask.tick] verseq %v op %v wait", verSeq, verMgr.prepareCommit.op)
				cnt++
				if cnt > 10 {
					verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingTimeOut
					err = fmt.Errorf("verseq %v op %v be set timeout", verSeq, verMgr.prepareCommit.op)
					log.LogInfof("action[createVer2PhaseTask] close lock due to err %v", err)
					verMgr.finishWork()
					if verMgr.prepareCommit.op == proto.CreateVersionPrepare {
						err = nil
					}
					return
				}
			}
		}
	}()
	wg.Wait()
	log.LogDebugf("action[createVer2PhaseTask] prepare phase finished")
	return
}

func (verMgr *VolVersionManager) createTaskToDataNode(cluster *Cluster, verSeq uint64, op uint8, force bool) (err error) {
	var (
		dpHost sync.Map
	)

	log.LogWarnf("action[createTaskToDataNode] vol %v verMgr.status %v verSeq %v op %v force %v", verMgr.vol.Name, verMgr.status, verSeq, op, force)
	for _, dp := range verMgr.vol.dataPartitions.clonePartitions() {
		for _, host := range dp.Hosts {
			dpHost.Store(host, nil)
		}
		dp.VerSeq = verSeq
	}

	tasks := make([]*proto.AdminTask, 0)
	cluster.dataNodes.Range(func(addr, dataNode interface{}) bool {
		if _, ok := dpHost.Load(addr); !ok {
			return true
		}
		node := dataNode.(*DataNode)
		node.checkLiveness()
		if !node.isActive && !force {
			err = fmt.Errorf("node %v not alive", node.Addr)
			verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
			return false
		}
		verMgr.prepareCommit.dataNodeArray.Store(node.Addr, TypeNoReply)
		verMgr.prepareCommit.nodeCnt++
		log.LogInfof("action[createTaskToDataNode] volume %v addr %v op %v verseq %v", verMgr.vol.Name, addr.(string), op, verSeq)
		task := node.createVersionTask(verMgr.vol.Name, verSeq, op, addr.(string))
		tasks = append(tasks, task)
		return true
	})

	if verMgr.prepareCommit.prepareInfo.Status != proto.VersionWorking {
		log.LogWarnf("action[verManager.createTask] vol %v status %v not working", verMgr.vol.Name, verMgr.status)
		return
	}
	log.LogInfof("action[verManager.createTask] verSeq %v, datanode task cnt %v", verSeq, len(tasks))
	cluster.addDataNodeTasks(tasks)

	return
}

func (verMgr *VolVersionManager) createTaskToMetaNode(cluster *Cluster, verSeq uint64, op uint8, force bool) (err error) {
	var (
		mpHost sync.Map
		ok     bool
	)
	log.LogInfof("action[verManager.createTaskToMetaNode] vol %v verSeq %v, dp cnt %v", verMgr.vol.Name, verSeq, len(verMgr.vol.MetaPartitions))
	verMgr.vol.mpsLock.RLock()
	for _, mp := range verMgr.vol.MetaPartitions {
		for _, host := range mp.Hosts {
			mpHost.Store(host, nil)
		}
		mp.VerSeq = verSeq
	}
	verMgr.vol.mpsLock.RUnlock()

	tasks := make([]*proto.AdminTask, 0)
	cluster.metaNodes.Range(func(addr, metaNode interface{}) bool {
		if _, ok = mpHost.Load(addr); !ok {
			return true
		}
		node := metaNode.(*MetaNode)
		if !node.IsActive && !force {
			err = fmt.Errorf("node %v not alive", node.Addr)
			verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
			return false
		}
		verMgr.prepareCommit.nodeCnt++
		verMgr.prepareCommit.metaNodeArray.Store(node.Addr, TypeNoReply)
		task := node.createVersionTask(verMgr.vol.Name, verSeq, op, addr.(string))
		tasks = append(tasks, task)
		return true
	})
	if verMgr.prepareCommit.prepareInfo.Status != proto.VersionWorking {
		return
	}

	log.LogInfof("action[verManager.createTaskToMetaNode] verSeq %v, metaNodes task cnt %v", verSeq, len(tasks))
	cluster.addMetaNodeTasks(tasks)
	return
}

func (verMgr *VolVersionManager) finishWork() {
	log.LogDebugf("action[finishWork] VolVersionManager finishWork!")
	atomic.StoreUint32(&verMgr.status, proto.VersionWorkingFinished)
}

func (verMgr *VolVersionManager) startWork() (err error) {
	var status uint32
	log.LogDebugf("action[VolVersionManager.startWork] status %v", verMgr.status)
	if status = atomic.LoadUint32(&verMgr.status); status == proto.VersionWorking {
		err = fmt.Errorf("have task still working,try it later")
		log.LogWarnf("action[VolVersionManager.startWork] %v", err)
		return
	}
	if !atomic.CompareAndSwapUint32(&verMgr.status, status, proto.VersionWorking) {
		err = fmt.Errorf("have task still working,try it later")
		log.LogWarnf("action[VolVersionManager.startWork] %v", err)
		return
	}
	return
}

func (verMgr *VolVersionManager) getLayInfo(verSeq uint64) (int, bool) {
	for idx, info := range verMgr.multiVersionList {
		if info.Ver == verSeq {
			return idx, true
		}
	}
	return 0, false
}

func (verMgr *VolVersionManager) createTask(cluster *Cluster, verSeq uint64, op uint8, force bool) (ver *proto.VolVersionInfo, err error) {
	log.LogInfof("action[VolVersionManager.createTask] vol %v verSeq %v op %v force %v", verMgr.vol.Name, verSeq, op, force)

	verMgr.RLock()
	defer verMgr.RUnlock()

	// del operation commit directly don't have prepare step to build prepare info,thus init here
	if op == proto.DeleteVersion {
		var (
			idx   int
			found bool
		)
		if idx, found = verMgr.getLayInfo(verSeq); !found {
			verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
			log.LogErrorf("action[VolVersionManager.createTask] vol %v op %v verSeq %v not found", verMgr.vol.Name, op, verSeq)
			return nil, fmt.Errorf("not found")
		}
		if idx == len(verMgr.multiVersionList)-1 {
			verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
			log.LogErrorf("action[VolVersionManager.createTask] vol %v op %v verSeq %v is uncommitted", verMgr.vol.Name, op, verSeq)
			return nil, fmt.Errorf("uncommited version")
		}
		if verMgr.multiVersionList[idx].Status == proto.VersionDeleting {
			log.LogErrorf("action[VolVersionManager.createTask] vol %v op %v verSeq %v is uncommitted", verMgr.vol.Name, op, verSeq)
			return nil, fmt.Errorf("version on deleting")
		}
		if verMgr.multiVersionList[idx].Status == proto.VersionDeleted {
			log.LogErrorf("action[VolVersionManager.createTask] vol %v op %v verSeq %v is uncommitted", verMgr.vol.Name, op, verSeq)
			return nil, fmt.Errorf("version alreay be deleted")
		}

		verMgr.prepareCommit.op = op
		verMgr.prepareCommit.prepareInfo =
			&proto.VolVersionInfo{
				Ver:    verSeq,
				Ctime:  time.Now(),
				Status: proto.VersionWorking,
			}
	}

	if err = verMgr.createTaskToDataNode(cluster, verSeq, op, force); err != nil {
		log.LogInfof("action[VolVersionManager.createTask] vol %v err %v", verMgr.vol.Name, err)
		return
	}

	if err = verMgr.createTaskToMetaNode(cluster, verSeq, op, force); err != nil {
		log.LogInfof("action[VolVersionManager.createTask] vol %v err %v", verMgr.vol.Name, err)
		return
	}

	log.LogInfof("action[VolVersionManager.createTask] exit")
	return
}

func (verMgr *VolVersionManager) init(cluster *Cluster) error {
	verMgr.c = cluster
	log.LogWarnf("action[VolVersionManager.init]")
	verMgr.multiVersionList = append(verMgr.multiVersionList, &proto.VolVersionInfo{
		Ver:    0,
		Ctime:  time.Now(),
		Status: 1,
	})
	if cluster.partition.IsRaftLeader() {
		return verMgr.Persist()
	}
	return nil
}

func (verMgr *VolVersionManager) loadMultiVersion(val []byte) (err error) {
	if err = json.Unmarshal(val, &verMgr.multiVersionList); err != nil {
		return
	}
	return nil
}

func (verMgr *VolVersionManager) getVersionInfo(verGet uint64) (verInfo *proto.VolVersionInfo, err error) {
	verMgr.RLock()
	defer verMgr.RUnlock()
	log.LogDebugf("action[getVersionInfo] verGet %v", verGet)
	for _, ver := range verMgr.multiVersionList {
		if ver.Ver == verGet {
			log.LogDebugf("action[getVersionInfo] ver %v", ver)
			return ver, nil
		}
		log.LogDebugf("action[getVersionInfo] ver %v", ver)
		if ver.Ver > verGet {
			log.LogDebugf("action[getVersionInfo] ver %v", ver)
			break
		}
	}
	msg := fmt.Sprintf("ver [%v] not found", verGet)
	log.LogInfof("action[getVersionInfo] %v", msg)
	return nil, fmt.Errorf("%v", msg)
}

func (verMgr *VolVersionManager) getLatestVer() (ver uint64) {
	verMgr.RLock()
	defer verMgr.RUnlock()

	size := len(verMgr.multiVersionList)
	if size == 0 {
		return 0
	}
	log.LogInfof("action[getLatestVer] ver len %v verMgr %v", size, verMgr)
	return verMgr.multiVersionList[size-1].Ver
}

func (verMgr *VolVersionManager) getVersionList() *proto.VolVersionInfoList {
	verMgr.RLock()
	defer verMgr.RUnlock()

	return &proto.VolVersionInfoList{
		VerList: verMgr.multiVersionList,
	}
}

type VolVarargs struct {
	zoneName                string
	description             string
	capacity                uint64 //GB
	deleteLockTime          int64  //h
	followerRead            bool
	authenticate            bool
	dpSelectorName          string
	dpSelectorParm          string
	coldArgs                *coldVolArgs
	domainId                uint64
	dpReplicaNum            uint8
	enablePosixAcl          bool
	dpReadOnlyWhenVolFull   bool
	enableQuota             bool
	enableTransaction       proto.TxOpMask
	txTimeout               int64
	txConflictRetryNum      int64
	txConflictRetryInterval int64
	txOpLimit               int
}

// Vol represents a set of meta partitionMap and data partitionMap
type Vol struct {
	ID                uint64
	Name              string
	Owner             string
	OSSAccessKey      string
	OSSSecretKey      string
	dpReplicaNum      uint8
	mpReplicaNum      uint8
	Status            uint8
	threshold         float32
	dataPartitionSize uint64 // byte
	Capacity          uint64 // GB
	VolType           int

	EbsBlkSize       int
	CacheCapacity    uint64
	CacheAction      int
	CacheThreshold   int
	CacheTTL         int
	CacheHighWater   int
	CacheLowWater    int
	CacheLRUInterval int
	CacheRule        string

	PreloadCacheOn          bool
	NeedToLowerReplica      bool
	FollowerRead            bool
	authenticate            bool
	crossZone               bool
	domainOn                bool
	defaultPriority         bool // old default zone first
	enablePosixAcl          bool
	enableTransaction       proto.TxOpMask
	txTimeout               int64
	txConflictRetryNum      int64
	txConflictRetryInterval int64
	txOpLimit               int
	zoneName                string
	MetaPartitions          map[uint64]*MetaPartition `graphql:"-"`
	mpsLock                 sync.RWMutex
	dataPartitions          *DataPartitionMap
	mpsCache                []byte
	viewCache               []byte
	createDpMutex           sync.RWMutex
	createMpMutex           sync.RWMutex
	createTime              int64
	DeleteLockTime          int64
	description             string
	dpSelectorName          string
	dpSelectorParm          string
	domainId                uint64
	qosManager              *QosCtrlManager
	DpReadOnlyWhenVolFull   bool
	aclMgr                  AclManager
	uidSpaceManager         *UidSpaceManager
	volLock                 sync.RWMutex
	quotaManager            *MasterQuotaManager
	enableQuota             bool
	VersionMgr              *VolVersionManager
}

func newVol(vv volValue) (vol *Vol) {

	vol = &Vol{ID: vv.ID, Name: vv.Name, MetaPartitions: make(map[uint64]*MetaPartition, 0)}

	if vol.threshold <= 0 {
		vol.threshold = defaultMetaPartitionMemUsageThreshold
	}

	vol.dataPartitions = newDataPartitionMap(vv.Name)
	vol.VersionMgr = newVersionMgr(vol)
	vol.dpReplicaNum = vv.DpReplicaNum
	vol.mpReplicaNum = vv.ReplicaNum
	vol.Owner = vv.Owner

	vol.dataPartitionSize = vv.DataPartitionSize
	vol.Capacity = vv.Capacity
	vol.FollowerRead = vv.FollowerRead
	vol.authenticate = vv.Authenticate
	vol.crossZone = vv.CrossZone
	vol.zoneName = vv.ZoneName
	vol.viewCache = make([]byte, 0)
	vol.mpsCache = make([]byte, 0)
	vol.createTime = vv.CreateTime
	vol.DeleteLockTime = vv.DeleteLockTime
	vol.description = vv.Description
	vol.defaultPriority = vv.DefaultPriority
	vol.domainId = vv.DomainId
	vol.enablePosixAcl = vv.EnablePosixAcl
	vol.enableQuota = vv.EnableQuota
	vol.enableTransaction = vv.EnableTransaction
	vol.txTimeout = vv.TxTimeout
	vol.txConflictRetryNum = vv.TxConflictRetryNum
	vol.txConflictRetryInterval = vv.TxConflictRetryInterval
	vol.txOpLimit = vv.TxOpLimit

	vol.VolType = vv.VolType
	vol.EbsBlkSize = vv.EbsBlkSize
	vol.CacheCapacity = vv.CacheCapacity
	vol.CacheAction = vv.CacheAction
	vol.CacheThreshold = vv.CacheThreshold
	vol.CacheTTL = vv.CacheTTL
	vol.CacheHighWater = vv.CacheHighWater
	vol.CacheLowWater = vv.CacheLowWater
	vol.CacheLRUInterval = vv.CacheLRUInterval
	vol.CacheRule = vv.CacheRule
	vol.Status = vv.Status

	limitQosVal := &qosArgs{
		qosEnable:     vv.VolQosEnable,
		diskQosEnable: vv.DiskQosEnable,
		iopsRVal:      vv.IopsRLimit,
		iopsWVal:      vv.IopsWLimit,
		flowRVal:      vv.FlowRlimit,
		flowWVal:      vv.FlowWlimit,
	}
	vol.initQosManager(limitQosVal)

	magnifyQosVal := &qosArgs{
		iopsRVal: uint64(vv.IopsRMagnify),
		iopsWVal: uint64(vv.IopsWMagnify),
		flowRVal: uint64(vv.FlowWMagnify),
		flowWVal: uint64(vv.FlowWMagnify),
	}
	vol.qosManager.volUpdateMagnify(magnifyQosVal)
	vol.DpReadOnlyWhenVolFull = vv.DpReadOnlyWhenVolFull
	return
}

func newVolFromVolValue(vv *volValue) (vol *Vol) {
	vol = newVol(*vv)
	// overwrite oss secure
	vol.OSSAccessKey, vol.OSSSecretKey = vv.OSSAccessKey, vv.OSSSecretKey
	vol.Status = vv.Status
	vol.dpSelectorName = vv.DpSelectorName
	vol.dpSelectorParm = vv.DpSelectorParm

	if vol.txTimeout == 0 {
		vol.txTimeout = proto.DefaultTransactionTimeout
	}
	if vol.txConflictRetryNum == 0 {
		vol.txConflictRetryNum = proto.DefaultTxConflictRetryNum
	}
	if vol.txConflictRetryInterval == 0 {
		vol.txConflictRetryInterval = proto.DefaultTxConflictRetryInterval
	}
	return vol
}

func (vol *Vol) getPreloadCapacity() uint64 {
	total := uint64(0)

	dps := vol.dataPartitions.partitions
	for _, dp := range dps {
		if proto.IsPreLoadDp(dp.PartitionType) {
			total += dp.total / util.GB
		}
	}

	if overSoldFactor <= 0 {
		return total
	}

	return uint64(float32(total) / overSoldFactor)
}

func (vol *Vol) initQosManager(limitArgs *qosArgs) {
	vol.qosManager = &QosCtrlManager{
		cliInfoMgrMap:        make(map[uint64]*ClientInfoMgr, 0),
		serverFactorLimitMap: make(map[uint32]*ServerFactorLimit, 0),
		qosEnable:            limitArgs.qosEnable,
		vol:                  vol,
		ClientHitTriggerCnt:  defaultClientTriggerHitCnt,
		ClientReqPeriod:      defaultClientReqPeriodSeconds,
	}

	if limitArgs.iopsRVal == 0 {
		limitArgs.iopsRVal = defaultIopsRLimit
	}
	if limitArgs.iopsWVal == 0 {
		limitArgs.iopsWVal = defaultIopsWLimit
	}
	if limitArgs.flowRVal == 0 {
		limitArgs.flowRVal = defaultFlowRLimit
	}
	if limitArgs.flowWVal == 0 {
		limitArgs.flowWVal = defaultFlowWLimit
	}
	arrLimit := [defaultLimitTypeCnt]uint64{limitArgs.iopsRVal, limitArgs.iopsWVal, limitArgs.flowRVal, limitArgs.flowWVal}
	arrType := [defaultLimitTypeCnt]uint32{proto.IopsReadType, proto.IopsWriteType, proto.FlowReadType, proto.FlowWriteType}

	for i := 0; i < defaultLimitTypeCnt; i++ {
		vol.qosManager.serverFactorLimitMap[arrType[i]] = &ServerFactorLimit{
			Name:       proto.QosTypeString(arrType[i]),
			Type:       arrType[i],
			Total:      arrLimit[i],
			Buffer:     arrLimit[i],
			requestCh:  make(chan interface{}, 10240),
			qosManager: vol.qosManager,
		}
		go vol.qosManager.serverFactorLimitMap[arrType[i]].dispatch()
	}

}

func (vol *Vol) refreshOSSSecure() (key, secret string) {
	vol.OSSAccessKey = util.RandomString(16, util.Numeric|util.LowerLetter|util.UpperLetter)
	vol.OSSSecretKey = util.RandomString(32, util.Numeric|util.LowerLetter|util.UpperLetter)
	return vol.OSSAccessKey, vol.OSSSecretKey
}

func (vol *Vol) addMetaPartition(mp *MetaPartition) {
	vol.mpsLock.Lock()
	defer vol.mpsLock.Unlock()
	if _, ok := vol.MetaPartitions[mp.PartitionID]; !ok {
		vol.MetaPartitions[mp.PartitionID] = mp
		return
	}
	// replace the old partition in the map with mp
	vol.MetaPartitions[mp.PartitionID] = mp
}

func (vol *Vol) metaPartition(partitionID uint64) (mp *MetaPartition, err error) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	mp, ok := vol.MetaPartitions[partitionID]
	if !ok {
		err = proto.ErrMetaPartitionNotExists
	}
	return
}

func (vol *Vol) maxPartitionID() (maxPartitionID uint64) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for id := range vol.MetaPartitions {
		if id > maxPartitionID {
			maxPartitionID = id
		}
	}
	return
}

func (vol *Vol) getDataPartitionsView() (body []byte, err error) {
	return vol.dataPartitions.updateResponseCache(false, 0, vol.VolType)
}

func (vol *Vol) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	return vol.dataPartitions.get(partitionID)
}

func (vol *Vol) initMetaPartitions(c *Cluster, count int) (err error) {
	// initialize k meta partitionMap at a time
	var (
		start uint64
		end   uint64
	)
	if count < defaultInitMetaPartitionCount {
		count = defaultInitMetaPartitionCount
	}
	if count > defaultMaxInitMetaPartitionCount {
		count = defaultMaxInitMetaPartitionCount
	}

	vol.createMpMutex.Lock()
	for index := 0; index < count; index++ {
		if index != 0 {
			start = end + 1
		}
		end = gConfig.MetaPartitionInodeIdStep * uint64(index+1)
		if index == count-1 {
			end = defaultMaxMetaPartitionInodeID
		}
		if err = vol.createMetaPartition(c, start, end); err != nil {
			log.LogErrorf("action[initMetaPartitions] vol[%v] init meta partition err[%v]", vol.Name, err)
			break
		}
	}
	vol.createMpMutex.Unlock()

	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	if len(vol.MetaPartitions) != count {
		err = fmt.Errorf("action[initMetaPartitions] vol[%v] init meta partition failed,mpCount[%v],expectCount[%v],err[%v]",
			vol.Name, len(vol.MetaPartitions), count, err)
	}
	return
}

func (vol *Vol) initDataPartitions(c *Cluster) (err error) {
	// initialize k data partitionMap at a time
	err = c.batchCreateDataPartition(vol, defaultInitDataPartitionCnt, true)
	return
}

func (vol *Vol) checkDataPartitions(c *Cluster) (cnt int) {
	if vol.getDataPartitionsCount() == 0 && vol.Status != markDelete && proto.IsHot(vol.VolType) {
		c.batchCreateDataPartition(vol, 1, false)
	}

	shouldDpInhibitWriteByVolFull := vol.shouldInhibitWriteBySpaceFull()

	partitions := vol.dataPartitions.clonePartitions()
	for _, dp := range partitions {

		if proto.IsPreLoadDp(dp.PartitionType) {
			now := time.Now().Unix()
			if now > dp.PartitionTTL {
				log.LogWarnf("[checkDataPartitions] dp(%d) is deleted because of ttl expired, now(%d), ttl(%d)", dp.PartitionID, now, dp.PartitionTTL)
				vol.deleteDataPartition(c, dp)
			}

			startTime := dp.dataNodeStartTime()
			if now-dp.createTime > 600 && dp.used == 0 && now-startTime > 600 {
				log.LogWarnf("[checkDataPartitions] dp(%d) is deleted because of clear, now(%d), create(%d), start(%d)",
					dp.PartitionID, now, dp.createTime, startTime)
				vol.deleteDataPartition(c, dp)
			}
		}

		dp.checkReplicaStatus(c.cfg.DataPartitionTimeOutSec)
		dp.checkStatus(c.Name, true, c.cfg.DataPartitionTimeOutSec, c, shouldDpInhibitWriteByVolFull)
		dp.checkLeader(c.Name, c.cfg.DataPartitionTimeOutSec)
		dp.checkMissingReplicas(c.Name, c.leaderInfo.addr, c.cfg.MissingDataPartitionInterval, c.cfg.IntervalToAlarmMissingDataPartition)
		dp.checkReplicaNum(c, vol)

		if time.Now().Unix()-vol.createTime < defaultIntervalToCheckHeartbeat*3 {
			dp.setReadWrite()
		}

		if dp.Status == proto.ReadWrite {
			cnt++
		}

		dp.checkDiskError(c.Name, c.leaderInfo.addr)

		dp.checkReplicationTask(c.Name, vol.dataPartitionSize)
	}

	return
}

func (vol *Vol) loadDataPartition(c *Cluster) {
	partitions, startIndex := vol.dataPartitions.getDataPartitionsToBeChecked(c.cfg.PeriodToLoadALLDataPartitions)
	if len(partitions) == 0 {
		return
	}
	c.waitForResponseToLoadDataPartition(partitions)
	msg := fmt.Sprintf("action[loadDataPartition] vol[%v],checkStartIndex:%v checkCount:%v",
		vol.Name, startIndex, len(partitions))
	log.LogInfo(msg)
}

func (vol *Vol) releaseDataPartitions(releaseCount int, afterLoadSeconds int64) {
	partitions, startIndex := vol.dataPartitions.getDataPartitionsToBeReleased(releaseCount, afterLoadSeconds)
	if len(partitions) == 0 {
		return
	}
	vol.dataPartitions.freeMemOccupiedByDataPartitions(partitions)
	msg := fmt.Sprintf("action[freeMemOccupiedByDataPartitions] vol[%v] release data partition start:%v releaseCount:%v",
		vol.Name, startIndex, len(partitions))
	log.LogInfo(msg)
}

func (vol *Vol) tryUpdateDpReplicaNum(c *Cluster, partition *DataPartition) (err error) {
	partition.RLock()
	defer partition.RUnlock()

	if partition.isRecover || vol.dpReplicaNum != 2 || partition.ReplicaNum != 3 || len(partition.Hosts) != 2 {
		return
	}

	if partition.isSpecialReplicaCnt() {
		partition.SingleDecommissionStatus = 0
		partition.SingleDecommissionAddr = ""
		return
	}
	oldReplicaNum := partition.ReplicaNum
	partition.ReplicaNum = partition.ReplicaNum - 1

	if err = c.syncUpdateDataPartition(partition); err != nil {
		partition.ReplicaNum = oldReplicaNum
	}
	return
}

func (vol *Vol) isOkUpdateRepCnt() (ok bool, rsp []uint64) {

	if proto.IsCold(vol.VolType) {
		return
	}
	ok = true
	dps := vol.cloneDataPartitionMap()
	for _, dp := range dps {
		if vol.dpReplicaNum != dp.ReplicaNum {
			rsp = append(rsp, dp.PartitionID)
			ok = false
			// output dps detail info
			if len(rsp) > 20 {
				return
			}
		}
	}
	return ok, rsp
}

func (vol *Vol) checkReplicaNum(c *Cluster) {
	if !vol.NeedToLowerReplica {
		return
	}
	var err error
	if proto.IsCold(vol.VolType) {
		return
	}

	dps := vol.cloneDataPartitionMap()
	cnt := 0
	for _, dp := range dps {
		host := dp.getToBeDecommissionHost(int(vol.dpReplicaNum))
		if host == "" {
			continue
		}
		if err = dp.removeOneReplicaByHost(c, host, vol.dpReplicaNum == dp.ReplicaNum); err != nil {
			if dp.isSpecialReplicaCnt() && len(dp.Hosts) > 1 {
				log.LogWarnf("action[checkReplicaNum] removeOneReplicaByHost host [%v],vol[%v],err[%v]", host, vol.Name, err)
				continue
			}
			log.LogErrorf("action[checkReplicaNum] removeOneReplicaByHost host [%v],vol[%v],err[%v]", host, vol.Name, err)
			continue
		}
		cnt++
		if cnt > 100 {
			return
		}
	}
	vol.NeedToLowerReplica = false
}

func (vol *Vol) checkMetaPartitions(c *Cluster) {
	var tasks []*proto.AdminTask
	metaPartitionInodeIdStep := gConfig.MetaPartitionInodeIdStep
	vol.checkSplitMetaPartition(c, metaPartitionInodeIdStep)
	maxPartitionID := vol.maxPartitionID()
	mps := vol.cloneMetaPartitionMap()
	var (
		doSplit bool
		err     error
	)
	for _, mp := range mps {
		doSplit = mp.checkStatus(c.Name, true, int(vol.mpReplicaNum), maxPartitionID, metaPartitionInodeIdStep)
		if doSplit && !c.cfg.DisableAutoCreate {
			nextStart := mp.MaxInodeID + metaPartitionInodeIdStep
			log.LogInfof(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits start[%v] maxinodeid:[%v] default step:[%v],nextStart[%v]",
				c.Name, vol.Name, mp.PartitionID, mp.Start, mp.MaxInodeID, metaPartitionInodeIdStep, nextStart))
			if err = vol.splitMetaPartition(c, mp, nextStart, metaPartitionInodeIdStep); err != nil {
				Warn(c.Name, fmt.Sprintf("cluster[%v],vol[%v],meta partition[%v] splits failed,err[%v]", c.Name, vol.Name, mp.PartitionID, err))
			}
		}

		mp.checkLeader(c.Name)
		mp.checkReplicaNum(c, vol.Name, vol.mpReplicaNum)
		mp.checkEnd(c, maxPartitionID)
		mp.reportMissingReplicas(c.Name, c.leaderInfo.addr, defaultMetaPartitionTimeOutSec, defaultIntervalToAlarmMissingMetaPartition)
		tasks = append(tasks, mp.replicaCreationTasks(c.Name, vol.Name)...)
	}
	c.addMetaNodeTasks(tasks)
}

func (vol *Vol) checkSplitMetaPartition(c *Cluster, metaPartitionInodeIdStep uint64) {
	maxPartitionID := vol.maxPartitionID()

	vol.mpsLock.RLock()
	partition, ok := vol.MetaPartitions[maxPartitionID]
	if !ok {
		vol.mpsLock.RUnlock()
		return
	}
	vol.mpsLock.RUnlock()

	liveReplicas := partition.getLiveReplicas()
	foundReadonlyReplica := false
	var readonlyReplica *MetaReplica
	for _, replica := range liveReplicas {
		if replica.Status == proto.ReadOnly {
			foundReadonlyReplica = true
			readonlyReplica = replica
			break
		}
	}
	if !foundReadonlyReplica {
		return
	}
	if readonlyReplica.metaNode.isWritable() {
		msg := fmt.Sprintf("action[checkSplitMetaPartition] vol[%v],max meta parition[%v] status is readonly\n",
			vol.Name, partition.PartitionID)
		Warn(c.Name, msg)
		return
	}

	if c.cfg.DisableAutoCreate {
		log.LogWarnf("action[checkSplitMetaPartition] vol[%v], mp [%v] disable auto create meta partition",
			vol.Name, partition.PartitionID)
		return
	}

	end := partition.MaxInodeID + metaPartitionInodeIdStep
	if err := vol.splitMetaPartition(c, partition, end, metaPartitionInodeIdStep); err != nil {
		msg := fmt.Sprintf("action[checkSplitMetaPartition],split meta partition[%v] failed,err[%v]\n",
			partition.PartitionID, err)
		Warn(c.Name, msg)
	}
}

func (vol *Vol) cloneMetaPartitionMap() (mps map[uint64]*MetaPartition) {
	mps = make(map[uint64]*MetaPartition, 0)
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		mps[mp.PartitionID] = mp
	}
	return
}

func (vol *Vol) cloneDataPartitionMap() (dps map[uint64]*DataPartition) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	dps = make(map[uint64]*DataPartition, 0)
	for _, dp := range vol.dataPartitions.partitionMap {
		dps[dp.PartitionID] = dp
	}
	return
}

func (vol *Vol) setStatus(status uint8) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	vol.Status = status
}

func (vol *Vol) status() uint8 {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.Status
}

func (vol *Vol) capacity() uint64 {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.Capacity
}

func (vol *Vol) autoDeleteDp(c *Cluster) {

	if vol.dataPartitions == nil {
		return
	}

	maxSize := overSoldCap(vol.CacheCapacity * util.GB)
	maxCnt := maxSize / vol.dataPartitionSize

	if maxSize%vol.dataPartitionSize != 0 {
		maxCnt++
	}

	partitions := vol.dataPartitions.clonePartitions()
	for _, dp := range partitions {
		if !proto.IsCacheDp(dp.PartitionType) {
			continue
		}

		if maxCnt > 0 {
			maxCnt--
			continue
		}

		log.LogInfof("[autoDeleteDp] start delete dp, id[%d]", dp.PartitionID)
		vol.deleteDataPartition(c, dp)
	}
}

func (vol *Vol) checkAutoDataPartitionCreation(c *Cluster) {

	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkAutoDataPartitionCreation occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkAutoDataPartitionCreation occurred panic")
		}
	}()

	if ok, _ := vol.needCreateDataPartition(); !ok {
		return
	}

	vol.setStatus(normal)
	log.LogInfof("action[autoCreateDataPartitions] vol[%v] before autoCreateDataPartitions", vol.Name)
	if !c.DisableAutoAllocate {
		vol.autoCreateDataPartitions(c)
	}
}

func (vol *Vol) shouldInhibitWriteBySpaceFull() bool {
	if !vol.DpReadOnlyWhenVolFull {
		return false
	}

	if vol.capacity() == 0 {
		return false
	}

	if !proto.IsHot(vol.VolType) {
		return false
	}

	usedSpace := vol.totalUsedSpace() / util.GB
	if usedSpace >= vol.capacity() {
		return true
	}

	return false
}

func (vol *Vol) needCreateDataPartition() (ok bool, err error) {

	ok = false
	if vol.status() == markDelete {
		err = proto.ErrVolNotExists
		return
	}

	if vol.capacity() == 0 {
		err = proto.ErrVolNoAvailableSpace
		return
	}

	if proto.IsHot(vol.VolType) {
		if vol.shouldInhibitWriteBySpaceFull() {
			vol.setAllDataPartitionsToReadOnly()
			err = proto.ErrVolNoAvailableSpace
			return
		}
		ok = true
		return
	}

	// cold
	if vol.CacheAction == proto.NoCache && vol.CacheRule == "" {
		err = proto.ErrVolNoCacheAndRule
		return
	}

	ok = true
	return
}

func (vol *Vol) autoCreateDataPartitions(c *Cluster) {

	if time.Since(vol.dataPartitions.lastAutoCreateTime) < time.Minute {
		return
	}

	if c.cfg.DisableAutoCreate {
		// if disable auto create, once alloc size is over capacity, not allow to create new dp
		allocSize := uint64(len(vol.dataPartitions.partitions)) * vol.dataPartitionSize
		totalSize := vol.capacity() * util.GB
		if allocSize > totalSize {
			return
		}

		if vol.dataPartitions.readableAndWritableCnt < minNumOfRWDataPartitions {
			c.batchCreateDataPartition(vol, minNumOfRWDataPartitions, false)
			log.LogWarnf("autoCreateDataPartitions: readWrite less than 10, alloc new 10 partitions, vol %s", vol.Name)
		}

		return
	}

	if proto.IsCold(vol.VolType) {

		vol.dataPartitions.lastAutoCreateTime = time.Now()
		maxSize := overSoldCap(vol.CacheCapacity * util.GB)
		allocSize := uint64(0)
		for _, dp := range vol.cloneDataPartitionMap() {
			if !proto.IsCacheDp(dp.PartitionType) {
				continue
			}

			allocSize += dp.total
		}

		if maxSize <= allocSize {
			log.LogInfof("action[autoCreateDataPartitions] (%s) no need to create again, alloc [%d], max [%d]", vol.Name, allocSize, maxSize)
			return
		}

		count := (maxSize-allocSize-1)/vol.dataPartitionSize + 1
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		c.batchCreateDataPartition(vol, int(count), false)
		return
	}

	if (vol.Capacity > 200000 && vol.dataPartitions.readableAndWritableCnt < 200) || vol.dataPartitions.readableAndWritableCnt < minNumOfRWDataPartitions {
		vol.dataPartitions.lastAutoCreateTime = time.Now()
		count := vol.calculateExpansionNum()
		log.LogInfof("action[autoCreateDataPartitions] vol[%v] count[%v]", vol.Name, count)
		c.batchCreateDataPartition(vol, count, false)
	}
}

// Calculate the expansion number (the number of data partitions to be allocated to the given volume)
func (vol *Vol) calculateExpansionNum() (count int) {
	c := float64(vol.Capacity) * volExpansionRatio * float64(util.GB) / float64(util.DefaultDataPartitionSize)
	switch {
	case c < minNumOfRWDataPartitions:
		count = minNumOfRWDataPartitions
	case c > maxNumberOfDataPartitionsForExpansion:
		count = maxNumberOfDataPartitionsForExpansion
	default:
		count = int(c)
	}
	return
}

func (vol *Vol) setAllDataPartitionsToReadOnly() {
	vol.dataPartitions.setAllDataPartitionsToReadOnly()
}

func (vol *Vol) totalUsedSpace() uint64 {
	return vol.totalUsedSpaceByMeta(false)
}

func (vol *Vol) totalUsedSpaceByMeta(byMeta bool) uint64 {
	if proto.IsCold(vol.VolType) || byMeta {
		return vol.ebsUsedSpace()
	}

	return vol.cfsUsedSpace()
}

func (vol *Vol) cfsUsedSpace() uint64 {
	return vol.dataPartitions.totalUsedSpace()
}

func (vol *Vol) sendViewCacheToFollower(c *Cluster) {
	var err error
	log.LogInfof("action[asyncSendPartitionsToFollower]")

	metadata := new(RaftCmd)
	metadata.Op = opSyncDataPartitionsView
	metadata.K = vol.Name
	metadata.V = vol.dataPartitions.getDataPartitionResponseCache()

	if err = c.submit(metadata); err != nil {
		log.LogErrorf("action[asyncSendPartitionsToFollower] error [%v]", err)
	}
	log.LogInfof("action[asyncSendPartitionsToFollower] finished")
}

func (vol *Vol) ebsUsedSpace() uint64 {

	size := uint64(0)
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()

	for _, pt := range vol.MetaPartitions {
		size += pt.dataSize()
	}

	return size
}

func (vol *Vol) updateViewCache(c *Cluster) {
	view := proto.NewVolView(vol.Name, vol.Status, vol.FollowerRead, vol.createTime, vol.CacheTTL, vol.VolType, vol.DeleteLockTime)
	view.SetOwner(vol.Owner)
	view.SetOSSSecure(vol.OSSAccessKey, vol.OSSSecretKey)
	mpViews := vol.getMetaPartitionsView()
	view.MetaPartitions = mpViews
	mpViewsReply := newSuccessHTTPReply(mpViews)
	mpsBody, err := json.Marshal(mpViewsReply)
	if err != nil {
		log.LogErrorf("action[updateViewCache] failed,vol[%v],err[%v]", vol.Name, err)
		return
	}
	vol.setMpsCache(mpsBody)
	//dpResps := vol.dataPartitions.getDataPartitionsView(0)
	//view.DataPartitions = dpResps
	view.DomainOn = vol.domainOn
	viewReply := newSuccessHTTPReply(view)
	body, err := json.Marshal(viewReply)
	if err != nil {
		log.LogErrorf("action[updateViewCache] failed,vol[%v],err[%v]", vol.Name, err)
		return
	}
	vol.setViewCache(body)
}

func (vol *Vol) getMetaPartitionsView() (mpViews []*proto.MetaPartitionView) {
	mps := make(map[uint64]*MetaPartition)
	vol.mpsLock.RLock()
	for key, mp := range vol.MetaPartitions {
		mps[key] = mp
	}
	vol.mpsLock.RUnlock()

	mpViews = make([]*proto.MetaPartitionView, 0)
	for _, mp := range mps {
		mpViews = append(mpViews, getMetaPartitionView(mp))
	}
	return
}

func (vol *Vol) setMpsCache(body []byte) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	vol.mpsCache = body
}

func (vol *Vol) getMpsCache() []byte {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.mpsCache
}

func (vol *Vol) setViewCache(body []byte) {
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	vol.viewCache = body
}

func (vol *Vol) getViewCache() []byte {
	vol.volLock.RLock()
	defer vol.volLock.RUnlock()
	return vol.viewCache
}

func (vol *Vol) deleteDataPartition(c *Cluster, dp *DataPartition) {

	var addrs []string
	for _, replica := range dp.Replicas {
		addrs = append(addrs, replica.Addr)
	}

	for _, addr := range addrs {

		if err := vol.deleteDataPartitionFromDataNode(c, dp.createTaskToDeleteDataPartition(addr)); err != nil {
			log.LogErrorf("[deleteDataPartitionFromDataNode] delete data replica from datanode fail, id %d, err %s", dp.PartitionID, err.Error())
		}
	}

	vol.dataPartitions.del(dp)

	err := c.syncDeleteDataPartition(dp)
	if err != nil {
		log.LogErrorf("[deleteDataPartition] delete data partition from store fail, [%d], err: %s", dp.PartitionID, err.Error())
		return
	}

	log.LogInfof("[deleteDataPartition] delete data partition success, [%d]", dp.PartitionID)
}

// Periodically check the volume's status.
// If an volume is marked as deleted, then generate corresponding delete task (meta partition or data partition)
// If all the meta partition and data partition of this volume have been deleted, then delete this volume.
func (vol *Vol) checkStatus(c *Cluster) {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkStatus occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkStatus occurred panic")
		}
	}()
	vol.updateViewCache(c)
	vol.volLock.Lock()
	defer vol.volLock.Unlock()
	if vol.Status != markDelete {
		return
	}
	log.LogInfof("action[volCheckStatus] vol[%v],status[%v]", vol.Name, vol.Status)
	metaTasks := vol.getTasksToDeleteMetaPartitions()
	dataTasks := vol.getTasksToDeleteDataPartitions()

	if len(metaTasks) == 0 && len(dataTasks) == 0 {
		vol.deleteVolFromStore(c)
	}
	go func() {
		for _, metaTask := range metaTasks {
			vol.deleteMetaPartitionFromMetaNode(c, metaTask)
		}

		for _, dataTask := range dataTasks {
			vol.deleteDataPartitionFromDataNode(c, dataTask)
		}
	}()

	return
}

func (vol *Vol) deleteMetaPartitionFromMetaNode(c *Cluster, task *proto.AdminTask) {
	mp, err := vol.metaPartition(task.PartitionID)
	if err != nil {
		return
	}
	metaNode, err := c.metaNode(task.OperatorAddr)
	if err != nil {
		return
	}

	mp.RLock()
	_, err = mp.getMetaReplica(task.OperatorAddr)
	mp.RUnlock()
	if err != nil {
		log.LogWarnf("deleteMetaPartitionFromMetaNode (%s) maybe alread been deleted", task.ToString())
		return
	}

	_, err = metaNode.Sender.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteMetaPartition] vol[%v],meta partition[%v],err[%v]", mp.volName, mp.PartitionID, err)
		return
	}
	mp.Lock()
	mp.removeReplicaByAddr(metaNode.Addr)
	mp.removeMissingReplica(metaNode.Addr)
	mp.Unlock()
	return
}

func (vol *Vol) deleteDataPartitionFromDataNode(c *Cluster, task *proto.AdminTask) (err error) {
	dp, err := vol.getDataPartitionByID(task.PartitionID)
	if err != nil {
		return
	}

	dataNode, err := c.dataNode(task.OperatorAddr)
	if err != nil {
		return
	}

	dp.RLock()
	_, ok := dp.hasReplica(task.OperatorAddr)
	dp.RUnlock()
	if !ok {
		log.LogWarnf("deleteDataPartitionFromDataNode task(%s) maybe already executed", task.ToString())
		return
	}

	_, err = dataNode.TaskManager.syncSendAdminTask(task)
	if err != nil {
		log.LogErrorf("action[deleteDataReplica] vol[%v],data partition[%v],err[%v]", dp.VolName, dp.PartitionID, err)
		return
	}

	dp.Lock()
	dp.removeReplicaByAddr(dataNode.Addr)
	dp.checkAndRemoveMissReplica(dataNode.Addr)
	if err = dp.update("deleteDataReplica", dp.VolName, dp.Peers, dp.Hosts, c); err != nil {
		dp.Unlock()
		return
	}
	dp.Unlock()

	return
}

func (vol *Vol) deleteVolFromStore(c *Cluster) (err error) {

	if err = c.syncDeleteVol(vol); err != nil {
		return
	}

	// delete the metadata of the meta and data partitionMap first
	vol.deleteDataPartitionsFromStore(c)
	vol.deleteMetaPartitionsFromStore(c)
	// then delete the volume
	c.deleteVol(vol.Name)
	c.volStatInfo.Delete(vol.Name)

	c.DelBucketLifecycle(vol.Name)
	return
}

func (vol *Vol) deleteMetaPartitionsFromStore(c *Cluster) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		c.syncDeleteMetaPartition(mp)
	}
	return
}

func (vol *Vol) deleteDataPartitionsFromStore(c *Cluster) {
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	for _, dp := range vol.dataPartitions.partitions {
		c.syncDeleteDataPartition(dp)
	}

}

func (vol *Vol) getTasksToDeleteMetaPartitions() (tasks []*proto.AdminTask) {
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	tasks = make([]*proto.AdminTask, 0)

	for _, mp := range vol.MetaPartitions {
		log.LogDebugf("get delete task from vol(%s) mp(%d)", vol.Name, mp.PartitionID)
		for _, replica := range mp.Replicas {
			log.LogDebugf("get delete task from vol(%s) mp(%d),replica(%v)", vol.Name, mp.PartitionID, replica.Addr)
			tasks = append(tasks, replica.createTaskToDeleteReplica(mp.PartitionID))
		}
	}
	return
}

func (vol *Vol) getTasksToDeleteDataPartitions() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()

	for _, dp := range vol.dataPartitions.partitions {
		for _, replica := range dp.Replicas {
			tasks = append(tasks, dp.createTaskToDeleteDataPartition(replica.Addr))
		}
	}
	return
}

func (vol *Vol) getDataPartitionsCount() (count int) {
	vol.volLock.RLock()
	count = len(vol.dataPartitions.partitionMap)
	vol.volLock.RUnlock()
	return
}

func (vol *Vol) String() string {
	return fmt.Sprintf("name[%v],dpNum[%v],mpNum[%v],cap[%v],status[%v]",
		vol.Name, vol.dpReplicaNum, vol.mpReplicaNum, vol.Capacity, vol.Status)
}

func (vol *Vol) doSplitMetaPartition(c *Cluster, mp *MetaPartition, end uint64, metaPartitionInodeIdStep uint64) (nextMp *MetaPartition, err error) {
	mp.Lock()
	defer mp.Unlock()

	if err = mp.canSplit(end, metaPartitionInodeIdStep); err != nil {
		return
	}

	log.LogWarnf("action[splitMetaPartition],partition[%v],start[%v],end[%v],new end[%v]", mp.PartitionID, mp.Start, mp.End, end)
	cmdMap := make(map[string]*RaftCmd, 0)
	oldEnd := mp.End
	mp.End = end

	updateMpRaftCmd, err := c.buildMetaPartitionRaftCmd(opSyncUpdateMetaPartition, mp)
	if err != nil {
		return
	}

	cmdMap[updateMpRaftCmd.K] = updateMpRaftCmd
	if nextMp, err = vol.doCreateMetaPartition(c, mp.End+1, defaultMaxMetaPartitionInodeID); err != nil {
		Warn(c.Name, fmt.Sprintf("action[updateEnd] clusterID[%v] partitionID[%v] create meta partition err[%v]",
			c.Name, mp.PartitionID, err))
		log.LogErrorf("action[updateEnd] partitionID[%v] err[%v]", mp.PartitionID, err)
		return
	}

	addMpRaftCmd, err := c.buildMetaPartitionRaftCmd(opSyncAddMetaPartition, nextMp)
	if err != nil {
		return
	}

	cmdMap[addMpRaftCmd.K] = addMpRaftCmd
	if err = c.syncBatchCommitCmd(cmdMap); err != nil {
		mp.End = oldEnd
		return nil, errors.NewError(err)
	}

	mp.updateInodeIDRangeForAllReplicas()
	mp.addUpdateMetaReplicaTask(c)
	return
}

func (vol *Vol) splitMetaPartition(c *Cluster, mp *MetaPartition, end uint64, metaPartitionInodeIdStep uint64) (err error) {
	if c.DisableAutoAllocate {
		return
	}

	vol.createMpMutex.Lock()
	defer vol.createMpMutex.Unlock()

	maxPartitionID := vol.maxPartitionID()
	if maxPartitionID != mp.PartitionID {
		err = fmt.Errorf("mp[%v] is not the last meta partition[%v]", mp.PartitionID, maxPartitionID)
		return
	}

	nextMp, err := vol.doSplitMetaPartition(c, mp, end, metaPartitionInodeIdStep)
	if err != nil {
		return
	}

	vol.addMetaPartition(nextMp)
	log.LogWarnf("action[splitMetaPartition],next partition[%v],start[%v],end[%v]", nextMp.PartitionID, nextMp.Start, nextMp.End)
	return
}

func (vol *Vol) createMetaPartition(c *Cluster, start, end uint64) (err error) {
	var mp *MetaPartition
	if mp, err = vol.doCreateMetaPartition(c, start, end); err != nil {
		return
	}
	if err = c.syncAddMetaPartition(mp); err != nil {
		return errors.NewError(err)
	}
	vol.addMetaPartition(mp)
	return
}

func (vol *Vol) doCreateMetaPartition(c *Cluster, start, end uint64) (mp *MetaPartition, err error) {
	var (
		hosts       []string
		partitionID uint64
		peers       []proto.Peer
		wg          sync.WaitGroup
	)

	errChannel := make(chan error, vol.mpReplicaNum)

	if c.isFaultDomain(vol) {
		if hosts, peers, err = c.getHostFromDomainZone(vol.domainId, TypeMetaPartition, vol.mpReplicaNum); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] getHostFromDomainZone err[%v]", err)
			return nil, errors.NewError(err)
		}

	} else {
		var excludeZone []string
		zoneNum := c.decideZoneNum(vol.crossZone)

		if hosts, peers, err = c.getHostFromNormalZone(TypeMetaPartition, excludeZone, nil, nil, int(vol.mpReplicaNum), zoneNum, vol.zoneName); err != nil {
			log.LogErrorf("action[doCreateMetaPartition] getHostFromNormalZone err[%v]", err)
			return nil, errors.NewError(err)
		}

	}

	log.LogInfof("target meta hosts:%v,peers:%v", hosts, peers)
	if partitionID, err = c.idAlloc.allocateMetaPartitionID(); err != nil {
		return nil, errors.NewError(err)
	}

	mp = newMetaPartition(partitionID, start, end, vol.mpReplicaNum, vol.Name, vol.ID, vol.VersionMgr.getLatestVer())
	mp.setHosts(hosts)
	mp.setPeers(peers)

	for _, host := range hosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			if err = c.syncCreateMetaPartitionToMetaNode(host, mp); err != nil {
				errChannel <- err
				return
			}
			mp.Lock()
			defer mp.Unlock()
			if err = mp.afterCreation(host, c); err != nil {
				errChannel <- err
			}
		}(host)
	}

	wg.Wait()

	select {
	case err = <-errChannel:
		for _, host := range hosts {
			wg.Add(1)
			go func(host string) {
				defer func() {
					wg.Done()
				}()
				mr, err := mp.getMetaReplica(host)
				if err != nil {
					return
				}
				task := mr.createTaskToDeleteReplica(mp.PartitionID)
				tasks := make([]*proto.AdminTask, 0)
				tasks = append(tasks, task)
				c.addMetaNodeTasks(tasks)
			}(host)
		}
		wg.Wait()
		return nil, errors.NewError(err)
	default:
		mp.Status = proto.ReadWrite
	}
	log.LogInfof("action[doCreateMetaPartition] success,volName[%v],partition[%v],start[%v],end[%v]", vol.Name, partitionID, start, end)
	return
}

func setVolFromArgs(args *VolVarargs, vol *Vol) {
	vol.zoneName = args.zoneName
	vol.Capacity = args.capacity
	vol.DeleteLockTime = args.deleteLockTime
	vol.FollowerRead = args.followerRead
	vol.authenticate = args.authenticate
	vol.enablePosixAcl = args.enablePosixAcl
	vol.DpReadOnlyWhenVolFull = args.dpReadOnlyWhenVolFull
	vol.enableQuota = args.enableQuota
	vol.enableTransaction = args.enableTransaction
	vol.txTimeout = args.txTimeout
	vol.txConflictRetryNum = args.txConflictRetryNum
	vol.txConflictRetryInterval = args.txConflictRetryInterval
	vol.txOpLimit = args.txOpLimit
	vol.dpReplicaNum = args.dpReplicaNum

	if proto.IsCold(vol.VolType) {
		coldArgs := args.coldArgs
		vol.CacheLRUInterval = coldArgs.cacheLRUInterval
		vol.CacheLowWater = coldArgs.cacheLowWater
		vol.CacheHighWater = coldArgs.cacheHighWater
		vol.CacheTTL = coldArgs.cacheTtl
		vol.CacheThreshold = coldArgs.cacheThreshold
		vol.CacheAction = coldArgs.cacheAction
		vol.CacheRule = coldArgs.cacheRule
		vol.CacheCapacity = coldArgs.cacheCap
		vol.EbsBlkSize = coldArgs.objBlockSize
	}

	vol.description = args.description

	vol.dpSelectorName = args.dpSelectorName
	vol.dpSelectorParm = args.dpSelectorParm
}

func getVolVarargs(vol *Vol) *VolVarargs {

	args := &coldVolArgs{
		objBlockSize:     vol.EbsBlkSize,
		cacheCap:         vol.CacheCapacity,
		cacheAction:      vol.CacheAction,
		cacheThreshold:   vol.CacheThreshold,
		cacheTtl:         vol.CacheTTL,
		cacheHighWater:   vol.CacheHighWater,
		cacheLowWater:    vol.CacheLowWater,
		cacheLRUInterval: vol.CacheLRUInterval,
		cacheRule:        vol.CacheRule,
	}

	return &VolVarargs{
		zoneName:                vol.zoneName,
		description:             vol.description,
		capacity:                vol.Capacity,
		deleteLockTime:          vol.DeleteLockTime,
		followerRead:            vol.FollowerRead,
		authenticate:            vol.authenticate,
		dpSelectorName:          vol.dpSelectorName,
		dpSelectorParm:          vol.dpSelectorParm,
		enablePosixAcl:          vol.enablePosixAcl,
		enableQuota:             vol.enableQuota,
		dpReplicaNum:            vol.dpReplicaNum,
		enableTransaction:       vol.enableTransaction,
		txTimeout:               vol.txTimeout,
		txConflictRetryNum:      vol.txConflictRetryNum,
		txConflictRetryInterval: vol.txConflictRetryInterval,
		txOpLimit:               vol.txOpLimit,
		coldArgs:                args,
		dpReadOnlyWhenVolFull:   vol.DpReadOnlyWhenVolFull,
	}
}

func (vol *Vol) initQuotaManager(c *Cluster) {
	vol.quotaManager = &MasterQuotaManager{
		MpQuotaInfoMap: make(map[uint64][]*proto.QuotaReportInfo),
		IdQuotaInfoMap: make(map[uint32]*proto.QuotaInfo),
		c:              c,
		vol:            vol,
	}
}

func (vol *Vol) loadQuotaManager(c *Cluster) (err error) {
	vol.quotaManager = &MasterQuotaManager{
		MpQuotaInfoMap: make(map[uint64][]*proto.QuotaReportInfo),
		IdQuotaInfoMap: make(map[uint32]*proto.QuotaInfo),
		c:              c,
		vol:            vol,
	}

	result, err := c.fsm.store.SeekForPrefix([]byte(quotaPrefix + strconv.FormatUint(vol.ID, 10) + keySeparator))
	if err != nil {
		err = fmt.Errorf("loadQuotaManager get quota failed, err [%v]", err)
		return err
	}

	for _, value := range result {
		var quotaInfo = &proto.QuotaInfo{}

		if err = json.Unmarshal(value, quotaInfo); err != nil {
			log.LogErrorf("loadQuotaManager Unmarshal fail err [%v]", err)
			return err
		}
		log.LogDebugf("loadQuotaManager info [%v]", quotaInfo)
		if vol.Name != quotaInfo.VolName {
			panic(fmt.Sprintf("vol name do not match vol name [%v], quotaInfo vol name [%v]", vol.Name, quotaInfo.VolName))
		}
		vol.quotaManager.IdQuotaInfoMap[quotaInfo.QuotaId] = quotaInfo
	}

	return err
}
