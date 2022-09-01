package master

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"sync/atomic"
	"time"
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
		verMgr.wait <- nil
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
			//verMgr.prepareCommit.reset()
			//verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingFinished
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
		if !node.isActive {
			if !force {
				err = fmt.Errorf("node %v not alive", node.Addr)
				verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
				return false
			}
			atomic.AddUint32(&verMgr.prepareCommit.commitCnt, 1)
			log.LogInfof("action[createTaskToDataNode] volume %v addr %v op %v verseq %v force commit in advance", verMgr.vol.Name, addr.(string), op, verSeq)
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
		if !node.IsActive {
			if !force {
				err = fmt.Errorf("node %v not alive", node.Addr)
				verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
				return false
			}
			atomic.AddUint32(&verMgr.prepareCommit.commitCnt, 1)
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

func (verMgr *VolVersionManager) initVer2PhaseTask(verSeq uint64, op uint8) (verRsp *proto.VolVersionInfo, err error, opRes uint8) {
	log.LogWarnf("action[VolVersionManager.initVer2PhaseTask] verMgr.status %v op %v", verMgr.status, op)
	if op == proto.CreateVersion {
		if err = verMgr.GenerateVer(verSeq, op); err != nil {
			log.LogInfof("action[VolVersionManager.initVer2PhaseTask] exit")
			return
		}
		op = proto.CreateVersionPrepare
		log.LogInfof("action[VolVersionManager.initVer2PhaseTask] CreateVersionPrepare")
	} else if op == proto.DeleteVersion {
		var (
			idx   int
			found bool
		)

		if ver, status := verMgr.getOldestVer(); ver != verSeq || status != proto.VersionNormal {
			err = fmt.Errorf("oldest is %v, status %v", ver, status)
			return
		}

		if idx, found = verMgr.getLayInfo(verSeq); !found {
			verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
			log.LogErrorf("action[VolVersionManager.initVer2PhaseTask] vol %v op %v verSeq %v not found", verMgr.vol.Name, op, verSeq)
			return nil, fmt.Errorf("not found"), op
		}
		if idx == len(verMgr.multiVersionList)-1 {
			verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
			log.LogErrorf("action[VolVersionManager.initVer2PhaseTask] vol %v op %v verSeq %v is uncommitted", verMgr.vol.Name, op, verSeq)
			return nil, fmt.Errorf("uncommited version"), op
		}
		if verMgr.multiVersionList[idx].Status == proto.VersionDeleting {
			log.LogErrorf("action[VolVersionManager.initVer2PhaseTask] vol %v op %v verSeq %v is uncommitted", verMgr.vol.Name, op, verSeq)
			return nil, fmt.Errorf("version on deleting"), op
		}
		if verMgr.multiVersionList[idx].Status == proto.VersionDeleted {
			log.LogErrorf("action[VolVersionManager.initVer2PhaseTask] vol %v op %v verSeq %v is uncommitted", verMgr.vol.Name, op, verSeq)
			return nil, fmt.Errorf("version alreay be deleted"), op
		}

		verMgr.prepareCommit.op = op
		verMgr.prepareCommit.prepareInfo =
			&proto.VolVersionInfo{
				Ver:    verSeq,
				Ctime:  time.Now(),
				Status: proto.VersionWorking,
			}
	}
	opRes = op
	return
}

func (verMgr *VolVersionManager) createVer2PhaseTask(cluster *Cluster, verSeq uint64, op uint8, force bool) (verRsp *proto.VolVersionInfo, err error) {
	if err = verMgr.startWork(); err != nil {
		return
	}
	if !proto.IsHot(verMgr.vol.VolType) {
		err = fmt.Errorf("vol need be hot one")
		log.LogErrorf("createVer2PhaseTask. %v", err)
		return
	}
	defer func() {
		if err != nil {
			log.LogWarnf("action[createVer2PhaseTask] close lock due to err %v", err)
			verMgr.finishWork()
		}
	}()

	if verRsp, err, op = verMgr.initVer2PhaseTask(verSeq, op); err != nil {
		return
	}

	if _, err = verMgr.createTask(cluster, verSeq, op, force); err != nil {
		log.LogInfof("action[createVer2PhaseTask] CreateVersionPrepare err %v", err)
		return
	}
	verMgr.prepareCommit.op = op
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
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
				if verMgr.prepareCommit.op == proto.DeleteVersion {
					if err == nil {
						verMgr.prepareCommit.reset()
						if err = verMgr.Persist(); err != nil {
							log.LogErrorf("action[createVer2PhaseTask] err %v", err)
							return
						}
						verMgr.finishWork()
						wg.Done()
					} else {
						verMgr.prepareCommit.reset()
						verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingAbnormal
						log.LogInfof("action[createVer2PhaseTask] prepare error %v", err)
					}
					return
				} else if verMgr.prepareCommit.op == proto.CreateVersionPrepare {
					if err == nil {
						verMgr.verSeq = verSeq
						verMgr.prepareCommit.reset()
						verMgr.prepareCommit.op = proto.CreateVersionCommit
						if err = verMgr.Persist(); err != nil {
							log.LogErrorf("action[createVer2PhaseTask] err %v", err)
							return
						}
						log.LogInfof("action[createVer2PhaseTask] prepare fin.start commit")
						if _, err = verMgr.createTask(cluster, verSeq, verMgr.prepareCommit.op, force); err != nil {
							log.LogInfof("action[createVer2PhaseTask] prepare error %v", err)
							return
						}
						if vLen := len(verMgr.multiVersionList); vLen > 1 {
							verRsp = verMgr.multiVersionList[vLen-2]
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
				if cnt > 5 {
					verMgr.prepareCommit.prepareInfo.Status = proto.VersionWorkingTimeOut
					err = fmt.Errorf("verseq %v op %v be set timeout", verSeq, verMgr.prepareCommit.op)
					log.LogInfof("action[createVer2PhaseTask] close lock due to err %v", err)
					verMgr.finishWork()
					if verMgr.prepareCommit.op == proto.CreateVersionCommit {
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

func (verMgr *VolVersionManager) loadMultiVersion(c *Cluster, val []byte) (err error) {
	verMgr.c = c
	if err = json.Unmarshal(val, &verMgr.multiVersionList); err != nil {
		return
	}
	return nil
}

func (verMgr *VolVersionManager) getVersionInfo(verGet uint64) (verInfo *proto.VolVersionInfo, err error) {
	verMgr.RLock()
	defer verMgr.RUnlock()

	if !proto.IsHot(verMgr.vol.VolType) {
		err = fmt.Errorf("vol need be hot one")
		log.LogErrorf("createVer2PhaseTask. %v", err)
		return
	}

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

func (verMgr *VolVersionManager) getOldestVer() (ver uint64, status uint8) {
	verMgr.RLock()
	defer verMgr.RUnlock()

	size := len(verMgr.multiVersionList)
	if size <= 1 {
		return 0, proto.VersionDeleteAbnormal
	}
	log.LogInfof("action[getLatestVer] ver len %v verMgr %v", size, verMgr)
	return verMgr.multiVersionList[0].Ver, verMgr.multiVersionList[0].Status
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
