package compact

import (
	"container/list"
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/sdk/mysql"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
	"sync"
)

type CmpMpTask struct {
	sync.RWMutex
	wg    sync.WaitGroup
	id    uint64
	Name  string
	State proto.MPCompactState
	task  *proto.Task

	elemP  *list.Element
	vol    *CompactVolumeInfo
	mc     *master.MasterClient
	info   *meta.MetaPartition
	leader string

	cmpEkCnt    uint64
	newEkCnt    uint64
	cmpInodeCnt uint64
	cmpCnt      uint64
	cmpSize     uint64
	cmpErrCnt   uint64
	cmpErrMsg   map[int]string

	inodes  []uint64
	last    int
}

func NewMpCmpTask(task *proto.Task, masterClient *master.MasterClient, vol *CompactVolumeInfo) *CmpMpTask {
	cmpMpTask := &CmpMpTask{id: task.MpId, mc: masterClient, vol: vol, task: task}
	cmpMpTask.Name = fmt.Sprintf("%s#%s#%d", task.Cluster, task.VolName, task.MpId)
	cmpMpTask.cmpErrMsg = make(map[int]string, 0)
	return cmpMpTask
}

func (mp *CmpMpTask) InitTask() {
	mp.Lock()
	defer mp.Unlock()
	mp.State = proto.MPCmpGetMPInfo
}

func (mp *CmpMpTask) GetMpInfo() (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("MP[%v] State[%v] failed:%s", mp.id, mp.State, err.Error())
			return
		}
		mp.State = proto.MPCmpGetProfPort
	}()
	var info *proto.MetaPartitionInfo
	cMP := &meta.MetaPartition{PartitionID: mp.id}
	info, err = mp.mc.ClientAPI().GetMetaPartition(mp.id)
	if err != nil {
		log.LogErrorf("get meta partition[%d] info failed, error [%s]", mp.id, err.Error())
		return
	}
	for _, replica := range info.Replicas {
		cMP.Members = append(cMP.Members, replica.Addr)
		if replica.IsLeader {
			cMP.LeaderAddr = replica.Addr
		}
		if replica.IsLearner {
			cMP.Learners = append(cMP.Learners, replica.Addr)
		}
	}
	cMP.Status = info.Status
	cMP.Start = info.Start
	cMP.End = info.End

	mp.Lock()
	defer mp.Unlock()
	mp.info = cMP
	if cMP.LeaderAddr == "" {
		return fmt.Errorf("get metapartition[%d] no leader", mp.id)
	}
	mp.leader = cMP.LeaderAddr
	return
}

func (mp *CmpMpTask) GetProfPort() (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("MP[%v] State[%v] failed:%s", mp.id, mp.State, err.Error())
			return
		}
		mp.State = proto.MPCmpListAllIno
	}()

	var leaderNodeInfo *proto.MetaNodeInfo
	if leaderNodeInfo, err = mp.mc.NodeAPI().GetMetaNode(mp.leader); err != nil {
		log.LogErrorf("get metanode[%s] info failed:%s", mp.leader, err.Error())
		return
	}

	mp.Lock()
	defer mp.Unlock()
	mp.leader = strings.Split(leaderNodeInfo.Addr, ":")[0] + ":" + leaderNodeInfo.ProfPort
	return
}

func (mp *CmpMpTask) ListAllIno() (err error) {
	defer func() {
		if err != nil {
			mp.State = proto.MPCmpStopped
			log.LogErrorf("[ListAllIno] list all inodes failed, mpName(%v) mpId(%v) state(%v) err(%v)", mp.Name, mp.id, mp.State, err)
			return
		}
		if len(mp.inodes) == 0 {
			mp.State = proto.MPCmpStopped
			return
		}
		mp.State = proto.MPCmpGetCmpInodes
	}()

	metaAdminApi := meta.NewMetaHttpClient(mp.leader, false)
	resp, err := metaAdminApi.ListAllInodesId(mp.id, 0, 0, 0)
	if err != nil {
		return
	}
	mp.Lock()
	defer mp.Unlock()
	mp.inodes = mp.inodes[:0]
	mp.inodes = append(mp.inodes, resp.Inodes...)
	return
}

func (mp *CmpMpTask) GetCmpInodes(isIgnoreCompactSwitch bool) (err error) {
	defer func() {
		if err != nil {
			mp.resetInode()
			mp.State = proto.MPCmpGetMPInfo
			return
		}
		mp.State = proto.MPCmpWaitSubTask
	}()

	end := mp.last + mp.vol.GetInodeCheckStep()
	if end > len(mp.inodes) {
		end = len(mp.inodes)
	}
	inos := mp.inodes[mp.last:end]
	log.LogDebugf("[GetCmpInodes] cmpMpTask.Name(%v) cmpMpTask.last(%v) end(%v) cmpMpTask.inodes:(%v)", mp.Name, mp.last, end, inos)
	inodeConcurrentPerMP := mp.vol.GetInodeConcurrentPerMP()
	minEkLen, minInodeSize, maxEkAvgSize := mp.vol.GetInodeFilterParams()
	cmpInodes, err := mp.vol.metaClient.GetCmpInode_ll(context.Background(), mp.id, inos, inodeConcurrentPerMP, minEkLen, minInodeSize, maxEkAvgSize)
	if err != nil {
		log.LogErrorf("[GetCmpInodes] cmpMpTask.vol.metaClient.GetCmpInode_ll: mpName(%v) mpId(%v) state(%v) err(%v)", mp.Name, mp.id, mp.State, err)
		return
	}
	maxIno := uint64(0)
	for _, cmpInode := range cmpInodes {
		mp.wg.Add(1)
		subTask := NewCmpInodeTask(mp, cmpInode, mp.vol)
		go func(subTask *CmpInodeTask) {
			defer mp.wg.Done()
			_, subTaskErr := subTask.RunOnce(isIgnoreCompactSwitch)
			if subTaskErr != nil {
				log.LogErrorf("[subTask] RunOnce, mpName(%v) mpId(%v) state(%v) err(%v)", mp.Name, mp.id, mp.State, subTaskErr)
			}
		}(subTask)

		if cmpInode.Inode.Inode > maxIno {
			maxIno = cmpInode.Inode.Inode
		}
	}
	mp.wg.Wait()

	if len(cmpInodes) == 0 {
		mp.last = end
	} else {
		for ; mp.last < len(mp.inodes); mp.last++ {
			if mp.inodes[mp.last] >= maxIno {
				//next run will start at this point
				break
			}
		}
		mp.last++
	}
	return
}

func (mp *CmpMpTask) WaitCmpSubTask() (err error) {
	if mp.last >= len(mp.inodes) {
		mp.resetInode()
		mp.State = proto.MPCmpStopped
	} else {
		mp.State = proto.MPCmpGetCmpInodes
	}
	return
}

func (mp *CmpMpTask) StopCmp() (err error) {
	mp.resetInode()
	return
}

func (mp *CmpMpTask) resetInode() {
	mp.last = 0
	mp.inodes = mp.inodes[:0]
}

func (mp *CmpMpTask) RunOnce(isIgnoreCompactSwitch bool) (finished bool, err error) {
	defer func() {
		mp.vol.DelMPRunningCnt(mp.id)
	}()
	if !mp.vol.AddMPRunningCnt(mp.id) {
		return true, nil
	}
	if len(mp.inodes) == 0 {
		mp.last = 0
		mp.State = proto.MPCmpGetMPInfo
	}
	defer func() {
		if mp.cmpInodeCnt <= 0 {
			return
		}
		var msg strings.Builder
		for _, errMsg := range mp.cmpErrMsg {
			msg.WriteString(errMsg + "##")
		}
		if err := mysql.AddCompactSummary(mp.task, mp.cmpEkCnt, mp.newEkCnt, mp.cmpInodeCnt, mp.cmpCnt, mp.cmpSize, mp.cmpErrCnt, msg.String()); err != nil {
			log.LogErrorf("AddCompactSummary add tasks to mysql failed, tasks(%v), err(%v)", mp.task, err)
		}
	}()
	for err == nil {
		if !isIgnoreCompactSwitch && !mp.vol.isRunning() {
			log.LogDebugf("cmpMpTask compact cancel because vol(%v) be stopped, cmpMpTask.Name(%v) cmpMpTask.State(%v)", mp.vol.Name, mp.Name, mp.State)
			mp.State = proto.MPCmpStopped
		}
		log.LogDebugf("cmpMpTask runonce cmpMpTask.Name(%v) cmpMpTask.State(%v)", mp.Name, mp.State)

		switch mp.State {
		case proto.MPCmpGetMPInfo:
			err = mp.GetMpInfo()
		case proto.MPCmpGetProfPort:
			err = mp.GetProfPort()
		case proto.MPCmpListAllIno:
			err = mp.ListAllIno()
		case proto.MPCmpGetCmpInodes:
			err = mp.GetCmpInodes(isIgnoreCompactSwitch)
		case proto.MPCmpWaitSubTask:
			err = mp.WaitCmpSubTask()
		case proto.MPCmpStopped:
			err = mp.StopCmp()
			finished = true
			return
		default:
			err = nil
			return
		}
	}
	return
}

func (mp *CmpMpTask) GetTaskName() string {
	return mp.Name
}

func (mp *CmpMpTask) GetTaskState() uint8 {
	return uint8(mp.State)
}

func (mp *CmpMpTask) GetTaskCompactVolumeInfo() *CompactVolumeInfo {
	return mp.vol
}

func (mp *CmpMpTask) ResumeTask() {
	mp.State = proto.MPCmpGetMPInfo
}

func (mp *CmpMpTask) GetClientMetaPartition() *meta.MetaPartition {
	mp.RLock()
	defer mp.RUnlock()
	return mp.info
}

func (mp *CmpMpTask) UpdateStatisticsInfo(info StatisticsInfo) {
	mp.Lock()
	defer mp.Unlock()
	mp.cmpCnt += info.CmpCnt
	mp.cmpInodeCnt += info.CmpInodeCnt
	mp.cmpEkCnt += info.CmpEkCnt
	mp.newEkCnt += info.NewEkCnt
	mp.cmpSize += info.CmpSize
	mp.cmpErrCnt += info.CmpErrCnt
	if info.CmpErrCode >= InodeOpenFailedCode && info.CmpErrCode <= InodeMergeFailedCode {
		mp.cmpErrMsg[info.CmpErrCode] = info.CmpErrMsg
	}
}
