package convertnode

import (
	"container/list"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ConvertingMpInfo struct {
	partitionID          uint64
	targetStoreMode      proto.StoreMode
	selectedReplNodeInfo *proto.SelectMetaNodeInfo
}

type ConvertTaskConfig struct {
	mpReplicaNum              uint8
	memModeMPTargetNum        int
	rocksModeReplicaTargetNum int
}

type ConvertTask struct {
	sync.RWMutex
	info                   *proto.ConvertTaskInfo
	mc                     *ClusterInfo
	elemP                  *list.Element
	elemC                  *list.Element
	mpInfo                 *proto.MetaPartitionInfo
	lastErr                error
	errCnt                 uint32
	db                     *DBInfo
	conf                   ConvertTaskConfig
	isContinueLastTask     bool
	isNeedConvertToMemMode bool
	convertingMP           *ConvertingMpInfo
	lastConvertMP          *ConvertingMpInfo
	nextStageStartRunTime  time.Time
}

func NewTaskInfo(id int32, clusterName, volName string, mc *ClusterInfo, db *DBInfo) *ConvertTask {
	task := new(ConvertTask)
	info := new(proto.ConvertTaskInfo)
	info.ProcessorID = id
	info.VolName = volName
	info.ClusterName = clusterName
	info.TaskState = proto.TaskInit
	info.SelectedMP = make([]uint64, 0)
	info.FinishedMP = make([]uint64, 0)

	task.info = info
	task.mc = mc
	task.db = db
	return task
}

func (task *ConvertTask) taskName() string {
	return fmt.Sprintf("%s_%s", task.info.ClusterName, task.info.VolName)
}

func (task *ConvertTask) setConvertingMpInfo(pid uint64, storeMode proto.StoreMode, selectedReplNode, oldNode string) {
	task.convertingMP = &ConvertingMpInfo{
		partitionID:          pid,
		targetStoreMode:      storeMode,
		selectedReplNodeInfo: &proto.SelectMetaNodeInfo{
			PartitionID: pid,
			OldNodeAddr: oldNode,
			NewNodeAddr: selectedReplNode,
		},
	}
}

func (task *ConvertTask) transform() *TaskInfo {
	return &TaskInfo{
		ClusterName:          task.info.ClusterName,
		VolumeName:           task.info.VolName,
		SelectedMP:           formatMetaPartitionsIdToString(task.info.SelectedMP),
		FinishedMP:           formatMetaPartitionsIdToString(task.info.FinishedMP),
		RunningMP:            task.info.RunningMP,
		Layout:               fmt.Sprintf("%v,%v", task.info.Layout.PercentOfMP, task.info.Layout.PercentOfReplica),
		TaskStage:            uint32(task.info.TaskState),
		MPConvertStage:       uint32(task.info.MPConvertState),
		SelectedReplNodeAddr: task.convertingMP.selectedReplNodeInfo.NewNodeAddr,
		OldNodeAddr:          task.convertingMP.selectedReplNodeInfo.OldNodeAddr,
	}
}

func (task *ConvertTask) genTaskViewInfo() *proto.ConvertTaskInfo {
	task.Lock()
	defer task.Unlock()
	r := new(proto.ConvertTaskInfo)
	r.ProcessorID = task.info.ProcessorID
	r.ClusterName = task.info.ClusterName
	r.VolName = task.info.VolName
	r.SelectedMP = append(r.SelectedMP, task.info.SelectedMP...)
	r.FinishedMP = append(r.FinishedMP, task.info.FinishedMP...)
	r.IgnoredMP = append(r.IgnoredMP, task.info.IgnoredMP...)
	r.RunningMP = task.info.RunningMP
	r.Layout = task.info.Layout
	r.TaskState = task.info.TaskState
	r.MPConvertState = task.info.MPConvertState
	r.ErrMsg = task.info.ErrMsg
	return r
}

func formatMetaPartitionsIdToString(partitionsId []uint64) string {
	strSlice := make([]string, 0, len(partitionsId))
	for _, pid := range partitionsId {
		strSlice = append(strSlice, strconv.FormatUint(pid, 10))
	}
	return strings.Join(strSlice, ",")
}

func (task *ConvertTask) mpGetMpDetailInfo() (err error) {
	if task.mpInfo, err = task.mc.clientAPI().GetMetaPartition(task.info.RunningMP, ""); err != nil {
		log.LogErrorf("action[mpGetMpDetailInfo] task[%s] MP[%v] GetMetaPartition failed, err[%v]",
			task.taskName(), task.info.VolName, err)
		return
	}

	if task.mpConvertFinished() {
		log.LogInfof("action[mpGetMpDetailInfo] task[%s] MP[%v] no need to convert", task.taskName(), task.mpInfo.PartitionID)
		task.info.MPConvertState = proto.MPStopped
		return
	}
	task.info.MPConvertState = proto.MPSelectLearner
	return
}

func (task *ConvertTask) mpSelectLearner() (err error) {
	defer func() {
		if err == nil {
			task.info.MPConvertState = proto.MPAddLearner
		}
	}()

	if task.isContinueLastTask && len(task.mpInfo.Replicas) > int(task.conf.mpReplicaNum) {
		log.LogInfof("action[mpSelectLearner] task[%s] MP[%v] member number more than conf replica number, so do nothing",
			task.taskName(), task.info.RunningMP)
		return
	}

	var (
		oldNodeAddr          string
		targetStoreMode      proto.StoreMode
		selectedReplNodeInfo *proto.SelectMetaNodeInfo
	)
	if task.isNeedConvertToMemMode || (int(task.mpInfo.RcokStoreCnt) > task.conf.rocksModeReplicaTargetNum) {
		targetStoreMode = proto.StoreModeMem
	} else {
		targetStoreMode = proto.StoreModeRocksDb
	}

	for _, replica := range task.mpInfo.Replicas {
		if replica.StoreMode != task.convertingMP.targetStoreMode {
			oldNodeAddr = replica.Addr
			break
		}
	}
	selectedReplNodeInfo, err = task.mc.adminAPI().SelectMetaReplicaReplaceNodeAddr(task.mpInfo.PartitionID, oldNodeAddr, int(targetStoreMode))
	if err != nil {
		log.LogErrorf("action[mpSelectLearner] task[%s] MP[%v] selectMetaReplicaReplaceNodeAddr err[%v]",
			task.taskName(), task.mpInfo.PartitionID, err)
		return
	}
	task.setConvertingMpInfo(task.info.RunningMP, targetStoreMode, selectedReplNodeInfo.NewNodeAddr, oldNodeAddr)

	log.LogInfof("action[mpSelectLearner] task[%s] MP[%v] selected leaner node info[oldNodeAddr:%s, selectedLearnerNodeAddr:%s, storeMode:%v]",
		task.taskName(), task.mpInfo.PartitionID, oldNodeAddr, selectedReplNodeInfo.NewNodeAddr, task.convertingMP.targetStoreMode)
	return
}

func (task *ConvertTask) mpConvertFinished() (finished bool) {
	//check mp is finished converting
	if len(task.mpInfo.Replicas) != int(task.conf.mpReplicaNum) {
		return
	}

	if (task.isNeedConvertToMemMode && int(task.mpInfo.MemStoreCnt) == task.conf.memModeMPTargetNum) ||
		(!task.isNeedConvertToMemMode && int(task.mpInfo.RcokStoreCnt) == task.conf.rocksModeReplicaTargetNum) {
		finished = true
	}
	return
}

func (task *ConvertTask) mpAddLearner() (err error) {
	defer func() {
		if err == nil {
			task.info.MPConvertState = proto.MPWaitLearnerSync
		}
	}()
	if task.isContinueLastTask && len(task.mpInfo.Replicas) > int(task.conf.mpReplicaNum) {
		log.LogInfof("action[mpAddLearner] task[%s] MP[%d] mp member number is more than replica number, so do nothing",
			task.taskName(), task.info.RunningMP)
		return
	}

	if err = task.db.PutTaskInfoToDB(task.transform()); err != nil {
		log.LogErrorf("action[mpAddLearner] task[%s] MP[%d] put task to data base failed:%v",
			task.taskName(), task.info.RunningMP, err)
		return
	}

	if err = task.mc.adminAPI().AddMetaReplicaLearner(task.info.RunningMP, task.convertingMP.selectedReplNodeInfo.NewNodeAddr,
		false, 80, proto.DefaultAddReplicaType, int(task.convertingMP.targetStoreMode)); err != nil {
		log.LogErrorf("action[mpAddLearner] task[%s] MP[%d] AddMetaReplicaLearner failed, err[%v]",
			task.taskName(), task.info.RunningMP, err)
		return
	}
	return
}

func (task *ConvertTask) mpWaitSync() (err error) {
	if task.mpInfo, err = task.mc.clientAPI().GetMetaPartition(task.info.RunningMP, ""); err != nil {
		log.LogErrorf("action[mpWaitSync] task[%s] MP[%v] get meta partition info failed, err[%v]",
			task.taskName(), task.info.RunningMP, err)
		return
	}

	var ok = false
	if ok, err = task.mpReplicasIsConsistent(); !ok || err != nil {
		return
	}

	task.info.MPConvertState = proto.MPPromteLearner
	log.LogInfof("action[mpWaitSync] task[%s] MP[%v] learner node[%s] can promote",
		task.taskName(), task.mpInfo.PartitionID, task.convertingMP.selectedReplNodeInfo.NewNodeAddr)
	return
}

func (task *ConvertTask) mpReplicasIsConsistent() (ok bool, err error) {
	var (
		wg             sync.WaitGroup
		mpReplicasInfo = make([]*meta.GetMPInfoResp, len(task.mpInfo.Replicas))
		errCh          = make(chan error, len(task.mpInfo.Replicas))
	)

	for index, replica := range task.mpInfo.Replicas {
		wg.Add(1)
		go func(index int, nodeAddr string) {
			defer wg.Done()
			var (
				metaNodeInfo *proto.MetaNodeInfo
				resp         *meta.GetMPInfoResp
			)
			if metaNodeInfo, err = task.mc.nodeAPI().GetMetaNode(nodeAddr); err != nil {
				log.LogErrorf("action[mpReplicasIsConsistent] task[%s] get meta node[%s] info failed:%v",
					task.taskName(), nodeAddr, err)
				errCh <- err
				return
			}

			metaHttpClient := meta.NewMetaHttpClient(fmt.Sprintf("%s:%s",strings.Split(nodeAddr, ":")[0], metaNodeInfo.ProfPort), false)
			if resp, err = metaHttpClient.GetMetaPartition(task.mpInfo.PartitionID); err != nil {
				log.LogErrorf("action[mpReplicasIsConsistent] task[%s] get MP[%v] info from meta node[%s] info failed:%v",
					task.taskName(), task.mpInfo.PartitionID, nodeAddr, err)
				errCh <- err
				return
			}
			mpReplicasInfo[index] = resp
		}(index, replica.Addr)
	}
	wg.Wait()

	select {
	case errInfo := <- errCh:
		err = errors.NewErrorf("get meta partition info failed")
		log.LogErrorf("action[mpReplicasIsConsistent] task[%s] MP[%v] get meta partition info failed:%v",
			task.taskName(), task.info.RunningMP, errInfo)
		return
	default:
	}

	inodeCntMinus := getMinusOfInodeCnt(mpReplicasInfo)
	dentryCntMinus := getMinusOfDentryCnt(mpReplicasInfo)
	applyIDMinus := getMinusOfApplyID(mpReplicasInfo)
	if uint64(inodeCntMinus) < defaultMinus && dentryCntMinus < defaultMinus && applyIDMinus < defaultMinus {
		log.LogInfof("action[mpReplicasIsConsistent] task[%s] MP[%v] replica meta data is consistent",
			task.taskName(), task.info.RunningMP)
		ok = true
	}
	return
}

func getMinusOfInodeCnt(mpReplicasInfo []*meta.GetMPInfoResp) (minus float64) {
	var (
		max uint64 = 0
		min uint64 = math.MaxUint64
	)
	for _, replica := range mpReplicasInfo {
		if replica.InodeCount > max {
			max = replica.InodeCount
		}
		if replica.InodeCount < min {
			min = replica.InodeCount
		}
	}
	return math.Abs(float64(max) - float64(min))
}

func getMinusOfDentryCnt(mpReplicasInfo []*meta.GetMPInfoResp) (minus float64) {
	var (
		max uint64 = 0
		min uint64 = math.MaxUint64
	)
	for _, replica := range mpReplicasInfo {
		if replica.DentryCount > max {
			max = replica.DentryCount
		}
		if replica.DentryCount < min {
			min = replica.DentryCount
		}
	}
	return math.Abs(float64(max) - float64(min))
}

func getMinusOfApplyID(mpReplicasInfo []*meta.GetMPInfoResp) (minus float64) {
	var (
		max uint64 = 0
		min uint64 = math.MaxUint64
	)
	for _, replica := range mpReplicasInfo {
		if replica.ApplyId > max {
			max = replica.ApplyId
		}
		if replica.ApplyId < min {
			min = replica.ApplyId
		}
	}
	return math.Abs(float64(max) - float64(min))
}

func (task *ConvertTask) mpPromoteLearner() (err error) {
	defer func() {
		if err == nil {
			task.info.MPConvertState = proto.MPWaitStable
		}
	}()

	if len(task.mpInfo.Learners) == 0 {
		task.nextStageStartRunTime = time.Now()
		return
	}

	if err = task.mc.adminAPI().PromoteMetaReplicaLearner(task.convertingMP.selectedReplNodeInfo.PartitionID, task.convertingMP.selectedReplNodeInfo.NewNodeAddr); err != nil {
		log.LogErrorf("action[mpPromoteLearner] task[%s] MP[%v] PromoteMetaReplicaLearner failed, err[%v]",
			task.taskName(), task.info.RunningMP, err)
		return
	}

	task.nextStageStartRunTime = time.Now().Add(time.Minute * 3)
	return
}

func (task *ConvertTask) mpWaitStable() (err error) {
	if time.Now().Before(task.nextStageStartRunTime) {
		return
	}

	if task.mpInfo, err = task.mc.clientAPI().GetMetaPartition(task.info.RunningMP, ""); err != nil {
		log.LogErrorf("action[mpWaitStable] task[%s] MP[%v] GetMetaPartition failed, err[%v]",
			task.taskName(), task.info.RunningMP, err)
		return
	}

	var ok = false
	if ok, err = task.mpReplicasIsConsistent(); !ok || err != nil {
		return
	}

	task.info.MPConvertState = proto.MPDelReplica
	return
}

func (task *ConvertTask) mpDelReplica() (err error) {
	if len(task.mpInfo.Replicas) <= int(task.conf.mpReplicaNum) {
		return errors.NewErrorf("task[%s] MP[%v] replica number less than or equal config replica number, " +
			"can not delete replica", task.taskName(), task.info.RunningMP)
	}

	if err = task.mc.adminAPI().DeleteMetaReplica(task.convertingMP.selectedReplNodeInfo.PartitionID, task.convertingMP.selectedReplNodeInfo.OldNodeAddr); err != nil {
		log.LogErrorf("action[mpDelReplica] task[%s] MP[%v] DeleteMetaReplica failed, err[%v]",
			task.taskName(), task.info.RunningMP, err)
		return
	}

	task.isContinueLastTask = false
	task.info.MPConvertState = proto.MPWaitInterval
	task.nextStageStartRunTime = time.Now().Add(time.Minute)
	return
}

func (task *ConvertTask) mpWaitInterval() (err error) {
	if time.Now().Before(task.nextStageStartRunTime) {
		return
	}

	if task.mpInfo, err = task.mc.clientAPI().GetMetaPartition(task.info.RunningMP, ""); err != nil {
		log.LogErrorf("action[mpWaitInterval] task[%s] MP[%v] GetMetaPartition failed, err[%v]",
			task.taskName(), task.info.RunningMP, err)
		return
	}

	if task.mpInfo.IsRecover {
		return
	}

	var ok = false
	if ok, err = task.mpReplicasIsConsistent(); !ok || err != nil {
		return
	}

	if task.mpConvertFinished() {
		log.LogInfof("action[mpWaitInterval] task[%s] MP[%v] convert finished", task.taskName(), task.info.RunningMP)
		task.info.MPConvertState = proto.MPStopped
	} else {
		log.LogInfof("action[mpWaitInterval] task[%s] MP[%v] continue convert next replica", task.taskName(), task.info.RunningMP)
		task.info.MPConvertState = proto.MPGetDetailInfo
	}
	return
}

func (task *ConvertTask) mpStopped() {
	task.info.FinishedMP = append(task.info.FinishedMP, task.info.RunningMP)
	task.mpInfo = nil
	task.info.RunningMP = 0
	task.setConvertingMpInfo(0,  proto.StoreModeDef, "", "")
	return
}

func (task *ConvertTask) mpRunOnce() (finished bool, err error) {
	log.LogInfof("action[meta partition convert loop runOnce] enter task[%s] MP[%d] run once, convert state [%s]",
		task.taskName(), task.info.RunningMP, task.info.MPConvertState.Str())
	defer func() {
		log.LogInfof("action[meta partition convert loop runOnce] exit task[%s] MP[%d] run once, convert state[%s], err[%v]",
			task.taskName(), task.info.RunningMP, task.info.MPConvertState.Str(), err)
	}()
	switch task.info.MPConvertState {
	case proto.MPGetDetailInfo:
		err = task.mpGetMpDetailInfo()
	case proto.MPSelectLearner:
		err = task.mpSelectLearner()
	case proto.MPAddLearner:
		err = task.mpAddLearner()
	case proto.MPWaitLearnerSync:
		err = task.mpWaitSync()
	case proto.MPPromteLearner:
		err = task.mpPromoteLearner()
	case proto.MPWaitStable:
		err = task.mpWaitStable()
	case proto.MPDelReplica:
		err = task.mpDelReplica()
	case proto.MPWaitInterval:
		err = task.mpWaitInterval()
	case proto.MPStopped:
		task.mpStopped()
		finished = true
	}
	return
}

func (task *ConvertTask) dealActionErr(curErr error) (err error) {
	if curErr == nil {
		task.info.ErrMsg = ""
		return
	}

	if task.lastErr != nil && strings.Compare(curErr.Error(), task.lastErr.Error()) != 0 {
		task.errCnt = 1
		task.lastErr = curErr
		return
	}

	task.lastErr = curErr
	task.errCnt++
	if task.errCnt > 10 {
		task.info.ErrMsg = curErr.Error()
		err = curErr
	}
	log.LogInfof("curr error:%s, last err:%s", curErr.Error(), task.lastErr.Error())
	return
}

func (task *ConvertTask) initTask() (err error) {
	task.lastConvertMP = task.convertingMP
	task.setConvertingMpInfo(0,  proto.StoreModeDef, "", "")
	task.info.ErrMsg = ""
	task.info.TaskState = proto.TaskGetVolumeInfo
	return
}

func (task *ConvertTask) getVolumeInfo() (err error) {
	defer func() {
		if err == nil {
			task.info.TaskState = proto.TaskSetVolumeState
		}
	}()

	volInfo, err := task.mc.adminAPI().GetVolumeSimpleInfo(task.info.VolName)
	if  err != nil {
		log.LogErrorf("action[getVolumeInfo] task[%s] get volume simple info failed:%v", task.taskName(), err)
		return
	}

	if volInfo.ConvertState == proto.VolConvertStInit || volInfo.ConvertState == proto.VolConvertStFinished {
		err = fmt.Errorf("can not start Convert Task , vol state[%v] is not support", volInfo.ConvertState)
		return
	}

	task.conf.mpReplicaNum = volInfo.MpReplicaNum
	task.info.Layout = volInfo.MpLayout
	return
}

func (task *ConvertTask) setVolumeState() (err error) {
	defer func() {
		if err == nil {
			task.info.TaskState = proto.TaskGetVolumeMPInfo
		}
	}()
	var vv *proto.SimpleVolView
	if vv, err = task.mc.adminAPI().GetVolumeSimpleInfo(task.info.VolName); err != nil {
		log.LogErrorf("action[setVolumeState] task[%s] get volume simple info failed:%v", task.taskName(), err)
		return
	}
	if err = task.mc.adminAPI().SetVolumeConvertTaskState(task.info.VolName, calcAuthKey(vv.Owner), int(proto.VolConvertStRunning)); err != nil {
		log.LogErrorf("action[setVolumeState] task[%s] set volume convert task state failed:%v", task.taskName(), err)
		return
	}
	return
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}

func (task *ConvertTask) getVolumeMPInfo() (err error) {
	mps, err := task.mc.clientAPI().GetPhysicalMetaPartitions(task.info.VolName)
	if err != nil {
		log.LogErrorf("action[getVolumeMPInfo] task[%s] get volume meta partitions failed:%v", task.taskName(), err)
		return
	}
	//sort by replica number, rocks count, pid
	sortMetaPartitions(mps)
	task.calcMpMemModeNumByLayout(len(mps))
	task.calcReplicaRocksModeNumByLayout()

	//select convert mps
	task.selectNeedConvertMps(mps)
	if len(task.info.SelectedMP) == 0 {
		if len(task.info.IgnoredMP) == 0 {
			log.LogInfof("action[getVolumeMPInfo] task[%s] select mps array length is zero, task finished", task.taskName())
			task.info.TaskState = proto.TaskFinished
		} else {
			log.LogInfof("action[getVolumeMPInfo] task[%s] select mps array length is zero, but ignore mps " +
				"array not empty, so wait a moment", task.taskName())
		}
		return
	}

	//set convert mp info
	task.info.TaskState = proto.TaskConvertLoop
	task.info.RunningMP = task.info.SelectedMP[0]
	if task.isContinueLastTask {
		task.convertingMP = task.lastConvertMP
	}
	task.lastConvertMP = nil
	task.info.MPConvertState = proto.MPGetDetailInfo
	return
}

func (task *ConvertTask)selectNeedConvertMps(mps []*proto.MetaPartitionView) {
	var (
		memModeMPNum                        = task.conf.memModeMPTargetNum
		rocksModeNum                        = len(mps) - memModeMPNum
		selectedMps, finishedMps, ignoreMps []uint64
	)

	//select mps
	for _, mp := range mps {
		switch {
		case len(mp.Members) > int(task.conf.mpReplicaNum):
			if task.lastConvertMP != nil && task.lastConvertMP.partitionID == mp.PartitionID {
				task.isContinueLastTask = true
				selectedMps = append(selectedMps, mp.PartitionID)
			} else {
				ignoreMps = append(ignoreMps, mp.PartitionID)
			}

		case len(mp.Members) == int(task.conf.mpReplicaNum):
			if memModeMPNum != 0 && mp.MemCount == task.conf.mpReplicaNum {
				finishedMps = append(finishedMps, mp.PartitionID)
				memModeMPNum--
			} else {
				if rocksModeNum != 0 && int(mp.RocksCount) == task.conf.rocksModeReplicaTargetNum {
					finishedMps = append(finishedMps, mp.PartitionID)
					rocksModeNum--
				} else {
					selectedMps = append(selectedMps, mp.PartitionID)
				}
			}

		default:
			ignoreMps = append(ignoreMps, mp.PartitionID)
		}
	}
	if memModeMPNum != 0 {
		task.isNeedConvertToMemMode = true
	}

	task.Lock()
	defer task.Unlock()
	task.info.SelectedMP = append(task.info.SelectedMP[:0], selectedMps...)
	task.info.FinishedMP = append(task.info.FinishedMP[:0], finishedMps...)
	task.info.IgnoredMP = append(task.info.IgnoredMP[:0], ignoreMps...)
	return
}

func sortMetaPartitions(mps []*proto.MetaPartitionView) {
	//sort by replica number, rocks count, pid
	sort.SliceStable(mps, func(i, j int) bool {
		if len(mps[i].Members) > len(mps[j].Members) {
			return true
		} else if len(mps[i].Members) == len(mps[j].Members) {
			if mps[i].RocksCount < mps[j].RocksCount {
				return true
			} else if mps[i].RocksCount == mps[j].RocksCount {
				return mps[i].PartitionID < mps[j].PartitionID
			} else {
				return false
			}
		} else {
			return false
		}
	})
	return
}

//calculate mp Layout
func (task *ConvertTask) calcMpMemModeNumByLayout(mpNum int) {
	num := float64(task.info.Layout.PercentOfMP) / 100 * float64(mpNum)
	if num > 1 {
		num = math.Floor(num)
	} else {
		num = math.Ceil(num)
	}
	task.conf.memModeMPTargetNum = mpNum - int(num)
	return
}

func (task *ConvertTask) calcReplicaRocksModeNumByLayout() {
	num := float64(task.info.Layout.PercentOfReplica) / 100 * float64(task.conf.mpReplicaNum)
	if num > 1 {
		num = math.Floor(num)
	} else {
		num = math.Ceil(num)
	}
	task.conf.rocksModeReplicaTargetNum = int(num)
	return
}

func (task *ConvertTask) convertLoop() (err error) {
	var mpConvertFinished = false
	if mpConvertFinished, err = task.mpRunOnce(); mpConvertFinished {
		task.info.TaskState = proto.TaskGetVolumeMPInfo
	}
	return
}

func (task *ConvertTask) stopTask() (isFinished bool, err error) {
	var vv *proto.SimpleVolView
	if vv, err = task.mc.adminAPI().GetVolumeSimpleInfo(task.info.VolName); err != nil {
		log.LogErrorf("action[stopTask] task[%s] get volume simple info failed, err[%v]", task.taskName(), err)
		return
	}
	if err = task.mc.adminAPI().SetVolumeConvertTaskState(task.info.VolName, calcAuthKey(vv.Owner), int(proto.VolConvertStFinished)); err != nil {
		log.LogErrorf("action[stopTask] task[%s] set volume convert state to finished failed, err[%v]", task.taskName(), err)
		return
	}
	task.info.TaskState = proto.TaskFinished
	isFinished = true
	return
}

func (task *ConvertTask) runOnce() (taskFinished bool, err error) {
	log.LogInfof("action[task runOnce]  enter task[%s] runonce, task state[%s]", task.taskName(), proto.TaskStateStr(task.info.TaskState))
	defer func() {
		log.LogInfof("action[task runOnce] exit task[%s] runonce, task state[%s], err[%v]", task.taskName(), proto.TaskStateStr(task.info.TaskState), err)
	}()
	switch task.info.TaskState {
	case proto.TaskInit:
		err = task.initTask()
	case proto.TaskGetVolumeInfo:
		err = task.getVolumeInfo()
	case proto.TaskSetVolumeState:
		err = task.setVolumeState()
	case proto.TaskGetVolumeMPInfo:
		err = task.getVolumeMPInfo()
	case proto.TaskConvertLoop:
		err = task.convertLoop()
	case proto.TaskFinished:
		taskFinished, err = task.stopTask()
	}
	return
}

func (task *ConvertTask) hasFinished() (isFinished bool) {
	return task.info.TaskState == proto.TaskFinished
}