package mdck

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MetaDataCheckResultInfo struct {
	VolumeName       string
	InconsistentMPs  []uint64
	CheckFailedMPs   []uint64
	InconsistentInfo []string
}

type MetaDataCheckTask struct {
	*proto.Task
	mpDataCrcInfoMap  sync.Map
	checkFailedMPs    []uint64
	inconsistentMPs   []uint64
	inconsistentInfo  []string
	masterClient      *master.MasterClient
	mailTo            []string
}

func NewMetaDataCheckTask(task *proto.Task, mc *master.MasterClient, mailTo []string) *MetaDataCheckTask {
	mdckTask := new(MetaDataCheckTask)
	mdckTask.Task = task
	mdckTask.masterClient = mc
	mdckTask.mailTo = mailTo
	return mdckTask
}

func (t *MetaDataCheckTask) RunOnce() {
	log.LogInfof("MetaDataCheckTask RunOnce cluster(%s) volume(%s) start check", t.Cluster, t.VolName)
	t.CheckMetaData()
	if len(t.inconsistentMPs) != 0 && len(t.checkFailedMPs) != 0 && len(t.inconsistentInfo) != 0 {
		t.EmailCheckResult()
	}
	log.LogInfof("MetaDataCheckTask RunOnce cluster(%s) volume(%s) finish check", t.Cluster, t.VolName)
}

func (t *MetaDataCheckTask) CheckMetaData() {
	var mpCh = make(chan uint64, 64)
	var lock sync.Mutex
	mps, err := t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
	if err != nil {
		return
	}
	go func() {
		for _, mp := range mps {
			mpCh <- mp.PartitionID
		}
		close(mpCh)
	}()

	var wg sync.WaitGroup
	for index := 0; index < 5; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mpID, ok := <- mpCh
				if !ok {
					return
				}
				isConsistent, inconsistentInfo, mismatchInodes, mismatchInodeInfo, errForCheck := t.checkMetaPartitionMetaData(mpID)
				lock.Lock()
				if errForCheck != nil {
					log.LogErrorf("CheckMetaData, cluster(%s) volume(%s) partitionID(%v) check failed:%v", t.Cluster, t.VolName, mpID, errForCheck)
					t.checkFailedMPs = append(t.checkFailedMPs, mpID)
					lock.Unlock()
					continue
				}
				if !isConsistent {
					t.inconsistentMPs = append(t.inconsistentMPs, mpID)
					if len(mismatchInodes) != 0 {
						t.inconsistentInfo = append(t.inconsistentInfo, fmt.Sprintf("%s mismatchInodeinfo %v", inconsistentInfo, mismatchInodeInfo))
					} else {
						t.inconsistentInfo = append(t.inconsistentInfo, inconsistentInfo)
					}
					log.LogInfof("CheckMetaData, cluster(%s) volume(%s) partitionID(%v) inconsistentInfo(%s) mismatchInodes %v, mismatchInodeInfo %v",
						t.Cluster, t.VolName, mpID, inconsistentInfo, mismatchInodes, mismatchInodeInfo)
				}
				lock.Unlock()
			}
		}()
	}
	wg.Wait()
	return
}

func (t *MetaDataCheckTask) EmailCheckResult() {
	result := &MetaDataCheckResultInfo{
		VolumeName:       t.VolName,
		InconsistentMPs:  t.inconsistentMPs,
		CheckFailedMPs:   t.checkFailedMPs,
		InconsistentInfo: t.inconsistentInfo,
	}
	emailSubject := fmt.Sprintf("%s %s 元数据三副本一致性检查结果", t.Cluster, t.VolName)
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("Inconsistent MetaPartitions: %v</br>", result.InconsistentMPs))
	sb.WriteString(fmt.Sprintf("CheckFailed MetaPartitions: %v</br>", result.CheckFailedMPs))
	sb.WriteString("InconsistentInfo:</br>")
	sb.WriteString(strings.Join(result.InconsistentInfo, "</br>"))
	sb.WriteString("</br>")
	emailContent := sb.String()
	errSendEmail := common.SendEmail(emailSubject, emailContent, t.mailTo)
	if errSendEmail != nil {
		log.LogErrorf("EmailCheckResult send email failed:%v", errSendEmail)
	}
	return
}

func (t *MetaDataCheckTask) checkMetaPartitionMetaData(pid uint64) (isCoincident bool, inConsistentInfo string, mismatchInodes []uint64, inodeMismatchInfo []string, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("checkMetaPartitionMetaData cluster(%s) volume(%s) partitionID(%v) check failed:%v",
				t.Cluster, t.VolName, pid, err)
		}
	}()
	log.LogDebugf("checkMetaPartitionMetaData cluster(%s) volume(%s) partitionID(%v) start check", t.Cluster, t.VolName, pid)
	mpInfo, err := t.masterClient.ClientAPI().GetMetaPartition(pid, t.VolName)
	if err != nil {
		log.LogErrorf("get meta partition info failed, mpid:%v, error:%v\n", pid, err)
		return
	}
	if len(mpInfo.Hosts) == 0 {
		err = fmt.Errorf("mp host count is zero")
		log.LogErrorf("checkMetaPartitionMetaData cluster(%s) volume(%s) partitionID(%v) hosts count is zero", t.Cluster, t.VolName, pid)
		return
	}

	var leaderAddr string
	for _, replica := range mpInfo.Replicas {
		if replica.IsLeader {
			leaderAddr = replica.Addr
			break
		}
	}

	var needCheckInodeTree = false
	if isCoincident, inConsistentInfo, needCheckInodeTree, err = t.doCheckMetaPartitionMetaData(pid, mpInfo.Hosts, leaderAddr); err != nil {
		log.LogErrorf("checkMetaPartitionMetaData cluster(%s) volume(%s) partitionID(%v) check meta data failed:%v",
			t.Cluster, t.VolName, pid, err)
		return
	}

	if isCoincident {
		log.LogInfof("checkMetaPartitionMetaData cluster(%s) volume(%s) partitionID(%v) metaData consistent", t.Cluster, t.VolName, pid)
		return
	}

	log.LogInfof("checkMetaPartitionMetaData cluster(%s) volume(%s) partitionID(%v) metaData inconsistentInfo:%s",
		t.Cluster, t.VolName, pid, inConsistentInfo)

	if !needCheckInodeTree {
		return
	}

	if mismatchInodes, err = t.doCheckInodeTree(pid, mpInfo.Hosts); err != nil {
		log.LogErrorf("cluster(%s) volume(%s) partitionID(%v) checkInodeTree failed:%v",
			t.Cluster, t.VolName, pid, err)
		return
	}

	if len(mismatchInodes) == 0 {
		return
	}

	log.LogInfof("checkMetaPartitionMetaData cluster(%s) volume(%s) partitionID(%v) mismatchInodes %v",
		t.Cluster, t.VolName, pid, mismatchInodes)

	mismatchInodes, inodeMismatchInfo =  t.doCheckMismatchInodes(mismatchInodes, pid, mpInfo.Hosts)
	log.LogInfof("checkMetaPartitionMetaData  cluster(%s) volume(%s) partitionID(%v) mismatchInodes after check: %v",
		t.Cluster, t.VolName, pid, mismatchInodes)
	return
}

func (t *MetaDataCheckTask) doCheckMetaPartitionMetaData(pid uint64, hosts []string, leaderAddr string) (isConsistent bool, inConsistentInfo string, needCheckInode bool, err error) {
	var (
		wg                     = new(sync.WaitGroup)
		applyIDSet             = make([]uint64, len(hosts))
		result                 = make([]*proto.MetaDataCRCSumInfo, len(hosts))
		retryCnt               = 3
		needHandleDelInodeTree = false
		leaderResultIndex      = -1
	)
	for retryCnt > 0 {
		errCh := make(chan error, len(hosts))
		for index, host := range hosts {
			wg.Add(1)
			go func(index int, host string) {
				defer wg.Done()
				hostSplitArr := strings.Split(host, ":")
				if len(hostSplitArr) != 2 {
					errCh <- fmt.Errorf("host(%s) with error format", host)
					return
				}
				addr := fmt.Sprintf("%s:%v", hostSplitArr[0], t.masterClient.MetaNodeProfPort)
				metaHttpClient := meta.NewMetaHttpClient(addr, false, false)
				r, e := metaHttpClient.GetMetaDataCrcSum(pid)
				if e != nil {
					errCh <- fmt.Errorf("get mp(%v) meta data crc sum from %s failed, error:%v", pid, addr, e)
					return
				}
				if leaderAddr == host {
					leaderResultIndex = index
				}
				applyIDSet[index] = r.ApplyID
				result[index] = r
			}(index, host)
		}
		wg.Wait()
		select {
		case err = <- errCh:
			retryCnt--
			log.LogErrorf("doCheckMetaPartitionMetaData cluster(%v) mp(%v) getMetaDataCrcSum failed:%v", t.Cluster, pid, err)
			continue
		default:
		}
		if !isSameApplyID(applyIDSet) {
			retryCnt--
			time.Sleep(time.Second * 1)
			continue
		}

		isConsistent, inConsistentInfo, needCheckInode, needHandleDelInodeTree = isSameMetaData(result)
		err = nil
		var needOfflineAddr = make([]string, 0)
		if needHandleDelInodeTree && leaderAddr != "" && leaderResultIndex != -1 {
			for index := 0; index < len(result); index++ {
				if result[index].CntSet[DelInodeType] != result[leaderResultIndex].CntSet[DelInodeType] {
					needOfflineAddr = append(needOfflineAddr, hosts[index])
				}
			}
		}
		log.LogInfof("doCheckMetaPartitionMetaData partitionID: %v, need offline addr: %v", pid, needOfflineAddr)
		return
	}
	err = fmt.Errorf("check failed")
	return
}

func (t *MetaDataCheckTask) doCheckInodeTree(pid uint64, hosts []string) (mismatchInodes []uint64, err error) {
	var (
		wg             sync.WaitGroup
		totalCRCSumSet = make([]uint32, len(hosts))
		applyIDSet     = make([]uint64, len(hosts))
		resultSet      = make([]*proto.InodesCRCSumInfo, len(hosts))
		retryCnt       = 3
	)
	for retryCnt > 0 {
		errCh := make(chan error, len(hosts))
		for index, host := range hosts {
			wg.Add(1)
			go func(index int, host string) {
				defer wg.Done()
				hostSplitArr := strings.Split(host, ":")
				if len(hostSplitArr) != 2 {
					errCh <- fmt.Errorf("host(%s) with error format", host)
					return
				}
				addr := fmt.Sprintf("%s:%v", hostSplitArr[0], t.masterClient.MetaNodeProfPort)
				metaHttpClient := meta.NewMetaHttpClient(addr, false, false)
				r, e := metaHttpClient.GetInodesCrcSum(pid)
				if e != nil {
					errCh <- fmt.Errorf("get mp(%v) meta data crc sum from %s failed, error:%v", pid, addr, e)
					return
				}
				totalCRCSumSet[index] = r.AllInodesCRCSum
				applyIDSet[index] = r.ApplyID
				resultSet[index] = r
			}(index, host)
		}
		wg.Wait()
		select {
		case err = <- errCh:
			retryCnt--
			continue
		default:
		}
		if !isSameApplyID(applyIDSet) {
			retryCnt--
			continue
		}

		if isSameCrcSum(totalCRCSumSet) {
			return
		}

		mismatchInodes = validateInodes(resultSet)
		err = nil
		return
	}
	err = fmt.Errorf("check inode tree failed")
	return
}

func (t *MetaDataCheckTask) doCheckMismatchInodes(needCheckMismatchInodes []uint64, pid uint64, hosts []string) (mismatchInodes []uint64, inodeMismatchInfo []string) {
	var (
		wg           sync.WaitGroup
	)
	for _, inode := range needCheckMismatchInodes {
		errCh := make(chan error, len(hosts))
		inodeInfoSet := make([]*proto.InodeInfo, len(hosts))
		extentsInfoSet := make([]*proto.GetExtentsResponse, len(hosts))
		for index, host := range hosts {
			wg.Add(1)
			go func(index int, host string) {
				defer wg.Done()
				hostSplitArr := strings.Split(host, ":")
				if len(hostSplitArr) != 2 {
					errCh <- fmt.Errorf("host(%s) with error format", host)
					return
				}
				addr := fmt.Sprintf("%s:%v", hostSplitArr[0], t.masterClient.MetaNodeProfPort)
				metaHttpClient := meta.NewMetaHttpClient(addr, false, false)
				inodeInfo, errGetInode := metaHttpClient.GetInodeInfo(pid, inode)
				if errGetInode != nil {
					errCh <- fmt.Errorf("get mp(%v) inode(%v) from %s failed, error:%v", pid, inode, addr, errGetInode)
					return
				}
				inodeInfoSet[index] = inodeInfo
				if proto.IsDir(inodeInfo.Mode) {
					return
				}
				extentsInfo, errGetEKs := metaHttpClient.GetExtentsByInode(pid, inode)
				if errGetEKs != nil {
					errCh <- fmt.Errorf("get mp(%v) inode(%v) eks from %s failed, error:%v", pid, inode, addr, errGetInode)
					return
				}
				extentsInfoSet[index] = extentsInfo
			}(index, host)
		}
		wg.Wait()
		select {
		case err := <-errCh:
			log.LogErrorf("check mp(%v) inode(%v) failed:%v", pid, inode, err)
			//mismatchInodes = append(mismatchInodes, inode)
			//continue
		default:
		}

		if isConsistent, diffInfo := checkInodeInfo(inodeInfoSet, extentsInfoSet); !isConsistent {
			log.LogErrorf("partitionID(%v) inodeID(%v) inode info mismatch", pid, inode)
			mismatchInodes = append(mismatchInodes, inode)
			inodeMismatchInfo = append(inodeMismatchInfo, fmt.Sprintf("{inode %v:%s}", inode, diffInfo))
		}
	}
	return
}

func checkInodeInfo(inodeInfoSet []*proto.InodeInfo, extentsInfoSet []*proto.GetExtentsResponse) (isConsistent bool, diffInfo string) {
	isConsistent = true
	//check nlink, mtime, ctime
	var inodeInfo *proto.InodeInfo
	for _, info := range inodeInfoSet {
		if info != nil {
			inodeInfo = info
		}
	}
	if inodeInfo == nil {
		diffInfo = fmt.Sprintf("diff info")
		isConsistent = false
		return
	}
	nlink := inodeInfo.Nlink
	mtime := inodeInfo.ModifyTime
	ctime := inodeInfo.CreateTime
	for _, info := range inodeInfoSet {
		if info == nil {
			diffInfo = fmt.Sprintf("diff info, isDir(%v)", strconv.FormatBool(proto.IsDir(inodeInfo.Mode)))
			log.LogInfof("inode %v with diff info", inodeInfo.Inode)
			isConsistent = false
			return
		}
		if info.Nlink != nlink {
			isConsistent = false
			diffInfo = fmt.Sprintf("diff nlink(%v:%v), isDir(%v)", nlink, info.Nlink, strconv.FormatBool(proto.IsDir(inodeInfo.Mode)))
			log.LogInfof("inode %v with diff nlink", inodeInfo.Inode)
			break
		}

		if info.ModifyTime != mtime {
			log.LogDebugf("inode %v with diff modify time [expect: %v, actual: %v]", inodeInfo.Inode, mtime.Format(proto.TimeFormat), info.ModifyTime.Format(proto.TimeFormat))
		}

		if  info.CreateTime != ctime {
			log.LogDebugf("inode %v with diff create time [expect: %v, actual: %v]", inodeInfo.Inode, ctime.Format(proto.TimeFormat), info.CreateTime.Format(proto.TimeFormat))
		}

	}

	if proto.IsDir(inodeInfo.Mode) {
		return
	}

	//check ek
	var eksInfo = extentsInfoSet[0]
	if eksInfo == nil {
		diffInfo = fmt.Sprintf("diff eks info")
		log.LogInfof("inode %v with diff eks info", inodeInfo.Inode)
		isConsistent = false
		return
	}

	for _, info := range extentsInfoSet {
		if info == nil {
			diffInfo = fmt.Sprintf("diff eks info")
			log.LogInfof("inode %v with diff eks info", inodeInfo.Inode)
			isConsistent = false
			return
		}

		if info.Size != eksInfo.Size || len(info.Extents) != len(eksInfo.Extents){
			diffInfo = fmt.Sprintf("diff size(%v:%v) or diff ekCount(%v:%v)", info.Size, eksInfo.Size,
				len(info.Extents), len(eksInfo.Extents))
			log.LogInfof("inode %v with diff size", inodeInfo.Inode)
			isConsistent = false
			return
		}

		for index, ek := range eksInfo.Extents {
			diffInfo = fmt.Sprintf("diff ek(%v_%v_%v_%v_%v:%v_%v_%v_%v_%v)",ek.FileOffset, ek.PartitionId,
				ek.ExtentId, ek.ExtentOffset, ek.Size, info.Extents[index].FileOffset,  info.Extents[index].PartitionId,
				info.Extents[index].ExtentId, info.Extents[index].ExtentOffset, info.Extents[index].Size)
			if ek.FileOffset != info.Extents[index].FileOffset || ek.PartitionId != info.Extents[index].PartitionId ||
				ek.ExtentId != info.Extents[index].ExtentId || ek.ExtentOffset != info.Extents[index].ExtentOffset ||
				ek.Size != info.Extents[index].Size {
				log.LogInfof("inode %v with diff eks info", inodeInfo.Inode)
				isConsistent = false
				return
			}
		}
	}
	return
}


func isSameApplyID(applyIDSet []uint64) bool {
	applyID := applyIDSet[0]
	for index := 0; index < len(applyIDSet); index++ {
		if applyIDSet[index] != applyID {
			return false
		}
	}
	return true
}

func isSameCrcSum(crcSumSet []uint32) bool {
	firstCrcSum := crcSumSet[0]
	for _, crcSum := range crcSumSet {
		if crcSum != firstCrcSum {
			return false
		}
	}
	return true
}

func isSameMetaData(r []*proto.MetaDataCRCSumInfo) (isConsistent bool, inConsistentInfo string, needCheckInode, needHandleDelInodeTree bool){
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("PartitionID %v mismatch info:", r[0].PartitionID))
	isConsistent = true
	for treeType := DentryType; treeType < MaxType; treeType++ {
		if ok := validateCnt(int(treeType), r); !ok {
			sb.WriteString(fmt.Sprintf("%s with different count;", treeType.String()))
			if treeType == InodeType {
				needCheckInode = true
			}
			if treeType == DelInodeType {
				needHandleDelInodeTree = true
			}
			isConsistent = false
			continue
		}
		if ok := validateCrcSum(int(treeType), r); !ok {
			sb.WriteString(fmt.Sprintf("%s with different data;", treeType.String()))
			if treeType == InodeType {
				needCheckInode = true
			}
			isConsistent = false
		}
	}
	inConsistentInfo = sb.String()
	return
}

func validateCnt(typeIndex int, r []*proto.MetaDataCRCSumInfo) bool {
	first := r[0]
	for index := 0; index < len(r); index++ {
		if first.CntSet[typeIndex] != r[index].CntSet[typeIndex] {
			return false
		}
	}
	return true
}

func validateCrcSum(typeIndex int, r []*proto.MetaDataCRCSumInfo) bool {
	first := r[0]
	for index := 0; index < len(r); index++ {
		if first.CRCSumSet[typeIndex] != r[index].CRCSumSet[typeIndex] {
			return false
		}
	}
	return true
}

func validateInodes(resultSet []*proto.InodesCRCSumInfo) (mismatchInodes []uint64){
	countSet := make([]int, len(resultSet))
	for index, r := range resultSet {
		countSet[index] = len(r.InodesID)
	}
	if !isSameCount(countSet) {
		return checkMissingInodes(resultSet)
	}
	return checkMismatchInodes(resultSet)
}

func isSameCount(countSet []int) bool {
	firstCnt := countSet[0]
	for _, count := range countSet {
		if firstCnt != count {
			return false
		}
	}
	return true
}

func checkMismatchInodes(resultSet []*proto.InodesCRCSumInfo) []uint64 {
	firstInodesCRCSumSet := resultSet[0].CRCSumSet
	mismatchInodesID := make([]uint64, 0)
	for inodeIndex, crcSum := range firstInodesCRCSumSet {
		for _, result := range resultSet {
			if result.CRCSumSet[inodeIndex] != crcSum {
				mismatchInodesID = append(mismatchInodesID, result.InodesID[inodeIndex])
				break
			}
		}
	}
	return mismatchInodesID
}

func checkMissingInodes(resultSet []*proto.InodesCRCSumInfo) []uint64 {
	inodesIDMap := make(map[uint64]bool)
	for _, result := range resultSet {
		for _, inodeID := range result.InodesID {
			inodesIDMap[inodeID] = true
		}
	}

	missingInodes := make(map[uint64]bool, 0)
	for _, result := range resultSet {
		inodeMap := make(map[uint64]bool, len(result.InodesID))
		for _, inodeID := range result.InodesID {
			inodeMap[inodeID] = true
		}

		for inodeID, _ := range inodesIDMap {
			if ok := inodeMap[inodeID]; !ok {
				missingInodes[inodeID] = true
			}
		}
	}
	missingInodesID := make([]uint64, 0, len(missingInodes))
	for inode, _ := range missingInodes {
		missingInodesID = append(missingInodesID, inode)
	}
	return missingInodesID
}