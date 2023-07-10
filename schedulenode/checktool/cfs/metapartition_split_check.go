package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"sort"
	"sync"
	"time"
)

const (
	ConcurrentNumber                       = 10
	maxCount                               = 20000000 // inodeCount+dentryCount
	createMpCount                          = 5
	defaultMetaPartitionInodeIDStep uint64 = 1 << 24
	maxMetaPartitionInodeID         uint64 = 1<<63 - 1
)

var (
	requireCheckMasterDomain = []string{"cn.chubaofs.jd.local", "cn.elasticdb.jd.local", "cn.chubaofs-seqwrite.jd.local"}
)

func (s *ChubaoFSMonitor) scheduleToCheckMetaPartitionSplit() {
	s.checkMetaPartitionSplit()
	for {
		t := time.NewTimer(30 * 60 * time.Second)
		select {
		case <-t.C:
			s.checkMetaPartitionSplit()
		}
	}
}

func (s *ChubaoFSMonitor) checkMetaPartitionSplit() {
	for _, host := range s.hosts {
		if !domainExist(host.host) {
			continue
		}
		cluster, err := getCluster(host)
		if err != nil {
			_, ok := err.(*json.SyntaxError)
			if ok {
				continue
			}
			log.LogErrorf("get cluster info from %v failed,err:%v", host.host, err)
			continue
		}
		shouldCreateMpVols, endIncorrectMpIds := s.checkMaxMpInodeCount(cluster.VolStat, host)
		for _, vol := range shouldCreateMpVols {
			go func(volName string, inodeCount, dentryCount uint64, host *ClusterHost) {
				s.checkAndCreateMp(volName, inodeCount, dentryCount, host)
			}(vol.volumeName, vol.inodeCount, vol.dentryCount, host)
		}
		for _, incorrect := range endIncorrectMpIds {
			s.umpMpEndValueAlarm(incorrect, host)
		}
	}
}

func (s *ChubaoFSMonitor) checkAndCreateMp(volName string, inodeCount, dentryCount uint64, host *ClusterHost) {
	var warnMsg string
	if err := s.checkAndCreateMetaPartitions(volName, host, createMpCount); err != nil {
		log.LogErrorf("check the mp of large inodeCount and dentryCount, host(%v) volume(%v) inodeCount(%v) dentryCount(%v) err(%v)", host.host, volName, inodeCount, dentryCount, err)
		warnMsg = fmt.Sprintf("check the mp of large inodeCount and dentryCount, host[%v] volume[%v] inodeCount[%v] dentryCount[%v] err", host.host, volName, inodeCount, dentryCount)
	} else {
		warnMsg = fmt.Sprintf("check the mp of large inodeCount and dentryCount, host[%v] volume[%v] create mp count(%v) success, because inodeCount[%v], dentryCount[%v]",
			host.host, volName, createMpCount, inodeCount, dentryCount)
	}
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, warnMsg)
}

func (s *ChubaoFSMonitor) umpMpEndValueAlarm(vMpIds VolumeEndIncorrectMpId, host *ClusterHost) {
	var warnMsg = fmt.Sprintf("check that the End value of non-maximum mpId is incorrect, host[%v] volume[%v] mpIds[%v]", host.host, vMpIds.volumeName, vMpIds.mpIds)
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, warnMsg)
}

func domainExist(domain string) bool {
	for _, md := range requireCheckMasterDomain {
		if domain == md {
			return true
		}
	}
	return false
}

type VolumeMpInodeDentryCount struct {
	volumeName                    string
	mpId, inodeCount, dentryCount uint64
}

type VolumeEndIncorrectMpId struct {
	volumeName string
	mpIds      []uint64
}

func (s *ChubaoFSMonitor) checkMaxMpInodeCount(vols []*VolSpaceStat, host *ClusterHost) (shouldCreateMpVols []VolumeMpInodeDentryCount, endIncorrectMpIds []VolumeEndIncorrectMpId) {
	var (
		err   error
		wg    sync.WaitGroup
		ch    = make(chan struct{}, ConcurrentNumber)
		mutex sync.Mutex
	)
	wg.Add(len(vols))
	for _, vol := range vols {
		ch <- struct{}{}
		go func(volName string, host *ClusterHost) {
			defer func() {
				<-ch
				wg.Done()
			}()
			var mps []*MetaPartition
			mps, err = getMetaPartitionsFromVolume(volName, host)
			if err != nil {
				log.LogErrorf("getMetaPartitionsFromVolume, volume(%v), host(%v), err(%v)", volName, host, err)
				return
			}
			if len(mps) == 0 {
				return
			}
			mpId, inodeCount, dentryCount, shouldSplit := s.checkMpInodeCount(mps, host.host, volName, host.isReleaseCluster)
			if shouldSplit {
				log.LogDebugf("volName:%v, maxMpId:%v, InodeCount:%v, DentryCount:%v should create mp.\n", volName, mpId, inodeCount, dentryCount)
				c := VolumeMpInodeDentryCount{
					volName,
					mpId,
					inodeCount,
					dentryCount,
				}
				mutex.Lock()
				shouldCreateMpVols = append(shouldCreateMpVols, c)
				mutex.Unlock()
			}
			endValueIncorrectMpIds := s.checkEachMpEndValue(mps)
			if len(endValueIncorrectMpIds) > 0 {
				mutex.Lock()
				endIncorrectMpIds = append(endIncorrectMpIds, VolumeEndIncorrectMpId{
					volumeName: volName,
					mpIds:      endValueIncorrectMpIds,
				})
				mutex.Unlock()
			}
		}(vol.Name, host)
	}
	wg.Wait()
	return
}

func (s *ChubaoFSMonitor) checkMpInodeCount(mpViews []*MetaPartition, host, volume string, isReleaseCluster bool) (mpId, inodeCount, dentryCount uint64, shouldSplit bool) {
	sort.SliceStable(mpViews, func(i, j int) bool {
		return mpViews[i].PartitionID > mpViews[j].PartitionID
	})
	if mpViews[0] != nil && isReleaseCluster {
		if releaseMp, err := cfs.GetMetaPartition(host, mpViews[0].PartitionID, isReleaseCluster); err != nil {
			log.LogErrorf("cfs GetMetaPartition, host(%v) volume(%v) mpId(%v) err(%v)", host, volume, mpViews[0].PartitionID, err)
		} else {
			mpViews[0].InodeCount = releaseMp.InodeCount
			mpViews[0].DentryCount = releaseMp.DentryCount
		}
	}
	if mpViews[0] != nil && mpViews[0].InodeCount+mpViews[0].DentryCount >= maxCount {
		return mpViews[0].PartitionID, mpViews[0].InodeCount, mpViews[0].DentryCount, true
	}
	return
}

func (s *ChubaoFSMonitor) checkEachMpEndValue(mpViews []*MetaPartition) (mpIds []uint64) {
	sort.SliceStable(mpViews, func(i, j int) bool {
		return mpViews[i].PartitionID > mpViews[j].PartitionID
	})
	mpViewLen := len(mpViews)
	if mpViewLen == 0 || mpViewLen == 1 {
		return
	}
	endCheckMpViews := mpViews[1:]
	for _, mpView := range endCheckMpViews {
		if mpView.End < maxMetaPartitionInodeID {
			continue
		}
		mpIds = append(mpIds, mpView.PartitionID)
	}
	return
}

func (s *ChubaoFSMonitor) checkAndCreateMetaPartitions(volName string, ch *ClusterHost, targetCount int) (err error) {
	for createdCount := 0; createdCount < targetCount; {
		var maxMetaPartitionId uint64
		if ch.isReleaseCluster {
			maxMetaPartitionId, err = getReleaseMaxMetaPartitionId(volName, ch)
			if maxMetaPartitionId == 0 {
				err = fmt.Errorf("getReleaseMaxMetaPartitionId check vol[%v] failed, host[%v] maxMetaPartitionId[%v] err[%v]\n", volName, ch.host, maxMetaPartitionId, err)
				return
			}
			if err != nil {
				err = fmt.Errorf("getReleaseMaxMetaPartitionId check vol[%v] failed, host[%v] err[%v]\n", volName, ch.host, err)
				return
			}
		} else {
			var vol *SimpleVolView
			vol, err = getVolSimpleView(volName, ch)
			if err != nil {
				err = fmt.Errorf("getVolSimpleView check vol[%v] failed, host[%v] err[%v]\n", volName, ch.host, err)
				return
			}
			maxMetaPartitionId = vol.MaxMetaPartitionID
		}
		hasLeader, mp, err1 := s.checkMetaPartitionHasLeader(ch, volName, maxMetaPartitionId)
		if err1 != nil {
			err = fmt.Errorf("checkMetaPartitionHasLeader err:%v", err1)
			return
		}
		if !hasLeader {
			time.Sleep(defaultWaitForMPLeader)
			continue
		}
		nextStart := getNextStart(mp.MaxInodeID, mp.Start, ch.isReleaseCluster)
		reqUrl := generateAllocateMetaPartitionURL(ch.host, volName, nextStart)
		_, err = doRequest(reqUrl, ch.isReleaseCluster)
		if err != nil {
			log.LogErrorf("checkAndCreateMetaPartitions vol:%v create mp, reqUrl:%v err:%v", volName, reqUrl, err)
		}
		log.LogWarnf("checkAndCreateMetaPartitions vol:%v create mp success, reqUrl:%v", volName, reqUrl)
		createdCount++
		time.Sleep(defaultWaitAfterCreateMP)
	}
	return
}

func (s *ChubaoFSMonitor) checkMetaPartitionHasLeader(ch *ClusterHost, volName string, id uint64) (hasLeader bool, mp *MetaPartition, err error) {
	var reqURL string
	if !ch.isReleaseCluster {
		reqURL = fmt.Sprintf("http://%v/metaPartition/get?name=%v&id=%v", ch.host, volName, id)
	} else {
		reqURL = fmt.Sprintf("http://%v/client/metaPartition?name=%v&id=%v", ch.host, volName, id)
	}
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	mp = &MetaPartition{}
	if err = json.Unmarshal(data, mp); err != nil {
		log.LogErrorf("unmarshal to mp data[%v],err[%v]", string(data), err)
		return
	}
	for _, replica := range mp.Replicas {
		if replica.IsLeader {
			hasLeader = true
		}
	}
	return
}

func getReleaseMaxMetaPartitionId(volume string, host *ClusterHost) (maxMetaPartitionId uint64, err error) {
	mps, err := getMetaPartitionsFromVolume(volume, host)
	if err != nil {
		log.LogErrorf("getReleaseMaxMetaPartitionId, volume(%v), host(%v), err(%v)", volume, host, err)
		return
	}
	if len(mps) == 0 {
		return 0, nil
	}
	sort.SliceStable(mps, func(i, j int) bool {
		return mps[i].PartitionID > mps[j].PartitionID
	})
	if mps[0] != nil {
		return mps[0].PartitionID, nil
	}
	return 0, nil
}

func getNextStart(maxInodeId, start uint64, isReleaseCluster bool) (nextStart uint64) {
	if !isReleaseCluster {
		return 0
	}
	if maxInodeId > start {
		nextStart = maxInodeId + defaultMetaPartitionInodeIDStep
	} else {
		nextStart = start + defaultMetaPartitionInodeIDStep
	}
	return
}
