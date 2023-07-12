package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

const (
	defaultCheckSpecificVolInterval = time.Hour * 1
	defaultWaitForMPLeader          = time.Minute * 2
	defaultWaitAfterCreateMP        = time.Minute * 2
	defaultWaitAfterCreateDp        = time.Minute * 1
)

const (
	defaultCrateDpMaxCount = 1000
)

const (
	ReadOnly    = 1
	ReadWrite   = 2
	Unavailable = -1
)

func (s *ChubaoFSMonitor) scheduleToCheckSpecificVol() {
	s.checkSpecificVols()
	for {
		t := time.NewTimer(defaultCheckSpecificVolInterval)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkSpecificVols()
		}
	}
}

func (s *ChubaoFSMonitor) checkSpecificVols() {
	// 保障最小可写MP DP
	log.LogInfof("checkSpecificVols begin")
	startTime := time.Now()
	wg := new(sync.WaitGroup)
	for _, v := range s.MinRWDPAndMPVols {
		wg.Add(1)
		go func(vol MinRWDPAndMPVolInfo) {
			defer wg.Done()
			s.checkSpecificVol(vol.VolName, vol.Host, vol.MinRWMPCount, vol.MinRWDPCount)
		}(v)
	}
	wg.Wait()
	log.LogInfof("checkSpecificVols end,cost[%v]", time.Since(startTime))
}

func (s *ChubaoFSMonitor) checkSpecificVol(volName, host string, minRwMpCount, minRwDpCount int) {
	ch := newClusterHost(host)
	vol, err := getVolSimpleView(volName, ch)
	if err != nil {
		log.LogErrorf("check vol[%v] failed,err[%v]\n", volName, err)
		return
	}
	if vol.RwDpCnt < minRwDpCount {
		count := minRwDpCount - vol.RwDpCnt
		msg := fmt.Sprintf("vol:%v totalDpCount:%v RwDpCnt:%v minRwCount:%v", volName, vol.RwDpCnt, vol.DpCnt, minRwDpCount)
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
		checkAndCreateDataPartitions(volName, ch, count, minRwDpCount)
	}
	rwMPCount, err := checkRwMetaPartitionCount(volName, ch)
	if err != nil {
		log.LogErrorf("action[checkSpecificVol] vol:%v host:%v checkRwMetaPartitionCount err:%v", vol.Name, ch.host, err)
		return
	}
	if rwMPCount < minRwMpCount {
		count := minRwMpCount - rwMPCount
		msg := fmt.Sprintf("vol:%v totalMPCount:%v rwMPCount:%v minRwCount:%v", vol.Name, vol.MpCnt, rwMPCount, minRwMpCount)
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
		log.LogWarn(msg)
		err = checkAndCreateMetaPartitions(volName, ch, count, minRwMpCount)
		if err != nil {
			log.LogErrorf("action[checkSpecificVol] vol:%v create mp rwMPCount:%v lack count:%v err:%v",
				vol.Name, rwMPCount, count, err)
		}
	}
}

func checkAndCreateDataPartitions(volName string, ch *ClusterHost, targetCount int, minRwDpCount int) {
	for targetCount > 0 {
		count := targetCount
		if count > defaultCrateDpMaxCount {
			count = defaultCrateDpMaxCount
		}
		reqUrl := generateAllocateDataPartitionURL(ch.host, volName, count)
		_, err := doRequest(reqUrl, ch.isReleaseCluster)
		if err != nil {
			log.LogErrorf("action[checkAndCreateDataPartitions] vol:%v lack count:%v create count:%v reqUrl:%v err:%v",
				volName, targetCount, count, reqUrl, err)
			return
		}
		log.LogWarnf("action[checkAndCreateDataPartitions] vol:%v lack count:%v create count:%v reqUrl:%v ",
			volName, targetCount, count, reqUrl)
		targetCount -= count
		time.Sleep(defaultWaitAfterCreateDp)

		vol, err := getVolSimpleView(volName, ch)
		if err != nil {
			log.LogErrorf("check vol[%v] failed,err[%v]\n", volName, err)
			return
		}
		if vol.RwDpCnt >= minRwDpCount {
			return
		}
	}
}

func checkRwMetaPartitionCount(volName string, ch *ClusterHost) (rwMPCount int, err error) {
	reqURL := fmt.Sprintf("http://%v/client/metaPartitions?name=%v", ch.host, volName)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	mpvs := make([]*MetaPartitionView, 0)
	if err = json.Unmarshal(data, &mpvs); err != nil {
		return
	}
	for _, mp := range mpvs {
		if mp.Status == ReadWrite {
			rwMPCount++
		}
	}
	return
}

func checkAndCreateMetaPartitions(volName string, ch *ClusterHost, targetCount int, minRwMpCount int) (err error) {
	for createdCount := 0; createdCount < targetCount; {
		var data []byte
		reqURL := fmt.Sprintf("http://%v/client/metaPartitions?name=%v", ch.host, volName)
		data, err = doRequest(reqURL, ch.isReleaseCluster)
		if err != nil {
			return
		}
		mpvs := make([]*MetaPartitionView, 0)
		if err = json.Unmarshal(data, &mpvs); err != nil {
			return
		}
		rwMPCount := 0
		for _, mp := range mpvs {
			if mp.Status == ReadWrite {
				rwMPCount++
			}
		}
		// 检查当前的数目
		lackCount := minRwMpCount - rwMPCount
		if lackCount <= 0 {
			log.LogDebugf("action[checkAndCreateMetaPartitions] vol:%v create mp, mpCount:%v rwMPCount:%v lack count:%v ",
				volName, len(mpvs), rwMPCount, lackCount)
			return
		}
		// 检查最大的 是不是 no leader
		vol, err1 := getVolSimpleView(volName, ch)
		if err1 != nil {
			err = fmt.Errorf("check vol[%v] failed,err[%v]\n", volName, err1)
			return
		}
		hasLeader, start, err1 := checkMetaPartitionHasLeader(ch, volName, vol.MaxMetaPartitionID)
		if err1 != nil {
			err = fmt.Errorf(" checkMetaPartitionHasLeader err:%v", err1)
			return
		}
		if !hasLeader {
			time.Sleep(defaultWaitForMPLeader)
			continue
		}
		// 执行创建mp
		reqUrl := generateAllocateMetaPartitionURL(ch.host, volName, start)
		_, err = doRequest(reqUrl, ch.isReleaseCluster)
		if err != nil {
			log.LogErrorf("action[checkAndCreateMetaPartitions] vol:%v create mp, mpCount:%v rwMPCount:%v lack count:%v reqUrl:%v err:%v",
				volName, len(mpvs), rwMPCount, lackCount, reqUrl, err)
		}
		log.LogWarnf("action[checkAndCreateMetaPartitions] vol:%v create mp, mpCount:%v rwMPCount:%v lack count:%v reqUrl:%v",
			volName, len(mpvs), rwMPCount, lackCount, reqUrl)
		createdCount++
		time.Sleep(defaultWaitAfterCreateMP)
	}
	return
}

func checkMetaPartitionHasLeader(ch *ClusterHost, volName string, id uint64) (hasLeader bool, start uint64, err error) {
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
	mp := &MetaPartition{}
	if err = json.Unmarshal(data, mp); err != nil {
		log.LogErrorf("unmarshal to mp data[%v],err[%v]", string(data), err)
		return
	}
	for _, replica := range mp.Replicas {
		if replica.IsLeader {
			hasLeader = true
		}
	}
	if start < mp.Start {
		start = mp.Start
	}
	if start < mp.MaxInodeID {
		start = mp.MaxInodeID
	}
	return
}

func generateAllocateMetaPartitionURL(host, volName string, start uint64) string {
	return fmt.Sprintf("http://%v/metaPartition/create?name=%v&start=%v", host, volName, start)
}
