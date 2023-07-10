package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"time"
)

func (s *ChubaoFSMonitor) scheduleToCompareMasterMetaDataDiff() {
	ticker := time.NewTicker(time.Duration(s.scheduleInterval) * time.Second)
	defer func() {
		ticker.Stop()
	}()
	s.compareMasterMetaDataDiff()
	for {
		select {
		case <-ticker.C:
			s.compareMasterMetaDataDiff()
		}
	}
}

func (s *ChubaoFSMonitor) compareMasterMetaDataDiff() {
	for _, host := range s.hosts {
		checkMetaDataWg.Add(1)
		go func(host *ClusterHost) {
			defer checkMetaDataWg.Done()
			log.LogInfof("compare master meta data diff [%v] begin\n", host)
			startTime := time.Now()
			result, err := compareMetaData(host)
			if err != nil {
				msg := fmt.Sprintf("compare master meta data diff from %v failed err[%v]", host, err)
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
				return
			}
			if !result {
				msg := fmt.Sprintf("compare master meta data diff from %v result is false", host)
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
				return
			}
			log.LogInfof("compareMasterMetaDataDiff [%v] end result[%v],cost[%v]\n", host, result, time.Since(startTime))
		}(host)
	}
	checkMetaDataWg.Wait()
}

func compareMetaData(host *ClusterHost) (result bool, err error) {
	//  curl "http://192.168.0.13:17010/metaData/compare?addr=all"
	reqURL := fmt.Sprintf("http://%v/metaData/compare?addr=all", host)
	data, err := doRequest(reqURL, true)
	if err != nil {
		return
	}
	compareResultSlice := strings.Split(string(data), "\n")
	log.LogInfof("compareResultSlice %v, %v", compareResultSlice, reqURL)
	if len(compareResultSlice) == 0 {
		log.LogErrorf("action[compareMetaData] compareResultSlice[%v] is 0\n", compareResultSlice)
		return
	}
	falseResult := 0
	trueResult := 0
	for _, compareResult := range compareResultSlice {
		if len(compareResult) == 0 {
			log.LogInfof("compareResult len is 0\n")
			continue
		}
		splitResult := strings.Split(compareResult, "result")
		if len(splitResult) < 2 {
			log.LogInfof("action[checkMetaDataCompare] result less than 2 splitResult:%v \n", splitResult)
			continue
		}
		if splitResult[1] == "[true]" {
			log.LogInfof("action[checkMetaDataCompare] reqURL[%v] result[true] %v \n", reqURL, compareResult)
			trueResult++
		} else if splitResult[1] == "[false]" {
			log.LogInfof("action[checkMetaDataCompare] reqURL[%v] result[false] %v \n", reqURL, compareResult)
			falseResult++
		}
	}
	if falseResult == 0 && trueResult > 0 {
		result = true
	}
	if falseResult == 0 && trueResult == 0 {
		log.LogErrorf("action[compareMetaData] false and true result is 0 \n")
	}
	return
}
