package cmd

import (
	"bufio"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

func rangeAllDataPartitions(limit uint64, specifyVols []string, specifyIds []uint64, doAfterVolumeFunc func(volume *proto.SimpleVolView), doPartitionFunc func(volumeName string, partition *proto.DataPartitionResponse) error) {
	var existString = func(strs []string, target string) (exist bool) {
		for _, s := range strs {
			if s == target {
				exist = true
				return
			}
		}
		return
	}

	var existId = func(ids []uint64, target uint64) (exist bool) {
		for _, s := range ids {
			if s == target {
				exist = true
				return
			}
		}
		return
	}
	cv, err := client.AdminAPI().GetCluster()
	if err != nil {
		log.LogErrorf("err: %v", err)
		return
	}
	log.LogInfof("action[rangeAllDataPartitions] cluster name: %v", cv.Name)
	scanLimitCh := make(chan bool, limit)
	for _, v := range cv.VolStatInfo {
		if specifyVols != nil && len(specifyVols) > 0 && !existString(specifyVols, v.Name) {
			continue
		}
		volume, err1 := client.AdminAPI().GetVolumeSimpleInfo(v.Name)
		if err1 != nil {
			log.LogErrorf("action[rangeAllDataPartitions] admin get volume: %v, err: %v", v.Name, err1)
			continue
		}
		clv, err1 := client.ClientAPI().GetVolume(volume.Name, calcAuthKey(volume.Owner))
		if err1 != nil {
			log.LogErrorf("action[rangeAllDataPartitions] client get volume: %v, err: %v", v.Name, err1)
			continue
		}
		log.LogInfof("action[rangeAllDataPartitions] scan volume:%v start", volume.Name)
		wg := sync.WaitGroup{}
		for _, dp := range clv.DataPartitions {
			if specifyIds != nil && len(specifyIds) > 0 && !existId(specifyIds, dp.PartitionID) {
				continue
			}
			wg.Add(1)
			scanLimitCh <- true
			go func(partition *proto.DataPartitionResponse) {
				defer func() {
					wg.Done()
					<-scanLimitCh
				}()
				err2 := doPartitionFunc(volume.Name, partition)
				if err2 != nil {
					log.LogErrorf(err2.Error())
				}
			}(dp)
		}
		wg.Wait()
		log.LogInfof("action[rangeAllDataPartitions] scan volume:%v, end", volume.Name)
		doAfterVolumeFunc(volume)
	}
}
//format: "partitionId extentId host"
func loadRepairExtents() (extentsMap map[string]map[uint64]map[uint64]bool) {
	extentsMap = make(map[string]map[uint64]map[uint64]bool, 0)
	buf := make([]byte, 2048)
	idsF, err := os.OpenFile("repair_extents", os.O_RDONLY, 0666)
	if err != nil {
		return
	}
	defer idsF.Close()
	o := bufio.NewReader(idsF)
	for {
		buf, _, err = o.ReadLine()
		if err == io.EOF {
			break
		}
		dpExtents := strings.Split(string(buf), " ")
		if len(dpExtents) < 3 {
			stdout("invalid dpExtent: %v", dpExtents)
			continue
		}
		var pid, eid uint64
		var host string
		pid, err = strconv.ParseUint(dpExtents[0], 10, 64)
		if err != nil {
			stdout("invalid dp: %v", dpExtents[0])
			continue
		}
		eid, err = strconv.ParseUint(dpExtents[1], 10, 64)
		if err != nil {
			stdout("invalid extent: %v", dpExtents[1])
			continue
		}
		host = dpExtents[2]
		if pid > 0 && eid > 0 {
			if _, ok := extentsMap[host]; !ok {
				extentsMap[host] = make(map[uint64]map[uint64]bool, 0)
			}
			if _, ok := extentsMap[host][pid]; !ok {
				extentsMap[host][pid] = make(map[uint64]bool, 0)
			}
			extentsMap[host][pid][eid] = true
		}
	}

	ekMap := make(map[string]string, 0)
	for k, v := range extentsMap {
		for pid, exts := range v {
			for e := range exts {
				ekStr := fmt.Sprintf("%d-%d", pid, e)
				if h, ok := ekMap[ekStr]; !ok {
					ekMap[ekStr] = k
				} else {
					delete(exts, e)
					if _, exist := extentsMap[h][pid][e]; exist {
						delete(extentsMap[h][pid], e)
					}
					fmt.Printf("found duplicate error extent:%v %v %v\n", ekStr, h, k)
				}
			}
		}
	}
	return
}
func loadSpecifiedPartitions() (ids []uint64) {
	ids = make([]uint64, 0)
	buf := make([]byte, 2048)
	var err error
	idsF, _ := os.OpenFile("ids", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer idsF.Close()
	o := bufio.NewReader(idsF)
	for {
		buf, _, err = o.ReadLine()
		if err == io.EOF {
			break
		}
		id, _ := strconv.ParseUint(string(buf), 10, 64)
		if id > 0 {
			ids = append(ids, id)
		}
	}
	return
}


func loadSpecifiedVolumes(volFilter, volExcludeFilter string) (vols []string) {
	volsF, err := os.OpenFile("vols", os.O_RDONLY, 0666)
	if err != nil {
		return
	}
	defer volsF.Close()
	r := bufio.NewReader(volsF)
	vols = make([]string, 0)
	buf := make([]byte, 2048)
	for {
		buf, _, err = r.ReadLine()
		if err == io.EOF {
			break
		}
		if volFilter != "" {
			if !strings.Contains(string(buf), volFilter) {
				continue
			}
		}
		if volExcludeFilter != "" {
			if strings.Contains(string(buf), volExcludeFilter) {
				continue
			}
		}
		vols = append(vols, string(buf))
	}
	return
}
