package img

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/img"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"runtime/debug"
	"strconv"
	"time"
)

func (s *VolStoreMonitor) scheduleToCheckVols() {
	s.checkImageVols()
	ticker := time.NewTicker(time.Duration(s.scheduleInterval) * time.Second)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-ticker.C:
			s.checkImageVols()
		}
	}
}

func (s *VolStoreMonitor) checkImageVols() {
	for _, host := range s.hosts {
		log.LogInfof("checkImageVols [%v] begin", host)
		startTime := time.Now()
		s.checkRWVols(host)
		log.LogInfof("checkImageVols [%v] end,cost[%v]", host, time.Since(startTime))
	}
}

func (s *VolStoreMonitor) checkRWVols(host string) {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("action[checkRWVols] panic r:%v", r)
			fmt.Println(msg)
			log.LogError(msg)
			debug.PrintStack()
		}
	}()
	needCreateVol, rwVolCount, availTB := s.checkNeedCreateVol(host, "ds_image")
	if needCreateVol {
		msg := fmt.Sprintf("host:%v rwVolCount:%v,availTB:%v less than minCount:%v or minAvail:%v", host, rwVolCount, availTB, s.minRWVolCount, s.minAvailOfAllVolsTB)
		checktool.WarnBySpecialUmpKey(UMPImageRwVolsKey, msg)
	}
}

func (s *VolStoreMonitor) checkNeedCreateVol(host string, clusterName string) (needCreateVol bool, rwVolCount int, availTB float64) {
	var (
		err      error
		volCount int
	)
	defer func() {
		log.LogInfo(fmt.Sprintf("checkRWVols host:%v volCount:%v rwVolCount:%v availTB:%v needCreateVol:%v err:%v",
			host, volCount, rwVolCount, availTB, needCreateVol, err))
	}()
	//检查可用vols数量
	vols, err := img.GetClusterVolView(host, clusterName)
	if err != nil {
		return
	}
	volCount = len(vols.Vols)
	for _, vol := range vols.Vols {
		if vol.VolStatus == img.VolReadWrite {
			rwVolCount++
		}
	}
	if rwVolCount < s.minRWVolCount {
		needCreateVol = true
	}

	//检查空间信息 AvailOfAllVols
	availTB, err = getAvailOfAllVols(host, clusterName)
	if err != nil {
		return
	}
	if availTB < s.minAvailOfAllVolsTB {
		needCreateVol = true
	}
	return
}

func getAvailOfAllVols(host, clusterName string) (availTB float64, err error) {
	space, err := img.GetClusterSpace(host, clusterName)
	if err != nil {
		return
	}
	if space == nil {
		return 0, fmt.Errorf("get space failed")
	}
	availTB, err = strconv.ParseFloat(space.AvailOfAllVols, 32)
	return
}
