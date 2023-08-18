package data_check

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

const (
	CheckTypeExtentCrc  = 0
	CheckTypeInodeEkNum = 1
	CheckTypeInodeNlink = 2
)

type CheckEngine struct {
	tinyOnly      bool
	tinyInUse     bool
	concurrency   uint64
	checkType     int
	specifyInodes []uint64
	path          string
	specifyDps    []uint64
	modifyTimeMin time.Time
	modifyTimeMax time.Time

	currentVol        string
	mc                *master.MasterClient
	cluster           string
	mnProf            uint16
	dnProf            uint16
	isStop            func() bool
	repairPersist     *RepairPersist
	onFail            func(uint32, string)
	checkedExtentsMap *sync.Map
}

func NewCheckEngine(outputDir string, mc *master.MasterClient, tinyOnly, tinyInUse bool, concurrency uint64, checkType int,
	modifyTimeMin, modifyTimeMax string, specifyInodes, specifyDps []uint64, path string, isStop func() bool) (checkEngine *CheckEngine, err error) {
	var modifyTimestampMin, modifyTimestampMax time.Time
	if modifyTimestampMin, err = parseTime(modifyTimeMin); err != nil {
		return
	}
	if modifyTimestampMax, err = parseTime(modifyTimeMax); err != nil {
		return
	}
	checkEngine = &CheckEngine{
		tinyOnly:      tinyOnly,
		tinyInUse:     tinyInUse,
		concurrency:   concurrency,
		checkType:     checkType,
		modifyTimeMin: modifyTimestampMin,
		modifyTimeMax: modifyTimestampMax,
		specifyInodes: specifyInodes,
		specifyDps:    specifyDps,

		mc:      mc,
		cluster: mc.Nodes()[0],
		mnProf:  mc.MetaNodeProfPort,
		dnProf:  mc.DataNodeProfPort,
		path:    path,
		isStop:  isStop,
	}
	checkEngine.repairPersist = NewRepairPersist(outputDir, checkEngine.cluster)
	go checkEngine.repairPersist.PersistResult()
	checkEngine.onFail = checkEngine.repairPersist.persistFailed
	return
}

func (checkEngine *CheckEngine) Reset() {
	checkEngine.checkedExtentsMap = &sync.Map{}
}

func (checkEngine *CheckEngine) Close() {
	checkEngine.repairPersist.Close()
}

func parseTime(timeStr string) (t time.Time, err error) {
	if timeStr != "" {
		t, err = time.Parse("2006-01-02 15:04:05", timeStr)
		if err != nil {
			return
		}
	} else {
		t = time.Unix(0, 0)
	}
	return
}

func ExecuteVolumeTask(outputDir string, id int64, concurrency uint32, filter proto.Filter, mc *master.MasterClient, modifyMin, modifyMax string, stopFunc func() bool) (err error) {
	var (
		checkEngine *CheckEngine
		checkVols   []string
	)
	log.LogInfof("ExecuteVolumeTask begin, taskID:%v ", id)
	defer func() {
		if err != nil {
			log.LogErrorf("ExecuteVolumeTask end, taskID:%v, err: %v", id, err)
		}
		log.LogInfof("ExecuteVolumeTask end, taskID:%v, err: %v", id, err)
	}()
	checkVols, err = getVolsByFilter(mc, filter)
	if err != nil {
		return
	}
	checkEngine, err = NewCheckEngine(
		outputDir,
		mc,
		false,
		false,
		uint64(concurrency),
		CheckTypeExtentCrc,
		modifyMin,
		modifyMax,
		make([]uint64, 0),
		make([]uint64, 0),
		"",
		stopFunc)
	if err != nil {
		return
	}
	defer checkEngine.Close()
	checkEngine.CheckVols(checkVols)

	checkEngine.Reset()
	checkEngine.CheckFailedVols()
	return
}

func ExecuteDataNodeTask(outputDir string, id int64, concurrency uint32, node string, mc *master.MasterClient, modifyMin string, checkTiny bool) (err error) {
	var checkEngine *CheckEngine
	defer func() {
		if err != nil {
			log.LogInfof("executeDataNodeTask end, taskID:%v, err:%v", id, err)
		}
	}()
	checkEngine, err = NewCheckEngine(
		outputDir,
		mc,
		checkTiny,
		false,
		uint64(concurrency),
		CheckTypeExtentCrc,
		modifyMin,
		"",
		make([]uint64, 0),
		make([]uint64, 0),
		"",
		func() bool {
			return false
		})
	if err != nil {
		return
	}
	defer checkEngine.Close()
	log.LogInfof("executeDataNodeTask begin, taskID:%v ", id)
	err = checkEngine.CheckDataNodeCrc(node)
	log.LogInfof("executeDataNodeTask end, taskID:%v ", id)
	return
}

func getVolsByFilter(mc *master.MasterClient, filter proto.Filter) (vols []string, err error) {
	var volsInfo []*proto.VolInfo
	for i := 1; i < 20; i++ {
		volsInfo, err = mc.AdminAPI().ListVols("")
		if err == nil {
			break
		}
	}
	if err != nil {
		return
	}
	vols = make([]string, 0)
	for _, v := range volsInfo {
		volume, e := mc.AdminAPI().GetVolumeSimpleInfo(v.Name)
		if e != nil {
			return vols, e
		}
		if len(filter.VolFilter) > 0 && !include(v.Name, filter.VolFilter) {
			continue
		}
		if len(filter.VolExcludeFilter) > 0 && include(v.Name, filter.VolExcludeFilter) {
			continue
		}
		if len(filter.ZoneFilter) > 0 && !include(volume.ZoneName, filter.ZoneFilter) {
			continue
		}
		if len(filter.ZoneExcludeFilter) > 0 && include(volume.ZoneName, filter.ZoneExcludeFilter) {
			continue
		}
		vols = append(vols, v.Name)
	}
	return
}

func include(val string, strs []string) bool {
	for _, s := range strs {
		if s == val {
			return true
		}
	}
	return false
}
