package data_check

import (
	"fmt"
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

const (
	DefaultCheckBlockKB = 128
)

type CheckEngine struct {
	config    proto.CheckTaskInfo
	checkType int
	path      string

	currentVol        string
	mc                *master.MasterClient
	cluster           string
	mnProf            uint16
	dnProf            uint16
	closed            bool
	closeCh           chan bool
	repairPersist     *BadExtentPersist
	onCheckFail       func(uint32, string)
	checkedExtentsMap *sync.Map
}

type BadExtentInfo struct {
	PartitionID  uint64
	ExtentID     uint64
	ExtentOffset uint64
	FileOffset   uint64
	Size         uint64
	Hosts        []string
	Inode        uint64
	Volume       string
}

func NewCheckEngine(config proto.CheckTaskInfo, outputDir string, mc *master.MasterClient, extentCheckType int, path string) (checkEngine *CheckEngine, err error) {
	checkEngine = &CheckEngine{
		config:    config,
		checkType: extentCheckType,
		mc:        mc,
		cluster:   mc.Nodes()[0],
		mnProf:    mc.MetaNodeProfPort,
		dnProf:    mc.DataNodeProfPort,
		path:      path,
		closed:    false,
		closeCh:   make(chan bool, 1),
	}

	checkEngine.repairPersist, err = NewRepairPersist(outputDir, checkEngine.cluster)
	if err != nil {
		return
	}
	go checkEngine.repairPersist.PersistResult()
	checkEngine.onCheckFail = checkEngine.repairPersist.persistFailed
	log.LogInfof("NewCheckEngine end")
	return
}

func (checkEngine *CheckEngine) Start() (err error) {
	log.LogInfof("CheckEngine started, Domain[%v], CheckType[%v], Config[%v]", checkEngine.cluster, checkEngine.checkType, checkEngine.config)
	switch checkEngine.config.CheckMod {
	case proto.NodeExtent:
		return checkEngine.checkNode()
	case proto.VolumeInode:
		return checkEngine.checkVols()
	default:
		return fmt.Errorf("invalid check dimension")
	}
}

func (checkEngine *CheckEngine) Reset() {
	checkEngine.checkedExtentsMap = &sync.Map{}
}

func (checkEngine *CheckEngine) Close() {
	if checkEngine.closeCh != nil {
		close(checkEngine.closeCh)
	}
	checkEngine.repairPersist.Close()
	checkEngine.closed = true
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
		if len(filter.VolFilter) > 0 && !proto.FuzzyMatchString(v.Name, filter.VolFilter) {
			continue
		}
		if len(filter.VolExcludeFilter) > 0 && proto.FuzzyMatchString(v.Name, filter.VolExcludeFilter) {
			continue
		}
		log.LogDebugf("add volume[%v] to check list", v.Name)
		vols = append(vols, v.Name)
	}
	return
}
