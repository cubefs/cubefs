package repaircrc_server

import (
	"fmt"
	"github.com/chubaofs/chubaofs/cli/cmd"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
	"regexp"
	"strings"
	"time"
)

type RepairCrcTask struct {
	TaskId        int64       `json:"task_id"`
	ClusterInfo   *ClusterInfo `json:"cluster_info"`
	LimitLevel    uint32       `json:"limit_level"`
	Filter        *Filter      `json:"filter"`
	Frequency     *Frequency   `json:"frequency"`
	ModifyTimeMin string       `json:"modify_time_min"`
	ModifyTimeMax string       `json:"modify_time_max"`
	RepairType    RepairType   `json:"repair_type"`
	CheckTiny     bool          `json:"check_tiny"`
	NodeAddress   string       `json:"node_address"`
	mc            *master.MasterClient
	stopC         chan bool
}

type Filter struct {
	VolFilter         string  `json:"vol_filter"`
	VolExcludeFilter  string  `json:"vol_exclude_filter"`
}

type Frequency struct {
	Interval     uint32   `json:"interval"`
	ExecuteCount uint32   `json:"execute_count"`
}

type ClusterInfo struct {
	Master     string  `json:"master"`
	DnProf     uint16  `json:"dn_prof"`
	MnProf     uint16  `json:"mn_prof"`
}

type RepairType uint8

const (
	RepairDataNode RepairType = iota
	RepairVolume
)

const (
	defaultIntervalHour = 24
)


func NewRepairTask() *RepairCrcTask {
	return &RepairCrcTask{
		Frequency: &Frequency{
			Interval: defaultIntervalHour,
		},
		ClusterInfo: new(ClusterInfo),
		Filter: new(Filter),
		stopC: make(chan bool, 8),
	}
}

func (t *RepairCrcTask) validTask() (err error) {
	if t.ClusterInfo.Master == "" || t.ClusterInfo.MnProf == 0 || t.ClusterInfo.DnProf == 0 {
		err = fmt.Errorf("cluster info illegal")
		return
	}
	re := regexp.MustCompile(`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)+([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$`)
	if strings.Contains(t.ClusterInfo.Master, ":") && !re.MatchString(strings.Split(t.ClusterInfo.Master, ":")[0]) {
		err = fmt.Errorf("master address illegal")
		return
	}
	if !strings.Contains(t.ClusterInfo.Master, ":") && !re.MatchString(t.ClusterInfo.Master) {
		err = fmt.Errorf("master address illegal")
		return
	}
	if t.Frequency.Interval == 0 || t.Frequency.ExecuteCount < 0 {
		err = fmt.Errorf("frequency info illegal")
		return
	}
	if t.RepairType > RepairVolume {
		err = fmt.Errorf("repair type illegal")
		return
	}
	if t.ModifyTimeMin != "" {
		if _, err = time.Parse("2006-01-02 15:04:05", t.ModifyTimeMin); err != nil {
			err = fmt.Errorf("modifyTimeMin illegal, err:%v", err)
			return
		}
	}
	if t.ModifyTimeMax != "" {
		if _, err = time.Parse("2006-01-02 15:04:05", t.ModifyTimeMax); err != nil {
			err = fmt.Errorf("modifyTimeMin illegal, err:%v", err)
			return
		}
	}
	if t.RepairType == RepairDataNode && t.NodeAddress == "" {
		err = fmt.Errorf("nodeAddress can not be empty when repair datanode")
		return
	}
	if t.NodeAddress != "" && !re.MatchString(strings.Split(t.NodeAddress, ":")[0]) {
		err = fmt.Errorf("nodeAddress illegal")
		return
	}
	return
}

func (t *RepairCrcTask) executeVolumeTask() (err error) {
	var (
		volsInfo []*proto.VolInfo
		para      *cmd.CheckParam
	)
	log.LogInfof("executeVolumeTask begin, taskID:%v ", t.TaskId)
	defer func() {
		log.LogInfof("executeVolumeTask end, taskID:%v ", t.TaskId)
	}()
	for i := 1; i<20; i++ {
		volsInfo, err = t.mc.AdminAPI().ListVols("")
		if err == nil {
			break
		}
	}
	if err != nil {
		return
	}
	mpConcurrency := t.LimitLevel
	inodeConcurrency := t.LimitLevel
	extentConcurrency := t.LimitLevel
	vols := make([]string, 0)
	for _, v := range volsInfo {
		if t.Filter.VolFilter != "" && !strings.Contains(v.Name, t.Filter.VolFilter) {
			continue
		}
		if t.Filter.VolExcludeFilter != "" && strings.Contains(v.Name, t.Filter.VolExcludeFilter) {
			continue
		}
		vols = append(vols, v.Name)
	}
	para, err = cmd.NewCheckParam(false, false, uint64(mpConcurrency), uint64(inodeConcurrency), uint64(extentConcurrency), checkTypeExtentReplica, t.ModifyTimeMin, t.ModifyTimeMax, 0, make([]uint64, 0), make([]uint64, 0))
	if err != nil {
		return
	}
	var checkStop = func() bool {
		select {
		case <- t.stopC:
			return true
		default:
			return false
		}
	}
	cmd.CheckVols(vols, t.mc, para,"", checkStop)
	return

}

func (t *RepairCrcTask) executeDataNodeTask() (err error) {
	log.LogInfof("executeDataNodeTask begin, taskID:%v ", t.TaskId)
	cmd.CheckDataNodeCrc(t.NodeAddress, t.mc, uint64(t.LimitLevel), checkTypeExtentReplica, t.ModifyTimeMin, t.CheckTiny)
	log.LogInfof("executeDataNodeTask end, taskID:%v ", t.TaskId)
	return
}
