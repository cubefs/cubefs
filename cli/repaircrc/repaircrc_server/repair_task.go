package repaircrc_server

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"regexp"
	"strings"
	"time"
)

type RepairCrcTask struct {
	proto.CheckCrcTaskInfo
	Frequency   *Frequency   `json:"frequency"`
	TaskId      int64        `json:"task_id"`
	ClusterInfo *ClusterInfo `json:"cluster_info"`
	mc          *master.MasterClient
	stopC       chan bool
}

type Frequency struct {
	Interval     uint32 `json:"interval"`
	ExecuteCount uint32 `json:"execute_count"`
}

type ClusterInfo struct {
	Master string `json:"master"`
	DnProf uint16 `json:"dn_prof"`
	MnProf uint16 `json:"mn_prof"`
}

const (
	defaultIntervalHour = 24
)

func NewRepairTask() *RepairCrcTask {
	return &RepairCrcTask{
		CheckCrcTaskInfo: proto.CheckCrcTaskInfo{
			Frequency: proto.Frequency{
				Interval: defaultIntervalHour,
			},
			Filter: proto.Filter{},
		},
		ClusterInfo: new(ClusterInfo),
		stopC:       make(chan bool, 8),
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
	if t.RepairType > proto.RepairVolume {
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
	if t.RepairType == proto.RepairDataNode && t.NodeAddress == "" {
		err = fmt.Errorf("nodeAddress can not be empty when repair datanode")
		return
	}
	if t.NodeAddress != "" && !re.MatchString(strings.Split(t.NodeAddress, ":")[0]) {
		err = fmt.Errorf("nodeAddress illegal")
		return
	}
	return
}
