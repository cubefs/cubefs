package repaircrc_server

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"regexp"
	"strings"
)

type RepairCrcTask struct {
	proto.CheckTaskInfo
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
		CheckTaskInfo: proto.CheckTaskInfo{
			Filter: proto.Filter{
				VolFilter:         make([]string, 0),
				VolExcludeFilter:  make([]string, 0),
				ZoneFilter:        make([]string, 0),
				ZoneExcludeFilter: make([]string, 0),
				NodeFilter:        make([]string, 0),
				NodeExcludeFilter: make([]string, 0),
				InodeFilter:       make([]uint64, 0),
				DpFilter:          make([]uint64, 0),
			},
		},
		ClusterInfo: new(ClusterInfo),
		stopC:       make(chan bool, 8),
	}
}

func (t *RepairCrcTask) IsValid() (err error) {
	if t.ClusterInfo.Master == "" || t.ClusterInfo.MnProf == 0 || t.ClusterInfo.DnProf == 0 {
		err = fmt.Errorf("cluster info illegal")
		return
	}
	ipReg := regexp.MustCompile(`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$|^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)+([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$`)
	if strings.Contains(t.ClusterInfo.Master, ":") && !ipReg.MatchString(strings.Split(t.ClusterInfo.Master, ":")[0]) {
		err = fmt.Errorf("master address illegal")
		return
	}
	if !strings.Contains(t.ClusterInfo.Master, ":") && !ipReg.MatchString(t.ClusterInfo.Master) {
		err = fmt.Errorf("master address illegal")
		return
	}
	if t.Frequency.Interval == 0 || t.Frequency.ExecuteCount < 0 {
		err = fmt.Errorf("frequency info illegal")
		return
	}
	return t.CheckTaskInfo.IsValid()
}
