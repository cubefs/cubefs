package crcworker

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/cli/cmd/data_check"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/rogpeppe/go-internal/modfile"
	"os"
	"strconv"
	"strings"
)

const (
	defaultOutputDir = "/tmp"

	ConfigKeyOutputDir = "outputDir"
	ConfigKeyUmpPrefix = "umpPrefix"
)
const (
	DefaultLimitLevel = 2
	CrcWorkerPeriod   = 24 * 60 * 60
)

var dataPortMap = map[string]string{
	"6000":  "6001",
	"17030": "17031",
	"17310": "17320",
}
var metaPortMap = map[string]string{
	"9021":  "9092",
	"17020": "17021",
	"17210": "17220",
}

type CrcWorker struct {
	worker.BaseWorker
	masterAddr map[string][]string
	outputDir  string
	mcw        map[string]*master.MasterClient
	stopC      chan bool
}

func NewCrcWorker() (cw *CrcWorker) {
	cw = &CrcWorker{
		stopC: make(chan bool, 1),
	}
	return cw
}

func NewCrcWorkerForScheduler() (cw *CrcWorker, err error) {
	cw = &CrcWorker{}
	return cw, nil
}

// Shutdown shuts down the current data node.
func (s *CrcWorker) Shutdown() {
	s.Control.Shutdown(s, doShutdown)
	return
}

func doShutdown(s common.Server) {
	m, ok := s.(*CrcWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (s *CrcWorker) Start(cfg *config.Config) (err error) {
	return s.Control.Start(s, cfg, doStart)
}

// Workflow of starting up a data node.
func doStart(server common.Server, cfg *config.Config) (err error) {
	s, ok := server.(*CrcWorker)
	if !ok {
		return errors.New("invalid node type")
	}
	s.StopC = make(chan struct{}, 0)
	if err = s.ParseBaseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}
	masters := make(map[string][]string)
	baseInfo := cfg.GetMap(config.ConfigKeyClusterAddr)
	var masterAddr []string
	for clusterName, value := range baseInfo {
		addresses := make([]string, 0)
		if valueSlice, ok := value.([]interface{}); ok {
			for _, item := range valueSlice {
				if addr, ok := item.(string); ok {
					addresses = append(addresses, addr)
				}
			}
		}
		if len(masterAddr) == 0 {
			masterAddr = addresses
		}
		masters[clusterName] = addresses
	}
	s.masterAddr = masters
	// used for cmd to report version
	if len(masterAddr) == 0 {
		cfg.SetStringSlice(proto.MasterAddr, masterAddr)
	}
	outputDir := cfg.GetString(ConfigKeyOutputDir)
	if modfile.IsDirectoryPath(outputDir) {
		s.outputDir = outputDir
	} else {
		log.LogErrorf("config not found outputDir and redirect to /tmp")
		s.outputDir = defaultOutputDir
	}
	// init ump monitor and alarm module
	exporter.Init(exporter.NewOption().WithCluster(proto.RoleScheduleNode).WithModule(proto.RoleCrcWorker))

	if err = s.initWorker(); err != nil {
		return
	}
	umpPrefix := cfg.GetString(ConfigKeyUmpPrefix)
	if umpPrefix != "" {
		data_check.UmpWarnKey = umpPrefix + "_" + data_check.UmpWarnKey
	}
	if err = s.RegisterWorker(proto.WorkerTypeCheckCrc, s.ConsumeTask); err != nil {
		log.LogErrorf("[doStart] register check crc worker failed, error(%v)", err)
		return
	}
	go s.registerHandler()
	return
}
func (s *CrcWorker) initWorker() (err error) {
	s.WorkerType = proto.WorkerTypeCheckCrc
	s.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)

	// init master client
	masterClient := make(map[string]*master.MasterClient)
	for cluster, addresses := range s.masterAddr {
		mc := master.NewMasterClient(addresses, false)
		masterClient[cluster] = mc
	}
	s.mcw = masterClient
	// init mysql client
	if err = mysql.InitMysqlClient(s.MysqlConfig); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}
	return
}
func (s *CrcWorker) GetCreatorDuration() int {
	return CrcWorkerPeriod
}

// CreateTask for scheduler node to produce single task
func (s *CrcWorker) CreateTask(clusterId string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	newTasks = make([]*proto.Task, 0)
	var taskAddFunc = func(task *proto.Task) {
		if int64(len(newTasks)) >= taskNum {
			return
		}
		if !isDuplicateTask(runningTasks, task) {
			if _, err = s.AddTask(task); err != nil {
				log.LogErrorf("failed to add task in cluster[%v], task info[%v], err:%v", clusterId, task.TaskInfo, err)
				return
			}
			newTasks = append(newTasks, task)
		}
	}
	switch clusterId {
	case "spark":
		task1 := newCheckVolumeCrcTask(clusterId, proto.Filter{
			ZoneFilter: []string{"ssd"},
		})
		taskAddFunc(task1)
		task2 := newCheckVolumeCrcTask(clusterId, proto.Filter{
			ZoneExcludeFilter: []string{"ssd"},
		})
		taskAddFunc(task2)
	case "mysql":
		task1 := newCheckVolumeCrcTask(clusterId, proto.Filter{
			VolFilter: []string{"orderdb-his"},
		})
		taskAddFunc(task1)
		task2 := newCheckVolumeCrcTask(clusterId, proto.Filter{
			VolExcludeFilter: []string{"orderdb-his"},
		})
		taskAddFunc(task2)
	default:
		task := newCheckVolumeCrcTask(clusterId, proto.Filter{})
		taskAddFunc(task)
	}
	return
}

func newCheckVolumeCrcTask(cluster string, filter proto.Filter) (task *proto.Task) {
	task = new(proto.Task)
	task.Cluster = cluster
	task.TaskType = proto.WorkerTypeCheckCrc
	crcTask := &proto.CheckTaskInfo{
		CheckTiny:           false,
		Concurrency:         DefaultLimitLevel,
		ExtentModifyTimeMin: "",
		ExtentModifyTimeMax: "",
		InodeModifyTimeMin:  "",
		InodeModifyTimeMax:  "",
		CheckMod:            proto.VolumeInode,
		QuickCheck:          true,
	}
	crcTask.Filter = filter
	crcTaskBytes, err := json.Marshal(crcTask)
	if err != nil {
		return
	}
	task.TaskInfo = string(crcTaskBytes)

	return
}

func (s *CrcWorker) registerHandler() (err error) {
	return
}

// ConsumeTask
// for worker node to consume single task
// if err is not empty, it candidate current task was failed, and will not retry it.
// if err is empty but restore is true, it candidate this task does not meet the processing conditions
// and task will be restored to queue to consume again
func (s *CrcWorker) ConsumeTask(task *proto.Task) (restore bool, err error) {
	defer func() {
		if err != nil {
			log.LogError("ConsumeTask: failed, err:%v", err)
		}
		log.LogInfof("ConsumeTask stop, taskID:%v", task.TaskId)
	}()
	err = os.MkdirAll(s.outputDir, 0755)
	if err != nil {
		log.LogErrorf("init output dir:%v failed, err:%v", s.outputDir, err)
		return true, nil
	}
	mc := s.mcw[task.Cluster]
	cluster, err := mc.AdminAPI().GetCluster()
	if err != nil {
		return true, nil
	}
	if len(cluster.DataNodes) < 1 {
		err = errors.NewErrorf("no datanode found")
		return
	}
	if len(cluster.MetaNodes) < 1 {
		err = errors.NewErrorf("no datanode found")
		return
	}
	dnProf := dataPortMap[strings.Split(cluster.DataNodes[0].Addr, ":")[1]]
	if dnProf == "" {
		err = fmt.Errorf("unknown data prof")
		return
	}
	mnProf := metaPortMap[strings.Split(cluster.MetaNodes[0].Addr, ":")[1]]
	if mnProf == "" {
		err = fmt.Errorf("unknown meta prof")
		return
	}
	dnPortNum, err := strconv.Atoi(dnProf)
	if err != nil {
		return
	}
	mc.DataNodeProfPort = uint16(dnPortNum)
	mnPortNum, err := strconv.Atoi(mnProf)
	if err != nil {
		return
	}
	mc.MetaNodeProfPort = uint16(mnPortNum)

	crcTaskInfo := proto.CheckTaskInfo{}
	err = json.Unmarshal([]byte(task.TaskInfo), &crcTaskInfo)
	if err != nil {
		return
	}
	if err = crcTaskInfo.IsValid(); err != nil {
		return
	}
	var checkEngine *data_check.CheckEngine
	checkEngine, err = data_check.NewCheckEngine(crcTaskInfo, s.outputDir, mc, data_check.CheckTypeExtentCrc, "", false)
	if err != nil {
		return
	}
	defer func() {
		checkEngine.Close()
	}()
	go func() {
		select {
		case <-s.stopC:
			checkEngine.Close()
		}
	}()
	err = checkEngine.Start()
	if err != nil {
		return
	}
	checkEngine.Reset()
	checkEngine.CheckFailedVols()
	return
}

func isDuplicateTask(runningTasks []*proto.Task, task *proto.Task) bool {
	for _, t := range runningTasks {
		if t.Cluster == task.Cluster && t.TaskType == task.TaskType {
			if t.TaskInfo == task.TaskInfo {
				return true
			}
		}
	}
	return false
}
