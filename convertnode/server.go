package convertnode

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
	"math"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// Regular expression used to verify the configuration of the service listening listenPort.
	// A valid service listening listenPort configuration is a string containing only numbers.
	regexpListen = regexp.MustCompile("^(\\d)+$")
)

type MetaPartitionLayout struct {
	PercentOfMP      uint32			`json:"percent_of_mp"`
	PercentOfReplica uint32			`json:"percent_of_replica"`
}

type ConvertNode struct {
	port       		string
	apiServer  		*http.Server
	control    		common.Control
	stopC        	chan bool

	processorLock 	sync.RWMutex
	processors   	[]*ProcessorInfo
	processorNum 	int

	clusterLock 	sync.RWMutex
	clusterMap		map[string]*ClusterInfo

	taskLock 		sync.RWMutex
	taskMap  		map[string]*ConvertTask

	ipAddr          string
	mySqlDB         *DBInfo
}

func NewServer() *ConvertNode {
	return &ConvertNode{}
}

func (m *ConvertNode) Start(cfg *config.Config) error {
	return m.control.Start(m, cfg, doStart)
}

func (m *ConvertNode) Shutdown() {
	m.control.Shutdown(m, doShutdown)
}

func (m *ConvertNode) Sync() {
	m.control.Sync()
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	m, ok := s.(*ConvertNode)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}
	m.stopC = make(chan bool)
	m.taskMap = make(map[string]*ConvertTask)
	m.clusterMap = make(map[string]*ClusterInfo)
	m.mySqlDB = new(DBInfo)

	// parse config
	if err = m.parseConfig(cfg); err != nil {
		return
	}

	//init
	if err = m.init(); err != nil {
		return
	}

	//Singleton check
	if err = m.SingletonCheck(); err != nil {
		log.LogErrorf("[doStart] singleton check failed:%v", err)
		return
	}

	//load info from mysql
	if err = m.loadInfoFromDataBase(); err != nil {
		return
	}

	// start http service
	m.startHTTPService()

	//start
	if err = m.startServer(); err != nil {
		return
	}

	return
}

func doShutdown(s common.Server) {
	m, ok := s.(*ConvertNode)
	if !ok {
		return
	}
	// 1.http Server
	if m.apiServer != nil {
		if err := m.apiServer.Shutdown(context.Background()); err != nil {
			log.LogErrorf("action[Shutdown] failed, err: %v", err)
		}
	}
	close(m.stopC)
	return
}

func (m *ConvertNode) parseMasterClient(cfg *config.Config) (err error) {
	clusters := cfg.GetSlice(ConfigCluster)
	for _, cluster := range clusters {
		if _, ok := m.clusterMap[cluster.(string)]; ok {
			log.LogInfof("cluster[%v] already exist", cluster)
		}
		addrs := cfg.GetSlice(cluster.(string))
		masters := make([]string, 0, len(addrs))
		for _, addr := range addrs {
			masters = append(masters, addr.(string))
		}
		m.clusterMap[cluster.(string)] = NewClusterInfo(cluster.(string), masters)
	}
	return
}

func (m *ConvertNode) parseConfig(cfg *config.Config) (err error) {

	listen := cfg.GetString(ConfigListenPort)
	if !regexpListen.MatchString(listen) {
		return fmt.Errorf("Port must be a string only contains numbers.")
	}
	m.port = listen

	processorNum := cfg.GetInt(ConfigProcessorNum)
	if processorNum <= 0 || processorNum > defaultMaxMultiTask{
		processorNum = defaultMultiTask
	}
	m.processorNum = int(processorNum)

	if err = m.mySqlDB.parseMysqlDBConfig(cfg); err != nil {
		return
	}

	log.LogInfof("action[parseConfig] load listen port(%v).", m.port)
	log.LogInfof("action[parseConfig] load multi task(%v).", m.processorNum)
	return
}

func (m *ConvertNode) registerAPIRoutes(router *mux.Router) {
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(CONVERTLIST).
		HandlerFunc(m.listConvertTask)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(CONVERTINFO).
		HandlerFunc(m.info)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(CONVERTSTART).
		HandlerFunc(m.start)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(CONVERTSTOP).
		HandlerFunc(m.stop)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(CONVERTADD).
		HandlerFunc(m.add)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(CONVERTDEL).
		HandlerFunc(m.del)
	router.NewRoute().Methods(http.MethodGet, http.MethodPost).
		Path(CONVERTSETPROCESSOR).
		HandlerFunc(m.setProcessorNum)
}

func (m *ConvertNode) startHTTPService() {
	router := mux.NewRouter().SkipClean(true)
	m.registerAPIRoutes(router)
	var server = &http.Server{
		Addr:    colonSplit + m.port,
		Handler: router,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.LogErrorf("serveAPI: serve http server failed: err(%v)", err)
			return
		}
	}()
	log.LogDebugf("startHTTPService successfully: port(%v)", m.port)
	m.apiServer = server
	return
}

func (m *ConvertNode)getLocalIp()(err error){

	//get local ip addr
	conn, err := net.Dial("tcp", m.mySqlDB.getServerAddr())
	if err != nil {
		log.LogErrorf("dial failed:%v", err)
		return
	}

	conn.Close()
	m.ipAddr = strings.Split(conn.LocalAddr().String(), colonSplit)[0]
	return
}

func (m *ConvertNode) init() (err error){

	if err = m.getLocalIp(); err != nil {
		log.LogErrorf("[init] get local ip failed:%v", err)
		return
	}

	//open database
	if err = m.mySqlDB.Open(); err != nil {
		log.LogErrorf("[init] init mysql database failed:%v", err)
		return
	}

	m.processors = make([]*ProcessorInfo, m.processorNum)
	for i := 0; i < m.processorNum; i++ {
		m.processors[i] = NewProcessor(uint32(i), m)
	}

	for i := 0; i < m.processorNum; i++ {
		if err = m.processors[i].init(); err != nil {
			return err
		}
	}
	return
}

func (m *ConvertNode) SingletonCheck() (err error) {
	var records []*SingletonCheckTable
	if records, err = m.mySqlDB.LoadIpAddrFromDataBase(); err != nil {
		log.LogErrorf("[SingletonCheck] scan %s table failed:%v", SingletonCheckTable{}.TableName(), err)
	}

	if len(records) == 0 {
		if err = m.mySqlDB.PutIpAddrToDataBase(m.ipAddr); err != nil {
			log.LogErrorf("[SingletonCheck] create singleton record failed:%v", err)
		}
		return
	}

	if strings.Compare(records[0].IpAddr, m.ipAddr) == 0 {
		log.LogInfof("[SingletonCheck] %s can start", m.ipAddr)
		return
	}

	err = fmt.Errorf("different ip addr [local addr:%s, table record:%s], can not start", m.ipAddr, records[0].IpAddr)
	return
}

func (m *ConvertNode) loadInfoFromDataBase() (err error) {
	//load cluster info from mysql database
	if err = m.loadClusterInfo(); err != nil {
		log.LogErrorf("[loadInfoFromDataBase] load cluster info failed:%v", err)
		return
	}

	//load task info from db
	if err = m.loadTaskInfo(); err != nil {
		log.LogErrorf("[loadInfoFromDataBase] load task info failed:%v", err)
		return
	}
	return
}

func (m *ConvertNode) loadClusterInfo() (err error) {
	clusters, err :=  m.mySqlDB.LoadAllClusterInfoFromDB()
	if err != nil {
		return
	}

	for _, cluster := range clusters {
		masters := strings.Split(cluster.MasterAddr, ",")
		m.clusterMap[cluster.ClusterName] = NewClusterInfo(cluster.ClusterName, masters)
	}
	return
}

func (m *ConvertNode) loadTaskInfo() (err error) {
	//load from mysql
	var taskRecs []*TaskInfo
	if taskRecs, err = m.mySqlDB.LoadAllTaskInfoFromDB(); err != nil {
		return
	}
	for _, taskRec := range taskRecs {
		mc, ok := m.clusterMap[taskRec.ClusterName]
		if !ok {
			log.LogErrorf("not support cluster[%s]", taskRec.ClusterName)
			continue
		}

		key := m.genTaskKey(taskRec.ClusterName, taskRec.VolumeName)
		if task, ok := m.taskMap[key]; ok {
			log.LogInfof("task [%v] already exist", task)
			continue
		}

		id := m.assignProcessor()
		task := NewTaskInfo(id, taskRec.ClusterName, taskRec.VolumeName, mc, m.mySqlDB)
		task.setConvertingMpInfo(taskRec.RunningMP, proto.StoreModeDef, taskRec.SelectedReplNodeAddr, taskRec.OldNodeAddr)
		m.processors[id].addTask(task)
		mc.AddTask(task)
		m.taskMap[key] = task
	}
	return
}

func (m *ConvertNode) startServer() (err error){
	m.processorLock.RLock()
	defer m.processorLock.RUnlock()
	for i := 0; i < m.processorNum; i++ {
		if err = m.processors[i].start(); err != nil {
			return err
		}
	}
	return
}

func (m *ConvertNode) stopServer() (err error){
	m.processorLock.RLock()
	defer m.processorLock.RUnlock()
	for i := 0; i < m.processorNum; i++ {
		if err = m.processors[i].stop(); err != nil {
			return err
		}
	}
	return
}

func (m *ConvertNode) assignProcessor() (id int32) {
	minTaskCount := uint32(math.MaxUint32)
	minIndex := 0

	for i, processor := range m.processors {
		count := processor.getTaskCount()
		if count == 0 {
			return int32(i)
		}

		if count < minTaskCount {
			minTaskCount = count
			minIndex = i
		}
	}
	return int32(minIndex)
}

func (m *ConvertNode) genTaskKey(clusterName, volName string) (string) {
	return fmt.Sprintf("%s_%s", clusterName, volName)
}

func (m *ConvertNode) addTask(clusterName, volName string) (err error){
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	mc, ok := m.clusterMap[clusterName]
	if !ok {
		return fmt.Errorf("can not support cluster[%s]", clusterName)
	}

	key := m.genTaskKey(clusterName, volName)
	if task, ok := m.taskMap[key]; ok {
		return fmt.Errorf("task [%s] alread exist", task.taskName())
	}

	id := m.assignProcessor()
	task := NewTaskInfo(id, clusterName, volName, mc, m.mySqlDB)
	task.setConvertingMpInfo(0, proto.StoreModeDef, "", "")
	m.processors[id].addTask(task)
	mc.AddTask(task)
	m.taskMap[key] = task
	if err = m.mySqlDB.PutTaskInfoToDB(task.transform()); err != nil {
		log.LogErrorf("action[addTask] put task[%s] to db failed, err[%v]", task.taskName(), err)
	}
	return
}

func (m *ConvertNode) getTask(clusterName, volName string) (task *ConvertTask, exist bool){
	key := m.genTaskKey(clusterName, volName)
	m.taskLock.RLock()
	defer m.taskLock.RUnlock()
	task, exist = m.taskMap[key]
	return
}

/*processor go call this func*/
func (m *ConvertNode) delTask(clusterName, volName string) (err error){
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	key := m.genTaskKey(clusterName, volName)
	task, ok := m.taskMap[key]
	if !ok {
		return fmt.Errorf("task [%v-%v] alread deleted", clusterName, volName)
	}

	//if processor id less than 0, task already remove from processor running list
	processorID := atomic.LoadInt32(&task.info.ProcessorID)
	if processorID >= 0 {
		m.processors[processorID].delTask(task)
	}
	task.mc.DelTask(task)
	delete(m.taskMap, key)
	if err = m.mySqlDB.DeleteTaskInfoFromDB(clusterName, volName); err != nil {
		log.LogErrorf("action[delTask] delete task[%s] from db failed, err[%v]", task.taskName(), err)
	}
	return
}

func (m *ConvertNode) startTask(clusterName, volName string) (err error){
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	key := m.genTaskKey(clusterName, volName)
	task, ok := m.taskMap[key]
	if !ok {
		return fmt.Errorf("task [%v-%v] not exist", clusterName, volName)
	}
	if atomic.LoadInt32(&task.info.ProcessorID) < 0 {
		task.info.TaskState = proto.TaskInit
		atomic.StoreInt32(&(task.info.ProcessorID), m.assignProcessor())
		m.processors[task.info.ProcessorID].addTask(task)
	}
	return
}

func (m *ConvertNode) stopTask(clusterName, volName string) (err error){
	m.taskLock.Lock()
	defer m.taskLock.Unlock()

	key := m.genTaskKey(clusterName, volName)
	task, ok := m.taskMap[key]
	if !ok {
		return fmt.Errorf("task [%v-%v] not exist", clusterName, volName)
	}

	processorID := atomic.LoadInt32(&task.info.ProcessorID)
	if processorID < 0 {
		return fmt.Errorf("task [%v-%v] already stop", clusterName, volName)
	}
	m.processors[processorID].delTask(task)
	return
}

func (m *ConvertNode) addClusterConf(clusterName string, nodes []string) (err error){
	m.clusterLock.Lock()
	defer m.clusterLock.Unlock()

	if len(nodes) == 0 {
		return fmt.Errorf("add cluster no node in param")
	}

	mc, ok := m.clusterMap[clusterName]
	if !ok {
		m.clusterMap[clusterName] = NewClusterInfo(clusterName, nodes)
		if err = m.mySqlDB.PutClusterInfoToDB(clusterName, nodes); err != nil {
			log.LogErrorf("action[addClusterConf] put cluster[%s] info to db failed, err[%v]", clusterName, err)
		}
		return
	}

	for _, node := range nodes {
		for _, existNode := range mc.Nodes() {
			if strings.Contains(existNode, node) {
				continue
			}
			mc.AddNode(node)
		}
	}
	if err = m.mySqlDB.UpdateClusterInfoToDB(clusterName, mc.Nodes()); err != nil {
		log.LogErrorf("action[addClusterConf] update cluster[%s] info to db failed, err[%v]", clusterName, err)
	}
	return
}

func (m *ConvertNode) getClusterConf(clusterName string) (mc *ClusterInfo, exist bool){
	m.clusterLock.RLock()
	defer m.clusterLock.RUnlock()
	mc, exist = m.clusterMap[clusterName]
	return
}

func (m *ConvertNode) delClusterConf(clusterName string, delNodes []string) (err error){
	m.clusterLock.Lock()
	defer m.clusterLock.Unlock()

	cluster, ok := m.clusterMap[clusterName]
	if !ok {
		_ = m.mySqlDB.DeleteClusterInfoFromDB(clusterName)
		return fmt.Errorf("cluster[%s] already del", clusterName)
	}

	if len(delNodes) == 0 {
		if cluster.TaskCount() != 0 {
			return fmt.Errorf("cluster[%s] has tasks[%d], can not del", clusterName, cluster.TaskCount())
		}
		delete(m.clusterMap, clusterName)
		return m.mySqlDB.DeleteClusterInfoFromDB(clusterName)
	}

	existNodes := make([]string, 0)
	for _, del := range delNodes {
		for _, node := range cluster.Nodes() {
			if strings.Contains(node, del) {
				continue
			}
			existNodes = append(existNodes, node)
		}
	}

	if len(existNodes) == 0 {
		if cluster.TaskCount() != 0 {
			return fmt.Errorf("cluster[%s] has tasks[%d], can not del", clusterName, cluster.TaskCount())
		}
		delete(m.clusterMap, clusterName)
		if err = m.mySqlDB.DeleteClusterInfoFromDB(clusterName); err != nil {
			log.LogErrorf("action[delClusterConf] delete cluster[%s] info from db failed, err[%v]", clusterName, err)
		}
	} else {
		cluster.UpdateNodes(existNodes)
		if err = m.mySqlDB.UpdateClusterInfoToDB(clusterName, existNodes); err != nil {
			log.LogErrorf("action[delClusterConf] update cluster[%s] info from db failed, err[%v]", clusterName, err)
		}
	}
	return
}

func (m *ConvertNode) getProcessorByStrId(idStr string) (processor *ProcessorInfo, exist bool){
	id, err := strconv.Atoi(idStr)
	if err != nil || (id < 0 || id >= m.processorNum){
		exist = false
		return
	}

	m.clusterLock.RLock()
	defer m.clusterLock.RUnlock()
	processor = m.processors[id]
	exist = true
	return
}
