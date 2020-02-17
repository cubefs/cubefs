package master

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type CodecNode struct {
	ID          uint64
	Addr        string
	ReportTime  time.Time
	isActive    bool
	TaskManager *AdminTaskManager
	sync.RWMutex
	//todo add field
}

type codecNodeValue struct {
	ID   uint64
	Addr string
}

func newCodecNodeValue(codecNode *CodecNode) *codecNodeValue {
	return &codecNodeValue{
		ID:   codecNode.ID,
		Addr: codecNode.Addr,
	}
}

func newCodecNode(addr, clusterID string) *CodecNode {
	node := new(CodecNode)
	node.Addr = addr
	node.TaskManager = newAdminTaskManager(addr, clusterID)
	return node
}

func (codecNode *CodecNode) updateMetric(resp *proto.CodecNodeHeartbeatResponse) {
	codecNode.Lock()
	defer codecNode.Unlock()
	codecNode.ReportTime = time.Now()
	codecNode.isActive = true
}

func (c *Cluster) checkCodecNodeHeartbeat() {
	tasks := make([]*proto.AdminTask, 0)
	c.codecNodes.Range(func(addr, codecNode interface{}) bool {
		node := codecNode.(*CodecNode)
		task := createHeartbeatTask(c.masterAddr(), node.Addr, proto.OpCodecNodeHeartbeat)
		tasks = append(tasks, task)
		return true
	})
	c.addDataNodeTasks(tasks)
}

func (m *Server) getAllCodecNodes(w http.ResponseWriter, r *http.Request) {
	//todo add cache
	nodes := make([]proto.CodecNodeClientView, 0)
	m.cluster.codecNodes.Range(func(key, value interface{}) bool {
		codecNode := value.(*CodecNode)
		nodes = append(nodes, proto.CodecNodeClientView{Addr: codecNode.Addr, IsActive: codecNode.isActive})
		return true
	})
	sendOkReply(w, r, newSuccessHTTPReply(nodes))
}

func (m *Server) addCodecNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr string
		id       uint64
		err      error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if id, err = m.cluster.addCodecNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(id))
}

func (m *Server) getCodecNode(w http.ResponseWriter, r *http.Request) {
	var (
		nodeAddr      string
		codecNode     *CodecNode
		codecNodeInfo *proto.CodecNodeInfo
		err           error
	)
	if nodeAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if codecNode, err = m.cluster.codecNode(nodeAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrCodecNodeNotExists))
		return
	}
	codecNodeInfo = &proto.CodecNodeInfo{
		ID:         codecNode.ID,
		Addr:       codecNode.Addr,
		ReportTime: codecNode.ReportTime,
		IsActive:   codecNode.isActive,
	}

	sendOkReply(w, r, newSuccessHTTPReply(codecNodeInfo))
}

// Decommission a codec node
func (m *Server) decommissionCodecNode(w http.ResponseWriter, r *http.Request) {
	var (
		node        *CodecNode
		rstMsg      string
		offLineAddr string
		err         error
	)

	if offLineAddr, err = parseAndExtractNodeAddr(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if node, err = m.cluster.codecNode(offLineAddr); err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrDataNodeNotExists))
		return
	}
	if err = m.cluster.decommissionCodecNode(node); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	rstMsg = fmt.Sprintf("decommission codec node [%v] successfully", offLineAddr)
	sendOkReply(w, r, newSuccessHTTPReply(rstMsg))
}

func (m *Server) handleCodecNodeTaskResponse(w http.ResponseWriter, r *http.Request) {
	tr, err := parseRequestToGetTaskResponse(r)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("%v", http.StatusOK)))
	m.cluster.handleCodecNodeTaskResponse(tr.OperatorAddr, tr)
}

func (c *Cluster) handleCodecNodeTaskResponse(nodeAddr string, task *proto.AdminTask) (err error) {
	if task == nil {
		return
	}
	log.LogDebugf(fmt.Sprintf("action[handleCodecNodeTaskResponse] receive Task response:%v from %v", task.ID, nodeAddr))
	var (
		codecNode *CodecNode
	)

	if codecNode, err = c.codecNode(nodeAddr); err != nil {
		goto errHandler
	}
	codecNode.TaskManager.DelTask(task)
	if err = unmarshalTaskResponse(task); err != nil {
		goto errHandler
	}

	switch task.OpCode {
	case proto.OpCodecNodeHeartbeat:
		response := task.Response.(*proto.CodecNodeHeartbeatResponse)
		err = c.dealCodecNodeHeartbeatResp(task.OperatorAddr, response)
	default:
		err := fmt.Errorf("unknown operate code %v", task.OpCode)
		log.LogError(err)
	}

	if err != nil {
		log.LogError(fmt.Sprintf("process task[%v] failed", task.ToString()))
	} else {
		log.LogInfof("process task:%v status:%v success", task.ID, task.Status)
	}
	return
errHandler:
	log.LogError(fmt.Sprintf("action[handleCodecNodeTaskResponse],nodeAddr %v,taskId %v,err %v",
		nodeAddr, task.ID, err.Error()))
	return
}

func (c *Cluster) dealCodecNodeHeartbeatResp(nodeAddr string, resp *proto.CodecNodeHeartbeatResponse) (err error) {
	var (
		codecNode *CodecNode
		logMsg    string
	)
	log.LogInfof("action[dealCodecNodeHeartbeatResp],clusterID[%v] receive nodeAddr[%v] heartbeat", c.Name, nodeAddr)
	if resp.Status == proto.TaskFailed {
		msg := fmt.Sprintf("action[dealCodecNodeHeartbeatResp],clusterID[%v] nodeAddr %v heartbeat failed,err %v",
			c.Name, nodeAddr, resp.Result)
		log.LogError(msg)
		Warn(c.Name, msg)
		return
	}

	if codecNode, err = c.codecNode(nodeAddr); err != nil {
		goto errHandler
	}

	codecNode.updateMetric(resp)
	logMsg = fmt.Sprintf("action[dealCodecNodeHeartbeatResp],metaNode:%v ReportTime:%v  success", codecNode.Addr, time.Now().Unix())
	log.LogInfof(logMsg)
	return
errHandler:
	logMsg = fmt.Sprintf("nodeAddr %v heartbeat error :%v", nodeAddr, errors.Stack(err))
	log.LogError(logMsg)
	return
}

func (c *Cluster) addCodecNode(nodeAddr string) (id uint64, err error) {
	c.dnMutex.Lock()
	defer c.dnMutex.Unlock()
	var codecNode *CodecNode
	if node, ok := c.codecNodes.Load(nodeAddr); ok {
		codecNode = node.(*CodecNode)
		return codecNode.ID, nil
	}

	codecNode = newCodecNode(nodeAddr, c.Name)
	// allocate codecNode id
	if id, err = c.idAlloc.allocateCommonID(); err != nil {
		goto errHandler
	}
	codecNode.ID = id
	if err = c.syncAddCodecNode(codecNode); err != nil {
		goto errHandler
	}
	c.codecNodes.Store(nodeAddr, codecNode)
	log.LogInfof("action[addCodecNode],clusterID[%v] codecNodeAddr:%v success",
		c.Name, nodeAddr)
	return
errHandler:
	err = fmt.Errorf("action[addCodecNode],clusterID[%v] codecNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

// key=#dn#id#Addr,value = json.Marshal(dnv)
func (c *Cluster) syncAddCodecNode(codecNode *CodecNode) (err error) {
	return c.syncPutCodecNodeInfo(opSyncPut, codecNode)
}

func (c *Cluster) syncDeleteCodecNode(codecNode *CodecNode) (err error) {
	return c.syncPutCodecNodeInfo(opSyncDelete, codecNode)
}

func (c *Cluster) syncPutCodecNodeInfo(opType uint32, codecNode *CodecNode) (err error) {
	metadata := new(RaftCmd)
	metadata.Op = opType
	metadata.K = codecNodePrefix + strconv.FormatUint(codecNode.ID, 10) + keySeparator + codecNode.Addr
	dnv := newCodecNodeValue(codecNode)
	metadata.V, err = json.Marshal(dnv)
	if err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) codecNode(addr string) (codecNode *CodecNode, err error) {
	value, ok := c.codecNodes.Load(addr)
	if !ok {
		err = errors.Trace(codecNodeNotFound(addr), "%v not found", addr)
		return
	}
	codecNode = value.(*CodecNode)
	return
}

func (c *Cluster) decommissionCodecNode(codecNode *CodecNode) (err error) {
	//todo
	return nil
}

func (c *Cluster) loadCodecNodes() (err error) {
	result, err := c.fsm.store.SeekForPrefix([]byte(codecNodePrefix))
	if err != nil {
		err = fmt.Errorf("action[loadCodecNodes],err:%v", err.Error())
		return err
	}

	for _, value := range result {
		cnv := &codecNodeValue{}
		if err = json.Unmarshal(value, cnv); err != nil {
			err = fmt.Errorf("action[loadCodecNodes],value:%v,unmarshal err:%v", string(value), err)
			return
		}
		codecNode := newCodecNode(cnv.Addr, c.Name)
		codecNode.ID = cnv.ID
		c.codecNodes.Store(codecNode.Addr, codecNode)
		log.LogInfof("action[loadCodecNodes],codecNode[%v]", codecNode.Addr)
	}
	return
}
