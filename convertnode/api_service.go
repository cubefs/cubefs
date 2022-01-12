package convertnode

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

func (m *ConvertNode) listConvertTask(w http.ResponseWriter, r *http.Request) {
	//buff, err := json.Marshal(m)
	//if err != nil {
	//	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
	//}

	view := &proto.ConvertNodeViewInfo{}
	view.Tasks = make([]*proto.ConvertTaskInfo, 0, len(m.taskMap))
	view.Clusters = make([]*proto.ConvertClusterInfo, 0, len(m.clusterMap))
	view.Processors = make([]*proto.ConvertProcessorInfo, 0, m.processorNum)
	view.Port = m.port

	m.taskLock.RLock()
	for _, task := range m.taskMap {
		view.Tasks = append(view.Tasks, task.info)
	}
	m.taskLock.RUnlock()

	m.processorLock.RLock()
	for _, processor := range m.processors {
		view.Processors = append(view.Processors, processor.info)
	}
	m.processorLock.RUnlock()

	m.clusterLock.RLock()
	for name, mc := range m.clusterMap {
		cluster := &proto.ConvertClusterInfo{ClusterName: name, Nodes: mc.Nodes()}
		view.Clusters = append(view.Clusters, cluster)
	}
	m.clusterLock.RUnlock()

	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: view})
	return
}

func (m *ConvertNode) info(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	clusterName := r.FormValue("cluster")
	volName := r.FormValue("vol")
	idStr := r.FormValue("processor")
	objectType := r.FormValue(CLIObjTypeKey)

	var (
		data interface{}
		key	 interface{}
		ok 	 bool
	)

	switch objectType {
	case CLIObjClusterType:
		key = clusterName
		if cluster, exist := m.getClusterConf(clusterName); exist {
			ok = true
			data = cluster.GenClusterDetailView()
		}
	case CLIObjProcessorType:
		key = idStr
		if processor, exist := m.getProcessorByStrId(idStr); exist {
			ok = true
			data = processor.GenProcessorDetailView()
		}

	case CLIObjTaskType:
		key = clusterName + "_" + volName
		if task, exist := m.getTask(clusterName, volName); exist {
			ok = true
			data = task.genTaskViewInfo()
		}
	default:
		m.listConvertTask(w, r)
		return
	}

	if !ok {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError,
					Msg: fmt.Sprintf("Does not have this item[%v]", key)})
		return
	}

	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: data})
	return
}

func (m *ConvertNode) start(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	clusterName := r.FormValue("cluster")
	volName := r.FormValue("vol")
	objectType := r.FormValue(CLIObjTypeKey)

	var(
		key interface{}
		err error
	)

	log.LogInfof("start %s request", objectType)
	switch objectType {
	case CLIObjServerType:
		key = "server"
		err = m.startServer()

	case CLIObjTaskType:
		key = clusterName + "_" + volName
		err = m.startTask(clusterName, volName)

	default:
		key = "unknown"
		err = fmt.Errorf("do not support type[%s]", objectType)
	}

	if err != nil {
		log.LogErrorf("start %s[%v] request failed:[%s]", objectType, key, err.Error())
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	log.LogInfof("start %s[%v] request successfully", objectType, key)
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("start %s[%v] request successfully", objectType, key)})
	return
}

func (m *ConvertNode) stop(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	clusterName := r.FormValue("cluster")
	volName := r.FormValue("vol")
	objectType := r.FormValue(CLIObjTypeKey)

	var(
		key interface{}
		err error
	)

	log.LogInfof("stop %s request", objectType)
	switch objectType {
	case CLIObjServerType:
		key = "server"
		err = m.stopServer()

	case CLIObjTaskType:
		key = clusterName + "_" + volName
		err = m.stopTask(clusterName, volName)

	default:
		key = "unknown"
		err = fmt.Errorf("do not support type[%s]", objectType)

	}

	if err != nil {
		log.LogErrorf("stop %s[%v] request failed:[%s]", objectType, key, err.Error())
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	log.LogInfof("stop %s[%v] request successfully", objectType, key)
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("stop %s[%v] request successfully", objectType, key)})
	return
}

func (m *ConvertNode) add(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	clusterName := r.FormValue("cluster")
	nodesAddr := r.FormValue("nodes")
	volName := r.FormValue("vol")
	objectType := r.FormValue(CLIObjTypeKey)

	var(
		key interface{}
		err error
	)

	log.LogInfof("add %s  value: cluster[%s] nodesAddr[%s] volName[%s] request",
		objectType, clusterName, nodesAddr, volName)
	switch objectType {
	case CLIObjClusterType:
		key = clusterName
		nodes := strings.Split(nodesAddr, ",")
		err = m.addClusterConf(clusterName, nodes)

	case CLIObjTaskType:
		key = clusterName + "_" + volName
		err = m.addTask(clusterName, volName)

	default:
		key = "unknown"
		err = fmt.Errorf("do not support type[%s]", objectType)
	}

	if err != nil {
		log.LogErrorf("add %s[%v] request failed:[%s]", objectType, key, err.Error())
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	log.LogInfof("add %s[%v] request successfully", objectType, key)
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess,
		Msg: fmt.Sprintf("add %s[%v] successfully", objectType, key)})
	return
}

func (m *ConvertNode) del(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	clusterName := r.FormValue("cluster")
	nodesAddr := r.FormValue("nodes")
	volName := r.FormValue("vol")
	objectType := r.FormValue(CLIObjTypeKey)

	var(
		key interface{}
		err error
	)

	log.LogInfof("del %s  value: cluster[%s] nodesAddr[%s] volName[%s] request",
		objectType, clusterName, nodesAddr, volName)
	switch objectType {
	case CLIObjClusterType:
		key = clusterName
		nodes := strings.Split(nodesAddr, ",")
		err = m.delClusterConf(clusterName, nodes)

	case CLIObjTaskType:
		key = clusterName + "_" + volName
		err = m.delTask(clusterName, volName)

	default:
		key = "unknown"
		err = fmt.Errorf("do not support type[%s]", objectType)
	}

	if err != nil {
		log.LogErrorf("del %s[%v] request failed:[%s]",
			objectType, key, err.Error())
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}

	log.LogInfof("del %s[%v] request successfully",
		objectType, key)
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess,
		Msg: fmt.Sprintf("del %s[%v] successfully", objectType, key)})
	return
}

func (m *ConvertNode) setProcessorNum(w http.ResponseWriter, r *http.Request) {
	var (
		bytes []byte
		err   error
	)
	if bytes, err = ioutil.ReadAll(r.Body); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	reportInfo := &statistics.ReportInfo{}
	if err = json.Unmarshal(bytes, reportInfo); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "insert hbase successfully"})
}

func sendReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))

	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}

	log.LogInfof("URL[%v], remoteAddr[%v], response[%v]", r.URL, r.RemoteAddr, string(reply[:]))
	return
}
