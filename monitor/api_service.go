package monitor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/statistics"
)

func (m *Monitor) collect(w http.ResponseWriter, r *http.Request) {
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

	if log.IsDebugEnabled() {
		detailBuilder := strings.Builder{}
		for _, data := range reportInfo.Infos {
			detailBuilder.WriteString(fmt.Sprintf("\t[time: %v, data: %v]\n",
				time.Unix(data.ReportTime, 0).Format("2006-01-02 15:04:05"), data))
		}
		log.LogDebugf("collect report[cluster: %v, module: %v, zone: %v, addr: %v]:\n%v",
			reportInfo.Cluster, reportInfo.Module, reportInfo.Zone, reportInfo.Addr, detailBuilder.String())
	}

	// send to jmq4
	clusters := m.clusters
	if m.mqProducer != nil && contains(clusters, reportInfo.Cluster) {
		epoch := atomic.AddUint64(&m.mqProducer.epoch, 1)
		index := epoch % uint64(m.mqProducer.produceNum)
		select {
		case m.mqProducer.msgChan[index] <- reportInfo:
		default:
			break
		}
	}

	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "insert hbase successfully"})
}

func (m *Monitor) setCluster(w http.ResponseWriter, r *http.Request) {
	cluster, err := extractCluster(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	clusters := strings.Split(cluster, ",")
	m.clusters = clusters
	log.LogInfof("setCluster: set to (%v)", m.clusters)
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("set cluster to (%v)", m.clusters)})
}

func (m *Monitor) getCluster(w http.ResponseWriter, r *http.Request) {
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("get cluster (%v)", m.clusters)})
}

func (m *Monitor) addCluster(w http.ResponseWriter, r *http.Request) {
	cluster, err := extractCluster(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if contains(m.clusters, cluster) {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("set cluster to (%v)", m.clusters)})
		return
	}
	clusters := append(m.clusters, cluster)
	m.clusters = clusters
	log.LogInfof("addCluster: set to (%v)", m.clusters)
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("set cluster to (%v)", m.clusters)})
}

func (m *Monitor) delCluster(w http.ResponseWriter, r *http.Request) {
	cluster, err := extractCluster(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	clusters := removeString(m.clusters, cluster)
	m.clusters = clusters
	log.LogInfof("delCluster: set to (%v)", m.clusters)
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("set cluster to (%v)", m.clusters)})
}

func (m *Monitor) setTopic(w http.ResponseWriter, r *http.Request) {
	topic, err := extractTopic(r)
	if err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if len(topic) > 0 {
		m.mqProducer.topic = strings.Split(topic, ",")
	}
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: fmt.Sprintf("set topic to (%v)", m.mqProducer.topic)})
}


func extractCluster(r *http.Request) (cluster string, err error) {
	if cluster = r.FormValue(clusterKey); cluster == "" {
		err = keyNotFound(clusterKey)
		return
	}
	return
}

func extractTopic(r *http.Request) (topic string, err error) {
	if topic = r.FormValue(topicKey); topic == "" {
		err = keyNotFound(topicKey)
		return
	}
	return
}

func keyNotFound(name string) (err error) {
	return errors.NewErrorf("parameter (%v) not found", name)
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}

func removeString(arr []string, element string) []string {
	if arr == nil || len(arr) == 0 {
		return arr
	}

	newArr := make([]string, 0)
	for _, e := range arr {
		if e == element {
			continue
		}
		newArr = append(newArr, e)
	}
	return newArr
}