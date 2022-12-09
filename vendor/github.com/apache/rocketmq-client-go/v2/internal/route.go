/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package internal

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

const (
	EnvNameServerAddr = "NAMESRV_ADDR"

	requestTimeout   = 3 * time.Second
	defaultTopic     = "TBW102"
	defaultQueueNums = 4
	MasterId         = int64(0)
)

func (s *namesrvs) cleanOfflineBroker() {
	// TODO optimize
	s.lockNamesrv.Lock()
	s.brokerAddressesMap.Range(func(key, value interface{}) bool {
		brokerName := key.(string)
		bd := value.(*BrokerData)
		for k, v := range bd.BrokerAddresses {
			isBrokerAddrExistInTopicRoute := false
			s.routeDataMap.Range(func(key, value interface{}) bool {
				trd := value.(*TopicRouteData)
				for idx := range trd.BrokerDataList {
					for _, v1 := range trd.BrokerDataList[idx].BrokerAddresses {
						if v1 == v {
							isBrokerAddrExistInTopicRoute = true
							return false
						}
					}
				}
				return true
			})
			if !isBrokerAddrExistInTopicRoute {
				delete(bd.BrokerAddresses, k)
				rlog.Info("the broker: [name=%s, ID=%d, addr=%s,] is offline, remove it", map[string]interface{}{
					"brokerName": brokerName,
					"brokerID":   k,
					"brokerAddr": v,
				})
			}
		}
		if len(bd.BrokerAddresses) == 0 {
			s.brokerAddressesMap.Delete(brokerName)
			rlog.Info("the broker name's host is offline, remove it", map[string]interface{}{
				"brokerName": brokerName,
			})
		}
		return true
	})
	s.lockNamesrv.Unlock()
}

// key is topic, value is TopicPublishInfo
type TopicPublishInfo struct {
	OrderTopic          bool
	HaveTopicRouterInfo bool
	MqList              []*primitive.MessageQueue
	RouteData           *TopicRouteData
	TopicQueueIndex     int32
}

func (info *TopicPublishInfo) isOK() (bIsTopicOk bool) {
	return len(info.MqList) > 0
}

func (info *TopicPublishInfo) fetchQueueIndex() int {
	length := len(info.MqList)
	if length <= 0 {
		return -1
	}
	qIndex := atomic.AddInt32(&info.TopicQueueIndex, 1)
	return int(qIndex) % length
}

func (s *namesrvs) UpdateTopicRouteInfo(topic string) (*TopicRouteData, bool, error) {
	return s.UpdateTopicRouteInfoWithDefault(topic, "", 0)
}

func (s *namesrvs) CheckTopicRouteHasTopic(topic string) bool {
	_, err := s.queryTopicRouteInfoFromServer(topic)
	if err != nil {
		return false
	}
	return true
}

func (s *namesrvs) UpdateTopicRouteInfoWithDefault(topic string, defaultTopic string, defaultQueueNum int) (*TopicRouteData, bool, error) {
	s.lockNamesrv.Lock()
	defer s.lockNamesrv.Unlock()

	var (
		routeData *TopicRouteData
		err       error
	)

	t := topic
	if len(defaultTopic) > 0 {
		t = defaultTopic
	}
	routeData, err = s.queryTopicRouteInfoFromServer(t)

	if err != nil {
		rlog.Warning("query topic route from server error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}

	if routeData == nil {
		rlog.Warning("queryTopicRouteInfoFromServer return nil", map[string]interface{}{
			rlog.LogKeyTopic: topic,
		})
		return nil, false, err
	}

	if len(defaultTopic) > 0 {
		for _, q := range routeData.QueueDataList {
			if q.ReadQueueNums > defaultQueueNum {
				q.ReadQueueNums = defaultQueueNum
				q.WriteQueueNums = defaultQueueNum
			}
		}
	}

	oldRouteData, exist := s.routeDataMap.Load(topic)

	changed := true
	if exist {
		changed = s.topicRouteDataIsChange(oldRouteData.(*TopicRouteData), routeData)
	}

	if changed {
		if s.bundleClient != nil {
			s.bundleClient.producerMap.Range(func(key, value interface{}) bool {
				p := value.(InnerProducer)
				updated := changed
				if !updated {
					updated = p.IsPublishTopicNeedUpdate(topic)
				}
				if updated {
					publishInfo := s.bundleClient.GetNameSrv().(*namesrvs).routeData2PublishInfo(topic, routeData)
					publishInfo.HaveTopicRouterInfo = true
					p.UpdateTopicPublishInfo(topic, publishInfo)
				}
				return true
			})
			s.bundleClient.consumerMap.Range(func(key, value interface{}) bool {
				consumer := value.(InnerConsumer)
				updated := changed
				if !updated {
					updated = consumer.IsSubscribeTopicNeedUpdate(topic)
				}
				if updated {
					consumer.UpdateTopicSubscribeInfo(topic, routeData2SubscribeInfo(topic, routeData))
				}

				return true
			})
			rlog.Info("change the route for clients", nil)
		}

		s.routeDataMap.Store(topic, routeData)
		rlog.Info("the topic route info changed", map[string]interface{}{
			rlog.LogKeyTopic:            topic,
			rlog.LogKeyValueChangedFrom: oldRouteData,
			rlog.LogKeyValueChangedTo:   routeData.String(),
		})
		for _, brokerData := range routeData.BrokerDataList {
			s.brokerAddressesMap.Store(brokerData.BrokerName, brokerData)
		}
	}

	return routeData.clone(), changed, nil
}

func (s *namesrvs) AddBroker(routeData *TopicRouteData) {
	for _, brokerData := range routeData.BrokerDataList {
		s.brokerAddressesMap.Store(brokerData.BrokerName, brokerData)
	}
}

func (s *namesrvs) FindBrokerAddrByTopic(topic string) string {
	v, exist := s.routeDataMap.Load(topic)
	if !exist {
		return ""
	}
	routeData := v.(*TopicRouteData)
	if len(routeData.BrokerDataList) == 0 {
		return ""
	}
	i := utils.AbsInt(rand.Int())
	bd := routeData.BrokerDataList[i%len(routeData.BrokerDataList)]
	addr := bd.BrokerAddresses[MasterId]
	if addr == "" && len(bd.BrokerAddresses) > 0 {
		i = i % len(bd.BrokerAddresses)
		for _, v := range bd.BrokerAddresses {
			if i <= 0 {
				addr = v
				break
			}
			i--
		}
	}
	return addr
}

func (s *namesrvs) FindBrokerAddrByName(brokerName string) string {
	bd, exist := s.brokerAddressesMap.Load(brokerName)

	if !exist {
		return ""
	}

	return bd.(*BrokerData).BrokerAddresses[MasterId]
}

func (s *namesrvs) FindBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) *FindBrokerResult {
	var (
		brokerAddr = ""
		slave      = false
		found      = false
	)

	rlog.Debug("broker id "+strconv.FormatInt(brokerId, 10), nil)

	v, exist := s.brokerAddressesMap.Load(brokerName)

	if !exist {
		return nil
	}
	data := v.(*BrokerData)
	if len(data.BrokerAddresses) == 0 {
		return nil
	}

	brokerAddr = data.BrokerAddresses[brokerId]
	slave = brokerId != MasterId
	if brokerAddr != "" {
		found = true
	}

	// not found && read from slave, try again use next brokerId
	if !found && slave {
		rlog.Debug("Not found broker addr and slave "+strconv.FormatBool(slave), nil)
		brokerAddr = data.BrokerAddresses[brokerId+1]
		found = brokerAddr != ""
	}

	// still not found && cloud use other broker addr, find anyone in BrokerAddresses
	if !found && !onlyThisBroker {
		rlog.Debug("STILL Not found broker addr", nil)
		for k, v := range data.BrokerAddresses {
			if v != "" {
				brokerAddr = v
				found = true
				slave = k != MasterId
				break
			}
		}
	}

	if found {
		rlog.Debug("Find broker addr "+brokerAddr, nil)
	}

	var result *FindBrokerResult
	if found {
		result = &FindBrokerResult{
			BrokerAddr:    brokerAddr,
			Slave:         slave,
			BrokerVersion: s.findBrokerVersion(brokerName, brokerAddr),
		}
	}

	return result
}

func (s *namesrvs) FetchSubscribeMessageQueues(topic string) ([]*primitive.MessageQueue, error) {
	routeData, err := s.queryTopicRouteInfoFromServer(topic)

	if err != nil {
		return nil, err
	}

	mqs := make([]*primitive.MessageQueue, 0)

	for _, qd := range routeData.QueueDataList {
		if queueIsReadable(qd.Perm) {
			for i := 0; i < qd.ReadQueueNums; i++ {
				mqs = append(mqs, &primitive.MessageQueue{Topic: topic, BrokerName: qd.BrokerName, QueueId: i})
			}
		}
	}
	return mqs, nil
}

func (s *namesrvs) FetchPublishMessageQueues(topic string) ([]*primitive.MessageQueue, error) {
	var (
		err       error
		routeData *TopicRouteData
	)

	v, exist := s.routeDataMap.Load(topic)
	if !exist {
		routeData, err = s.queryTopicRouteInfoFromServer(topic)
		if err != nil {
			rlog.Error("queryTopicRouteInfoFromServer failed", map[string]interface{}{
				rlog.LogKeyTopic: topic,
			})
			return nil, err
		}
		s.routeDataMap.Store(topic, routeData)
		s.AddBroker(routeData)
	} else {
		routeData = v.(*TopicRouteData)
	}

	if err != nil {
		return nil, err
	}
	publishInfo := s.routeData2PublishInfo(topic, routeData)

	return publishInfo.MqList, nil
}

func (s *namesrvs) AddBrokerVersion(brokerName, brokerAddr string, version int32) {
	s.brokerLock.Lock()
	defer s.brokerLock.Unlock()

	m, exist := s.brokerVersionMap[brokerName]
	if !exist {
		m = make(map[string]int32, 4)
		s.brokerVersionMap[brokerName] = m
	}
	m[brokerAddr] = version
}

func (s *namesrvs) findBrokerVersion(brokerName, brokerAddr string) int32 {
	s.brokerLock.RLock()
	defer s.brokerLock.RUnlock()

	versions, exist := s.brokerVersionMap[brokerName]

	if !exist {
		return 0
	}

	return versions[brokerAddr]
}

func (s *namesrvs) queryTopicRouteInfoFromServer(topic string) (*TopicRouteData, error) {
	request := &GetRouteInfoRequestHeader{
		Topic: topic,
	}

	var (
		response *remote.RemotingCommand
		err      error
	)

	//if s.Size() == 0, response will be nil, lead to panic below.
	if s.Size() == 0 {
		rlog.Error("namesrv list empty. UpdateNameServerAddress should be called first.", map[string]interface{}{
			"namesrv": s,
			"topic":   topic,
		})
		return nil, primitive.NewRemotingErr("namesrv list empty")
	}

	for i := 0; i < s.Size(); i++ {
		rc := remote.NewRemotingCommand(ReqGetRouteInfoByTopic, request, nil)
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		response, err = s.nameSrvClient.InvokeSync(ctx, s.getNameServerAddress(), rc)

		if err == nil {
			cancel()
			break
		}
		cancel()
	}
	if err != nil {
		rlog.Error("connect to namesrv failed.", map[string]interface{}{
			"namesrv": s,
			"topic":   topic,
		})
		return nil, primitive.NewRemotingErr(err.Error())
	}

	switch response.Code {
	case ResSuccess:
		if response.Body == nil {
			return nil, primitive.NewMQClientErr(response.Code, response.Remark)
		}
		routeData := &TopicRouteData{}

		err = routeData.decode(string(response.Body))
		if err != nil {
			rlog.Warning("decode TopicRouteData error: %s", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
				"topic":                  topic,
			})
			return nil, err
		}
		return routeData, nil
	case ResTopicNotExist:
		return nil, errors.ErrTopicNotExist
	default:
		return nil, primitive.NewMQClientErr(response.Code, response.Remark)
	}
}

func (s *namesrvs) topicRouteDataIsChange(oldData *TopicRouteData, newData *TopicRouteData) bool {
	if oldData == nil || newData == nil {
		return true
	}
	oldDataCloned := oldData.clone()
	newDataCloned := newData.clone()

	sort.Slice(oldDataCloned.QueueDataList, func(i, j int) bool {
		return strings.Compare(oldDataCloned.QueueDataList[i].BrokerName, oldDataCloned.QueueDataList[j].BrokerName) > 0
	})
	sort.Slice(oldDataCloned.BrokerDataList, func(i, j int) bool {
		return strings.Compare(oldDataCloned.BrokerDataList[i].BrokerName, oldDataCloned.BrokerDataList[j].BrokerName) > 0
	})
	sort.Slice(newDataCloned.QueueDataList, func(i, j int) bool {
		return strings.Compare(newDataCloned.QueueDataList[i].BrokerName, newDataCloned.QueueDataList[j].BrokerName) > 0
	})
	sort.Slice(newDataCloned.BrokerDataList, func(i, j int) bool {
		return strings.Compare(newDataCloned.BrokerDataList[i].BrokerName, newDataCloned.BrokerDataList[j].BrokerName) > 0
	})

	return !oldDataCloned.equals(newDataCloned)
}

func (s *namesrvs) routeData2PublishInfo(topic string, data *TopicRouteData) *TopicPublishInfo {
	publishInfo := &TopicPublishInfo{
		RouteData:  data,
		OrderTopic: false,
	}

	if data.OrderTopicConf != "" {
		brokers := strings.Split(data.OrderTopicConf, ";")
		for _, broker := range brokers {
			item := strings.Split(broker, ":")
			nums, _ := strconv.Atoi(item[1])
			for i := 0; i < nums; i++ {
				mq := &primitive.MessageQueue{
					Topic:      topic,
					BrokerName: item[0],
					QueueId:    i,
				}
				publishInfo.MqList = append(publishInfo.MqList, mq)
			}
		}

		publishInfo.OrderTopic = true
		return publishInfo
	}

	qds := data.QueueDataList
	sort.Slice(qds, func(i, j int) bool {
		return i-j >= 0
	})

	for _, qd := range qds {
		if !queueIsWriteable(qd.Perm) {
			continue
		}

		var bData *BrokerData
		for _, bd := range data.BrokerDataList {
			if bd.BrokerName == qd.BrokerName {
				bData = bd
				break
			}
		}

		if bData == nil || bData.BrokerAddresses[MasterId] == "" {
			continue
		}

		for i := 0; i < qd.WriteQueueNums; i++ {
			mq := &primitive.MessageQueue{
				Topic:      topic,
				BrokerName: qd.BrokerName,
				QueueId:    i,
			}
			publishInfo.MqList = append(publishInfo.MqList, mq)
		}
	}

	return publishInfo
}

// TopicRouteData TopicRouteData
type TopicRouteData struct {
	OrderTopicConf string
	QueueDataList  []*QueueData  `json:"queueDatas"`
	BrokerDataList []*BrokerData `json:"brokerDatas"`
}

func (routeData *TopicRouteData) decode(data string) error {
	res := gjson.Parse(data)
	err := jsoniter.Unmarshal([]byte(res.Get("queueDatas").String()), &routeData.QueueDataList)

	if err != nil {
		return err
	}

	bds := res.Get("brokerDatas").Array()
	routeData.BrokerDataList = make([]*BrokerData, len(bds))
	for idx, v := range bds {
		bd := &BrokerData{
			BrokerName:      v.Get("brokerName").String(),
			Cluster:         v.Get("cluster").String(),
			BrokerAddresses: make(map[int64]string, 0),
		}
		addrs := v.Get("brokerAddrs").String()
		strs := strings.Split(addrs[1:len(addrs)-1], ",")
		if strs != nil {
			for _, str := range strs {
				i := strings.Index(str, ":")
				if i < 0 {
					continue
				}
				id, _ := strconv.ParseInt(str[0:i], 10, 64)
				bd.BrokerAddresses[id] = strings.Replace(str[i+1:], "\"", "", -1)
			}
		}
		routeData.BrokerDataList[idx] = bd
	}
	return nil
}

func (routeData *TopicRouteData) clone() *TopicRouteData {
	cloned := &TopicRouteData{
		OrderTopicConf: routeData.OrderTopicConf,
		QueueDataList:  make([]*QueueData, len(routeData.QueueDataList)),
		BrokerDataList: make([]*BrokerData, len(routeData.BrokerDataList)),
	}

	for index, value := range routeData.QueueDataList {
		cloned.QueueDataList[index] = value
	}

	for index, value := range routeData.BrokerDataList {
		cloned.BrokerDataList[index] = value
	}

	return cloned
}

func (routeData *TopicRouteData) equals(data *TopicRouteData) bool {
	if len(routeData.BrokerDataList) != len(data.BrokerDataList) {
		return false
	}
	if len(routeData.QueueDataList) != len(data.QueueDataList) {
		return false
	}

	for idx := range routeData.BrokerDataList {
		if !routeData.BrokerDataList[idx].Equals(data.BrokerDataList[idx]) {
			return false
		}
	}

	for idx := range routeData.QueueDataList {
		if !routeData.QueueDataList[idx].Equals(data.QueueDataList[idx]) {
			return false
		}
	}
	return true
}

func (routeData *TopicRouteData) String() string {
	data, _ := jsoniter.Marshal(routeData)
	return string(data)
}

// QueueData QueueData
type QueueData struct {
	BrokerName     string `json:"brokerName"`
	ReadQueueNums  int    `json:"readQueueNums"`
	WriteQueueNums int    `json:"writeQueueNums"`
	Perm           int    `json:"perm"`
	TopicSynFlag   int    `json:"topicSynFlag"`
}

func (q *QueueData) Equals(qd *QueueData) bool {
	if q.BrokerName != qd.BrokerName {
		return false
	}

	if q.ReadQueueNums != qd.ReadQueueNums {
		return false
	}

	if q.WriteQueueNums != qd.WriteQueueNums {
		return false
	}

	if q.Perm != qd.Perm {
		return false
	}

	if q.TopicSynFlag != qd.TopicSynFlag {
		return false
	}

	return true
}

// BrokerData BrokerData
type BrokerData struct {
	Cluster             string           `json:"cluster"`
	BrokerName          string           `json:"brokerName"`
	BrokerAddresses     map[int64]string `json:"brokerAddrs"`
	brokerAddressesLock sync.RWMutex
}

func (b *BrokerData) Equals(bd *BrokerData) bool {
	if b.Cluster != bd.Cluster {
		return false
	}

	if b.BrokerName != bd.BrokerName {
		return false
	}

	if len(b.BrokerAddresses) != len(bd.BrokerAddresses) {
		return false
	}

	for k, v := range b.BrokerAddresses {
		if bd.BrokerAddresses[k] != v {
			return false
		}
	}

	return true
}
