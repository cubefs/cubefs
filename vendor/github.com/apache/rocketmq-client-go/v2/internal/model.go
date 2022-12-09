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
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	jsoniter "github.com/json-iterator/go"
)

type FindBrokerResult struct {
	BrokerAddr    string
	Slave         bool
	BrokerVersion int32
}

type (
	// groupName of consumer
	consumeType string

	ServiceState int32
)

const (
	StateCreateJust ServiceState = iota
	StateStartFailed
	StateRunning
	StateShutdown
)

type SubscriptionData struct {
	ClassFilterMode bool      `json:"classFilterMode"`
	Topic           string    `json:"topic"`
	SubString       string    `json:"subString"`
	Tags            utils.Set `json:"tagsSet"`
	Codes           utils.Set `json:"codeSet"`
	SubVersion      int64     `json:"subVersion"`
	ExpType         string    `json:"expressionType"`
}

type producerData struct {
	GroupName string `json:"groupName"`
}

func (p producerData) UniqueID() string {
	return p.GroupName
}

type consumerData struct {
	GroupName         string              `json:"groupName"`
	CType             consumeType         `json:"consumeType"`
	MessageModel      string              `json:"messageModel"`
	Where             string              `json:"consumeFromWhere"`
	SubscriptionDatas []*SubscriptionData `json:"subscriptionDataSet"`
	UnitMode          bool                `json:"unitMode"`
}

func (c consumerData) UniqueID() string {
	return c.GroupName
}

type heartbeatData struct {
	ClientId      string    `json:"clientID"`
	ProducerDatas utils.Set `json:"producerDataSet"`
	ConsumerDatas utils.Set `json:"consumerDataSet"`
}

func NewHeartbeatData(clientID string) *heartbeatData {
	return &heartbeatData{
		ClientId:      clientID,
		ProducerDatas: utils.NewSet(),
		ConsumerDatas: utils.NewSet(),
	}
}

func (data *heartbeatData) encode() []byte {
	d, err := jsoniter.Marshal(data)
	if err != nil {
		rlog.Error("marshal heartbeatData error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil
	}
	rlog.Debug("heartbeat: "+string(d), nil)
	return d
}

const (
	PropNameServerAddr         = "PROP_NAMESERVER_ADDR"
	PropThreadPoolCoreSize     = "PROP_THREADPOOL_CORE_SIZE"
	PropConsumeOrderly         = "PROP_CONSUMEORDERLY"
	PropConsumeType            = "PROP_CONSUME_TYPE"
	PropClientVersion          = "PROP_CLIENT_VERSION"
	PropConsumerStartTimestamp = "PROP_CONSUMER_START_TIMESTAMP"
)

type ProcessQueueInfo struct {
	CommitOffset            int64 `json:"commitOffset"`
	CachedMsgMinOffset      int64 `json:"cachedMsgMinOffset"`
	CachedMsgMaxOffset      int64 `json:"cachedMsgMaxOffset"`
	CachedMsgCount          int   `json:"cachedMsgCount"`
	CachedMsgSizeInMiB      int64 `json:"cachedMsgSizeInMiB"`
	TransactionMsgMinOffset int64 `json:"transactionMsgMinOffset"`
	TransactionMsgMaxOffset int64 `json:"transactionMsgMaxOffset"`
	TransactionMsgCount     int   `json:"transactionMsgCount"`
	Locked                  bool  `json:"locked"`
	TryUnlockTimes          int64 `json:"tryUnlockTimes"`
	LastLockTimestamp       int64 `json:"lastLockTimestamp"`
	Dropped                 bool  `json:"dropped"`
	LastPullTimestamp       int64 `json:"lastPullTimestamp"`
	LastConsumeTimestamp    int64 `json:"lastConsumeTimestamp"`
}

type ConsumeStatus struct {
	PullRT            float64 `json:"pullRT"`
	PullTPS           float64 `json:"pullTPS"`
	ConsumeRT         float64 `json:"consumeRT"`
	ConsumeOKTPS      float64 `json:"consumeOKTPS"`
	ConsumeFailedTPS  float64 `json:"consumeFailedTPS"`
	ConsumeFailedMsgs int64   `json:"consumeFailedMsgs"`
}

type ConsumerRunningInfo struct {
	Properties       map[string]string
	SubscriptionData map[*SubscriptionData]bool
	MQTable          map[primitive.MessageQueue]ProcessQueueInfo
	StatusTable      map[string]ConsumeStatus
	JStack           string // just follow java request param name, but pass golang stack here.
}

func (info ConsumerRunningInfo) Encode() ([]byte, error) {
	data, err := json.Marshal(info.Properties)
	if err != nil {
		return nil, err
	}
	jsonData := fmt.Sprintf("{\"%s\":%s", "properties", string(data))

	data, err = json.Marshal(info.StatusTable)
	if err != nil {
		return nil, err
	}
	jsonData = fmt.Sprintf("%s,\"%s\":%s", jsonData, "statusTable", string(data))

	subs := make([]*SubscriptionData, len(info.SubscriptionData))
	idx := 0
	for k := range info.SubscriptionData {
		subs[idx] = k
		idx++
	}

	// make sure test case table
	sort.Slice(subs, func(i, j int) bool {
		sub1 := subs[i]
		sub2 := subs[j]
		if sub1.ClassFilterMode != sub2.ClassFilterMode {
			return sub1.ClassFilterMode == false
		}
		com := strings.Compare(sub1.Topic, sub1.Topic)
		if com != 0 {
			return com > 0
		}

		com = strings.Compare(sub1.SubString, sub1.SubString)
		if com != 0 {
			return com > 0
		}

		if sub1.SubVersion != sub2.SubVersion {
			return sub1.SubVersion > sub2.SubVersion
		}

		com = strings.Compare(sub1.ExpType, sub1.ExpType)
		if com != 0 {
			return com > 0
		}

		v1, _ := sub1.Tags.MarshalJSON()
		v2, _ := sub2.Tags.MarshalJSON()
		com = bytes.Compare(v1, v2)
		if com != 0 {
			return com > 0
		}

		v1, _ = sub1.Codes.MarshalJSON()
		v2, _ = sub2.Codes.MarshalJSON()
		com = bytes.Compare(v1, v2)
		if com != 0 {
			return com > 0
		}
		return true
	})

	data, err = json.Marshal(subs)
	if err != nil {
		return nil, err
	}
	jsonData = fmt.Sprintf("%s,\"%s\":%s", jsonData, "subscriptionSet", string(data))

	tableJson := ""
	keys := make([]primitive.MessageQueue, 0)

	for k := range info.MQTable {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		q1 := keys[i]
		q2 := keys[j]
		com := strings.Compare(q1.Topic, q2.Topic)
		if com != 0 {
			return com < 0
		}

		com = strings.Compare(q1.BrokerName, q2.BrokerName)
		if com != 0 {
			return com < 0
		}

		return q1.QueueId < q2.QueueId
	})

	for idx := range keys {
		dataK, err := json.Marshal(keys[idx])
		if err != nil {
			return nil, err
		}
		dataV, err := json.Marshal(info.MQTable[keys[idx]])
		tableJson = fmt.Sprintf("%s,%s:%s", tableJson, string(dataK), string(dataV))
	}
	tableJson = strings.TrimLeft(tableJson, ",")

	jsonData = fmt.Sprintf("%s,\"%s\":%s, \"%s\":\"%s\" }",
		jsonData, "mqTable", fmt.Sprintf("{%s}", tableJson),
		"jstack", info.JStack)

	return []byte(jsonData), nil
}

func NewConsumerRunningInfo() *ConsumerRunningInfo {
	return &ConsumerRunningInfo{
		Properties:       make(map[string]string),
		SubscriptionData: make(map[*SubscriptionData]bool),
		MQTable:          make(map[primitive.MessageQueue]ProcessQueueInfo),
		StatusTable:      make(map[string]ConsumeStatus),
	}
}

type ConsumeMessageDirectlyResult struct {
	Order          bool          `json:"order"`
	AutoCommit     bool          `json:"autoCommit"`
	ConsumeResult  ConsumeResult `json:"consumeResult"`
	Remark         string        `json:"remark"`
	SpentTimeMills int64         `json:"spentTimeMills"`
}

type ConsumeResult int

const (
	ConsumeSuccess ConsumeResult = iota
	ConsumeRetryLater
	Rollback
	Commit
	ThrowException
	ReturnNull
)

func (result ConsumeMessageDirectlyResult) Encode() ([]byte, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return data, nil
}

type ResetOffsetBody struct {
	OffsetTable map[primitive.MessageQueue]int64 `json:"offsetTable"`
}

// Decode note: the origin implementation for parse json is in gson format.
// this func should support both gson and fastjson schema.
func (resetOffsetBody *ResetOffsetBody) Decode(body []byte) {
	validJSON := gjson.ValidBytes(body)

	var offsetTable map[primitive.MessageQueue]int64

	if validJSON {
		offsetTable = parseGsonFormat(body)
	} else {
		offsetTable = parseFastJsonFormat(body)
	}

	resetOffsetBody.OffsetTable = offsetTable
}

func parseGsonFormat(body []byte) map[primitive.MessageQueue]int64 {
	result := gjson.ParseBytes(body)

	rlog.Debug("offset table string "+result.Get("offsetTable").String(), nil)

	offsetTable := make(map[primitive.MessageQueue]int64, 0)

	offsetStr := result.Get("offsetTable").String()
	if len(offsetStr) <= 2 {
		rlog.Warning("parse reset offset table json get nothing in body", map[string]interface{}{
			"origin json": offsetStr,
		})
		return offsetTable
	}

	offsetTableArray := strings.Split(offsetStr, "],[")

	for index, v := range offsetTableArray {
		kvArray := strings.Split(v, "},")

		var kstr, vstr string
		if index == len(offsetTableArray)-1 {
			vstr = kvArray[1][:len(kvArray[1])-2]
		} else {
			vstr = kvArray[1]
		}
		offset, err := strconv.ParseInt(vstr, 10, 64)
		if err != nil {
			rlog.Error("Unmarshal offset error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			return nil
		}

		if index == 0 {
			kstr = kvArray[0][2:len(kvArray[0])] + "}"
		} else {
			kstr = kvArray[0] + "}"
		}
		kObj := new(primitive.MessageQueue)
		err = jsoniter.Unmarshal([]byte(kstr), &kObj)
		if err != nil {
			rlog.Error("Unmarshal message queue error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err,
			})
			return nil
		}
		offsetTable[*kObj] = offset
	}

	return offsetTable
}

func parseFastJsonFormat(body []byte) map[primitive.MessageQueue]int64 {
	offsetTable := make(map[primitive.MessageQueue]int64)

	jsonStr := string(body)
	offsetStr := gjson.Get(jsonStr, "offsetTable").String()

	if len(offsetStr) <= 2 {
		rlog.Warning("parse reset offset table json get nothing in body", map[string]interface{}{
			"origin json": jsonStr,
		})
		return offsetTable
	}

	trimStr := offsetStr[2 : len(offsetStr)-1]

	split := strings.Split(trimStr, ",{")

	for _, v := range split {
		tuple := strings.Split(v, "}:")

		queueStr := "{" + tuple[0] + "}"

		var err error
		// ignore err for now
		offset, err := strconv.Atoi(tuple[1])

		var queue primitive.MessageQueue
		err = json.Unmarshal([]byte(queueStr), &queue)

		if err != nil {
			rlog.Error("parse reset offset table json get nothing in body", map[string]interface{}{
				"origin json": jsonStr,
			})
		}

		offsetTable[queue] = int64(offset)
	}

	return offsetTable
}
