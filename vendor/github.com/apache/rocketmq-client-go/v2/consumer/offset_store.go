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

package consumer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	jsoniter "github.com/json-iterator/go"
)

type readType int

const (
	_ReadFromMemory readType = iota
	_ReadFromStore
	_ReadMemoryThenStore
)

var (
	_LocalOffsetStorePath = os.Getenv("rocketmq.client.localOffsetStoreDir")
)

func init() {
	if _LocalOffsetStorePath == "" {
		_LocalOffsetStorePath = filepath.Join(os.Getenv("HOME"), ".rocketmq_client_go")
	}
}

//go:generate mockgen -source offset_store.go -destination mock_offset_store.go -self_package github.com/apache/rocketmq-client-go/v2/consumer  --package consumer OffsetStore
type OffsetStore interface {
	persist(mqs []*primitive.MessageQueue)
	remove(mq *primitive.MessageQueue)
	read(mq *primitive.MessageQueue, t readType) int64
	readWithException(mq *primitive.MessageQueue, t readType) (int64, error)
	update(mq *primitive.MessageQueue, offset int64, increaseOnly bool)
}

type OffsetSerializeWrapper struct {
	OffsetTable map[MessageQueueKey]int64 `json:"offsetTable"`
}

type MessageQueueKey primitive.MessageQueue

func (mq MessageQueueKey) MarshalText() (text []byte, err error) {
	repr := struct {
		Topic      string `json:"topic"`
		BrokerName string `json:"brokerName"`
		QueueId    int    `json:"queueId"`
	}{
		Topic:      mq.Topic,
		BrokerName: mq.BrokerName,
		QueueId:    mq.QueueId,
	}
	text, err = jsoniter.Marshal(repr)
	return
}

func (mq *MessageQueueKey) UnmarshalText(text []byte) error {
	repr := struct {
		Topic      string `json:"topic"`
		BrokerName string `json:"brokerName"`
		QueueId    int    `json:"queueId"`
	}{}
	err := jsoniter.Unmarshal(text, &repr)
	if err != nil {
		return err
	}
	mq.Topic = repr.Topic
	mq.QueueId = repr.QueueId
	mq.BrokerName = repr.BrokerName

	return nil
}

type localFileOffsetStore struct {
	group       string
	path        string
	OffsetTable *sync.Map // concurrent safe , map[MessageQueueKey]int64
	// mutex for offset file
	mutex sync.Mutex
}

func NewLocalFileOffsetStore(clientID, group string) OffsetStore {
	store := &localFileOffsetStore{
		group:       group,
		path:        filepath.Join(_LocalOffsetStorePath, clientID, group, "offset.json"),
		OffsetTable: new(sync.Map),
	}
	store.load()
	return store
}

func (local *localFileOffsetStore) load() {
	local.mutex.Lock()
	defer local.mutex.Unlock()
	data, err := utils.FileReadAll(local.path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		rlog.Info("read from local store error, try to use bak file", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		data, err = utils.FileReadAll(filepath.Join(local.path, ".bak"))
	}
	if err != nil {
		rlog.Info("read from local store bak file error", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return
	}
	datas := make(map[MessageQueueKey]int64)

	wrapper := OffsetSerializeWrapper{
		OffsetTable: datas,
	}

	err = jsoniter.Unmarshal(data, &wrapper)
	if err != nil {
		rlog.Warning("unmarshal local offset error", map[string]interface{}{
			"local_path":             local.path,
			rlog.LogKeyUnderlayError: err.Error(),
		})
		return
	}

	if datas != nil {
		for k, v := range datas {
			local.OffsetTable.Store(k, v)
		}
	}
}

// Deprecated: Use readWithException instead.
func (local *localFileOffsetStore) read(mq *primitive.MessageQueue, t readType) int64 {
	result, _ := local.readWithException(mq, t)
	return result
}

func (local *localFileOffsetStore) readWithException(mq *primitive.MessageQueue, t readType) (int64, error) {
	switch t {
	case _ReadFromMemory, _ReadMemoryThenStore:
		off := readFromMemory(local.OffsetTable, mq)
		if off >= 0 || (off == -1 && t == _ReadFromMemory) {
			return off, nil
		}
		fallthrough
	case _ReadFromStore:
		local.load()
		return readFromMemory(local.OffsetTable, mq), nil
	default:

	}
	return -1, nil
}

func (local *localFileOffsetStore) update(mq *primitive.MessageQueue, offset int64, increaseOnly bool) {
	local.mutex.Lock()
	defer local.mutex.Unlock()
	rlog.Debug("update offset", map[string]interface{}{
		rlog.LogKeyMessageQueue: mq,
		"new_offset":            offset,
	})
	key := MessageQueueKey(*mq)
	localOffset, exist := local.OffsetTable.Load(key)
	if !exist {
		local.OffsetTable.Store(key, offset)
		return
	}
	if increaseOnly {
		if localOffset.(int64) < offset {
			local.OffsetTable.Store(key, offset)
		}
	} else {
		local.OffsetTable.Store(key, offset)
	}
}

func (local *localFileOffsetStore) persist(mqs []*primitive.MessageQueue) {
	if len(mqs) == 0 {
		return
	}
	local.mutex.Lock()
	defer local.mutex.Unlock()

	datas := make(map[MessageQueueKey]int64)
	local.OffsetTable.Range(func(key, value interface{}) bool {
		k := key.(MessageQueueKey)
		v := value.(int64)
		datas[k] = v
		return true
	})

	wrapper := OffsetSerializeWrapper{
		OffsetTable: datas,
	}
	data, _ := jsoniter.Marshal(wrapper)
	utils.CheckError(fmt.Sprintf("persist offset to %s", local.path), utils.WriteToFile(local.path, data))
}

func (local *localFileOffsetStore) remove(mq *primitive.MessageQueue) {
	// nothing to do
}

type remoteBrokerOffsetStore struct {
	group       string
	OffsetTable map[primitive.MessageQueue]int64 `json:"OffsetTable"`
	client      internal.RMQClient
	namesrv     internal.Namesrvs
	mutex       sync.RWMutex
}

func NewRemoteOffsetStore(group string, client internal.RMQClient, namesrv internal.Namesrvs) OffsetStore {
	return &remoteBrokerOffsetStore{
		group:       group,
		client:      client,
		namesrv:     namesrv,
		OffsetTable: make(map[primitive.MessageQueue]int64),
	}
}

func (r *remoteBrokerOffsetStore) persist(mqs []*primitive.MessageQueue) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if len(mqs) == 0 {
		return
	}

	used := make(map[primitive.MessageQueue]struct{}, 0)
	for _, mq := range mqs {
		used[*mq] = struct{}{}
	}

	for mq, off := range r.OffsetTable {
		if _, ok := used[mq]; !ok {
			delete(r.OffsetTable, mq)
			continue
		}
		err := r.updateConsumeOffsetToBroker(r.group, mq, off)
		if err != nil {
			rlog.Warning("update offset to broker error", map[string]interface{}{
				rlog.LogKeyConsumerGroup: r.group,
				rlog.LogKeyMessageQueue:  mq.String(),
				rlog.LogKeyUnderlayError: err.Error(),
				"offset":                 off,
			})
		} else {
			rlog.Info("update offset to broker success", map[string]interface{}{
				rlog.LogKeyConsumerGroup: r.group,
				rlog.LogKeyMessageQueue:  mq.String(),
				"offset":                 off,
			})
		}
	}
}

func (r *remoteBrokerOffsetStore) remove(mq *primitive.MessageQueue) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.OffsetTable, *mq)
	rlog.Warning("delete mq from offset table", map[string]interface{}{
		rlog.LogKeyConsumerGroup: r.group,
		rlog.LogKeyMessageQueue:  mq,
	})
}

// Deprecated: Use readWithException instead.
func (r *remoteBrokerOffsetStore) read(mq *primitive.MessageQueue, t readType) int64 {
	result, _ := r.readWithException(mq, t)
	return result
}

func (r *remoteBrokerOffsetStore) readWithException(mq *primitive.MessageQueue, t readType) (int64, error) {
	r.mutex.RLock()
	switch t {
	case _ReadFromMemory, _ReadMemoryThenStore:
		off, exist := r.OffsetTable[*mq]
		if exist {
			r.mutex.RUnlock()
			return off, nil
		}
		if t == _ReadFromMemory {
			r.mutex.RUnlock()
			return -1, nil
		}
		fallthrough
	case _ReadFromStore:
		off, err := r.fetchConsumeOffsetFromBroker(r.group, mq)
		if err != nil {
			rlog.Error("fetch offset of mq from broker error", map[string]interface{}{
				rlog.LogKeyConsumerGroup: r.group,
				rlog.LogKeyMessageQueue:  mq.String(),
				rlog.LogKeyUnderlayError: err,
			})
			r.mutex.RUnlock()
			return -1, err
		}
		rlog.Warning("fetch offset of mq from broker success", map[string]interface{}{
			rlog.LogKeyConsumerGroup: r.group,
			rlog.LogKeyMessageQueue:  mq.String(),
			"offset":                 off,
		})
		r.mutex.RUnlock()
		r.update(mq, off, true)
		return off, nil
	default:
	}

	return -1, nil
}

func (r *remoteBrokerOffsetStore) update(mq *primitive.MessageQueue, offset int64, increaseOnly bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	localOffset, exist := r.OffsetTable[*mq]
	if !exist {
		r.OffsetTable[*mq] = offset
		return
	}
	if increaseOnly {
		if localOffset < offset {
			r.OffsetTable[*mq] = offset
		}
	} else {
		r.OffsetTable[*mq] = offset
	}
}

func (r *remoteBrokerOffsetStore) fetchConsumeOffsetFromBroker(group string, mq *primitive.MessageQueue) (int64, error) {
	broker := r.namesrv.FindBrokerAddrByName(mq.BrokerName)
	if broker == "" {
		r.namesrv.UpdateTopicRouteInfo(mq.Topic)
		broker = r.namesrv.FindBrokerAddrByName(mq.BrokerName)
	}
	if broker == "" {
		return int64(-1), fmt.Errorf("broker: %s address not found", mq.BrokerName)
	}
	queryOffsetRequest := &internal.QueryConsumerOffsetRequestHeader{
		ConsumerGroup: group,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
	}
	cmd := remote.NewRemotingCommand(internal.ReqQueryConsumerOffset, queryOffsetRequest, nil)
	res, err := r.client.InvokeSync(context.Background(), broker, cmd, 3*time.Second)
	if err != nil {
		return -1, err
	}
	if res.Code != internal.ResSuccess {
		return -2, fmt.Errorf("broker response code: %d, remarks: %s", res.Code, res.Remark)
	}

	off, err := strconv.ParseInt(res.ExtFields["offset"], 10, 64)

	if err != nil {
		return -1, err
	}

	return off, nil
}

func (r *remoteBrokerOffsetStore) updateConsumeOffsetToBroker(group string, mq primitive.MessageQueue, off int64) error {
	broker := r.namesrv.FindBrokerAddrByName(mq.BrokerName)
	if broker == "" {
		r.namesrv.UpdateTopicRouteInfo(mq.Topic)
		broker = r.namesrv.FindBrokerAddrByName(mq.BrokerName)
	}
	if broker == "" {
		return fmt.Errorf("broker: %s address not found", mq.BrokerName)
	}

	updateOffsetRequest := &internal.UpdateConsumerOffsetRequestHeader{
		ConsumerGroup: group,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
		CommitOffset:  off,
	}
	cmd := remote.NewRemotingCommand(internal.ReqUpdateConsumerOffset, updateOffsetRequest, nil)
	return r.client.InvokeOneWay(context.Background(), broker, cmd, 5*time.Second)
}

func readFromMemory(table *sync.Map, mq *primitive.MessageQueue) int64 {
	localOffset, exist := table.Load(MessageQueueKey(*mq))
	if !exist {
		return -1
	}

	return localOffset.(int64)
}
