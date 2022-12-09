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
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type TraceBean struct {
	Topic       string
	MsgId       string
	OffsetMsgId string
	Tags        string
	Keys        string
	StoreHost   string
	ClientHost  string
	StoreTime   int64
	RetryTimes  int
	BodyLength  int
	MsgType     primitive.MessageType
}

type TraceTransferBean struct {
	transData string
	// not duplicate
	transKey []string
}

type TraceType string

const (
	Pub       TraceType = "Pub"
	SubBefore TraceType = "SubBefore"
	SubAfter  TraceType = "SubAfter"

	contentSplitter = '\001'
	fieldSplitter   = '\002'
)

type TraceContext struct {
	TraceType   TraceType
	TimeStamp   int64
	RegionId    string
	RegionName  string
	GroupName   string
	CostTime    int64
	IsSuccess   bool
	RequestId   string
	ContextCode int
	TraceBeans  []TraceBean
}

func (ctx *TraceContext) marshal2Bean() *TraceTransferBean {
	buffer := bytes.NewBufferString("")
	switch ctx.TraceType {
	case Pub:
		bean := ctx.TraceBeans[0]
		buffer.WriteString(string(ctx.TraceType))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.FormatInt(ctx.TimeStamp, 10))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(ctx.RegionId)
		buffer.WriteRune(contentSplitter)
		ss := strings.Split(ctx.GroupName, "%")
		if len(ss) == 2 {
			buffer.WriteString(ss[1])
		} else {
			buffer.WriteString(ctx.GroupName)
		}

		buffer.WriteRune(contentSplitter)
		ssTopic := strings.Split(bean.Topic, "%")
		if len(ssTopic) == 2 {
			buffer.WriteString(ssTopic[1])
		} else {
			buffer.WriteString(bean.Topic)
		}
		//buffer.WriteString(bean.Topic)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.MsgId)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.Tags)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.Keys)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.StoreHost)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.Itoa(bean.BodyLength))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.FormatInt(ctx.CostTime, 10))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.Itoa(int(bean.MsgType)))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.OffsetMsgId)
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(strconv.FormatBool(ctx.IsSuccess))
		buffer.WriteRune(contentSplitter)
		buffer.WriteString(bean.ClientHost)
		buffer.WriteRune(fieldSplitter)
	case SubBefore:
		for _, bean := range ctx.TraceBeans {
			buffer.WriteString(string(ctx.TraceType))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatInt(ctx.TimeStamp, 10))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.RegionId)
			buffer.WriteRune(contentSplitter)
			ss := strings.Split(ctx.GroupName, "%")
			if len(ss) == 2 {
				buffer.WriteString(ss[1])
			} else {
				buffer.WriteString(ctx.GroupName)
			}
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.RequestId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(bean.MsgId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.Itoa(bean.RetryTimes))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(nullWrap(bean.Keys))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(bean.ClientHost)
			buffer.WriteRune(fieldSplitter)
		}
	case SubAfter:
		for _, bean := range ctx.TraceBeans {
			buffer.WriteString(string(ctx.TraceType))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.RequestId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(bean.MsgId)
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatInt(ctx.CostTime, 10))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatBool(ctx.IsSuccess))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(nullWrap(bean.Keys))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.Itoa(ctx.ContextCode))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(strconv.FormatInt(ctx.TimeStamp, 10))
			buffer.WriteRune(contentSplitter)
			buffer.WriteString(ctx.GroupName)
			buffer.WriteRune(fieldSplitter)
		}
	}
	transferBean := new(TraceTransferBean)
	transferBean.transData = buffer.String()
	for _, bean := range ctx.TraceBeans {
		transferBean.transKey = append(transferBean.transKey, bean.MsgId)
		if len(bean.Keys) > 0 {
			transferBean.transKey = append(transferBean.transKey, bean.Keys)
		}
	}
	return transferBean
}

// compatible with java console.
func nullWrap(s string) string {
	if len(s) == 0 {
		return "null"
	}
	return s
}

type traceDispatcherType int

const (
	RmqSysTraceTopic = "RMQ_SYS_TRACE_TOPIC"

	ProducerType traceDispatcherType = iota
	ConsumerType

	maxMsgSize = 128000 - 10*1000
	batchSize  = 100

	TraceTopicPrefix = SystemTopicPrefix + "TRACE_DATA_"
	TraceGroupName   = "_INNER_TRACE_PRODUCER"
)

type TraceDispatcher interface {
	GetTraceTopicName() string

	Start()
	Append(ctx TraceContext) bool
	Close()
}

type traceDispatcher struct {
	ctx     context.Context
	cancel  context.CancelFunc
	running bool

	traceTopic string
	access     primitive.AccessChannel

	ticker  *time.Ticker
	input   chan TraceContext
	batchCh chan []*TraceContext

	discardCount int64

	// support deliver trace message to other cluster.
	namesrvs *namesrvs
	// round robin index
	rrindex int32
	cli     RMQClient
}

func NewTraceDispatcher(traceCfg *primitive.TraceConfig) *traceDispatcher {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	t := traceCfg.TraceTopic
	if len(t) == 0 {
		t = RmqSysTraceTopic
	}

	if traceCfg.Access == primitive.Cloud {
		t = TraceTopicPrefix + traceCfg.TraceTopic
	}

	if len(traceCfg.NamesrvAddrs) == 0 && traceCfg.Resolver == nil {
		panic("no NamesrvAddrs or Resolver configured")
	}

	var srvs *namesrvs
	var err error
	if len(traceCfg.NamesrvAddrs) > 0 {
		srvs, err = NewNamesrv(primitive.NewPassthroughResolver(traceCfg.NamesrvAddrs), nil)
	} else {
		srvs, err = NewNamesrv(traceCfg.Resolver, nil)
	}

	if err != nil {
		panic(errors.Wrap(err, "new Namesrv failed."))
	}
	if !traceCfg.Credentials.IsEmpty() {
		srvs.SetCredentials(traceCfg.Credentials)
	}

	cliOp := DefaultClientOptions()
	cliOp.GroupName = traceCfg.GroupName
	cliOp.NameServerAddrs = traceCfg.NamesrvAddrs
	cliOp.InstanceName = "INNER_TRACE_CLIENT_DEFAULT"
	cliOp.RetryTimes = 0
	cliOp.Namesrv = srvs
	cliOp.Credentials = traceCfg.Credentials
	cli := GetOrNewRocketMQClient(cliOp, nil)
	if cli == nil {
		return nil
	}
	cliOp.Namesrv = cli.GetNameSrv()
	return &traceDispatcher{
		ctx:    ctx,
		cancel: cancel,

		traceTopic: t,
		access:     traceCfg.Access,
		input:      make(chan TraceContext, 1024),
		batchCh:    make(chan []*TraceContext, 2048),
		cli:        cli,
		namesrvs:   srvs,
	}
}

func (td *traceDispatcher) GetTraceTopicName() string {
	return td.traceTopic
}

func (td *traceDispatcher) Start() {
	td.running = true
	td.cli.Start()
	go primitive.WithRecover(func() {
		td.process()
	})
}

func (td *traceDispatcher) Close() {
	td.running = false
	td.ticker.Stop()
	td.cancel()
}

func (td *traceDispatcher) Append(ctx TraceContext) bool {
	if !td.running {
		rlog.Error("traceDispatcher is closed.", nil)
		return false
	}
	select {
	case td.input <- ctx:
		return true
	default:
		rlog.Warning("buffer full", map[string]interface{}{
			"discardCount": atomic.AddInt64(&td.discardCount, 1),
			"TraceContext": ctx,
		})
		return false
	}
}

// process
func (td *traceDispatcher) process() {
	var count int
	var batch []TraceContext
	maxWaitDuration := 5 * time.Millisecond
	maxWaitTime := maxWaitDuration.Nanoseconds()
	td.ticker = time.NewTicker(maxWaitDuration)
	lastput := time.Now()
	for {
		select {
		case ctx := <-td.input:
			count++
			lastput = time.Now()
			batch = append(batch, ctx)
			if count == batchSize {
				count = 0
				batchSend := batch
				go primitive.WithRecover(func() {
					td.batchCommit(batchSend)
				})
				batch = make([]TraceContext, 0)
			}
		case <-td.ticker.C:
			delta := time.Since(lastput).Nanoseconds()
			if delta > maxWaitTime {
				count++
				lastput = time.Now()
				if len(batch) > 0 {
					batchSend := batch
					go primitive.WithRecover(func() {
						td.batchCommit(batchSend)
					})
					batch = make([]TraceContext, 0)
				}
			}
		case <-td.ctx.Done():
			batchSend := batch
			go primitive.WithRecover(func() {
				td.batchCommit(batchSend)
			})
			batch = make([]TraceContext, 0)

			now := time.Now().UnixNano() / int64(time.Millisecond)
			end := now + 500
			for now < end {
				now = time.Now().UnixNano() / int64(time.Millisecond)
				runtime.Gosched()
			}
			rlog.Info(fmt.Sprintf("------end trace send %v %v", td.input, td.batchCh), nil)
		}
	}
}

// batchCommit commit slice of TraceContext. convert the ctxs to keyed pair(key is Topic + regionid).
// flush according key one by one.
func (td *traceDispatcher) batchCommit(ctxs []TraceContext) {
	keyedCtxs := make(map[string][]TraceTransferBean)
	for _, ctx := range ctxs {
		if len(ctx.TraceBeans) == 0 {
			return
		}
		topic := ctx.TraceBeans[0].Topic
		regionID := ctx.RegionId
		key := topic
		if len(regionID) > 0 {
			key = fmt.Sprintf("%s%c%s", topic, contentSplitter, regionID)
		}
		keyedCtxs[key] = append(keyedCtxs[key], *ctx.marshal2Bean())
	}

	for k, v := range keyedCtxs {
		arr := strings.Split(k, string([]byte{contentSplitter}))
		topic := k
		regionID := ""
		if len(arr) > 1 {
			topic = arr[0]
			regionID = arr[1]
		}
		td.flush(topic, regionID, v)
	}
}

type Keyset map[string]struct{}

func (ks Keyset) slice() []string {
	slice := make([]string, len(ks))
	for k, _ := range ks {
		slice = append(slice, k)
	}
	return slice
}

// flush data in batch.
func (td *traceDispatcher) flush(topic, regionID string, data []TraceTransferBean) {
	if len(data) == 0 {
		return
	}

	keyset := make(Keyset)
	var builder strings.Builder
	flushed := true
	for _, bean := range data {
		for _, k := range bean.transKey {
			keyset[k] = struct{}{}
		}
		builder.WriteString(bean.transData)
		flushed = false

		if builder.Len() > maxMsgSize {
			td.sendTraceDataByMQ(keyset, regionID, builder.String())
			builder.Reset()
			keyset = make(Keyset)
			flushed = true
		}
	}
	if !flushed {
		td.sendTraceDataByMQ(keyset, regionID, builder.String())
	}
}

func (td *traceDispatcher) sendTraceDataByMQ(keySet Keyset, regionID string, data string) {
	traceTopic := td.traceTopic
	if td.access == primitive.Cloud {
		traceTopic = td.traceTopic + regionID
	}
	msg := primitive.NewMessage(traceTopic, []byte(data))
	msg.WithKeys(keySet.slice())

	mq, addr := td.findMq(regionID)
	if mq == nil {
		return
	}

	var req = td.buildSendRequest(mq, msg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := td.cli.InvokeAsync(ctx, addr, req, func(command *remote.RemotingCommand, e error) {
		cancel()
		resp := primitive.NewSendResult()
		if e != nil {
			rlog.Info("send trace data error.", map[string]interface{}{
				"traceData": data,
			})
		} else {
			td.cli.ProcessSendResponse(mq.BrokerName, command, resp, msg)
			rlog.Debug("send trace data success:", map[string]interface{}{
				"SendResult": resp,
				"traceData":  data,
			})
		}
	})
	if err != nil {
		cancel()
		rlog.Info("send trace data error when invoke", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
	}
}

func (td *traceDispatcher) findMq(regionID string) (*primitive.MessageQueue, string) {
	traceTopic := td.traceTopic
	if td.access == primitive.Cloud {
		traceTopic = td.traceTopic + regionID
	}
	mqs, err := td.namesrvs.FetchPublishMessageQueues(traceTopic)
	if err != nil {
		rlog.Error("fetch publish message queues failed", map[string]interface{}{
			rlog.LogKeyUnderlayError: err,
		})
		return nil, ""
	}
	i := atomic.AddInt32(&td.rrindex, 1)
	if i < 0 {
		i = 0
		atomic.StoreInt32(&td.rrindex, 0)
	}
	i %= int32(len(mqs))
	mq := mqs[i]

	brokerName := mq.BrokerName
	addr := td.namesrvs.FindBrokerAddrByName(brokerName)

	return mq, addr
}

func (td *traceDispatcher) buildSendRequest(mq *primitive.MessageQueue,
	msg *primitive.Message) *remote.RemotingCommand {
	req := &SendMessageRequestHeader{
		ProducerGroup: TraceGroupName,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
		BornTimestamp: time.Now().UnixNano() / int64(time.Millisecond),
		Flag:          msg.Flag,
		Properties:    msg.MarshallProperties(),
	}

	return remote.NewRemotingCommand(ReqSendMessage, req, msg.Body)
}
