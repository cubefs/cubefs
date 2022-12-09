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

package producer

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	errors2 "github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type defaultProducer struct {
	group       string
	client      internal.RMQClient
	state       int32
	options     producerOptions
	publishInfo sync.Map
	callbackCh  chan interface{}

	interceptor primitive.Interceptor

	startOnce    sync.Once
	ShutdownOnce sync.Once
}

func NewDefaultProducer(opts ...Option) (*defaultProducer, error) {
	defaultOpts := defaultProducerOptions()
	for _, apply := range opts {
		apply(&defaultOpts)
	}
	srvs, err := internal.NewNamesrv(defaultOpts.Resolver, defaultOpts.RemotingClientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "new Namesrv failed.")
	}
	if !defaultOpts.Credentials.IsEmpty() {
		srvs.SetCredentials(defaultOpts.Credentials)
	}
	defaultOpts.Namesrv = srvs

	producer := &defaultProducer{
		group:      defaultOpts.GroupName,
		callbackCh: make(chan interface{}),
		options:    defaultOpts,
	}
	producer.client = internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, producer.callbackCh)
	if producer.client == nil {
		return nil, fmt.Errorf("GetOrNewRocketMQClient faild")
	}
	defaultOpts.Namesrv = producer.client.GetNameSrv()

	producer.interceptor = primitive.ChainInterceptors(producer.options.Interceptors...)

	return producer, nil
}

func (p *defaultProducer) Start() error {
	var err error
	p.startOnce.Do(func() {
		err = p.client.RegisterProducer(p.group, p)
		if err != nil {
			rlog.Error("the producer group has been created, specify another one", map[string]interface{}{
				rlog.LogKeyProducerGroup: p.group,
			})
			err = errors2.ErrProducerCreated
			return
		}
		p.client.Start()
		atomic.StoreInt32(&p.state, int32(internal.StateRunning))
	})
	return err
}

func (p *defaultProducer) Shutdown() error {
	p.ShutdownOnce.Do(func() {
		atomic.StoreInt32(&p.state, int32(internal.StateShutdown))
		p.client.UnregisterProducer(p.group)
		p.client.Shutdown()
	})
	return nil
}

func (p *defaultProducer) checkMsg(msgs ...*primitive.Message) error {
	if atomic.LoadInt32(&p.state) != int32(internal.StateRunning) {
		return errors2.ErrNotRunning
	}

	if len(msgs) == 0 {
		return errors2.ErrMessageEmpty
	}

	if len(msgs[0].Topic) == 0 {
		return errors2.ErrTopicEmpty
	}

	topic := msgs[0].Topic
	for _, msg := range msgs {
		if msg.Topic != topic {
			return errors2.ErrMultipleTopics
		}
	}

	return nil
}

func (p *defaultProducer) encodeBatch(msgs ...*primitive.Message) *primitive.Message {
	if len(msgs) == 1 {
		return msgs[0]
	}

	// encode batch
	batch := new(primitive.Message)
	batch.Topic = msgs[0].Topic
	batch.Queue = msgs[0].Queue
	batch.Body = MarshalMessageBatch(msgs...)
	batch.Batch = true
	return batch
}

func MarshalMessageBatch(msgs ...*primitive.Message) []byte {
	buffer := bytes.NewBufferString("")
	for _, msg := range msgs {
		data := msg.Marshal()
		buffer.Write(data)
	}
	return buffer.Bytes()
}

func (p *defaultProducer) prepareSendRequest(msg *primitive.Message, ttl time.Duration) (string, error) {
	correlationId := uuid.NewV4().String()
	requestClientId := p.client.ClientID()
	msg.WithProperty(primitive.PropertyCorrelationID, correlationId)
	msg.WithProperty(primitive.PropertyMessageReplyToClient, requestClientId)
	msg.WithProperty(primitive.PropertyMessageTTL, strconv.Itoa(int(ttl.Seconds())))

	rlog.Debug("message info:", map[string]interface{}{
		"clientId":      requestClientId,
		"correlationId": correlationId,
		"ttl":           ttl.Seconds(),
	})

	nameSrv, err := internal.GetNamesrv(requestClientId)
	if err != nil {
		return "", errors.Wrap(err, "GetNameServ err")
	}

	if !nameSrv.CheckTopicRouteHasTopic(msg.Topic) {
		p.tryToFindTopicPublishInfo(msg.Topic)
		p.client.SendHeartbeatToAllBrokerWithLock()
	}
	return correlationId, nil
}

// Request Send messages to consumer
func (p *defaultProducer) Request(ctx context.Context, timeout time.Duration, msg *primitive.Message) (*primitive.Message, error) {
	if err := p.checkMsg(msg); err != nil {
		return nil, err
	}

	p.messagesWithNamespace(msg)
	correlationId, err := p.prepareSendRequest(msg, timeout)
	if err != nil {
		return nil, err
	}

	requestResponseFuture := internal.NewRequestResponseFuture(correlationId, timeout, nil)
	internal.RequestResponseFutureMap.SetRequestResponseFuture(requestResponseFuture)
	defer internal.RequestResponseFutureMap.RemoveRequestResponseFuture(correlationId)

	f := func(ctx context.Context, result *primitive.SendResult, err error) {
		if err != nil {
			requestResponseFuture.SendRequestOk = false
			requestResponseFuture.ResponseMsg = nil
			requestResponseFuture.CauseErr = err
			return
		}
		requestResponseFuture.SendRequestOk = true
	}

	if p.interceptor != nil {
		ctx = primitive.WithMethod(ctx, primitive.SendAsync)

		return nil, p.interceptor(ctx, msg, nil, func(ctx context.Context, req, reply interface{}) error {
			return p.sendAsync(ctx, msg, f)
		})
	}
	if err := p.sendAsync(ctx, msg, f); err != nil {
		return nil, errors.Wrap(err, "sendAsync error")
	}

	return requestResponseFuture.WaitResponseMessage(msg)
}

// RequestAsync  Async Send messages to consumer
func (p *defaultProducer) RequestAsync(ctx context.Context, timeout time.Duration, callback internal.RequestCallback, msg *primitive.Message) error {
	if err := p.checkMsg(msg); err != nil {
		return err
	}

	p.messagesWithNamespace(msg)
	correlationId, err := p.prepareSendRequest(msg, timeout)
	if err != nil {
		return err
	}

	requestResponseFuture := internal.NewRequestResponseFuture(correlationId, timeout, callback)
	internal.RequestResponseFutureMap.SetRequestResponseFuture(requestResponseFuture)

	f := func(ctx context.Context, result *primitive.SendResult, err error) {
		if err != nil {
			requestResponseFuture.SendRequestOk = false
			requestResponseFuture.ResponseMsg = nil
			requestResponseFuture.CauseErr = err
			internal.RequestResponseFutureMap.RemoveRequestResponseFuture(correlationId)
			return
		}
		requestResponseFuture.SendRequestOk = true
	}

	var resErr error
	if p.interceptor != nil {
		ctx = primitive.WithMethod(ctx, primitive.SendAsync)
		resErr = p.interceptor(ctx, msg, nil, func(ctx context.Context, req, reply interface{}) error {
			return p.sendAsync(ctx, msg, f)
		})
	}
	resErr = p.sendAsync(ctx, msg, f)
	if resErr != nil {
		internal.RequestResponseFutureMap.RemoveRequestResponseFuture(correlationId)
	}
	return resErr
}

func (p *defaultProducer) SendSync(ctx context.Context, msgs ...*primitive.Message) (*primitive.SendResult, error) {
	if err := p.checkMsg(msgs...); err != nil {
		return nil, err
	}

	p.messagesWithNamespace(msgs...)

	msg := p.encodeBatch(msgs...)

	resp := primitive.NewSendResult()
	if p.interceptor != nil {
		ctx = primitive.WithMethod(ctx, primitive.SendSync)
		producerCtx := &primitive.ProducerCtx{
			ProducerGroup:     p.group,
			CommunicationMode: primitive.SendSync,
			BornHost:          utils.LocalIP,
			Message:           *msg,
			SendResult:        resp,
		}
		ctx = primitive.WithProducerCtx(ctx, producerCtx)

		err := p.interceptor(ctx, msg, resp, func(ctx context.Context, req, reply interface{}) error {
			var err error
			realReq := req.(*primitive.Message)
			realReply := reply.(*primitive.SendResult)
			err = p.sendSync(ctx, realReq, realReply)
			return err
		})
		return resp, err
	}

	err := p.sendSync(ctx, msg, resp)
	return resp, err
}

func (p *defaultProducer) sendSync(ctx context.Context, msg *primitive.Message, resp *primitive.SendResult) error {

	retryTime := 1 + p.options.RetryTimes

	var (
		err error
	)

	var producerCtx *primitive.ProducerCtx
	for retryCount := 0; retryCount < retryTime; retryCount++ {
		mq := p.selectMessageQueue(msg)
		if mq == nil {
			err = fmt.Errorf("the topic=%s route info not found", msg.Topic)
			continue
		}

		addr := p.client.GetNameSrv().FindBrokerAddrByName(mq.BrokerName)
		if addr == "" {
			return fmt.Errorf("topic=%s route info not found", mq.Topic)
		}

		if p.interceptor != nil {
			producerCtx = primitive.GetProducerCtx(ctx)
			producerCtx.BrokerAddr = addr
			producerCtx.MQ = *mq
		}

		res, _err := p.client.InvokeSync(ctx, addr, p.buildSendRequest(mq, msg), 3*time.Second)
		if _err != nil {
			err = _err
			continue
		}
		return p.client.ProcessSendResponse(mq.BrokerName, res, resp, msg)
	}
	return err
}

func (p *defaultProducer) SendAsync(ctx context.Context, f func(context.Context, *primitive.SendResult, error), msgs ...*primitive.Message) error {
	if err := p.checkMsg(msgs...); err != nil {
		return err
	}

	p.messagesWithNamespace(msgs...)

	msg := p.encodeBatch(msgs...)

	if p.interceptor != nil {
		ctx = primitive.WithMethod(ctx, primitive.SendAsync)

		return p.interceptor(ctx, msg, nil, func(ctx context.Context, req, reply interface{}) error {
			return p.sendAsync(ctx, msg, f)
		})
	}
	return p.sendAsync(ctx, msg, f)
}

func (p *defaultProducer) sendAsync(ctx context.Context, msg *primitive.Message, h func(context.Context, *primitive.SendResult, error)) error {

	mq := p.selectMessageQueue(msg)
	if mq == nil {
		return errors.Errorf("the topic=%s route info not found", msg.Topic)
	}

	addr := p.client.GetNameSrv().FindBrokerAddrByName(mq.BrokerName)
	if addr == "" {
		return errors.Errorf("topic=%s route info not found", mq.Topic)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	err := p.client.InvokeAsync(ctx, addr, p.buildSendRequest(mq, msg), func(command *remote.RemotingCommand, err error) {
		cancel()
		resp := primitive.NewSendResult()
		if err != nil {
			h(ctx, nil, err)
		} else {
			p.client.ProcessSendResponse(mq.BrokerName, command, resp, msg)
			h(ctx, resp, nil)
		}
	})

	if err != nil {
		cancel()
	}

	return err
}

func (p *defaultProducer) SendOneWay(ctx context.Context, msgs ...*primitive.Message) error {
	if err := p.checkMsg(msgs...); err != nil {
		return err
	}

	p.messagesWithNamespace(msgs...)

	msg := p.encodeBatch(msgs...)

	if p.interceptor != nil {
		ctx = primitive.WithMethod(ctx, primitive.SendOneway)
		return p.interceptor(ctx, msg, nil, func(ctx context.Context, req, reply interface{}) error {
			return p.sendOneWay(ctx, msg)
		})
	}

	return p.sendOneWay(ctx, msg)
}

func (p *defaultProducer) sendOneWay(ctx context.Context, msg *primitive.Message) error {
	retryTime := 1 + p.options.RetryTimes

	var err error
	for retryCount := 0; retryCount < retryTime; retryCount++ {
		mq := p.selectMessageQueue(msg)
		if mq == nil {
			err = fmt.Errorf("the topic=%s route info not found", msg.Topic)
			continue
		}

		addr := p.client.GetNameSrv().FindBrokerAddrByName(mq.BrokerName)
		if addr == "" {
			return fmt.Errorf("topic=%s route info not found", mq.Topic)
		}

		_err := p.client.InvokeOneWay(ctx, addr, p.buildSendRequest(mq, msg), 3*time.Second)
		if _err != nil {
			err = _err
			continue
		}
		return nil
	}
	return err
}

func (p *defaultProducer) messagesWithNamespace(msgs ...*primitive.Message) {

	if p.options.Namespace == "" {
		return
	}

	for _, msg := range msgs {
		msg.Topic = p.options.Namespace + "%" + msg.Topic
	}
}

func (p *defaultProducer) tryCompressMsg(msg *primitive.Message) bool {
	if msg.Compress {
		return true
	}
	if msg.Batch {
		return false
	}
	if len(msg.Body) < p.options.CompressMsgBodyOverHowmuch {
		return false
	}
	compressedBody, e := utils.Compress(msg.Body, p.options.CompressLevel)
	if e != nil {
		return false
	}
	msg.CompressedBody = compressedBody
	msg.Compress = true
	return true
}

func (p *defaultProducer) buildSendRequest(mq *primitive.MessageQueue,
	msg *primitive.Message) *remote.RemotingCommand {
	if !msg.Batch && msg.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex) == "" {
		msg.WithProperty(primitive.PropertyUniqueClientMessageIdKeyIndex, primitive.CreateUniqID())
	}

	var (
		sysFlag      = 0
		transferBody = msg.Body
	)

	if p.tryCompressMsg(msg) {
		transferBody = msg.CompressedBody
		sysFlag = primitive.SetCompressedFlag(sysFlag)
	}
	v := msg.GetProperty(primitive.PropertyTransactionPrepared)
	if v != "" {
		tranMsg, err := strconv.ParseBool(v)
		if err == nil && tranMsg {
			sysFlag |= primitive.TransactionPreparedType
		}
	}

	req := &internal.SendMessageRequestHeader{
		ProducerGroup:  p.group,
		Topic:          mq.Topic,
		QueueId:        mq.QueueId,
		SysFlag:        sysFlag,
		BornTimestamp:  time.Now().UnixNano() / int64(time.Millisecond),
		Flag:           msg.Flag,
		Properties:     msg.MarshallProperties(),
		ReconsumeTimes: 0,
		UnitMode:       p.options.UnitMode,
		Batch:          msg.Batch,
	}

	msgType := msg.GetProperty(primitive.PropertyMsgType)
	if msgType == internal.ReplyMessageFlag {
		return remote.NewRemotingCommand(internal.ReqSendReplyMessage, req, msg.Body)
	}

	cmd := internal.ReqSendMessage
	if msg.Batch {
		cmd = internal.ReqSendBatchMessage
		reqv2 := &internal.SendMessageRequestV2Header{SendMessageRequestHeader: req}
		return remote.NewRemotingCommand(cmd, reqv2, transferBody)
	}

	return remote.NewRemotingCommand(cmd, req, transferBody)
}

func (p *defaultProducer) tryToFindTopicPublishInfo(topic string) *internal.TopicPublishInfo {
	v, exist := p.publishInfo.Load(topic)
	if !exist {
		data, changed, err := p.client.GetNameSrv().UpdateTopicRouteInfo(topic)
		if err != nil && primitive.IsRemotingErr(err) {
			return nil
		}
		p.client.UpdatePublishInfo(topic, data, changed)
		v, exist = p.publishInfo.Load(topic)
	}

	if !exist {
		data, changed, _ := p.client.GetNameSrv().UpdateTopicRouteInfoWithDefault(topic, p.options.CreateTopicKey, p.options.DefaultTopicQueueNums)
		p.client.UpdatePublishInfo(topic, data, changed)
		v, exist = p.publishInfo.Load(topic)
	}

	if !exist {
		return nil
	}

	result := v.(*internal.TopicPublishInfo)
	if result == nil || !result.HaveTopicRouterInfo {
		return nil
	}

	if len(result.MqList) <= 0 {
		rlog.Error("can not find proper message queue", nil)
		return nil
	}
	return result
}

func (p *defaultProducer) selectMessageQueue(msg *primitive.Message) *primitive.MessageQueue {
	topic := msg.Topic
	result := p.tryToFindTopicPublishInfo(topic)
	if result == nil {
		return nil
	}
	return p.options.Selector.Select(msg, result.MqList)
}

func (p *defaultProducer) PublishTopicList() []string {
	topics := make([]string, 0)
	p.publishInfo.Range(func(key, value interface{}) bool {
		topics = append(topics, key.(string))
		return true
	})
	return topics
}

func (p *defaultProducer) UpdateTopicPublishInfo(topic string, info *internal.TopicPublishInfo) {
	if topic == "" || info == nil {
		return
	}
	p.publishInfo.Store(topic, info)
}

func (p *defaultProducer) IsPublishTopicNeedUpdate(topic string) bool {
	v, exist := p.publishInfo.Load(topic)
	if !exist {
		return true
	}
	info := v.(*internal.TopicPublishInfo)
	return len(info.MqList) == 0
}

func (p *defaultProducer) IsUnitMode() bool {
	return false
}

type transactionProducer struct {
	producer *defaultProducer
	listener primitive.TransactionListener
}

// TODO: checkLocalTransaction
func NewTransactionProducer(listener primitive.TransactionListener, opts ...Option) (*transactionProducer, error) {
	producer, err := NewDefaultProducer(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "NewDefaultProducer failed.")
	}
	return &transactionProducer{
		producer: producer,
		listener: listener,
	}, nil
}

func (tp *transactionProducer) Start() error {
	go primitive.WithRecover(func() {
		tp.checkTransactionState()
	})
	return tp.producer.Start()
}
func (tp *transactionProducer) Shutdown() error {
	return tp.producer.Shutdown()
}

// TODO: check addr
func (tp *transactionProducer) checkTransactionState() {
	for ch := range tp.producer.callbackCh {
		switch callback := ch.(type) {
		case *internal.CheckTransactionStateCallback:
			localTransactionState := tp.listener.CheckLocalTransaction(callback.Msg)
			uniqueKey := callback.Msg.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex)
			if uniqueKey == "" {
				uniqueKey = callback.Msg.MsgId
			}
			transactionId := callback.Msg.GetProperty(primitive.PropertyTransactionID)
			if transactionId == "" {
				transactionId = callback.Header.TransactionId
			}
			if transactionId == "" {
				transactionId = callback.Msg.TransactionId
			}
			header := &internal.EndTransactionRequestHeader{
				CommitLogOffset:      callback.Header.CommitLogOffset,
				ProducerGroup:        tp.producer.group,
				TranStateTableOffset: callback.Header.TranStateTableOffset,
				FromTransactionCheck: true,
				MsgID:                uniqueKey,
				TransactionId:        transactionId,
				CommitOrRollback:     tp.transactionState(localTransactionState),
			}

			req := remote.NewRemotingCommand(internal.ReqENDTransaction, header, nil)
			req.Remark = tp.errRemark(nil)

			err := tp.producer.client.InvokeOneWay(context.Background(), callback.Addr.String(), req,
				tp.producer.options.SendMsgTimeout)
			if err != nil {
				rlog.Error("send ReqENDTransaction to broker error", map[string]interface{}{
					"callback":               callback.Addr.String(),
					"request":                req.String(),
					rlog.LogKeyUnderlayError: err,
				})
			}
		default:
			rlog.Error(fmt.Sprintf("unknown type %v", ch), nil)
		}
	}
}

func (tp *transactionProducer) SendMessageInTransaction(ctx context.Context, msg *primitive.Message) (*primitive.TransactionSendResult, error) {
	msg.WithProperty(primitive.PropertyTransactionPrepared, "true")
	msg.WithProperty(primitive.PropertyProducerGroup, tp.producer.options.GroupName)

	rsp, err := tp.producer.SendSync(ctx, msg)
	if err != nil {
		return nil, err
	}
	localTransactionState := primitive.UnknowState
	switch rsp.Status {
	case primitive.SendOK:
		if len(rsp.TransactionID) > 0 {
			msg.WithProperty("__transactionId__", rsp.TransactionID)
		}
		transactionId := msg.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex)
		if len(transactionId) > 0 {
			msg.TransactionId = transactionId
		}
		localTransactionState = tp.listener.ExecuteLocalTransaction(msg)
		if localTransactionState != primitive.CommitMessageState {
			rlog.Error("executeLocalTransaction but state unexpected", map[string]interface{}{
				"localState": localTransactionState,
				"message":    msg,
			})
		}

	case primitive.SendFlushDiskTimeout, primitive.SendFlushSlaveTimeout, primitive.SendSlaveNotAvailable:
		localTransactionState = primitive.RollbackMessageState
	default:
	}

	tp.endTransaction(*rsp, err, localTransactionState)

	transactionSendResult := &primitive.TransactionSendResult{
		SendResult: rsp,
		State:      localTransactionState,
	}

	return transactionSendResult, nil
}

func (tp *transactionProducer) endTransaction(result primitive.SendResult, err error, state primitive.LocalTransactionState) error {
	var msgID *primitive.MessageID
	if len(result.OffsetMsgID) > 0 {
		msgID, _ = primitive.UnmarshalMsgID([]byte(result.OffsetMsgID))
	} else {
		msgID, _ = primitive.UnmarshalMsgID([]byte(result.MsgID))
	}
	// 估计没有反序列化回来
	brokerAddr := tp.producer.client.GetNameSrv().FindBrokerAddrByName(result.MessageQueue.BrokerName)
	requestHeader := &internal.EndTransactionRequestHeader{
		TransactionId:        result.TransactionID,
		CommitLogOffset:      msgID.Offset,
		ProducerGroup:        tp.producer.group,
		TranStateTableOffset: result.QueueOffset,
		MsgID:                result.MsgID,
		CommitOrRollback:     tp.transactionState(state),
	}

	req := remote.NewRemotingCommand(internal.ReqENDTransaction, requestHeader, nil)
	req.Remark = tp.errRemark(err)

	return tp.producer.client.InvokeOneWay(context.Background(), brokerAddr, req, tp.producer.options.SendMsgTimeout)
}

func (tp *transactionProducer) errRemark(err error) string {
	if err != nil {
		return "executeLocalTransactionBranch exception: " + err.Error()
	}
	return ""
}

func (tp *transactionProducer) transactionState(state primitive.LocalTransactionState) int {
	switch state {
	case primitive.CommitMessageState:
		return primitive.TransactionCommitType
	case primitive.RollbackMessageState:
		return primitive.TransactionRollbackType
	case primitive.UnknowState:
		return primitive.TransactionNotType
	default:
		return primitive.TransactionNotType
	}
}
