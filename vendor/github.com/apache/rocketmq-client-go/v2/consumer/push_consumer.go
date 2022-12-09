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
	"math"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	errors2 "github.com/apache/rocketmq-client-go/v2/errors"

	"github.com/pkg/errors"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

// In most scenarios, this is the mostly recommended usage to consume messages.
//
// Technically speaking, this push client is virtually a wrapper of the underlying pull service. Specifically, on
// arrival of messages pulled from brokers, it roughly invokes the registered callback handler to feed the messages.
//
// See quick start/Consumer in the example module for a typical usage.
//
// <strong>Thread Safety:</strong> After initialization, the instance can be regarded as thread-safe.

const (
	Mb = 1024 * 1024
)

type PushConsumerCallback struct {
	topic string
	f     func(context.Context, ...*primitive.MessageExt) (ConsumeResult, error)
}

func (callback PushConsumerCallback) UniqueID() string {
	return callback.topic
}

type pushConsumer struct {
	*defaultConsumer
	queueFlowControlTimes        int
	queueMaxSpanFlowControlTimes int
	consumeFunc                  utils.Set
	submitToConsume              func(*processQueue, *primitive.MessageQueue)
	subscribedTopic              map[string]string
	interceptor                  primitive.Interceptor
	queueLock                    *QueueLock
	done                         chan struct{}
	closeOnce                    sync.Once
}

func NewPushConsumer(opts ...Option) (*pushConsumer, error) {
	defaultOpts := defaultPushConsumerOptions()
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

	if defaultOpts.Namespace != "" {
		defaultOpts.GroupName = defaultOpts.Namespace + "%" + defaultOpts.GroupName
	}

	dc := &defaultConsumer{
		client:         internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil),
		consumerGroup:  defaultOpts.GroupName,
		cType:          _PushConsume,
		state:          int32(internal.StateCreateJust),
		prCh:           make(chan PullRequest, 4),
		model:          defaultOpts.ConsumerModel,
		consumeOrderly: defaultOpts.ConsumeOrderly,
		fromWhere:      defaultOpts.FromWhere,
		allocate:       defaultOpts.Strategy,
		option:         defaultOpts,
	}
	if dc.client == nil {
		return nil, fmt.Errorf("GetOrNewRocketMQClient faild")
	}
	defaultOpts.Namesrv = dc.client.GetNameSrv()

	p := &pushConsumer{
		defaultConsumer: dc,
		subscribedTopic: make(map[string]string, 0),
		queueLock:       newQueueLock(),
		done:            make(chan struct{}, 1),
		consumeFunc:     utils.NewSet(),
	}
	dc.mqChanged = p.messageQueueChanged
	if p.consumeOrderly {
		p.submitToConsume = p.consumeMessageOrderly
	} else {
		p.submitToConsume = p.consumeMessageCurrently
	}

	p.interceptor = primitive.ChainInterceptors(p.option.Interceptors...)

	return p, nil
}

func (pc *pushConsumer) Start() error {
	var err error
	pc.once.Do(func() {
		rlog.Info("the consumer start beginning", map[string]interface{}{
			rlog.LogKeyConsumerGroup: pc.consumerGroup,
			"messageModel":           pc.model,
			"unitMode":               pc.unitMode,
		})
		atomic.StoreInt32(&pc.state, int32(internal.StateStartFailed))
		pc.validate()

		err = pc.client.RegisterConsumer(pc.consumerGroup, pc)
		if err != nil {
			rlog.Error("the consumer group has been created, specify another one", map[string]interface{}{
				rlog.LogKeyConsumerGroup: pc.consumerGroup,
			})
			err = errors2.ErrCreated
			return
		}

		err = pc.defaultConsumer.start()
		if err != nil {
			return
		}

		go func() {
			// todo start clean msg expired
			for {
				select {
				case pr := <-pc.prCh:
					go func() {
						pc.pullMessage(&pr)
					}()
				case <-pc.done:
					rlog.Info("push consumer close pullConsumer listener.", map[string]interface{}{
						rlog.LogKeyConsumerGroup: pc.consumerGroup,
					})
					return
				}
			}
		}()

		go primitive.WithRecover(func() {
			// initial lock.
			if !pc.consumeOrderly {
				return
			}

			time.Sleep(1000 * time.Millisecond)
			pc.lockAll()

			lockTicker := time.NewTicker(pc.option.RebalanceLockInterval)
			defer lockTicker.Stop()
			for {
				select {
				case <-lockTicker.C:
					pc.lockAll()
				case <-pc.done:
					rlog.Info("push consumer close tick.", map[string]interface{}{
						rlog.LogKeyConsumerGroup: pc.consumerGroup,
					})
					return
				}
			}
		})
	})

	if err != nil {
		return err
	}

	pc.client.UpdateTopicRouteInfo()
	for k := range pc.subscribedTopic {
		_, exist := pc.topicSubscribeInfoTable.Load(k)
		if !exist {
			pc.client.Shutdown()
			return fmt.Errorf("the topic=%s route info not found, it may not exist", k)
		}
	}
	pc.client.CheckClientInBroker()
	pc.client.SendHeartbeatToAllBrokerWithLock()
	pc.client.RebalanceImmediately()

	return err
}

func (pc *pushConsumer) Shutdown() error {
	var err error
	pc.closeOnce.Do(func() {
		close(pc.done)

		pc.client.UnregisterConsumer(pc.consumerGroup)
		err = pc.defaultConsumer.shutdown()
	})

	return err
}

func (pc *pushConsumer) Subscribe(topic string, selector MessageSelector,
	f func(context.Context, ...*primitive.MessageExt) (ConsumeResult, error)) error {
	if atomic.LoadInt32(&pc.state) == int32(internal.StateStartFailed) ||
		atomic.LoadInt32(&pc.state) == int32(internal.StateShutdown) {
		return errors2.ErrStartTopic
	}

	if pc.option.Namespace != "" {
		topic = pc.option.Namespace + "%" + topic
	}
	data := buildSubscriptionData(topic, selector)
	pc.subscriptionDataTable.Store(topic, data)
	pc.subscribedTopic[topic] = ""

	pc.consumeFunc.Add(&PushConsumerCallback{
		f:     f,
		topic: topic,
	})
	return nil
}

func (pc *pushConsumer) Unsubscribe(topic string) error {
	if pc.option.Namespace != "" {
		topic = pc.option.Namespace + "%" + topic
	}
	pc.subscriptionDataTable.Delete(topic)
	return nil
}

func (pc *pushConsumer) Suspend() {
	pc.suspend()
}

func (pc *pushConsumer) Resume() {
	pc.resume()
}

func (pc *pushConsumer) Rebalance() {
	pc.defaultConsumer.doBalance()
}

func (pc *pushConsumer) RebalanceIfNotPaused() {
	pc.defaultConsumer.doBalanceIfNotPaused()
}

func (pc *pushConsumer) PersistConsumerOffset() error {
	return pc.defaultConsumer.persistConsumerOffset()
}

func (pc *pushConsumer) UpdateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue) {
	pc.defaultConsumer.updateTopicSubscribeInfo(topic, mqs)
}

func (pc *pushConsumer) IsSubscribeTopicNeedUpdate(topic string) bool {
	return pc.defaultConsumer.isSubscribeTopicNeedUpdate(topic)
}

func (pc *pushConsumer) SubscriptionDataList() []*internal.SubscriptionData {
	return pc.defaultConsumer.SubscriptionDataList()
}

func (pc *pushConsumer) IsUnitMode() bool {
	return pc.unitMode
}

func (pc *pushConsumer) GetcType() string {
	return string(pc.cType)
}

func (pc *pushConsumer) GetModel() string {
	return pc.model.String()
}

func (pc *pushConsumer) GetWhere() string {
	switch pc.fromWhere {
	case ConsumeFromLastOffset:
		return "CONSUME_FROM_LAST_OFFSET"
	case ConsumeFromFirstOffset:
		return "CONSUME_FROM_FIRST_OFFSET"
	case ConsumeFromTimestamp:
		return "CONSUME_FROM_TIMESTAMP"
	default:
		return "UNKOWN"
	}

}

func (pc *pushConsumer) ConsumeMessageDirectly(msg *primitive.MessageExt, brokerName string) *internal.ConsumeMessageDirectlyResult {
	var msgs = []*primitive.MessageExt{msg}
	var mq = &primitive.MessageQueue{
		Topic:      msg.Topic,
		BrokerName: brokerName,
		QueueId:    msg.Queue.QueueId,
	}

	beginTime := time.Now()
	pc.resetRetryAndNamespace(msgs)
	var result ConsumeResult

	var err error
	msgCtx := &primitive.ConsumeMessageContext{
		Properties:    make(map[string]string),
		ConsumerGroup: pc.consumerGroup,
		MQ:            mq,
		Msgs:          msgs,
	}
	ctx := context.Background()
	ctx = primitive.WithConsumerCtx(ctx, msgCtx)
	ctx = primitive.WithMethod(ctx, primitive.ConsumerPush)
	concurrentCtx := primitive.NewConsumeConcurrentlyContext()
	concurrentCtx.MQ = *mq
	ctx = primitive.WithConcurrentlyCtx(ctx, concurrentCtx)

	result, err = pc.consumeInner(ctx, msgs)

	consumeRT := time.Now().Sub(beginTime)

	res := &internal.ConsumeMessageDirectlyResult{
		Order:          false,
		AutoCommit:     true,
		SpentTimeMills: int64(consumeRT / time.Millisecond),
	}

	if err != nil {
		msgCtx.Properties[primitive.PropCtxType] = string(primitive.ExceptionReturn)
		res.ConsumeResult = internal.ThrowException
		res.Remark = err.Error()
	} else if result == ConsumeSuccess {
		msgCtx.Properties[primitive.PropCtxType] = string(primitive.SuccessReturn)
		res.ConsumeResult = internal.ConsumeSuccess
	} else if result == ConsumeRetryLater {
		msgCtx.Properties[primitive.PropCtxType] = string(primitive.FailedReturn)
		res.ConsumeResult = internal.ConsumeRetryLater
	}

	pc.stat.increaseConsumeRT(pc.consumerGroup, mq.Topic, int64(consumeRT/time.Millisecond))

	return res
}

func (pc *pushConsumer) GetConsumerRunningInfo(stack bool) *internal.ConsumerRunningInfo {
	info := internal.NewConsumerRunningInfo()

	pc.subscriptionDataTable.Range(func(key, value interface{}) bool {
		topic := key.(string)
		info.SubscriptionData[value.(*internal.SubscriptionData)] = true
		status := internal.ConsumeStatus{
			PullRT:            pc.stat.getPullRT(pc.consumerGroup, topic).avgpt,
			PullTPS:           pc.stat.getPullTPS(pc.consumerGroup, topic).tps,
			ConsumeRT:         pc.stat.getConsumeRT(pc.consumerGroup, topic).avgpt,
			ConsumeOKTPS:      pc.stat.getConsumeOKTPS(pc.consumerGroup, topic).tps,
			ConsumeFailedTPS:  pc.stat.getConsumeFailedTPS(pc.consumerGroup, topic).tps,
			ConsumeFailedMsgs: pc.stat.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + pc.consumerGroup).sum,
		}
		info.StatusTable[topic] = status
		return true
	})

	pc.processQueueTable.Range(func(key, value interface{}) bool {
		mq := key.(primitive.MessageQueue)
		pq := value.(*processQueue)
		pInfo := pq.currentInfo()
		pInfo.CommitOffset, _ = pc.storage.readWithException(&mq, _ReadMemoryThenStore)
		info.MQTable[mq] = pInfo
		return true
	})

	if stack {
		var buffer strings.Builder

		err := pprof.Lookup("goroutine").WriteTo(&buffer, 2)
		if err != nil {
			rlog.Error("error when get stack ", map[string]interface{}{
				"error": err,
			})
		} else {
			info.JStack = buffer.String()
		}
	}

	nsAddr := ""
	for _, value := range pc.client.GetNameSrv().AddrList() {
		nsAddr += fmt.Sprintf("%s;", value)
	}
	info.Properties[internal.PropNameServerAddr] = nsAddr
	info.Properties[internal.PropConsumeType] = string(pc.cType)
	info.Properties[internal.PropConsumeOrderly] = strconv.FormatBool(pc.consumeOrderly)
	info.Properties[internal.PropThreadPoolCoreSize] = "-1"
	info.Properties[internal.PropConsumerStartTimestamp] = strconv.FormatInt(pc.consumerStartTimestamp, 10)
	return info
}

func (pc *pushConsumer) messageQueueChanged(topic string, mqAll, mqDivided []*primitive.MessageQueue) {
	v, exit := pc.subscriptionDataTable.Load(topic)
	if !exit {
		return
	}
	data := v.(*internal.SubscriptionData)
	newVersion := time.Now().UnixNano()
	rlog.Info("the MessageQueue changed, version also updated", map[string]interface{}{
		rlog.LogKeyValueChangedFrom: data.SubVersion,
		rlog.LogKeyValueChangedTo:   newVersion,
	})
	data.SubVersion = newVersion

	// TODO: optimize
	count := 0
	pc.processQueueTable.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count > 0 {
		if pc.option.PullThresholdForTopic != -1 {
			newVal := pc.option.PullThresholdForTopic / count
			if newVal == 0 {
				newVal = 1
			}
			rlog.Info("The PullThresholdForTopic is changed", map[string]interface{}{
				rlog.LogKeyValueChangedFrom: pc.option.PullThresholdForTopic,
				rlog.LogKeyValueChangedTo:   newVal,
			})
			pc.option.PullThresholdForTopic = newVal
		}

		if pc.option.PullThresholdSizeForTopic != -1 {
			newVal := pc.option.PullThresholdSizeForTopic / count
			if newVal == 0 {
				newVal = 1
			}
			rlog.Info("The PullThresholdSizeForTopic is changed", map[string]interface{}{
				rlog.LogKeyValueChangedFrom: pc.option.PullThresholdSizeForTopic,
				rlog.LogKeyValueChangedTo:   newVal,
			})
			pc.option.PullThresholdSizeForTopic = newVal
		}
	}
	pc.client.SendHeartbeatToAllBrokerWithLock()
}

func (pc *pushConsumer) validate() {
	internal.ValidateGroup(pc.consumerGroup)

	if pc.consumerGroup == internal.DefaultConsumerGroup {
		// TODO FQA
		rlog.Error(fmt.Sprintf("consumerGroup can't equal [%s], please specify another one.", internal.DefaultConsumerGroup), nil)
	}

	if len(pc.subscribedTopic) == 0 {
		rlog.Error("number of subscribed topics is 0.", nil)
	}

	if pc.option.ConsumeConcurrentlyMaxSpan < 1 || pc.option.ConsumeConcurrentlyMaxSpan > 65535 {
		if pc.option.ConsumeConcurrentlyMaxSpan == 0 {
			pc.option.ConsumeConcurrentlyMaxSpan = 1000
		} else {
			rlog.Error("option.ConsumeConcurrentlyMaxSpan out of range [1, 65535]", nil)
		}
	}

	if pc.option.PullThresholdForQueue < 1 || pc.option.PullThresholdForQueue > 65535 {
		if pc.option.PullThresholdForQueue == 0 {
			pc.option.PullThresholdForQueue = 1024
		} else {
			rlog.Error("option.PullThresholdForQueue out of range [1, 65535]", nil)
		}
	}

	if pc.option.PullThresholdForTopic < 1 || pc.option.PullThresholdForTopic > 6553500 {
		if pc.option.PullThresholdForTopic == 0 {
			pc.option.PullThresholdForTopic = 102400
		} else {
			rlog.Error("option.PullThresholdForTopic out of range [1, 6553500]", nil)
		}
	}

	if pc.option.PullThresholdSizeForQueue < 1 || pc.option.PullThresholdSizeForQueue > 1024 {
		if pc.option.PullThresholdSizeForQueue == 0 {
			pc.option.PullThresholdSizeForQueue = 512
		} else {
			rlog.Error("option.PullThresholdSizeForQueue out of range [1, 1024]", nil)
		}
	}

	if pc.option.PullThresholdSizeForTopic < 1 || pc.option.PullThresholdSizeForTopic > 102400 {
		if pc.option.PullThresholdSizeForTopic == 0 {
			pc.option.PullThresholdSizeForTopic = 51200
		} else {
			rlog.Error("option.PullThresholdSizeForTopic out of range [1, 102400]", nil)
		}
	}

	if pc.option.PullInterval < 0 || pc.option.PullInterval > 65535*time.Millisecond {
		rlog.Error("option.PullInterval out of range [0, 65535]", nil)
	}

	if pc.option.ConsumeMessageBatchMaxSize < 1 || pc.option.ConsumeMessageBatchMaxSize > 1024 {
		if pc.option.ConsumeMessageBatchMaxSize == 0 {
			pc.option.ConsumeMessageBatchMaxSize = 1
		} else {
			rlog.Error("option.ConsumeMessageBatchMaxSize out of range [1, 1024]", nil)
		}
	}

	if pc.option.PullBatchSize < 1 || pc.option.PullBatchSize > 1024 {
		if pc.option.PullBatchSize == 0 {
			pc.option.PullBatchSize = 32
		} else {
			rlog.Error("option.PullBatchSize out of range [1, 1024]", nil)
		}
	}
}

func (pc *pushConsumer) pullMessage(request *PullRequest) {
	rlog.Debug("start a new Pull Message task for PullRequest", map[string]interface{}{
		rlog.LogKeyPullRequest: request.String(),
	})
	var sleepTime time.Duration
	pq := request.pq
	go primitive.WithRecover(func() {
		for {
			select {
			case <-pc.done:
				rlog.Info("push consumer close pullMessage.", map[string]interface{}{
					rlog.LogKeyConsumerGroup: pc.consumerGroup,
				})
				return
			default:
				pc.submitToConsume(request.pq, request.mq)
				if request.pq.IsDroppd() {
					rlog.Info("push consumer quit pullMessage for dropped queue.", map[string]interface{}{
						rlog.LogKeyConsumerGroup: pc.consumerGroup,
					})
					return
				}
			}
		}
	})

	for {
	NEXT:
		select {
		case <-pc.done:
			rlog.Info("push consumer close message handle.", map[string]interface{}{
				rlog.LogKeyConsumerGroup: pc.consumerGroup,
			})
			return
		default:
		}

		if pq.IsDroppd() {
			rlog.Debug("the request was dropped, so stop task", map[string]interface{}{
				rlog.LogKeyPullRequest: request.String(),
			})
			return
		}
		if sleepTime > 0 {
			rlog.Debug(fmt.Sprintf("pull MessageQueue: %d sleep %d ms for mq: %v", request.mq.QueueId, sleepTime/time.Millisecond, request.mq), nil)
			time.Sleep(sleepTime)
		}
		// reset time
		sleepTime = pc.option.PullInterval
		pq.lastPullTime.Store(time.Now())
		err := pc.makeSureStateOK()
		if err != nil {
			rlog.Warning("consumer state error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err.Error(),
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		if pc.pause {
			rlog.Debug(fmt.Sprintf("consumer [%s] of [%s] was paused, execute pull request [%s] later",
				pc.option.InstanceName, pc.consumerGroup, request.String()), nil)
			sleepTime = _PullDelayTimeWhenSuspend
			goto NEXT
		}

		cachedMessageSizeInMiB := int(pq.cachedMsgSize.Load() / Mb)
		if pq.cachedMsgCount.Load() > pc.option.PullThresholdForQueue {
			if pc.queueFlowControlTimes%1000 == 0 {
				rlog.Warning("the cached message count exceeds the threshold, so do flow control", map[string]interface{}{
					"PullThresholdForQueue": pc.option.PullThresholdForQueue,
					"minOffset":             pq.Min(),
					"maxOffset":             pq.Max(),
					"count":                 pq.cachedMsgCount,
					"size(MiB)":             cachedMessageSizeInMiB,
					"flowControlTimes":      pc.queueFlowControlTimes,
					rlog.LogKeyPullRequest:  request.String(),
				})
			}
			pc.queueFlowControlTimes++
			sleepTime = _PullDelayTimeWhenFlowControl
			goto NEXT
		}

		if cachedMessageSizeInMiB > pc.option.PullThresholdSizeForQueue {
			if pc.queueFlowControlTimes%1000 == 0 {
				rlog.Warning("the cached message size exceeds the threshold, so do flow control", map[string]interface{}{
					"PullThresholdSizeForQueue": pc.option.PullThresholdSizeForQueue,
					"minOffset":                 pq.Min(),
					"maxOffset":                 pq.Max(),
					"count":                     pq.cachedMsgCount,
					"size(MiB)":                 cachedMessageSizeInMiB,
					"flowControlTimes":          pc.queueFlowControlTimes,
					rlog.LogKeyPullRequest:      request.String(),
				})
			}
			pc.queueFlowControlTimes++
			sleepTime = _PullDelayTimeWhenFlowControl
			goto NEXT
		}

		if !pc.consumeOrderly {
			if pq.getMaxSpan() > pc.option.ConsumeConcurrentlyMaxSpan {
				if pc.queueMaxSpanFlowControlTimes%1000 == 0 {
					rlog.Warning("the queue's messages span too long, so do flow control", map[string]interface{}{
						"ConsumeConcurrentlyMaxSpan": pc.option.ConsumeConcurrentlyMaxSpan,
						"minOffset":                  pq.Min(),
						"maxOffset":                  pq.Max(),
						"maxSpan":                    pq.getMaxSpan(),
						"flowControlTimes":           pc.queueFlowControlTimes,
						rlog.LogKeyPullRequest:       request.String(),
					})
				}
				pc.queueMaxSpanFlowControlTimes++
				sleepTime = _PullDelayTimeWhenFlowControl
				goto NEXT
			}
		} else {
			if pq.IsLock() {
				if !request.lockedFirst {
					offset, err := pc.computePullFromWhereWithException(request.mq)
					if err != nil {
						rlog.Warning("computePullFromWhere from broker error", map[string]interface{}{
							rlog.LogKeyUnderlayError: err.Error(),
						})
						sleepTime = _PullDelayTimeWhenError
						goto NEXT
					}

					brokerBusy := offset < request.nextOffset
					rlog.Info("the first time to pull message, so fix offset from broker, offset maybe changed", map[string]interface{}{
						rlog.LogKeyPullRequest:      request.String(),
						rlog.LogKeyValueChangedFrom: request.nextOffset,
						rlog.LogKeyValueChangedTo:   offset,
						"brokerBusy":                brokerBusy,
					})
					if brokerBusy {
						rlog.Info("[NOTIFY_ME] the first time to pull message, but pull request offset larger than "+
							"broker consume offset", map[string]interface{}{"offset": offset})
					}
					request.lockedFirst = true
					request.nextOffset = offset
				}
			} else {
				rlog.Info("pull message later because not locked in broker", map[string]interface{}{
					rlog.LogKeyPullRequest: request.String(),
				})
				sleepTime = _PullDelayTimeWhenError
				goto NEXT
			}
		}

		v, exist := pc.subscriptionDataTable.Load(request.mq.Topic)
		if !exist {
			rlog.Info("find the consumer's subscription failed", map[string]interface{}{
				rlog.LogKeyPullRequest: request.String(),
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}
		beginTime := time.Now()
		var (
			commitOffsetEnable bool
			commitOffsetValue  int64
			subExpression      string
		)

		if pc.model == Clustering {
			commitOffsetValue, _ = pc.storage.readWithException(request.mq, _ReadFromMemory)
			if commitOffsetValue > 0 {
				commitOffsetEnable = true
			}
		}

		sd := v.(*internal.SubscriptionData)
		classFilter := sd.ClassFilterMode
		if pc.option.PostSubscriptionWhenPull && !classFilter {
			subExpression = sd.SubString
		}

		sysFlag := buildSysFlag(commitOffsetEnable, true, subExpression != "", classFilter)

		pullRequest := &internal.PullMessageRequestHeader{
			ConsumerGroup:        pc.consumerGroup,
			Topic:                request.mq.Topic,
			QueueId:              int32(request.mq.QueueId),
			QueueOffset:          request.nextOffset,
			MaxMsgNums:           pc.option.PullBatchSize,
			SysFlag:              sysFlag,
			CommitOffset:         commitOffsetValue,
			SubExpression:        subExpression,
			ExpressionType:       string(TAG),
			SuspendTimeoutMillis: 20 * time.Second,
		}
		//
		//if data.ExpType == string(TAG) {
		//	pullRequest.SubVersion = 0
		//} else {
		//	pullRequest.SubVersion = data.SubVersion
		//}

		brokerResult := pc.defaultConsumer.tryFindBroker(request.mq)
		if brokerResult == nil {
			rlog.Warning("no broker found for mq", map[string]interface{}{
				rlog.LogKeyPullRequest: request.mq.String(),
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		if brokerResult.Slave {
			pullRequest.SysFlag = clearCommitOffsetFlag(pullRequest.SysFlag)
		}

		result, err := pc.client.PullMessage(context.Background(), brokerResult.BrokerAddr, pullRequest)
		if err != nil {
			rlog.Warning("pull message from broker error", map[string]interface{}{
				rlog.LogKeyBroker:        brokerResult.BrokerAddr,
				rlog.LogKeyUnderlayError: err.Error(),
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		if result.Status == primitive.PullBrokerTimeout {
			rlog.Warning("pull broker timeout", map[string]interface{}{
				rlog.LogKeyBroker: brokerResult.BrokerAddr,
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		pc.processPullResult(request.mq, result, sd)

		switch result.Status {
		case primitive.PullFound:
			rlog.Debug(fmt.Sprintf("Topic: %s, QueueId: %d found messages.", request.mq.Topic, request.mq.QueueId), nil)
			prevRequestOffset := request.nextOffset
			request.nextOffset = result.NextBeginOffset

			rt := time.Now().Sub(beginTime) / time.Millisecond
			pc.stat.increasePullRT(pc.consumerGroup, request.mq.Topic, int64(rt))

			msgFounded := result.GetMessageExts()
			firstMsgOffset := int64(math.MaxInt64)
			if len(msgFounded) != 0 {
				firstMsgOffset = msgFounded[0].QueueOffset
				pc.stat.increasePullTPS(pc.consumerGroup, request.mq.Topic, len(msgFounded))
				pq.putMessage(msgFounded...)
			}
			if result.NextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset {
				rlog.Warning("[BUG] pull message result maybe data wrong", map[string]interface{}{
					"nextBeginOffset":   result.NextBeginOffset,
					"firstMsgOffset":    firstMsgOffset,
					"prevRequestOffset": prevRequestOffset,
				})
			}
		case primitive.PullNoNewMsg, primitive.PullNoMsgMatched:
			request.nextOffset = result.NextBeginOffset
			pc.correctTagsOffset(request)
		case primitive.PullOffsetIllegal:
			rlog.Warning("the pull request offset illegal", map[string]interface{}{
				rlog.LogKeyPullRequest: request.String(),
				"result":               result.String(),
			})
			request.nextOffset = result.NextBeginOffset
			pq.WithDropped(true)
			time.Sleep(10 * time.Second)
			pc.storage.update(request.mq, request.nextOffset, false)
			pc.storage.persist([]*primitive.MessageQueue{request.mq})
			pc.processQueueTable.Delete(*request.mq)
			rlog.Warning(fmt.Sprintf("fix the pull request offset: %s", request.String()), nil)
		default:
			rlog.Warning(fmt.Sprintf("unknown pull status: %v", result.Status), nil)
			sleepTime = _PullDelayTimeWhenError
		}
	}
}

func (pc *pushConsumer) correctTagsOffset(pr *PullRequest) {
	if pr.pq.cachedMsgCount.Load() <= 0 {
		pc.storage.update(pr.mq, pr.nextOffset, true)
	}
}

func (pc *pushConsumer) sendMessageBack(brokerName string, msg *primitive.MessageExt, delayLevel int) bool {
	var brokerAddr string
	if len(brokerName) != 0 {
		brokerAddr = pc.defaultConsumer.client.GetNameSrv().FindBrokerAddrByName(brokerName)
	} else {
		brokerAddr = msg.StoreHost
	}
	_, err := pc.client.InvokeSync(context.Background(), brokerAddr, pc.buildSendBackRequest(msg, delayLevel), 3*time.Second)
	if err != nil {
		return false
	}
	return true
}

func (pc *pushConsumer) buildSendBackRequest(msg *primitive.MessageExt, delayLevel int) *remote.RemotingCommand {
	req := &internal.ConsumerSendMsgBackRequestHeader{
		Group:             pc.consumerGroup,
		OriginTopic:       msg.Topic,
		Offset:            msg.CommitLogOffset,
		DelayLevel:        delayLevel,
		OriginMsgId:       msg.MsgId,
		MaxReconsumeTimes: pc.getMaxReconsumeTimes(),
	}

	return remote.NewRemotingCommand(internal.ReqConsumerSendMsgBack, req, nil)
}

func (pc *pushConsumer) suspend() {
	pc.pause = true
	rlog.Info(fmt.Sprintf("suspend consumer: %s", pc.consumerGroup), nil)
}

func (pc *pushConsumer) resume() {
	pc.pause = false
	pc.doBalance()
	rlog.Info(fmt.Sprintf("resume consumer: %s", pc.consumerGroup), nil)
}

func (pc *pushConsumer) ResetOffset(topic string, table map[primitive.MessageQueue]int64) {
	//topic := cmd.ExtFields["topic"]
	//group := cmd.ExtFields["group"]
	//if topic == "" || group == "" {
	//	rlog.Warning("received reset offset command from: %s, but missing params.", from)
	//	return
	//}
	//t, err := strconv.ParseInt(cmd.ExtFields["timestamp"], 10, 64)
	//if err != nil {
	//	rlog.Warning("received reset offset command from: %s, but parse time error: %s", err.Error())
	//	return
	//}
	//rlog.Infof("invoke reset offset operation from broker. brokerAddr=%s, topic=%s, group=%s, timestamp=%v",
	//	from, topic, group, t)
	//
	//offsetTable := make(map[MessageQueue]int64, 0)
	//err = json.Unmarshal(cmd.Body, &offsetTable)
	//if err != nil {
	//	rlog.Warning("received reset offset command from: %s, but parse offset table: %s", err.Error())
	//	return
	//}
	//v, exist := c.consumerMap.Load(group)
	//if !exist {
	//	rlog.Infof("[reset-offset] consumer dose not exist. group=%s", group)
	//	return
	//}
	pc.suspend()
	defer pc.resume()

	mqs := make([]*primitive.MessageQueue, 0)
	copyPc := sync.Map{}
	pc.processQueueTable.Range(func(key, value interface{}) bool {
		mq := key.(primitive.MessageQueue)
		pq := value.(*processQueue)
		if _, ok := table[mq]; ok && mq.Topic == topic {
			pq.WithDropped(true)
			pq.clear()
		}
		mqs = append(mqs, &mq)
		copyPc.Store(&mq, pq)
		return true
	})
	time.Sleep(10 * time.Second)
	for _, mq := range mqs {
		if _, ok := table[*mq]; ok {
			pc.storage.update(mq, table[*mq], false)
			v, exist := copyPc.Load(mq)
			if !exist {
				continue
			}
			pq := v.(*processQueue)
			pc.removeUnnecessaryMessageQueue(mq, pq)
			pc.processQueueTable.Delete(mq)
		}
	}
}

func (pc *pushConsumer) removeUnnecessaryMessageQueue(mq *primitive.MessageQueue, pq *processQueue) bool {
	pc.defaultConsumer.removeUnnecessaryMessageQueue(mq, pq)
	if !pc.consumeOrderly || Clustering != pc.model {
		return true
	}
	// TODO orderly
	return true
}

func (pc *pushConsumer) consumeInner(ctx context.Context, subMsgs []*primitive.MessageExt) (ConsumeResult, error) {
	if len(subMsgs) == 0 {
		return ConsumeRetryLater, errors.New("msg list empty")
	}

	f, exist := pc.consumeFunc.Contains(subMsgs[0].Topic)

	// fix lost retry message
	if !exist && strings.HasPrefix(subMsgs[0].Topic, internal.RetryGroupTopicPrefix) {
		f, exist = pc.consumeFunc.Contains(subMsgs[0].GetProperty(primitive.PropertyRetryTopic))
	}

	if !exist {
		return ConsumeRetryLater, fmt.Errorf("the consume callback missing for topic: %s", subMsgs[0].Topic)
	}

	callback, ok := f.(*PushConsumerCallback)
	if !ok {
		return ConsumeRetryLater, fmt.Errorf("the consume callback assert failed for topic: %s", subMsgs[0].Topic)
	}
	if pc.interceptor == nil {
		return callback.f(ctx, subMsgs...)
	} else {
		var container ConsumeResultHolder
		err := pc.interceptor(ctx, subMsgs, &container, func(ctx context.Context, req, reply interface{}) error {
			msgs := req.([]*primitive.MessageExt)
			r, e := callback.f(ctx, msgs...)

			realReply := reply.(*ConsumeResultHolder)
			realReply.ConsumeResult = r

			msgCtx, _ := primitive.GetConsumerCtx(ctx)
			msgCtx.Success = realReply.ConsumeResult == ConsumeSuccess
			if realReply.ConsumeResult == ConsumeSuccess {
				msgCtx.Properties[primitive.PropCtxType] = string(primitive.SuccessReturn)
			} else {
				msgCtx.Properties[primitive.PropCtxType] = string(primitive.FailedReturn)
			}
			return e
		})
		return container.ConsumeResult, err
	}
}

// resetRetryAndNamespace modify retry message.
func (pc *pushConsumer) resetRetryAndNamespace(subMsgs []*primitive.MessageExt) {
	groupTopic := internal.RetryGroupTopicPrefix + pc.consumerGroup
	beginTime := time.Now()
	for idx := range subMsgs {
		msg := subMsgs[idx]
		retryTopic := msg.GetProperty(primitive.PropertyRetryTopic)
		if retryTopic == "" && groupTopic == msg.Topic {
			msg.Topic = retryTopic
		}
		subMsgs[idx].WithProperty(primitive.PropertyConsumeStartTime, strconv.FormatInt(
			beginTime.UnixNano()/int64(time.Millisecond), 10))
	}
}

func (pc *pushConsumer) consumeMessageCurrently(pq *processQueue, mq *primitive.MessageQueue) {
	msgs := pq.getMessages()
	if msgs == nil {
		return
	}
	for count := 0; count < len(msgs); count++ {
		var subMsgs []*primitive.MessageExt
		if count+pc.option.ConsumeMessageBatchMaxSize > len(msgs) {
			subMsgs = msgs[count:]
			count = len(msgs)
		} else {
			next := count + pc.option.ConsumeMessageBatchMaxSize
			subMsgs = msgs[count:next]
			count = next - 1
		}
		go primitive.WithRecover(func() {
		RETRY:
			if pq.IsDroppd() {
				rlog.Info("the message queue not be able to consume, because it was dropped", map[string]interface{}{
					rlog.LogKeyMessageQueue:  mq.String(),
					rlog.LogKeyConsumerGroup: pc.consumerGroup,
				})
				return
			}

			beginTime := time.Now()
			pc.resetRetryAndNamespace(subMsgs)
			var result ConsumeResult

			var err error
			msgCtx := &primitive.ConsumeMessageContext{
				Properties:    make(map[string]string),
				ConsumerGroup: pc.consumerGroup,
				MQ:            mq,
				Msgs:          subMsgs,
			}
			ctx := context.Background()
			ctx = primitive.WithConsumerCtx(ctx, msgCtx)
			ctx = primitive.WithMethod(ctx, primitive.ConsumerPush)
			concurrentCtx := primitive.NewConsumeConcurrentlyContext()
			concurrentCtx.MQ = *mq
			ctx = primitive.WithConcurrentlyCtx(ctx, concurrentCtx)

			result, err = pc.consumeInner(ctx, subMsgs)

			consumeRT := time.Now().Sub(beginTime)
			if err != nil {
				msgCtx.Properties[primitive.PropCtxType] = string(primitive.ExceptionReturn)
			} else if consumeRT >= pc.option.ConsumeTimeout {
				msgCtx.Properties[primitive.PropCtxType] = string(primitive.TimeoutReturn)
			} else if result == ConsumeSuccess {
				msgCtx.Properties[primitive.PropCtxType] = string(primitive.SuccessReturn)
			} else if result == ConsumeRetryLater {
				msgCtx.Properties[primitive.PropCtxType] = string(primitive.FailedReturn)
			}

			pc.stat.increaseConsumeRT(pc.consumerGroup, mq.Topic, int64(consumeRT/time.Millisecond))

			if !pq.IsDroppd() {
				msgBackFailed := make([]*primitive.MessageExt, 0)
				msgBackSucceed := make([]*primitive.MessageExt, 0)
				if result == ConsumeSuccess {
					pc.stat.increaseConsumeOKTPS(pc.consumerGroup, mq.Topic, len(subMsgs))
					msgBackSucceed = subMsgs
				} else {
					pc.stat.increaseConsumeFailedTPS(pc.consumerGroup, mq.Topic, len(subMsgs))
					if pc.model == BroadCasting {
						for i := 0; i < len(subMsgs); i++ {
							rlog.Warning("BROADCASTING, the message consume failed, drop it", map[string]interface{}{
								"message": subMsgs[i],
							})
						}
					} else {
						for i := 0; i < len(subMsgs); i++ {
							msg := subMsgs[i]
							if pc.sendMessageBack(mq.BrokerName, msg, concurrentCtx.DelayLevelWhenNextConsume) {
								msgBackSucceed = append(msgBackSucceed, msg)
							} else {
								msg.ReconsumeTimes += 1
								msgBackFailed = append(msgBackFailed, msg)
							}
						}
					}
				}

				offset := pq.removeMessage(msgBackSucceed...)

				if offset >= 0 && !pq.IsDroppd() {
					pc.storage.update(mq, int64(offset), true)
				}
				if len(msgBackFailed) > 0 {
					subMsgs = msgBackFailed
					time.Sleep(5 * time.Second)
					goto RETRY
				}
			} else {
				rlog.Warning("processQueue is dropped without process consume result.", map[string]interface{}{
					rlog.LogKeyMessageQueue: mq,
					"message":               subMsgs,
				})
			}
		})
	}
}

func (pc *pushConsumer) consumeMessageOrderly(pq *processQueue, mq *primitive.MessageQueue) {
	if pq.IsDroppd() {
		rlog.Warning("the message queue not be able to consume, because it's dropped.", map[string]interface{}{
			rlog.LogKeyMessageQueue: mq.String(),
		})
		return
	}

	lock := pc.queueLock.fetchLock(*mq)
	lock.Lock()
	defer lock.Unlock()
	if pc.model == BroadCasting || (pq.IsLock() && !pq.isLockExpired()) {
		beginTime := time.Now()

		continueConsume := true
		for continueConsume {
			if pq.IsDroppd() {
				rlog.Warning("the message queue not be able to consume, because it's dropped.", map[string]interface{}{
					rlog.LogKeyMessageQueue: mq.String(),
				})
				break
			}
			if pc.model == Clustering {
				if !pq.IsLock() {
					rlog.Warning("the message queue not locked, so consume later", map[string]interface{}{
						rlog.LogKeyMessageQueue: mq.String(),
					})
					pc.tryLockLaterAndReconsume(mq, 10)
					return
				}
				if pq.isLockExpired() {
					rlog.Warning("the message queue lock expired, so consume later", map[string]interface{}{
						rlog.LogKeyMessageQueue: mq.String(),
					})
					pc.tryLockLaterAndReconsume(mq, 10)
					return
				}
			}
			interval := time.Now().Sub(beginTime)
			if interval > pc.option.MaxTimeConsumeContinuously {
				time.Sleep(10 * time.Millisecond)
				return
			}
			batchSize := pc.option.ConsumeMessageBatchMaxSize
			msgs := pq.takeMessages(batchSize)

			pc.resetRetryAndNamespace(msgs)

			if len(msgs) == 0 {
				continueConsume = false
				break
			}

			// TODO: add message consumer hook
			beginTime = time.Now()

			ctx := context.Background()
			msgCtx := &primitive.ConsumeMessageContext{
				Properties:    make(map[string]string),
				ConsumerGroup: pc.consumerGroup,
				MQ:            mq,
				Msgs:          msgs,
			}
			ctx = primitive.WithConsumerCtx(ctx, msgCtx)
			ctx = primitive.WithMethod(ctx, primitive.ConsumerPush)

			orderlyCtx := primitive.NewConsumeOrderlyContext()
			orderlyCtx.MQ = *mq
			ctx = primitive.WithOrderlyCtx(ctx, orderlyCtx)

			pq.lockConsume.Lock()
			result, _ := pc.consumeInner(ctx, msgs)
			pq.lockConsume.Unlock()

			if result == Rollback || result == SuspendCurrentQueueAMoment {
				rlog.Warning("consumeMessage Orderly return not OK", map[string]interface{}{
					rlog.LogKeyConsumerGroup: pc.consumerGroup,
					"messages":               msgs,
					rlog.LogKeyMessageQueue:  mq,
				})
			}

			// just put consumeResult in consumerMessageCtx
			//interval = time.Now().Sub(beginTime)
			//consumeReult := SuccessReturn
			//if interval > pc.option.ConsumeTimeout {
			//	consumeReult = TimeoutReturn
			//} else if SuspendCurrentQueueAMoment == result {
			//	consumeReult = FailedReturn
			//} else if ConsumeSuccess == result {
			//	consumeReult = SuccessReturn
			//}

			// process result
			commitOffset := int64(-1)
			if pc.option.AutoCommit {
				switch result {
				case Commit, Rollback:
					rlog.Warning("the message queue consume result is illegal, we think you want to ack these message: %v", map[string]interface{}{
						rlog.LogKeyMessageQueue: mq,
					})
				case ConsumeSuccess:
					commitOffset = pq.commit()
				case SuspendCurrentQueueAMoment:
					if pc.checkReconsumeTimes(msgs) {
						pq.makeMessageToCosumeAgain(msgs...)
						time.Sleep(time.Duration(orderlyCtx.SuspendCurrentQueueTimeMillis) * time.Millisecond)
						continueConsume = false
					} else {
						commitOffset = pq.commit()
					}
				default:
				}
			} else {
				switch result {
				case ConsumeSuccess:
				case Commit:
					commitOffset = pq.commit()
				case Rollback:
					// pq.rollback
					time.Sleep(time.Duration(orderlyCtx.SuspendCurrentQueueTimeMillis) * time.Millisecond)
					continueConsume = false
				case SuspendCurrentQueueAMoment:
					if pc.checkReconsumeTimes(msgs) {
						time.Sleep(time.Duration(orderlyCtx.SuspendCurrentQueueTimeMillis) * time.Millisecond)
						continueConsume = false
					}
				default:
				}
			}
			if commitOffset > 0 && !pq.IsDroppd() {
				_ = pc.updateOffset(mq, commitOffset)
			}
		}
	} else {
		if pq.IsDroppd() {
			rlog.Warning("the message queue not be able to consume, because it's dropped.", map[string]interface{}{
				rlog.LogKeyMessageQueue: mq.String(),
			})
		}
		pc.tryLockLaterAndReconsume(mq, 100)
	}
}

func (pc *pushConsumer) checkReconsumeTimes(msgs []*primitive.MessageExt) bool {
	suspend := false
	if len(msgs) != 0 {
		maxReconsumeTimes := pc.getOrderlyMaxReconsumeTimes()
		for _, msg := range msgs {
			if msg.ReconsumeTimes > maxReconsumeTimes {
				rlog.Warning(fmt.Sprintf("msg will be send to retry topic due to ReconsumeTimes > %d, \n", maxReconsumeTimes), nil)
				msg.WithProperty("RECONSUME_TIME", strconv.Itoa(int(msg.ReconsumeTimes)))
				if !pc.sendMessageBack("", msg, -1) {
					suspend = true
					msg.ReconsumeTimes += 1
				}
			} else {
				suspend = true
				msg.ReconsumeTimes += 1
			}
		}
	}
	return suspend
}

func (pc *pushConsumer) getOrderlyMaxReconsumeTimes() int32 {
	if pc.option.MaxReconsumeTimes == -1 {
		return math.MaxInt32
	} else {
		return pc.option.MaxReconsumeTimes
	}
}

func (pc *pushConsumer) getMaxReconsumeTimes() int32 {
	if pc.option.MaxReconsumeTimes == -1 {
		return 16
	} else {
		return pc.option.MaxReconsumeTimes
	}
}

func (pc *pushConsumer) tryLockLaterAndReconsume(mq *primitive.MessageQueue, delay int64) {
	time.Sleep(time.Duration(delay) * time.Millisecond)
	if pc.lock(mq) == true {
		pc.submitConsumeRequestLater(10)
	} else {
		pc.submitConsumeRequestLater(3000)
	}
}

func (pc *pushConsumer) submitConsumeRequestLater(suspendTimeMillis int64) {
	if suspendTimeMillis == -1 {
		suspendTimeMillis = int64(pc.option.SuspendCurrentQueueTimeMillis / time.Millisecond)
	}
	if suspendTimeMillis < 10 {
		suspendTimeMillis = 10
	} else if suspendTimeMillis > 30000 {
		suspendTimeMillis = 30000
	}
	time.Sleep(time.Duration(suspendTimeMillis) * time.Millisecond)
}
