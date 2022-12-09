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
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	errors2 "github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

const (
	clientVersion        = "v2.1.0"
	defaultTraceRegionID = "DefaultRegion"

	// tracing message switch
	_TraceOff = "false"

	// Pulling topic information interval from the named server
	_PullNameServerInterval = 30 * time.Second

	// Sending heart beat interval to all broker
	_HeartbeatBrokerInterval = 30 * time.Second

	// Offset persistent interval for consumer
	_PersistOffsetInterval = 5 * time.Second

	// Rebalance interval
	_RebalanceInterval = 20 * time.Second
)

var (
	ErrServiceState = errors2.ErrService

	_VIPChannelEnable = false
)

func init() {
	if os.Getenv("com.rocketmq.sendMessageWithVIPChannel") != "" {
		value, err := strconv.ParseBool(os.Getenv("com.rocketmq.sendMessageWithVIPChannel"))
		if err == nil {
			_VIPChannelEnable = value
		}
	}
}

type InnerProducer interface {
	PublishTopicList() []string
	UpdateTopicPublishInfo(topic string, info *TopicPublishInfo)
	IsPublishTopicNeedUpdate(topic string) bool
	IsUnitMode() bool
}

type InnerConsumer interface {
	PersistConsumerOffset() error
	UpdateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue)
	IsSubscribeTopicNeedUpdate(topic string) bool
	SubscriptionDataList() []*SubscriptionData
	Rebalance()
	RebalanceIfNotPaused()
	IsUnitMode() bool
	GetConsumerRunningInfo(stack bool) *ConsumerRunningInfo
	ConsumeMessageDirectly(msg *primitive.MessageExt, brokerName string) *ConsumeMessageDirectlyResult
	GetcType() string
	GetModel() string
	GetWhere() string
	ResetOffset(topic string, table map[primitive.MessageQueue]int64)
}

func DefaultClientOptions() ClientOptions {
	opts := ClientOptions{
		InstanceName:         "DEFAULT",
		RetryTimes:           3,
		ClientIP:             utils.LocalIP,
		RemotingClientConfig: &remote.DefaultRemotingClientConfig,
	}
	return opts
}

type ClientOptions struct {
	GroupName            string
	NameServerAddrs      primitive.NamesrvAddr
	Namesrv              Namesrvs
	ClientIP             string
	InstanceName         string
	UnitMode             bool
	UnitName             string
	VIPChannelEnabled    bool
	RetryTimes           int
	Interceptors         []primitive.Interceptor
	Credentials          primitive.Credentials
	Namespace            string
	Resolver             primitive.NsResolver
	RemotingClientConfig *remote.RemotingClientConfig
}

func (opt *ClientOptions) ChangeInstanceNameToPID() {
	if opt.InstanceName == "DEFAULT" {
		opt.InstanceName = fmt.Sprintf("%d#%d", os.Getpid(), time.Now().UnixNano())
	}
}

func (opt *ClientOptions) String() string {
	return fmt.Sprintf("ClientOption [ClientIP=%s, InstanceName=%s, "+
		"UnitMode=%v, UnitName=%s, VIPChannelEnabled=%v]", opt.ClientIP,
		opt.InstanceName, opt.UnitMode, opt.UnitName, opt.VIPChannelEnabled)
}

//go:generate mockgen -source client.go -destination mock_client.go -self_package github.com/apache/rocketmq-client-go/v2/internal  --package internal RMQClient
type RMQClient interface {
	Start()
	Shutdown()

	ClientID() string

	RegisterProducer(group string, producer InnerProducer) error
	UnregisterProducer(group string)
	InvokeSync(ctx context.Context, addr string, request *remote.RemotingCommand,
		timeoutMillis time.Duration) (*remote.RemotingCommand, error)
	InvokeAsync(ctx context.Context, addr string, request *remote.RemotingCommand,
		f func(*remote.RemotingCommand, error)) error
	InvokeOneWay(ctx context.Context, addr string, request *remote.RemotingCommand,
		timeoutMillis time.Duration) error
	CheckClientInBroker()
	SendHeartbeatToAllBrokerWithLock()
	UpdateTopicRouteInfo()

	ProcessSendResponse(brokerName string, cmd *remote.RemotingCommand, resp *primitive.SendResult, msgs ...*primitive.Message) error

	RegisterConsumer(group string, consumer InnerConsumer) error
	UnregisterConsumer(group string)
	PullMessage(ctx context.Context, brokerAddrs string, request *PullMessageRequestHeader) (*primitive.PullResult, error)
	RebalanceImmediately()
	UpdatePublishInfo(topic string, data *TopicRouteData, changed bool)

	GetNameSrv() Namesrvs
}

var _ RMQClient = new(rmqClient)

type rmqClient struct {
	option ClientOptions
	// group -> InnerProducer
	producerMap sync.Map

	// group -> InnerConsumer
	consumerMap sync.Map
	once        sync.Once

	remoteClient remote.RemotingClient
	hbMutex      sync.Mutex
	close        bool
	rbMutex      sync.Mutex
	done         chan struct{}
	shutdownOnce sync.Once

	instanceCount int32
}

func (c *rmqClient) GetNameSrv() Namesrvs {
	return c.option.Namesrv
}

var clientMap sync.Map

func GetOrNewRocketMQClient(option ClientOptions, callbackCh chan interface{}) RMQClient {
	client := &rmqClient{
		option:       option,
		remoteClient: remote.NewRemotingClient(option.RemotingClientConfig),
		done:         make(chan struct{}),
	}
	actual, loaded := clientMap.LoadOrStore(client.ClientID(), client)

	if loaded {
		// compare namesrv address
		client = actual.(*rmqClient)
		now := option.Namesrv.(*namesrvs).resolver.Resolve()
		old := client.GetNameSrv().(*namesrvs).resolver.Resolve()
		if len(now) != len(old) {
			rlog.Error("different namesrv option in the same instance", map[string]interface{}{
				"NewNameSrv":    now,
				"BeforeNameSrv": old,
			})
			return nil
		}
		sort.Strings(now)
		sort.Strings(old)
		for i := 0; i < len(now); i++ {
			if now[i] != old[i] {
				rlog.Error("different namesrv option in the same instance", map[string]interface{}{
					"NewNameSrv":    now,
					"BeforeNameSrv": old,
				})
				return nil
			}
		}
	} else {
		client.remoteClient.RegisterRequestFunc(ReqNotifyConsumerIdsChanged, func(req *remote.RemotingCommand, addr net.Addr) *remote.RemotingCommand {
			rlog.Info("receive broker's notification to consumer group", map[string]interface{}{
				rlog.LogKeyConsumerGroup: req.ExtFields["consumerGroup"],
			})
			client.RebalanceIfNotPaused()
			return nil
		})
		client.remoteClient.RegisterRequestFunc(ReqCheckTransactionState, func(req *remote.RemotingCommand, addr net.Addr) *remote.RemotingCommand {
			header := new(CheckTransactionStateRequestHeader)
			header.Decode(req.ExtFields)
			msgExts := primitive.DecodeMessage(req.Body)
			if len(msgExts) == 0 {
				rlog.Warning("checkTransactionState, decode message failed", nil)
				return nil
			}
			msgExt := msgExts[0]
			// TODO: add namespace support
			transactionID := msgExt.GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex)
			if len(transactionID) > 0 {
				msgExt.TransactionId = transactionID
			}
			group := msgExt.GetProperty(primitive.PropertyProducerGroup)
			if group == "" {
				rlog.Warning("checkTransactionState, pick producer group failed", nil)
				return nil
			}
			if option.GroupName != group {
				rlog.Warning("producer group is not equal", nil)
				return nil
			}
			callback := &CheckTransactionStateCallback{
				Addr:   addr,
				Msg:    msgExt,
				Header: *header,
			}
			callbackCh <- callback
			return nil
		})

		client.remoteClient.RegisterRequestFunc(ReqGetConsumerRunningInfo, func(req *remote.RemotingCommand, addr net.Addr) *remote.RemotingCommand {
			rlog.Info("receive get consumer running info request...", nil)
			header := new(GetConsumerRunningInfoHeader)
			header.Decode(req.ExtFields)
			val, exist := clientMap.Load(header.clientID)
			res := remote.NewRemotingCommand(ResError, nil, nil)
			if !exist {
				res.Remark = fmt.Sprintf("Can't find specified client instance of: %s", header.clientID)
			} else {
				cli, ok := val.(*rmqClient)
				var runningInfo *ConsumerRunningInfo
				if ok {
					runningInfo = cli.getConsumerRunningInfo(header.consumerGroup, header.jstackEnable)
				}
				if runningInfo != nil {
					res.Code = ResSuccess
					data, err := runningInfo.Encode()
					if err != nil {
						res.Remark = fmt.Sprintf("json marshal error: %s", err.Error())
					} else {
						res.Body = data
					}
				} else {
					res.Remark = "there is unexpected error when get running info, please check log"
				}
			}
			return res
		})

		client.remoteClient.RegisterRequestFunc(ReqConsumeMessageDirectly, func(req *remote.RemotingCommand, addr net.Addr) *remote.RemotingCommand {
			rlog.Info("receive consume message directly request...", nil)
			header := new(ConsumeMessageDirectlyHeader)
			header.Decode(req.ExtFields)
			val, exist := clientMap.Load(header.clientID)
			res := remote.NewRemotingCommand(ResError, nil, nil)
			if !exist {
				res.Remark = fmt.Sprintf("Can't find specified client instance of: %s", header.clientID)
			} else {
				cli, ok := val.(*rmqClient)
				msg := primitive.DecodeMessage(req.Body)[0]
				var consumeMessageDirectlyResult *ConsumeMessageDirectlyResult
				if ok {
					consumeMessageDirectlyResult = cli.consumeMessageDirectly(msg, header.consumerGroup, header.brokerName)
				}
				if consumeMessageDirectlyResult != nil {
					res.Code = ResSuccess
					data, err := consumeMessageDirectlyResult.Encode()
					if err != nil {
						res.Remark = fmt.Sprintf("json marshal error: %s", err.Error())
					} else {
						res.Body = data
					}
				} else {
					res.Remark = "there is unexpected error when consume message directly, please check log"
				}
			}
			return res
		})

		client.remoteClient.RegisterRequestFunc(ReqResetConsumerOffset, func(req *remote.RemotingCommand, addr net.Addr) *remote.RemotingCommand {
			rlog.Info("receive reset consumer offset request...", map[string]interface{}{
				rlog.LogKeyBroker:        addr.String(),
				rlog.LogKeyTopic:         req.ExtFields["topic"],
				rlog.LogKeyConsumerGroup: req.ExtFields["group"],
				rlog.LogKeyTimeStamp:     req.ExtFields["timestamp"],
			})
			header := new(ResetOffsetHeader)
			header.Decode(req.ExtFields)

			body := new(ResetOffsetBody)
			body.Decode(req.Body)

			client.resetOffset(header.topic, header.group, body.OffsetTable)
			return nil
		})

		client.remoteClient.RegisterRequestFunc(ReqPushReplyMessageToClient, func(req *remote.RemotingCommand, addr net.Addr) *remote.RemotingCommand {
			receiveTime := time.Now().UnixNano() / int64(time.Millisecond)
			rlog.Info("receive push reply to client request...", map[string]interface{}{
				rlog.LogKeyBroker:        addr.String(),
				rlog.LogKeyTopic:         req.ExtFields["topic"],
				rlog.LogKeyConsumerGroup: req.ExtFields["group"],
				rlog.LogKeyTimeStamp:     req.ExtFields["timestamp"],
			})

			header := new(ReplyMessageRequestHeader)
			header.Decode(req.ExtFields)

			var msgExt primitive.MessageExt
			msgExt.Topic = header.topic
			msgExt.Queue = &primitive.MessageQueue{
				QueueId: header.queueId,
				Topic:   header.topic,
			}
			msgExt.StoreTimestamp = header.storeTimestamp
			msgExt.BornHost = header.bornHost
			msgExt.StoreHost = header.storeHost

			body := req.Body
			if (header.sysFlag & primitive.FlagCompressed) == primitive.FlagCompressed {
				body = utils.UnCompress(req.Body)
			}
			msgExt.Body = body
			msgExt.Flag = header.flag
			msgExt.UnmarshalProperties([]byte(header.properties))
			msgExt.WithProperty(primitive.PropertyReplyMessageArriveTime, strconv.FormatInt(receiveTime, 10))
			msgExt.BornTimestamp = header.bornTimestamp
			msgExt.ReconsumeTimes = header.reconsumeTimes

			client.getReplyMessageRequest(&msgExt, header.bornHost)

			res := remote.NewRemotingCommand(ResError, nil, nil)
			res.Code = ResSuccess
			return res
		})
	}
	return client
}

func (c *rmqClient) Start() {
	//ctx, cancel := context.WithCancel(context.Background())
	//c.cancel = cancel
	atomic.AddInt32(&c.instanceCount, 1)
	c.once.Do(func() {
		if !c.option.Credentials.IsEmpty() {
			c.remoteClient.RegisterInterceptor(remote.ACLInterceptor(c.option.Credentials))
		}
		go primitive.WithRecover(func() {
			op := func() {
				c.GetNameSrv().UpdateNameServerAddress()
			}
			time.Sleep(10 * time.Second)
			op()

			ticker := time.NewTicker(2 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					op()
				case <-c.done:
					rlog.Info("The RMQClient stopping update name server domain info.", map[string]interface{}{
						"clientID": c.ClientID(),
					})
					return
				}
			}
		})

		// schedule update route info
		go primitive.WithRecover(func() {
			// delay
			op := func() {
				c.UpdateTopicRouteInfo()
			}
			time.Sleep(10 * time.Millisecond)
			op()

			ticker := time.NewTicker(_PullNameServerInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					op()
				case <-c.done:
					rlog.Info("The RMQClient stopping update topic route info.", map[string]interface{}{
						"clientID": c.ClientID(),
					})
					return
				}
			}
		})

		go primitive.WithRecover(func() {
			op := func() {
				c.GetNameSrv().cleanOfflineBroker()
				c.SendHeartbeatToAllBrokerWithLock()
			}

			time.Sleep(time.Second)
			op()

			ticker := time.NewTicker(_HeartbeatBrokerInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					op()
				case <-c.done:
					rlog.Info("The RMQClient stopping clean off line broker and heart beat", map[string]interface{}{
						"clientID": c.ClientID(),
					})
					return
				}
			}
		})

		// schedule persist offset
		go primitive.WithRecover(func() {
			op := func() {
				c.consumerMap.Range(func(key, value interface{}) bool {
					consumer := value.(InnerConsumer)
					err := consumer.PersistConsumerOffset()
					if err != nil {
						rlog.Error("persist offset failed", map[string]interface{}{
							rlog.LogKeyUnderlayError: err,
						})
					}
					return true
				})
			}
			time.Sleep(10 * time.Second)
			op()

			ticker := time.NewTicker(_PersistOffsetInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					op()
				case <-c.done:
					rlog.Info("The RMQClient stopping persist offset", map[string]interface{}{
						"clientID": c.ClientID(),
					})
					return
				}
			}
		})

		go primitive.WithRecover(func() {
			ticker := time.NewTicker(_RebalanceInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					c.RebalanceIfNotPaused()
				case <-c.done:
					rlog.Info("The RMQClient stopping do rebalance", map[string]interface{}{
						"clientID": c.ClientID(),
					})
					return
				}
			}
		})
	})
}

func (c *rmqClient) removeClient() {
	rlog.Info("will remove client from clientMap", map[string]interface{}{
		"clientID": c.ClientID(),
	})
	clientMap.Delete(c.ClientID())
}

func (c *rmqClient) Shutdown() {
	if atomic.AddInt32(&c.instanceCount, -1) > 0 {
		return
	}

	c.shutdownOnce.Do(func() {
		close(c.done)
		c.close = true
		c.remoteClient.ShutDown()
		c.removeClient()
	})
}

func (c *rmqClient) ClientID() string {
	id := c.option.ClientIP + "@"
	if c.option.InstanceName == "DEFAULT" {
		id += strconv.Itoa(os.Getpid())
	} else {
		id += c.option.InstanceName
	}
	if c.option.UnitName != "" {
		id += "@" + c.option.UnitName
	}
	return id
}

func (c *rmqClient) InvokeSync(ctx context.Context, addr string, request *remote.RemotingCommand,
	timeoutMillis time.Duration) (*remote.RemotingCommand, error) {
	if c.close {
		return nil, ErrServiceState
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeoutMillis)
	defer cancel()
	return c.remoteClient.InvokeSync(ctx, addr, request)
}

func (c *rmqClient) InvokeAsync(ctx context.Context, addr string, request *remote.RemotingCommand,
	f func(*remote.RemotingCommand, error)) error {
	if c.close {
		return ErrServiceState
	}
	return c.remoteClient.InvokeAsync(ctx, addr, request, func(future *remote.ResponseFuture) {
		f(future.ResponseCommand, future.Err)
	})

}

func (c *rmqClient) InvokeOneWay(ctx context.Context, addr string, request *remote.RemotingCommand,
	timeoutMillis time.Duration) error {
	if c.close {
		return ErrServiceState
	}
	return c.remoteClient.InvokeOneWay(ctx, addr, request)
}

func (c *rmqClient) CheckClientInBroker() {
}

// TODO
func (c *rmqClient) SendHeartbeatToAllBrokerWithLock() {
	c.hbMutex.Lock()
	defer c.hbMutex.Unlock()
	hbData := NewHeartbeatData(c.ClientID())

	c.producerMap.Range(func(key, value interface{}) bool {
		pData := producerData{
			GroupName: key.(string),
		}
		hbData.ProducerDatas.Add(pData)
		return true
	})

	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		cData := consumerData{
			GroupName:         key.(string),
			CType:             consumeType(consumer.GetcType()),
			MessageModel:      strings.ToUpper(consumer.GetModel()),
			Where:             consumer.GetWhere(),
			UnitMode:          consumer.IsUnitMode(),
			SubscriptionDatas: consumer.SubscriptionDataList(),
		}
		hbData.ConsumerDatas.Add(cData)
		return true
	})
	if hbData.ProducerDatas.Len() == 0 && hbData.ConsumerDatas.Len() == 0 {
		rlog.Info("sending heartbeat, but no producer and no consumer", nil)
		return
	}
	c.GetNameSrv().(*namesrvs).brokerAddressesMap.Range(func(key, value interface{}) bool {
		brokerName := key.(string)
		data := value.(*BrokerData)
		for id, addr := range data.BrokerAddresses {
			rlog.Debug("try to send heart beat to broker", map[string]interface{}{
				"brokerName": brokerName,
				"brokerId":   id,
				"brokerAddr": addr,
			})
			if hbData.ConsumerDatas.Len() == 0 && id != 0 {
				rlog.Debug("notice, will not send heart beat to broker", map[string]interface{}{
					"brokerName": brokerName,
					"brokerId":   id,
					"brokerAddr": addr,
				})
				continue
			}
			cmd := remote.NewRemotingCommand(ReqHeartBeat, nil, hbData.encode())

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			response, err := c.remoteClient.InvokeSync(ctx, addr, cmd)
			if err != nil {
				cancel()
				rlog.Warning("send heart beat to broker error", map[string]interface{}{
					rlog.LogKeyUnderlayError: err,
				})
				return true
			}
			cancel()
			if response.Code == ResSuccess {
				c.GetNameSrv().(*namesrvs).AddBrokerVersion(brokerName, addr, int32(response.Version))
				rlog.Debug("send heart beat to broker success", map[string]interface{}{
					"brokerName": brokerName,
					"brokerId":   id,
					"brokerAddr": addr,
				})
			} else {
				rlog.Warning("send heart beat to broker failed", map[string]interface{}{
					"brokerName":   brokerName,
					"brokerId":     id,
					"brokerAddr":   addr,
					"responseCode": response.Code,
				})
			}
		}
		return true
	})
}

func (c *rmqClient) UpdateTopicRouteInfo() {
	publishTopicSet := make(map[string]bool, 0)
	c.producerMap.Range(func(key, value interface{}) bool {
		producer := value.(InnerProducer)
		list := producer.PublishTopicList()
		for idx := range list {
			publishTopicSet[list[idx]] = true
		}
		return true
	})
	for topic := range publishTopicSet {
		data, changed, _ := c.GetNameSrv().UpdateTopicRouteInfo(topic)
		c.UpdatePublishInfo(topic, data, changed)
	}

	subscribedTopicSet := make(map[string]bool, 0)
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		list := consumer.SubscriptionDataList()
		for idx := range list {
			subscribedTopicSet[list[idx].Topic] = true
		}
		return true
	})

	for topic := range subscribedTopicSet {
		data, changed, _ := c.GetNameSrv().UpdateTopicRouteInfo(topic)
		c.updateSubscribeInfo(topic, data, changed)
	}
}

func (c *rmqClient) ProcessSendResponse(brokerName string, cmd *remote.RemotingCommand, resp *primitive.SendResult, msgs ...*primitive.Message) error {
	var status primitive.SendStatus
	switch cmd.Code {
	case ResFlushDiskTimeout:
		status = primitive.SendFlushDiskTimeout
	case ResFlushSlaveTimeout:
		status = primitive.SendFlushSlaveTimeout
	case ResSlaveNotAvailable:
		status = primitive.SendSlaveNotAvailable
	case ResSuccess:
		status = primitive.SendOK
	default:
		status = primitive.SendUnknownError
		return errors.New(cmd.Remark)
	}

	msgIDs := make([]string, 0)
	for i := 0; i < len(msgs); i++ {
		msgIDs = append(msgIDs, msgs[i].GetProperty(primitive.PropertyUniqueClientMessageIdKeyIndex))
	}
	uniqueMsgId := strings.Join(msgIDs, ",")

	regionId := cmd.ExtFields[primitive.PropertyMsgRegion]
	trace := cmd.ExtFields[primitive.PropertyTraceSwitch]

	if regionId == "" {
		regionId = defaultTraceRegionID
	}

	qId, _ := strconv.Atoi(cmd.ExtFields["queueId"])
	off, _ := strconv.ParseInt(cmd.ExtFields["queueOffset"], 10, 64)

	resp.Status = status
	resp.MsgID = uniqueMsgId
	resp.OffsetMsgID = cmd.ExtFields["msgId"]
	resp.MessageQueue = &primitive.MessageQueue{
		Topic:      msgs[0].Topic,
		BrokerName: brokerName,
		QueueId:    qId,
	}
	resp.QueueOffset = off
	resp.TransactionID = cmd.ExtFields["transactionId"]
	resp.RegionID = regionId
	resp.TraceOn = trace != "" && trace != _TraceOff
	return nil
}

// PullMessage with sync
func (c *rmqClient) PullMessage(ctx context.Context, brokerAddrs string, request *PullMessageRequestHeader) (*primitive.PullResult, error) {
	cmd := remote.NewRemotingCommand(ReqPullMessage, request, nil)
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := c.remoteClient.InvokeSync(ctx, brokerAddrs, cmd)
	if err != nil {
		return nil, err
	}

	return c.processPullResponse(res)
}

func (c *rmqClient) processPullResponse(response *remote.RemotingCommand) (*primitive.PullResult, error) {

	pullResult := &primitive.PullResult{}
	switch response.Code {
	case ResSuccess:
		pullResult.Status = primitive.PullFound
	case ResPullNotFound:
		pullResult.Status = primitive.PullNoNewMsg
	case ResPullRetryImmediately:
		pullResult.Status = primitive.PullNoMsgMatched
	case ResPullOffsetMoved:
		pullResult.Status = primitive.PullOffsetIllegal
	default:
		return nil, fmt.Errorf("unknown Response Code: %d, remark: %s", response.Code, response.Remark)
	}

	c.decodeCommandCustomHeader(pullResult, response)
	pullResult.SetBody(response.Body)

	return pullResult, nil
}

func (c *rmqClient) decodeCommandCustomHeader(pr *primitive.PullResult, cmd *remote.RemotingCommand) {
	v, exist := cmd.ExtFields["maxOffset"]
	if exist {
		pr.MaxOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = cmd.ExtFields["minOffset"]
	if exist {
		pr.MinOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = cmd.ExtFields["nextBeginOffset"]
	if exist {
		pr.NextBeginOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = cmd.ExtFields["suggestWhichBrokerId"]
	if exist {
		pr.SuggestWhichBrokerId, _ = strconv.ParseInt(v, 10, 64)
	}
}

func (c *rmqClient) RegisterConsumer(group string, consumer InnerConsumer) error {
	_, exist := c.consumerMap.Load(group)
	if exist {
		rlog.Warning("the consumer group exist already", map[string]interface{}{
			rlog.LogKeyConsumerGroup: group,
		})
		return fmt.Errorf("the consumer group exist already")
	}
	c.consumerMap.Store(group, consumer)
	return nil
}

func (c *rmqClient) UnregisterConsumer(group string) {
	c.consumerMap.Delete(group)
}

func (c *rmqClient) RegisterProducer(group string, producer InnerProducer) error {
	_, exist := c.producerMap.Load(group)
	if exist {
		rlog.Warning("the producer group exist already", map[string]interface{}{
			rlog.LogKeyProducerGroup: group,
		})
		return fmt.Errorf("the producer group exist already")
	}
	c.producerMap.Store(group, producer)
	return nil
}

func (c *rmqClient) UnregisterProducer(group string) {
	c.producerMap.Delete(group)
}

func (c *rmqClient) RebalanceImmediately() {
	c.rbMutex.Lock()
	defer c.rbMutex.Unlock()
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		consumer.Rebalance()
		return true
	})
}

func (c *rmqClient) RebalanceIfNotPaused() {
	c.rbMutex.Lock()
	defer c.rbMutex.Unlock()
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		consumer.RebalanceIfNotPaused()
		return true
	})
}

func (c *rmqClient) UpdatePublishInfo(topic string, data *TopicRouteData, changed bool) {
	if data == nil {
		return
	}

	c.producerMap.Range(func(key, value interface{}) bool {
		p := value.(InnerProducer)
		updated := changed
		if !updated {
			updated = p.IsPublishTopicNeedUpdate(topic)
		}
		if updated {
			publishInfo := c.GetNameSrv().(*namesrvs).routeData2PublishInfo(topic, data)
			publishInfo.HaveTopicRouterInfo = true
			p.UpdateTopicPublishInfo(topic, publishInfo)
		}
		return true
	})
}

func (c *rmqClient) updateSubscribeInfo(topic string, data *TopicRouteData, changed bool) {
	if data == nil {
		return
	}
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		updated := changed
		if !updated {
			updated = consumer.IsSubscribeTopicNeedUpdate(topic)
		}
		if updated {
			consumer.UpdateTopicSubscribeInfo(topic, routeData2SubscribeInfo(topic, data))
		}

		return true
	})
}

func (c *rmqClient) isNeedUpdateSubscribeInfo(topic string) bool {
	var result bool
	c.consumerMap.Range(func(key, value interface{}) bool {
		consumer := value.(InnerConsumer)
		if consumer.IsSubscribeTopicNeedUpdate(topic) {
			result = true
			return false
		}
		return true
	})
	return result
}

func (c *rmqClient) resetOffset(topic string, group string, offsetTable map[primitive.MessageQueue]int64) {
	consumer, exist := c.consumerMap.Load(group)
	if !exist {
		rlog.Warning("group "+group+" do not exists", nil)
		return
	}
	consumer.(InnerConsumer).ResetOffset(topic, offsetTable)
}

func (c *rmqClient) getConsumerRunningInfo(group string, stack bool) *ConsumerRunningInfo {
	consumer, exist := c.consumerMap.Load(group)
	if !exist {
		return nil
	}
	info := consumer.(InnerConsumer).GetConsumerRunningInfo(stack)
	if info != nil {
		info.Properties[PropClientVersion] = clientVersion
	}
	return info
}

func (c *rmqClient) getReplyMessageRequest(msg *primitive.MessageExt, bornHost string) {
	correlationId := msg.GetProperty(primitive.PropertyCorrelationID)
	if err := RequestResponseFutureMap.SetResponseToRequestResponseFuture(correlationId, &msg.Message); err != nil {
		rlog.Warning("receive reply message, but not matched any request", map[string]interface{}{
			"CorrelationId": correlationId,
			"ReplyHost":     bornHost,
		})
		return
	}
	RequestResponseFutureMap.RemoveRequestResponseFuture(correlationId)
}

func (c *rmqClient) consumeMessageDirectly(msg *primitive.MessageExt, group string, brokerName string) *ConsumeMessageDirectlyResult {
	consumer, exist := c.consumerMap.Load(group)
	if !exist {
		return nil
	}
	res := consumer.(InnerConsumer).ConsumeMessageDirectly(msg, brokerName)
	return res
}

func routeData2SubscribeInfo(topic string, data *TopicRouteData) []*primitive.MessageQueue {
	list := make([]*primitive.MessageQueue, 0)
	for idx := range data.QueueDataList {
		qd := data.QueueDataList[idx]
		if queueIsReadable(qd.Perm) {
			for i := 0; i < qd.ReadQueueNums; i++ {
				list = append(list, &primitive.MessageQueue{
					Topic:      topic,
					BrokerName: qd.BrokerName,
					QueueId:    i,
				})
			}
		}
	}
	return list
}

func brokerVIPChannel(brokerAddr string) string {
	if !_VIPChannelEnable {
		return brokerAddr
	}
	var brokerAddrNew strings.Builder
	ipAndPort := strings.Split(brokerAddr, ":")
	port, err := strconv.Atoi(ipAndPort[1])
	if err != nil {
		return ""
	}
	brokerAddrNew.WriteString(ipAndPort[0])
	brokerAddrNew.WriteString(":")
	brokerAddrNew.WriteString(strconv.Itoa(port - 2))
	return brokerAddrNew.String()
}
