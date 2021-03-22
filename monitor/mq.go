package monitor

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/statistics"
)

type MQProducer struct {
	cfsCluster string
	topic      string
	config     *sarama.Config
	producer   sarama.AsyncProducer
	msgChan    chan *statistics.ReportInfo
	stopC      chan bool
}

type JMQConfig struct {
	address  string
	topic    string
	clientID string
}

func initMQProducer(cluster string, jmqConf *JMQConfig) (mqProducer *MQProducer, err error) {
	if jmqConf.address == "" || jmqConf.topic == "" || jmqConf.clientID == "" {
		log.LogWarnf("initMQProducer: no configuration, address(%v) topic(%v) clientID(%v)", jmqConf.address, jmqConf.topic, jmqConf.clientID)
		return
	}
	mqProducer = &MQProducer{
		cfsCluster: cluster,
		topic:      jmqConf.topic,
	}
	// todo check config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.ClientID = jmqConf.clientID

	mqProducer.producer, err = sarama.NewAsyncProducer([]string{jmqConf.address}, config)
	if err != nil {
		log.LogErrorf("init MQ producer err: %v, address(%v)", err, jmqConf.address)
		return
	}
	mqProducer.config = config
	mqProducer.msgChan = make(chan *statistics.ReportInfo, 10240)
	mqProducer.stopC = make(chan bool)

	go mqProducer.Produce()

	log.LogInfof("start MQ producer successfully: address(%v) topic(%v) clientID(%v)", jmqConf.address, jmqConf.topic, jmqConf.clientID)
	return
}

func (mqProducer *MQProducer) closeMQProducer() {
	close(mqProducer.stopC)
	mqProducer.producer.AsyncClose()
}

func (mqProducer *MQProducer) Produce() {
	for {
		select {
		case reportInfo := <-mqProducer.msgChan:
			for _, info := range reportInfo.Infos {
				if info.IsTotal {
					continue
				}
				mqMsg := constructMQMessage(mqProducer.cfsCluster, reportInfo.Module, reportInfo.Addr, info)
				// send message
				proMsg := &sarama.ProducerMessage{
					Topic: mqProducer.topic,
					//Key:	sarama.StringEncoder(""),
					Value: sarama.ByteEncoder(mqMsg),
				}

				mqProducer.producer.Input() <- proMsg

				// todo remove
				select {
				case success := <-mqProducer.producer.Successes():
					log.LogDebugf("produce to mq successfully: msg(%s) offset(%v), timestamp(%v)", string(mqMsg), success.Offset, success.Timestamp.Unix())
				case produceErr := <-mqProducer.producer.Errors():
					log.LogDebugf("produce to mq error: %v", produceErr)
				}
			}
		case <-mqProducer.stopC:
			return
		}
	}
}

func constructMQMessage(cluster, module, ip string, data *statistics.MonitorData) []byte {
	msg := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v", data.ReportTime, cluster, module, ip, data.Action, data.VolName, data.PartitionID,
		data.Size, data.Count)
	return []byte(msg)
}
