package monitor

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/statistics"
)

type MQProducer struct {
	topic      	[]string
	config     	*sarama.Config
	producer   	[]sarama.SyncProducer
	produceNum	int64
	msgChan    	[]chan *statistics.ReportInfo
	stopC      	chan bool
	epoch		uint64
}

type JMQConfig struct {
	address  	string
	topic    	[]string
	clientID 	string
	produceNum	int64
}

func initMQProducer(jmqConf *JMQConfig) (mqProducer *MQProducer, err error) {
	if jmqConf.address == "" || len(jmqConf.topic) <= 0 || jmqConf.clientID == "" {
		log.LogWarnf("initMQProducer: no configuration, address(%v) topic(%v) clientID(%v)", jmqConf.address, jmqConf.topic, jmqConf.clientID)
		return
	}
	mqProducer = &MQProducer{
		topic:      jmqConf.topic,
		produceNum: jmqConf.produceNum,
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.ClientID = jmqConf.clientID

	mqProducer.config = config
	mqProducer.msgChan = make([]chan *statistics.ReportInfo, mqProducer.produceNum)
	mqProducer.producer = make([]sarama.SyncProducer, mqProducer.produceNum)
	mqProducer.stopC = make(chan bool)

	for i := 0; i < len(mqProducer.msgChan); i++ {
		mqProducer.producer[i], err = sarama.NewSyncProducer([]string{jmqConf.address}, config)
		if err != nil {
			log.LogErrorf("init MQ producer err: %v, address(%v)", err, jmqConf.address)
			return
		}
		mqProducer.msgChan[i] = make(chan *statistics.ReportInfo, 10240)
		go mqProducer.Produce(mqProducer.msgChan[i], mqProducer.producer[i], i)
	}

	log.LogInfof("start MQ producer successfully: address(%v) topic(%v) clientID(%v)", jmqConf.address, jmqConf.topic, jmqConf.clientID)
	return
}

func (mqProducer *MQProducer) closeMQProducer() {
	close(mqProducer.stopC)
	for i := 0; i < len(mqProducer.producer); i++ {
		mqProducer.producer[i].Close()
	}
}

func (mqProducer *MQProducer) Produce(msgChan chan *statistics.ReportInfo, producer sarama.SyncProducer, index int) {
	log.LogInfof("start producing to mq: index(%v) total producer(%v)", index, mqProducer.produceNum)
	for {
		select {
		case reportInfo := <-msgChan:
			start := time.Now()
			proMsgs := make(map[int][]*sarama.ProducerMessage)
			for i := 0; i < len(mqProducer.topic); i++ {
				proMsgs[i] = make([]*sarama.ProducerMessage, 0)
			}
			for _, info := range reportInfo.Infos {
				if info.IsTotal {
					continue
				}
				mqMsg := constructMQMessage(reportInfo.Cluster, reportInfo.Module, reportInfo.Addr, info)
				for topicID, topic := range mqProducer.topic {
					if len(topic) <= 0 {
						continue
					}
					proMsg := &sarama.ProducerMessage{
						Topic: topic,
						Value: sarama.ByteEncoder(mqMsg),
					}
					proMsgs[topicID] = append(proMsgs[topicID], proMsg)
				}
			}
			// send message
			for i := 0; i < len(mqProducer.topic); i++ {
				if err := producer.SendMessages(proMsgs[i]); err != nil {
					log.LogErrorf("produce to mq error: %v, topic(%v) msgLen(%v) index(%v)", err, mqProducer.topic[i], len(proMsgs[i]), index)
				}
				log.LogDebugf("produce to mq: cluster(%v) module(%v) ip(%v) topic(%v) msgLen(%v) index(%v) msgChan(%v) cost(%v)",
					reportInfo.Cluster, reportInfo.Module, reportInfo.Addr, mqProducer.topic[i], len(proMsgs[i]), index, len(msgChan), time.Since(start))
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
