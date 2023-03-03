package ump

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"
	jmtp_client_go "github.com/jmtp/jmtp-client-go"
	"github.com/jmtp/jmtp-client-go/jmtpclient"
	v1 "github.com/jmtp/jmtp-client-go/protocol/v1"
)

const (
	testJmtpAddr                = "test"
	jmtpTimeoutSec              = 2
	jmtpHeartbeatSec            = 10
	jmtpChSize                  = 102400
	reportTypeTp     ReportType = 0
	reportTypeBiz    ReportType = 1
	reportTypeAlarm  ReportType = 3
	reportTypeAlive  ReportType = 4
	maxTPKeyCount               = 20000
	maxJmtpBatch                = 100
)

const (
	reportTypeIndexTp uint = iota
	reportTypeIndexAlarm
	reportTypeIndexAlive
	reportTypeIndexMax
)

var (
	umpJmtpAddress string
	umpJmtpBatch   uint = 1
)

type ReportType uint

type JmtpWrite struct {
	client      *jmtpclient.JmtpClient
	url         string
	wg          sync.WaitGroup
	aliveCh     chan interface{}
	alarmCh     chan interface{}
	buff        [reportTypeIndexMax]*bytes.Buffer
	jsonEncoder [reportTypeIndexMax]*json.Encoder
	// pending log
	inflight int32
	// Issue a signal to this channel when inflight hits zero.
	empty      chan struct{}
	stopC      chan struct{}
	tpKeyCount uint
}

func NewJmtpWrite() (jmtp *JmtpWrite, err error) {
	defer func() {
		if r := recover(); r != nil {
			jmtp = nil
			err = fmt.Errorf("NewJmtpWrite err(%v)", r)
		}
	}()
	config := &jmtpclient.Config{
		Url:           umpJmtpAddress,
		TimeoutSec:    jmtpTimeoutSec,
		HeartbeatSec:  jmtpHeartbeatSec,
		SerializeType: 0,
		ApplicationId: 0,
		InstanceId:    0,
	}
	var client *jmtpclient.JmtpClient
	if config.Url == testJmtpAddr {
		client = &jmtpclient.JmtpClient{}
	} else {
		client, err = jmtpclient.NewJmtpClient(config, func(packet jmtp_client_go.JmtpPacket, err error) {
			if err != nil {
				log.LogDebugf("JmtpWrite callback: packet(%v) err(%v)", packet, err)
			}
		})
		if err != nil {
			return
		}
		if err = client.Connect(); err != nil {
			return
		}
	}

	jmtp = &JmtpWrite{}
	jmtp.client = client
	jmtp.url = config.Url
	jmtp.aliveCh = make(chan interface{}, jmtpChSize)
	jmtp.alarmCh = make(chan interface{}, jmtpChSize)
	for i := 0; i < int(reportTypeIndexMax); i++ {
		jmtp.buff[i] = bytes.NewBuffer([]byte{})
		jmtp.jsonEncoder[i] = json.NewEncoder(jmtp.buff[i])
		jmtp.jsonEncoder[i].SetEscapeHTML(false)
	}
	jmtp.empty = make(chan struct{})
	jmtp.stopC = make(chan struct{})
	jmtp.backGroupWrite()
	return
}

func (jmtp *JmtpWrite) backGroupWrite() {
	jmtp.wg.Add(4)
	go jmtp.backGroupWriteTP()
	go jmtp.backGroupWriteAlive()
	go jmtp.backGroupWriteAlarm()
	go jmtp.checkTPKeyMap()
}

func (jmtp *JmtpWrite) backGroupWriteTP() {
	defer jmtp.wg.Done()
	var (
		body        []byte
		recordCount uint
		eventCount  uint
	)
	for {
		if GetUmpCollectMethod() != CollectMethodJMTP {
			time.Sleep(checkUmpWaySleepTime)
		}
		select {
		case <-jmtp.stopC:
			return
		default:
			FunctionTPKeyMap.Range(func(key, value interface{}) bool {
				if GetUmpCollectMethod() != CollectMethodJMTP {
					return false
				}
				if jmtp.tpKeyCount >= maxTPKeyCount {
					FunctionTPKeyMap.Delete(key)
					return true
				}
				v, ok := value.(*sync.Map)
				if !ok {
					return true
				}
				FunctionTPKeyMap.Delete(key)
				umpLog, count := functionTPToLogFormat(key.(string), v)
				err := jmtp.jsonEncoder[reportTypeIndexTp].Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, jmtp.buff[reportTypeIndexTp].Bytes()...)
				jmtp.buff[reportTypeIndexTp].Reset()
				recordCount++
				eventCount += uint(count)
				if recordCount >= umpJmtpBatch {
					jmtp.send(body, reportTypeTp, recordCount, eventCount)
					recordCount = 0
					eventCount = 0
					body = make([]byte, 0)
				}
				return true
			})
			if recordCount > 0 {
				jmtp.send(body, reportTypeTp, recordCount, eventCount)
				recordCount = 0
				eventCount = 0
				body = make([]byte, 0)
			}
			time.Sleep(writeTpSleepTime)
		}
	}
}

func (jmtp *JmtpWrite) checkTPKeyMap() {
	defer jmtp.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-jmtp.stopC:
			return
		case <-ticker.C:
			var count uint = 0
			FunctionTPKeyMap.Range(func(key, value interface{}) bool {
				count++
				return true
			})
			jmtp.tpKeyCount = count
		}
	}
}

func (jmtp *JmtpWrite) backGroupWriteAlive() {
	defer jmtp.wg.Done()
	ticker := time.NewTicker(aliveTickerTime)
	defer ticker.Stop()
	aliveKeyMap := new(sync.Map)
	var (
		body  []byte
		count uint
	)

	for {
		select {
		case <-jmtp.stopC:
			return
		case aliveLog := <-jmtp.aliveCh:
			alive := aliveLog.(*SystemAlive)
			aliveKeyMap.Store(alive.Key, "")
		case <-ticker.C:
			aliveKeyMap.Range(func(key, value interface{}) bool {
				aliveKeyMap.Delete(key)
				umpLog := aliveToLogFormat(key.(string))
				err := jmtp.jsonEncoder[reportTypeIndexAlive].Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, jmtp.buff[reportTypeIndexAlive].Bytes()...)
				jmtp.buff[reportTypeIndexAlive].Reset()
				count++
				if count >= umpJmtpBatch {
					jmtp.send(body, reportTypeAlive, count, count)
					count = 0
					body = make([]byte, 0)
				}
				return true
			})
			if count > 0 {
				jmtp.send(body, reportTypeAlive, count, count)
				count = 0
				body = make([]byte, 0)
			}
		}
	}
}

func (jmtp *JmtpWrite) backGroupWriteAlarm() {
	defer jmtp.wg.Done()
	ticker := time.NewTicker(alarmTickerTime)
	defer ticker.Stop()
	businessKeyMap := new(sync.Map)
	var (
		body        []byte
		toSendCount uint
		eventCount  int
	)

	for {
		select {
		case <-jmtp.stopC:
			return
		case businessLog := <-jmtp.alarmCh:
			alarmLog := businessLog.(*BusinessAlarm)
			businessKeyMap.Store(alarmLog.Key, alarmLog.Detail)
			eventCount++
		case <-ticker.C:
			businessKeyMap.Range(func(key, value interface{}) bool {
				businessKeyMap.Delete(key)
				umpLog := alarmToLogFormat(key.(string), value.(string))
				err := jmtp.jsonEncoder[reportTypeIndexAlarm].Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, jmtp.buff[reportTypeIndexAlarm].Bytes()...)
				jmtp.buff[reportTypeIndexAlarm].Reset()
				toSendCount++
				if toSendCount >= umpJmtpBatch {
					jmtp.send(body, reportTypeAlarm, toSendCount, toSendCount)
					toSendCount = 0
					body = make([]byte, 0)
				}
				return true
			})
			if toSendCount > 0 {
				jmtp.send(body, reportTypeAlarm, toSendCount, toSendCount)
				toSendCount = 0
				body = make([]byte, 0)
			}
			// Issue a signal to this channel when inflight hits zero.
			if atomic.AddInt32(&jmtp.inflight, -int32(eventCount)) <= 0 {
				select {
				case jmtp.empty <- struct{}{}:
				default:
				}
			}
			eventCount = 0
		}
	}
}

func (jmtp *JmtpWrite) send(body []byte, reportType ReportType, recordCount uint, eventCount uint) {
	var (
		write  int
		err    error
		report *v1.Report
	)
	defer func() {
		if jmtp.url == testJmtpAddr {
			return
		}
		//log.LogDebugf("JmtpWrite send: type(%v) bodyLen(%v) write(%v) recordCount(%v) eventCount(%v)", reportType, len(body), write, recordCount, eventCount)
		if r := recover(); r != nil || err != nil {
			if err = jmtp.client.Reconnect(); err != nil {
				log.LogWarnf("JmtpWrite Reconnect fail err(%v)", err)
				return
			}
			if write, err = jmtp.client.SendPacket(report); err != nil {
				log.LogWarnf("JmtpWrite send: type(%v) bodyLen(%v) write(%v) recordCount(%v) eventCount(%v) err(%v) recover(%v)", reportType, len(body), write, recordCount, eventCount, err, r)
			}
		}
	}()
	report = &v1.Report{}
	report.PacketId = []byte{0x01}
	report.ReportType = int16(reportType)
	report.Payload = body
	report.SerializeType = 3
	if jmtp.url == testJmtpAddr {
		log.LogDebugf("JmtpWrite send: type(%v) bodyLen(%v) write(%v) recordCount(%v) eventCount(%v)", reportType, len(body), write, recordCount, eventCount)
	} else {
		write, err = jmtp.client.SendPacket(report)
	}
}

func (jmtp *JmtpWrite) stop() {
	defer func() {
		_ = recover()
	}()
	close(jmtp.stopC)
	jmtp.wg.Wait()
	if jmtp.url != testJmtpAddr {
		jmtp.client.Destroy()
	}
}
