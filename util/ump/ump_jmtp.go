package ump

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	jmtp_client_go "github.com/jmtp/jmtp-client-go"
	"github.com/jmtp/jmtp-client-go/jmtpclient"
	v1 "github.com/jmtp/jmtp-client-go/protocol/v1"
)

const (
	defaultJmtpAddr                  = "jmtp://proxy.ump.jd.local:20890"
	jmtpTimeoutSec                   = 2
	jmtpHeartbeatSec                 = 10
	jmtpChSize                       = 102400
	reportTypeTp          ReportType = 0
	reportTypeBiz         ReportType = 1
	reportTypeAlarm       ReportType = 3
	reportTypeAlive       ReportType = 4
	payloadMaxRecordCount            = 1
	maxTPKeyCount                    = 20000
)

type ReportType uint

type JmtpWrite struct {
	client      *jmtpclient.JmtpClient
	wg          sync.WaitGroup
	aliveCh     chan interface{}
	alarmCh     chan interface{}
	buff        *bytes.Buffer
	jsonEncoder *json.Encoder
	// pending log
	inflight int32
	// Issue a signal to this channel when inflight hits zero.
	empty      chan struct{}
	stopC      chan struct{}
	tpKeyCount uint
}

func NewJmtpWrite(jmtpAddr string) (jmtp *JmtpWrite, err error) {
	defer func() {
		if r := recover(); r != nil {
			jmtp = nil
			err = fmt.Errorf("NewJmtpWrite err(%v)", r)
		}
	}()
	if jmtpAddr == "" {
		jmtpAddr = defaultJmtpAddr
	}
	config := &jmtpclient.Config{
		Url:           jmtpAddr,
		TimeoutSec:    jmtpTimeoutSec,
		HeartbeatSec:  jmtpHeartbeatSec,
		SerializeType: 0,
		ApplicationId: 0,
		InstanceId:    0,
	}
	var client *jmtpclient.JmtpClient
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
	jmtp = &JmtpWrite{}
	jmtp.client = client
	jmtp.aliveCh = make(chan interface{}, jmtpChSize)
	jmtp.alarmCh = make(chan interface{}, jmtpChSize)
	jmtp.buff = bytes.NewBuffer([]byte{})
	jmtp.jsonEncoder = json.NewEncoder(jmtp.buff)
	jmtp.jsonEncoder.SetEscapeHTML(false)
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
		recordCount int
		eventCount  int
	)
	for {
		if UmpCollectWay != proto.UmpCollectByJmtpClient {
			time.Sleep(10 * time.Second)
		}
		select {
		case <-jmtp.stopC:
			return
		default:
			FunctionTPKeyMap.Range(func(key, value interface{}) bool {
				if UmpCollectWay != proto.UmpCollectByJmtpClient {
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
				err := jmtp.jsonEncoder.Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, jmtp.buff.Bytes()...)
				jmtp.buff.Reset()
				recordCount++
				eventCount += count
				if recordCount >= payloadMaxRecordCount {
					jmtp.send(body, reportTypeTp, eventCount)
					recordCount = 0
					eventCount = 0
					body = make([]byte, 0)
				}
				return true
			})
			if recordCount > 0 {
				jmtp.send(body, reportTypeTp, eventCount)
				recordCount = 0
				eventCount = 0
				body = make([]byte, 0)
			}
			time.Sleep(time.Second)
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
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	aliveKeyMap := new(sync.Map)
	var (
		body []byte
		num  int
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
				err := jmtp.jsonEncoder.Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, jmtp.buff.Bytes()...)
				jmtp.buff.Reset()
				num++
				if num >= payloadMaxRecordCount {
					jmtp.send(body, reportTypeAlive, num)
					num = 0
					body = make([]byte, 0)
				}
				return true
			})
			if num > 0 {
				jmtp.send(body, reportTypeAlive, num)
				num = 0
				body = make([]byte, 0)
			}
		}
	}
}

func (jmtp *JmtpWrite) backGroupWriteAlarm() {
	defer jmtp.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	businessKeyMap := new(sync.Map)
	var (
		body []byte
		num  int
	)

	for {
		select {
		case <-jmtp.stopC:
			return
		case businessLog := <-jmtp.alarmCh:
			alarmLog := businessLog.(*BusinessAlarm)
			businessKeyMap.Store(alarmLog.Key, alarmLog.Detail)
		case <-ticker.C:
			businessKeyMap.Range(func(key, value interface{}) bool {
				businessKeyMap.Delete(key)
				umpLog := alarmToLogFormat(key.(string), value.(string))
				err := jmtp.jsonEncoder.Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, jmtp.buff.Bytes()...)
				jmtp.buff.Reset()
				num++
				if num >= payloadMaxRecordCount {
					jmtp.send(body, reportTypeAlarm, num)
					num = 0
					body = make([]byte, 0)
				}
				return true
			})
			if num > 0 {
				jmtp.send(body, reportTypeAlarm, num)
				num = 0
				body = make([]byte, 0)
			}
			// Issue a signal to this channel when inflight hits zero.
			if atomic.AddInt32(&jmtp.inflight, -1) <= 0 {
				select {
				case jmtp.empty <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (jmtp *JmtpWrite) send(body []byte, reportType ReportType, eventCount int) {
	var (
		write  int
		err    error
		report *v1.Report
	)
	defer func() {
		if r := recover(); r != nil || err != nil {
			if err = jmtp.client.Reconnect(); err != nil {
				log.LogWarnf("JmtpWrite Reconnect fail err(%v)", err)
				return
			}
			if write, err = jmtp.client.SendPacket(report); err != nil {
				log.LogWarnf("JmtpWrite send: type(%v) bodyLen(%v) write(%v) eventCount(%v) err(%v) recover(%v)", reportType, len(body), write, eventCount, err, r)
			}
		}
	}()
	report = &v1.Report{}
	report.PacketId = []byte{0x01}
	report.ReportType = int16(reportType)
	report.Payload = body
	report.SerializeType = 3
	write, err = jmtp.client.SendPacket(report)
}

func (jmtp *JmtpWrite) stop() {
	defer func() {
		_ = recover()
	}()
	close(jmtp.stopC)
	jmtp.wg.Wait()
	jmtp.client.Destroy()
}
