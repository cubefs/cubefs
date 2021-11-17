// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ump

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type FunctionTp struct {
	Time         string `json:"time"`
	Key          string `json:"key"`
	HostName     string `json:"hostname"`
	ProcessState string `json:"processState"`
	ElapsedTime  string `json:"elapsedTime"`
}

type FunctionTpGroupBy struct {
	currTime     time.Time
	Time         string
	Key          string
	HostName     string
	ProcessState string
	ElapsedTime  string
	Count        string

	elapsedTime int64
	count       int64
}

type SystemAlive struct {
	Key      string `json:"key"`
	HostName string `json:"hostname"`
	Time     string `json:"time"`
}

type BusinessAlarm struct {
	Time         string `json:"time"`
	Key          string `json:"key"`
	HostName     string `json:"hostname"`
	BusinessType string `json:"type"`
	Value        string `json:"value"`
	Detail       string `json:"detail"`
}

const (
	FunctionTpSufixx        = "tp.log"
	FunctionTpGroupBySufixx = "groupby_tp.log"
	SystemAliveSufixx       = "alive.log"
	BusinessAlarmSufixx     = "business.log"
	LogFileOpt              = os.O_RDWR | os.O_CREATE | os.O_APPEND
	ChSize                  = 102400
	BusinessAlarmType       = "BusinessAlarm"
	SystemAliveType         = "SystemAlive"
	FunctionTpType          = "FunctionTp"
	HostNameFile            = "/proc/sys/kernel/hostname"
	MaxLogSize              = 1024 * 1024 * 10
)

var (
	FunctionTpLogWrite        = &LogWrite{logCh: make(chan interface{}, ChSize)}
	FunctionTpGroupByLogWrite = &LogWrite{logCh: make(chan interface{}, ChSize)}
	SystemAliveLogWrite       = &LogWrite{logCh: make(chan interface{}, ChSize)}
	BusinessAlarmLogWrite     = &LogWrite{logCh: make(chan interface{}, ChSize), empty: make(chan struct{}, 1)}
	UmpDataDir                = "/export/home/tomcat/UMP-Monitor/logs/"
)

type LogWrite struct {
	logCh       chan interface{}
	logName     string
	logSize     int64
	seq         int
	logSufixx   string
	logFp       *os.File
	sigCh       chan bool
	bf          *bytes.Buffer
	jsonEncoder *json.Encoder
	// pending log
	inflight int32
	// Issue a signal to this channel when inflight hits zero.
	empty chan struct{}
}

func (lw *LogWrite) initLogFp(sufixx string) (err error) {
	var fi os.FileInfo
	lw.seq = 0
	lw.sigCh = make(chan bool, 1)
	lw.logSufixx = sufixx
	lw.logName = fmt.Sprintf("%s%s%s", UmpDataDir, "ump_", lw.logSufixx)
	lw.bf = bytes.NewBuffer([]byte{})
	lw.jsonEncoder = json.NewEncoder(lw.bf)
	lw.jsonEncoder.SetEscapeHTML(false)
	if lw.logFp, err = os.OpenFile(lw.logName, LogFileOpt, 0777); err != nil {
		return
	}
	os.Chmod(lw.logName, 0777)

	if fi, err = lw.logFp.Stat(); err != nil {
		return
	}
	lw.logSize = fi.Size()

	return
}

func (lw *LogWrite) backGroundCheckFile() (err error) {
	if lw.logSize <= MaxLogSize {
		return
	}
	lw.logFp.Close()
	lw.seq++
	if lw.seq > 3 {
		lw.seq = 1
	}

	name := fmt.Sprintf("%s%s%s.%d", UmpDataDir, "ump_", lw.logSufixx, lw.seq)
	if _, err = os.Stat(name); err == nil {
		os.Remove(name)
	}
	os.Rename(lw.logName, name)

	if lw.logFp, err = os.OpenFile(lw.logName, LogFileOpt, 0777); err != nil {
		lw.seq--
		return
	}
	os.Chmod(lw.logName, 0777)

	if err = os.Truncate(lw.logName, 0); err != nil {
		lw.seq--
		return
	}
	lw.logSize = 0

	return
}

func (lw *LogWrite) backGroupWriteForGroupByTP() {

	for {
		var (
			body []byte
		)
		for index:=0;index<FunctionTPMapCount;index++{
			FuncationTPMap[index].Range(func(key, value interface{}) bool {
				v := value.(*FunctionTpGroupBy)
				v.Count = strconv.FormatInt(v.count, 10)
				v.Time = v.currTime.Format(LogTimeForMat)
				v.ElapsedTime = strconv.FormatInt(v.elapsedTime/v.count, 10)
				lw.jsonEncoder.Encode(v)
				FunctionTpGroupByPool.Put(v)
				body = append(body, lw.bf.Bytes()...)
				lw.bf.Reset()
				FuncationTPMap[index].Delete(key.(string))
				return true
			})
			time.Sleep(time.Second)
		}
		if lw.backGroundCheckFile() != nil {
			continue
		}
		lw.logFp.Write(body)
		lw.logSize += (int64)(len(body))
		body = make([]byte, 0)
	}
	return
}

func (lw *LogWrite) backGroundWrite(umpType string) {

	for {
		var (
			body []byte
		)
		obj := <-lw.logCh
		switch umpType {
		case FunctionTpType:
			tp := obj.(*FunctionTp)
			lw.jsonEncoder.Encode(tp)
			body = append(body, lw.bf.Bytes()...)
			lw.bf.Reset()
			FunctionTpPool.Put(tp)
		case SystemAliveType:
			alive := obj.(*SystemAlive)
			lw.jsonEncoder.Encode(alive)
			body = append(body, lw.bf.Bytes()...)
			lw.bf.Reset()
			SystemAlivePool.Put(alive)
		case BusinessAlarmType:
			alarm := obj.(*BusinessAlarm)
			lw.jsonEncoder.Encode(alarm)
			body = append(body, lw.bf.Bytes()...)
			lw.bf.Reset()
			AlarmPool.Put(alarm)
		}
		if lw.backGroundCheckFile() != nil {
			continue
		}
		body = append(body, []byte("\n")...)
		lw.logFp.Write(body)
		lw.logSize += (int64)(len(body))
		body = make([]byte, 0)
		if umpType == BusinessAlarmType && atomic.AddInt32(&lw.inflight, -1) <= 0 {
			select {
			case lw.empty <- struct{}{}:
			default:
			}
		}
	}
}

func initLogName(module string) (err error) {
	if err = os.MkdirAll(UmpDataDir, 0777); err != nil {
		return
	}
	os.Chmod(UmpDataDir, 0777)

	if HostName, err = GetLocalIpAddr(); err != nil {
		return
	}

	if err = FunctionTpLogWrite.initLogFp(module + "_" + FunctionTpSufixx); err != nil {
		return
	}
	if err = FunctionTpGroupByLogWrite.initLogFp(module + "_" + FunctionTpGroupBySufixx); err != nil {
		return
	}
	if err = SystemAliveLogWrite.initLogFp(module + "_" + SystemAliveSufixx); err != nil {
		return
	}

	if err = BusinessAlarmLogWrite.initLogFp(module + "_" + BusinessAlarmSufixx); err != nil {
		return
	}

	return
}

func GetLocalIpAddr() (localAddr string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, addr := range addrs {
		if ipNet, isIpNet := addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			if ipv4 := ipNet.IP.To4(); ipv4 != nil {
				localAddr = ipv4.String()
				return
			}
		}
	}
	err = fmt.Errorf("cannot get local ip")
	return
}

func backGroudWrite() {
	go FunctionTpLogWrite.backGroundWrite(FunctionTpType)
	go SystemAliveLogWrite.backGroundWrite(SystemAliveType)
	go BusinessAlarmLogWrite.backGroundWrite(BusinessAlarmType)
	go FunctionTpGroupByLogWrite.backGroupWriteForGroupByTP()
}
