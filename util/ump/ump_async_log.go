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
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

type FunctionTp struct {
	Time         string `json:"time"`
	Key          string `json:"key"`
	HostName     string `json:"hostname"`
	ProcessState string `json:"processState"`
	ElapsedTime  string `json:"elapsedTime"`
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
	FunctionTpSufixx    = "tp.log"
	SystemAliveSufixx   = "alive.log"
	BusinessAlarmSufixx = "business.log"
	LogFileOpt          = os.O_RDWR | os.O_CREATE | os.O_APPEND
	ChSize              = 102400
	BusinessAlarmType   = "BusinessAlarm"
	SystemAliveType     = "SystemAlive"
	FunctionTpType      = "FunctionTp"
	HostNameFile        = "/proc/sys/kernel/hostname"
	MaxLogSize          = 1024 * 1024 * 10
)

var (
	FunctionTpLogWrite    = &LogWrite{logCh: make(chan interface{}, ChSize)}
	SystemAliveLogWrite   = &LogWrite{logCh: make(chan interface{}, ChSize)}
	BusinessAlarmLogWrite = &LogWrite{logCh: make(chan interface{}, ChSize)}
	UmpDataDir            = "/export/home/tomcat/UMP-Monitor/logs/"
)

type LogWrite struct {
	logCh     chan interface{}
	logName   string
	logSize   int64
	seq       int
	logSufixx string
	logFp     *os.File
	sigCh     chan bool
}

func (lw *LogWrite) initLogFp(sufixx string) (err error) {
	var fi os.FileInfo
	lw.seq = 0
	lw.sigCh = make(chan bool, 1)
	lw.logSufixx = sufixx
	lw.logName = fmt.Sprintf("%s%s%s", UmpDataDir, "ump_", lw.logSufixx)
	if lw.logFp, err = os.OpenFile(lw.logName, LogFileOpt, 0666); err != nil {
		return
	}
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

	if lw.logFp, err = os.OpenFile(lw.logName, LogFileOpt, 0666); err != nil {
		lw.seq--
		return
	}
	if err = os.Truncate(lw.logName, 0); err != nil {
		lw.seq--
		return
	}
	lw.logSize = 0

	return
}

func (lw *LogWrite) backGroundWrite(umpType string) {
	var (
		body []byte
	)

	for {
		obj := <-lw.logCh
		switch umpType {
		case FunctionTpType:
			tp := obj.(*FunctionTp)
			body, _ = json.Marshal(tp)
			FunctionTpPool.Put(tp)
		case SystemAliveType:
			alive := obj.(*SystemAlive)
			body, _ = json.Marshal(alive)
			SystemAlivePool.Put(alive)
		case BusinessAlarmType:
			alarm := obj.(*BusinessAlarm)
			body, _ = json.Marshal(alarm)
			AlarmPool.Put(alarm)
		}
		if lw.backGroundCheckFile() != nil {
			continue
		}
		body = append(body, []byte("\n")...)
		lw.logFp.Write(body)
		lw.logSize += (int64)(len(body))
	}
}

func initLogName(module, dataDir string) (err error) {
	if dataDir != "" {
		UmpDataDir = dataDir
		if !strings.HasSuffix(UmpDataDir, "/") {
			UmpDataDir += "/"
		}
	}
	if err = os.MkdirAll(UmpDataDir, 0666); err != nil {
		return
	}

	if HostName, err = GetLocalIpAddr(); err != nil {
		return
	}

	if err = FunctionTpLogWrite.initLogFp(module + "_" + FunctionTpSufixx); err != nil {
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
}
