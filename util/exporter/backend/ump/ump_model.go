package ump

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	logStartMarker            = "@"
	logFormatVersion          = "629"
	elapsedTimeCountSeparator = ","
)

type FunctionTp struct {
	Time         string `json:"time"`
	Key          string `json:"key"`
	HostName     string `json:"hostname"`
	ProcessState string `json:"processState"`
	ElapsedTime  string `json:"elapsedTime"`
}

type FunctionTpGroupBy struct {
	//currTime     time.Time
	//Time         string
	Key string
	//HostName     string
	ProcessState string
	//ElapsedTime  string
	//Count        string

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

type LogFormatV629 struct {
	Time     int64               `json:"t"`
	HostName string              `json:"h"`
	AppName  string              `json:"a"`
	Version  string              `json:"v"`
	Logs     []map[string]string `json:"l"`
}

func aliveToLogFormat(key string) *LogFormatV629 {
	umpLog := &LogFormatV629{
		Time:     time.Now().Unix(),
		HostName: HostName,
		AppName:  AppName,
		Version:  logFormatVersion,
		Logs:     make([]map[string]string, 0),
	}
	keyMap := make(map[string]string, 0)
	keyMap["k"] = key
	umpLog.Logs = append(umpLog.Logs, keyMap)
	return umpLog
}

func alarmToLogFormat(key string, val string) *LogFormatV629 {
	umpLog := &LogFormatV629{
		Time:     time.Now().Unix(),
		HostName: HostName,
		AppName:  AppName,
		Version:  logFormatVersion,
		Logs:     make([]map[string]string, 0),
	}
	keyMap := make(map[string]string, 0)
	keyMap["k"] = key
	keyMap["ty"] = "0"
	keyMap["v"] = "0"
	keyMap["d"] = val
	umpLog.Logs = append(umpLog.Logs, keyMap)
	return umpLog
}

func functionTPToLogFormat(key string, val *sync.Map) (*LogFormatV629, int) {
	umpLog := &LogFormatV629{
		Time:     time.Now().Unix(),
		HostName: HostName,
		AppName:  AppName,
		Version:  logFormatVersion,
		Logs:     make([]map[string]string, 0),
	}
	elapsedMap := make(map[string]string, 0)
	var elapsedTimeCountStr strings.Builder
	count := 0
	val.Range(func(key1, value1 interface{}) bool {
		elapsedTime := key1.(int64)
		elapsedTimeCountStr.WriteString(strconv.Itoa(int(elapsedTime)))
		elapsedTimeCountStr.WriteString(elapsedTimeCountSeparator)
		tpObj := value1.(*FunctionTpGroupBy)
		elapsedTimeCountStr.WriteString(strconv.Itoa(int(tpObj.count)))
		count += int(tpObj.count)
		elapsedTimeCountStr.WriteString(elapsedTimeCountSeparator)
		return true
	})
	timeCount := strings.TrimSuffix(elapsedTimeCountStr.String(), elapsedTimeCountSeparator)
	elapsedMap["e"] = timeCount
	elapsedMap["k"] = key
	umpLog.Logs = append(umpLog.Logs, elapsedMap)
	return umpLog, count
}
