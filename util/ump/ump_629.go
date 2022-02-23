package ump

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	elapsedTimeCountSeparator = ","
	logStartMarker            = "@"
	logFormatVersion          = "629"
)

type LogFormatV629 struct {
	Time     int64               `json:"t"`
	HostName string              `json:"h"`
	AppName  string              `json:"a"`
	Version  string              `json:"v"`
	Logs     []map[string]string `json:"l"`
}

func (lw *LogWrite) backGroupWriteForGroupByTPV629() {
	for {
		var body []byte

		FunctionTPKeyMap.Range(func(key, value interface{}) bool {
			umpLog := &LogFormatV629{
				Time:     time.Now().Unix(),
				HostName: HostName,
				AppName:  AppName,
				Version:  logFormatVersion,
				Logs:     make([]map[string]string, 0),
			}
			v, ok := value.(*sync.Map)
			if !ok {
				return true
			}
			FunctionTPKeyMap.Delete(key)
			elapsedMap := make(map[string]string, 0)
			var elapsedTimeCountStr strings.Builder
			v.Range(func(key1, value1 interface{}) bool {
				elapsedTime := key1.(int64)
				elapsedTimeCountStr.WriteString(strconv.Itoa(int(elapsedTime)))
				elapsedTimeCountStr.WriteString(elapsedTimeCountSeparator)
				tpObj := value1.(*FunctionTpGroupBy)
				elapsedTimeCountStr.WriteString(strconv.Itoa(int(tpObj.count)))
				elapsedTimeCountStr.WriteString(elapsedTimeCountSeparator)
				return true
			})
			timeCount := strings.TrimSuffix(elapsedTimeCountStr.String(), elapsedTimeCountSeparator)
			elapsedMap["e"] = timeCount
			elapsedMap["k"] = key.(string)
			umpLog.Logs = append(umpLog.Logs, elapsedMap)

			err := lw.jsonEncoder.Encode(umpLog)
			if err != nil {
				return true
			}
			body = append(body, logStartMarker...)
			body = append(body, lw.bf.Bytes()...)
			lw.bf.Reset()
			return true
		})
		time.Sleep(time.Second)
		if lw.backGroundCheckFile() != nil {
			continue
		}
		n, _ := lw.logFp.Write(body)
		lw.logSize += (int64)(n)
		body = make([]byte, 0)
	}
}

func (lw *LogWrite) backGroupAliveWriteV629() {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	aliveKeyMap := new(sync.Map)

	for {
		var body []byte
		select {
		case aliveLog := <-lw.logCh:
			alive := aliveLog.(*SystemAlive)
			aliveKeyMap.Store(alive.Key, "")
		case <-ticker.C:
			aliveKeyMap.Range(func(key, value interface{}) bool {
				umpLog := &LogFormatV629{
					Time:     time.Now().Unix(),
					HostName: HostName,
					AppName:  AppName,
					Version:  logFormatVersion,
					Logs:     make([]map[string]string, 0),
				}
				aliveKeyMap.Delete(key)
				keyMap := make(map[string]string, 0)
				keyMap["k"] = key.(string)
				umpLog.Logs = append(umpLog.Logs, keyMap)
				err := lw.jsonEncoder.Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, logStartMarker...)
				body = append(body, lw.bf.Bytes()...)
				lw.bf.Reset()
				return true
			})
			if lw.backGroundCheckFile() != nil {
				continue
			}
			n, _ := lw.logFp.Write(body)
			lw.logSize += (int64)(n)
			body = make([]byte, 0)
		}
	}
}

func (lw *LogWrite) backGroupBusinessWriteV629() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	businessKeyMap := new(sync.Map)

	for {
		var body []byte
		select {
		case businessLog := <-lw.logCh:
			alarmLog := businessLog.(*BusinessAlarm)
			businessKeyMap.Store(alarmLog.Key, alarmLog.Detail)
		case <-ticker.C:
			businessKeyMap.Range(func(key, value interface{}) bool {
				umpLog := &LogFormatV629{
					Time:     time.Now().Unix(),
					HostName: HostName,
					AppName:  AppName,
					Version:  logFormatVersion,
					Logs:     make([]map[string]string, 0),
				}
				businessKeyMap.Delete(key)
				keyMap := make(map[string]string, 0)
				keyMap["k"] = key.(string)
				keyMap["ty"] = "0"
				keyMap["v"] = "0"
				keyMap["d"] = value.(string)
				umpLog.Logs = append(umpLog.Logs, keyMap)
				err := lw.jsonEncoder.Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, logStartMarker...)
				body = append(body, lw.bf.Bytes()...)
				lw.bf.Reset()
				return true
			})
			if lw.backGroundCheckFile() != nil {
				continue
			}
			n, _ := lw.logFp.Write(body)
			lw.logSize += (int64)(n)
			body = make([]byte, 0)
			// Issue a signal to this channel when inflight hits zero.
			if atomic.AddInt32(&lw.inflight, -1) <= 0 {
				select {
				case lw.empty <- struct{}{}:
				default:
				}
			}
		}
	}
}
