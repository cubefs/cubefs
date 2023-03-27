package ump

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
)

func (lw *LogWrite) backGroupWriteForGroupByTPV629() {
	defer wg.Done()
	for {
		if GetUmpCollectWay() != proto.UmpCollectByFile {
			time.Sleep(checkUmpWaySleepTime)
		}
		select {
		case <-lw.stopC:
			lw.logFp.Close()
			return
		default:
			var body []byte
			FunctionTPKeyMap.Range(func(key, value interface{}) bool {
				if GetUmpCollectWay() != proto.UmpCollectByFile {
					return false
				}
				v, ok := value.(*sync.Map)
				if !ok {
					return true
				}
				FunctionTPKeyMap.Delete(key)
				umpLog, _ := functionTPToLogFormat(key.(string), v)
				err := lw.jsonEncoder.Encode(umpLog)
				if err != nil {
					return true
				}
				body = append(body, logStartMarker...)
				body = append(body, lw.bf.Bytes()...)
				lw.bf.Reset()
				return true
			})
			time.Sleep(writeTpSleepTime)
			if lw.backGroundCheckFile() != nil {
				continue
			}
			n, _ := lw.logFp.Write(body)
			lw.logSize += (int64)(n)
			body = make([]byte, 0)
		}
	}
}

func (lw *LogWrite) backGroupAliveWriteV629() {
	defer wg.Done()
	ticker := time.NewTicker(aliveTickerTime)
	defer ticker.Stop()
	aliveKeyMap := new(sync.Map)

	for {
		var body []byte
		select {
		case <-lw.stopC:
			lw.logFp.Close()
			return
		case aliveLog := <-lw.logCh:
			alive := aliveLog.(*SystemAlive)
			aliveKeyMap.Store(alive.Key, "")
		case <-ticker.C:
			aliveKeyMap.Range(func(key, value interface{}) bool {
				aliveKeyMap.Delete(key)
				umpLog := aliveToLogFormat(key.(string))
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
	defer wg.Done()
	ticker := time.NewTicker(alarmTickerTime)
	defer ticker.Stop()
	businessKeyMap := new(sync.Map)
	var (
		body  []byte
		count int
	)

	for {
		select {
		case <-lw.stopC:
			lw.logFp.Close()
			return
		case businessLog := <-lw.logCh:
			alarmLog := businessLog.(*BusinessAlarm)
			businessKeyMap.Store(alarmLog.Key, alarmLog.Detail)
			count++
		case <-ticker.C:
			businessKeyMap.Range(func(key, value interface{}) bool {
				businessKeyMap.Delete(key)
				umpLog := alarmToLogFormat(key.(string), value.(string))
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
				count = 0
				continue
			}
			n, _ := lw.logFp.Write(body)
			lw.logSize += (int64)(n)
			body = make([]byte, 0)
			// Issue a signal to this channel when inflight hits zero.
			if atomic.AddInt32(&lw.inflight, -int32(count)) <= 0 {
				select {
				case lw.empty <- struct{}{}:
				default:
				}
			}
			count = 0
		}
	}
}
