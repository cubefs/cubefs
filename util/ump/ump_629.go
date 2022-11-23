package ump

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/proto"
)

func (lw *LogWrite) backGroupWriteForGroupByTPV629() {
	defer wg.Done()
	for {
		if UmpCollectWay != proto.UmpCollectByFile {
			time.Sleep(10 * time.Second)
		}
		select {
		case <-lw.stopC:
			lw.logFp.Close()
			return
		default:
			var body []byte
			FunctionTPKeyMap.Range(func(key, value interface{}) bool {
				if UmpCollectWay != proto.UmpCollectByFile {
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
			time.Sleep(time.Second)
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
	ticker := time.NewTicker(20 * time.Second)
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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	businessKeyMap := new(sync.Map)

	for {
		var body []byte
		select {
		case <-lw.stopC:
			lw.logFp.Close()
			return
		case businessLog := <-lw.logCh:
			alarmLog := businessLog.(*BusinessAlarm)
			businessKeyMap.Store(alarmLog.Key, alarmLog.Detail)
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
