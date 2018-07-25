package ump

import (
	"runtime"
	"strconv"
	"sync"
	"time"
)

type TpObject struct {
	startTime time.Time
	endTime   time.Time
	umpType   interface{}
}

func NewTpObject() (o *TpObject) {
	o = new(TpObject)
	o.startTime = time.Now()
	return
}

const (
	TpMethod        = "TP"
	HeartbeatMethod = "Heartbeat"
	FunctionError   = "FunctionError"
)

var (
	HostName      string
	LogTimeForMat = "20060102150405000"
	AlarmPool     = &sync.Pool{New: func() interface{} {
		return new(BusinessAlarm)
	}}
	TpObjectPool = &sync.Pool{New: func() interface{} {
		return new(TpObject)
	}}
	SystemAlivePool = &sync.Pool{New: func() interface{} {
		return new(SystemAlive)
	}}
	FunctionTpPool = &sync.Pool{New: func() interface{} {
		return new(FunctionTp)
	}}
)

func InitUmp(module string) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	if err := initLogName(module); err != nil {
		panic("init UMP Monitor failed " + err.Error())
	}

	backGroudWrite()
}

func BeforeTP(key string) (o *TpObject) {
	o = TpObjectPool.Get().(*TpObject)
	o.startTime = time.Now()
	tp := FunctionTpPool.Get().(*FunctionTp)
	tp.HostName = HostName
	tp.Time = time.Now().Format(LogTimeForMat)
	tp.Key = key
	tp.ProcessState = "0"
	o.umpType = tp

	return
}

func AfterTP(o *TpObject, err error) {
	tp := o.umpType.(*FunctionTp)
	tp.ElapsedTime = strconv.FormatInt((int64)(time.Since(o.startTime)/1e6), 10)
	TpObjectPool.Put(o)
	tp.ProcessState = "0"
	if err != nil {
		tp.ProcessState = "1"
	}
	select {
	case FunctionTpLogWrite.logCh <- tp:
	default:
	}

	return
}

func Alive(key string) {
	alive := SystemAlivePool.Get().(*SystemAlive)
	alive.HostName = HostName
	alive.Key = key
	alive.Time = time.Now().Format(LogTimeForMat)
	select {
	case SystemAliveLogWrite.logCh <- alive:
	default:
	}
	return
}

func Alarm(key, detail string) {
	alarm := AlarmPool.Get().(*BusinessAlarm)
	alarm.Time = time.Now().Format(LogTimeForMat)
	alarm.Key = key
	alarm.HostName = HostName
	alarm.BusinessType = "0"
	alarm.Value = "0"
	alarm.Detail = detail
	if len(alarm.Detail) > 512 {
		rs := []rune(detail)
		alarm.Detail = string(rs[0:510])
	}

	select {
	case BusinessAlarmLogWrite.logCh <- alarm:
	default:
	}
	return
}
