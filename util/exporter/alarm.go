package exporter

import (
	"fmt"
	"sync"
)

var (
	AlarmPool = &sync.Pool{New: func() interface{} {
		return new(Alarm)
	}}
	//AlarmGroup  sync.Map
	AlarmCh = make(chan *Alarm, ChSize)
)

func collectAlarm() {
	for {
		m := <-AlarmCh
		AlarmPool.Put(m)
	}
}

type Alarm struct {
	Counter
}

func NewAlarm(name string) (a *Alarm) {
	if !enabled {
		return
	}
	a = AlarmPool.Get().(*Alarm)
	a.name = metricsName(fmt.Sprintf("%s_alarm", name))
	a.Add(1)
	return
}

func (c *Alarm) publish() {
	select {
	case AlarmCh <- c:
	default:
	}
}
