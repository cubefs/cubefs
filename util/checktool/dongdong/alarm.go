package dongdong

import (
	"bytes"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"net"
	"time"
)

const (
	AlarmTimeForMate = "2006-01-02 15:04:05"
)

var (
	HostName string
)

type DDAlarm struct {
	robot *DDRobot
	gid   int
	erp   string
	app   string
}

func NewGroupDDAlarm(gid int, app string) (alarm *DDAlarm, err error) {
	alarm = new(DDAlarm)
	alarm.gid = gid
	alarm.app = app
	alarm.robot, err = NewChinaDDRobot(app)
	if err != nil {
		log.LogErrorf("failed to init china robot for app: %v, err: %v", app, err.Error())
	}
	return
}

func NewERPDDAlarm(erp string, app string) (alarm *DDAlarm, err error) {
	alarm = new(DDAlarm)
	alarm.erp = erp
	alarm.robot, err = NewChinaDDRobot(app)
	if err != nil {
		log.LogErrorf("failed to init china robot for app: %v, err: %v", app, err.Error())
	}
	return
}

func (alarm *DDAlarm) alarmToGroup(key, detail string) (err error) {
	msg := buildAlarmMessage(alarm.app, key, detail)
	return alarm.robot.SendGroupMsg(alarm.gid, msg)
}

func (alarm *DDAlarm) alarmToERP(key, detail string) (err error) {
	msg := buildAlarmMessage(alarm.app, key, detail)
	return alarm.robot.SendERPMsg(alarm.erp, msg)
}

func buildAlarmMessage(app, key, detail string) string {
	if len(detail) > 512 {
		rs := []rune(detail)
		detail = string(rs[0:510])
	}
	buff := bytes.NewBuffer(nil)
	buff.Grow(128)
	buff.WriteString(fmt.Sprintf("【警告】%v\n", detail))
	buff.WriteString(fmt.Sprintf("【时间】%v\n", time.Now().Format(AlarmTimeForMate)))
	buff.WriteString(fmt.Sprintf("【主机】%v\n", HostName))
	buff.WriteString(fmt.Sprintf("【应用】%v\n", app))
	if key != "" {
		buff.WriteString(fmt.Sprintf("【采集点】%v\n", key))
	}
	return buff.String()
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
