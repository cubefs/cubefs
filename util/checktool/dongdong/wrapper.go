package dongdong

type CommonAlarm struct {
	app   string
	gid   int
	alarm *DDAlarm
}

func NewCommonAlarm(app string) (alarm *CommonAlarm, err error) {
	alarm = new(CommonAlarm)
	gid := 1025258503
	alarm.app = app
	alarm.gid = gid
	alarm.alarm, err = NewGroupDDAlarm(gid, app)
	return
}

func (alarm *CommonAlarm) Alarm(key, detail string) (err error) {
	return alarm.alarm.alarmToGroup(key, detail)
}

func NewCommonAlarmWithGid(gid int, app string) (alarm *CommonAlarm, err error) {
	alarm = new(CommonAlarm)
	alarm.gid = gid
	alarm.app = app
	alarm.alarm, err = NewGroupDDAlarm(gid, app)
	return
}
