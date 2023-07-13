package dongdong

import (
	"fmt"
	"testing"
	"time"
)

func TestNewCheckToolAlarm(t *testing.T) {
	_, err := NewCommonAlarm("storage_monitor")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	time.Sleep(1 * time.Hour)
}

func TestCheckToolAlarm_Alarm(t *testing.T) {
	alarm, err := NewCommonAlarm("storage_monitor")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	for i := 0; i < 5; i++ {
		alarm.Alarm("oss_upload", fmt.Sprintf("-----system error: %v, time: %v", i, time.Now().Format(AlarmTimeForMate)))
	}
	time.Sleep(1 * time.Hour)
}
