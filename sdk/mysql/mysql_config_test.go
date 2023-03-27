package mysql

import (
	"github.com/cubefs/cubefs/proto"
	"testing"
)

func TestScheduleConfig(t *testing.T) {
	var err error
	var scs []*proto.ScheduleConfig
	var sc1Exist, sc2Exist bool

	// add new schedule config
	ct := proto.ScheduleConfigTypeMigrateThreshold
	ck := "smartVolume1"
	cv := "0.3"
	sc1 := proto.NewScheduleConfig(ct, ck, cv)

	if err = AddScheduleConfig(sc1); err != nil {
		t.Fatalf(err.Error())
	}

	cvGlobal := "0.5"
	sc2 := proto.NewScheduleConfig(ct, proto.ScheduleConfigMigrateThresholdGlobalKey, cvGlobal)
	if err = AddScheduleConfig(sc2); err != nil {
		t.Fatalf(err.Error())
	}

	// delete
	defer func() {
		if err = DeleteScheduleConfig(sc1); err != nil {
			t.Errorf(err.Error())
		}
		if err = DeleteScheduleConfig(sc2); err != nil {
			t.Errorf(err.Error())
		}
	}()

	// select
	if scs, err = SelectScheduleConfig(ct); err != nil {
		t.Fatalf(err.Error())
	}
	for _, sc := range scs {
		if sc.ConfigType == ct && sc.ConfigKey == ck && sc.ConfigValue == cv {
			sc1Exist = true
		}
		if sc.ConfigType == ct && sc.ConfigKey == proto.ScheduleConfigMigrateThresholdGlobalKey && sc.ConfigValue == cvGlobal {
			sc2Exist = true
		}
	}
	if !sc1Exist {
		t.Fatalf("sc1 not found")
	}
	if !sc2Exist {
		t.Fatalf("sc2 not found")
	}

	// update
	cvNew := "0.6"
	sc1.ConfigValue = cvNew
	if err = UpdateScheduleConfig(sc1); err != nil {
		t.Fatalf(err.Error())
	}

	// select again
	sc1Exist = false
	if scs, err = SelectScheduleConfig(ct); err != nil {
		t.Fatalf(err.Error())
	}
	for _, sc := range scs {
		if sc.ConfigType == ct && sc.ConfigKey == ck && sc.ConfigValue == cvNew {
			sc1Exist = true
		}
	}
	if !sc1Exist {
		t.Fatalf("sc1 not found after update")
	}
}
