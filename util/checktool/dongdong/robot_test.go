package dongdong

import (
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"testing"
	"time"
)

func TestNewChinaDDRobot(t *testing.T) {
	_, err := NewChinaDDRobot("test")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	time.Sleep(1 * time.Minute)
	log.LogFlush()
}

func TestDDRobot_SendERPMsg(t *testing.T) {
	robot, err := NewChinaDDRobot("test")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	err = robot.SendERPMsg("zhangzhenshan3", "test message from robot")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	time.Sleep(20 * time.Second)
	log.LogFlush()
}

func TestDDRobot_SendERPMsg2(t *testing.T) {
	robot, err := NewChinaDDRobot("test")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	for i := 0; i < 20; i++ {
		err = robot.SendERPMsg("zhangzhenshan3", fmt.Sprintf("test message from robot: %v", i))
		if err != nil {
			t.Error(err.Error())
			t.FailNow()
		}
		time.Sleep(1 * time.Second)
		fmt.Println(i)
	}
	time.Sleep(1 * time.Hour)
	log.LogFlush()
}

func TestDDRobot_SendGroupMsg(t *testing.T) {
	robot, err := NewChinaDDRobot("test")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	err = robot.SendGroupMsg(1025538441, "test message from robot")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	time.Sleep(10 * time.Second)
	log.LogFlush()
}
