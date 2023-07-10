package dongdong

import (
	"testing"
	"time"
)

func TestRefreshToken(t *testing.T) {
	api := NewDDAPI(DDDomain, DDKey, DDSecret, "ee")
	if api == nil {
		t.FailNow()
	}
	time.Sleep(2 * time.Hour)
}

func TestSendERPTextMsg(t *testing.T) {
	api := NewDDAPI(DDDomain, DDKey, DDSecret, "ee")
	if api == nil {
		t.FailNow()
	}
	err := api.SendMsgToERP("zhangzhenshan3", "test msg from robot")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
}

func TestSendGroupTextMsg(t *testing.T) {
	api := NewDDAPI(DDDomain, DDKey, DDSecret, "ee")
	if api == nil {
		t.FailNow()
	}
	err := api.SendMsgToGroup(1025538441, "test msg from robot")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	time.Sleep(5 * time.Second)
}
