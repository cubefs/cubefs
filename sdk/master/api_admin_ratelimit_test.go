package master

import (
	"github.com/chubaofs/chubaofs/proto"
	"testing"
)

func TestGetLimitInfo(t *testing.T) {
	info, err := testMc.AdminAPI().GetLimitInfo("")
	if err != nil {
		t.Fatalf("GetLimitInfo failed, info %v, err %v", info, err)
	}
}

func TestSetRateLimit(t *testing.T) {
	testVolName := "ltptest"
	info := proto.RateLimitInfo{Volume: testVolName}
	err := testMc.AdminAPI().SetRateLimit(&info)
	if err != nil {
		t.Fatalf("SetRateLimit failed, err %v", err)
	}
}