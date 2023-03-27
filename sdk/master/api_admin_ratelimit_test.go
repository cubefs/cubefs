package master

import (
	"github.com/cubefs/cubefs/proto"
	"testing"
)

func TestGetLimitInfo(t *testing.T) {
	info, err := testMc.AdminAPI().GetLimitInfo("")
	if err != nil {
		t.Fatalf("GetLimitInfo failed, info %v, err %v", info, err)
	}
}

func TestSetClientReaddirOpRateLimit(t *testing.T) {
	testVolName := "ltptest"
	info := proto.RateLimitInfo{
		Volume: 			testVolName,
		Opcode:				0x26,
		ClientVolOpRate: 	-1,
		MetaNodeReqRate            : -2,
		MetaNodeReqOpRate          : -2,
		DataNodeRepairTaskCount    : -2,
		DataNodeRepairTaskSSDZone  : -2,
		DataNodeReqRate            : -2,
		DataNodeReqOpRate          : -2,
		DataNodeReqVolOpRate       : -2,
		DataNodeReqVolPartRate     : -2,
		DataNodeReqVolOpPartRate   : -2,
		ClientReadVolRate          : -2,
		ClientWriteVolRate         : -2,
		ExtentMergeIno             : "",
		ExtentMergeSleepMs         : -2,
		ClientReadRate             : -2,
		ClientWriteRate            : -2,
		ObjectVolActionRate        : -2,
		DnFixTinyDeleteRecordLimit : -2,
		DataNodeRepairTaskZoneCount: -2,
		MetaNodeDumpWaterLevel     : -2,
	}
	err := testMc.AdminAPI().SetRateLimit(&info)
	if err != nil {
		t.Fatalf("Set readdir rate limit failed, err %v", err)
	}
}