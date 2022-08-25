package master

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/chubaofs/chubaofs/proto"
	"strings"
	"testing"
)

var (
	testVolName              = "unittestVol"
	testOwner                = "test"
	testMpcount              = 3
	testDpSize        uint64 = 120
	testCapacity      uint64 = 1
	testReplicas             = 3
	testTrashDays            = 10
	testMpReplicas           = 3
	testFollowerRead         = true
	testForceROW             = false
	testIsSmart              = false
	testEnableWriteCache	 = false
	testAutoRepair           = false
	testVolWriteMutex        = false
	testZoneName             = "default"
	testMc                   = NewMasterClient([]string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"}, false)
	testStoreMode            = 1
	testMpLyout              = "0,0"
)

func TestVolCreate(t *testing.T) {
	err := testMc.AdminAPI().CreateVolume(testVolName, testOwner, testMpcount, testDpSize, testCapacity,
		testReplicas, testMpReplicas, testTrashDays, testStoreMode, 0, testFollowerRead, testAutoRepair, testVolWriteMutex, testForceROW, testIsSmart, testEnableWriteCache, false, testZoneName, testMpLyout, "",0, proto.CompactDefaultName, 4, 2, false)
	if err != nil {
		t.Errorf("create vol failed: err(%v) vol(%v)", err, testVolName)
	}
	defaultVol := "defaultVol"
	err = testMc.AdminAPI().CreateDefaultVolume(defaultVol, testOwner)
	if err != nil && !strings.Contains(err.Error(), "duplicate vol"){
		t.Errorf("create vol failed: err(%v) vol(%v)", err, defaultVol)
	}
}

func TestUpdateVol(t *testing.T) {
	authKey := calcAuthKey(testOwner)
	extentCap := uint64(2)
	updateFollowerRead := false
	trashDays := 15
	err := testMc.AdminAPI().UpdateVolume(testVolName, extentCap, testReplicas, testMpReplicas, trashDays, testStoreMode, 0,
		updateFollowerRead, false, false, false, false, false, false, false, false, authKey, testZoneName,
		testMpLyout, "", 0, 0, 0, "default")
	if err != nil {
		t.Errorf("update vol failed: err(%v) vol(%v)", err, testVolName)
		t.FailNow()
	}
	vol, err := testMc.ClientAPI().GetVolume(testVolName, authKey)
	if err != nil {
		t.Errorf("GetVolume failed: err(%v) vol(%v) expectSet(%v) actualSet(%v)", err, testVolName, vol, updateFollowerRead)
		t.FailNow()
	}
	volInfo, err := testMc.AdminAPI().GetVolumeSimpleInfo(testVolName)
	if err != nil || volInfo.Name != testVolName {
		t.Errorf("get vol info failed: err(%v) vol(%v)", err, testVolName)
		t.FailNow()
	}
	if volInfo.TrashRemainingDays != uint32(trashDays) {
		t.Errorf("trashDays(%v) is not expected(%v)", volInfo.TrashRemainingDays, trashDays)
	}
	volStat, err := testMc.ClientAPI().GetVolumeStat(testVolName)
	if err != nil || volStat.TotalSize != extentCap*1024*1024*1024 {
		t.Errorf("GetVolumeStat failed: err(%v) vol(%v) expectCap(%v) actualCap(%v)", err, testVolName, volStat, extentCap*1024*1024*1024)
	}
	vol, err = testMc.ClientAPI().GetVolumeWithoutAuthKey(testVolName)
	if err != nil {
		t.Errorf("GetVolumeWithoutAuthKey failed: err(%v) vol(%v) expectSet(%v) actualSet(%v)", err, testVolName, vol, updateFollowerRead)
	}
}

func TestDeleteVol(t *testing.T) {
	authKey := calcAuthKey(testOwner)
	err := testMc.AdminAPI().DeleteVolume(testVolName, authKey)
	if err != nil {
		t.Errorf("delete vols failed: err(%v) vol(%v)", err, testVolName)
	}
	vols, err := testMc.AdminAPI().ListVols("")
	if err != nil || len(vols) <= 0 {
		t.Errorf("list vols failed: err(%v) expect vol length larger than 0 but(%v)", err, len(vols))
	}
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}

func TestVolCreateAndUpdate_InnerDataCase01(t *testing.T) {
	volName := "testInnerDataCase01"
	authKey := calcAuthKey(testOwner)
	extentCap := uint64(2)
	updateFollowerRead := false
	trashDays := 15
	err := testMc.AdminAPI().CreateVolume(volName, testOwner, testMpcount, testDpSize, testCapacity,
		testReplicas, testMpReplicas, testTrashDays, int(proto.StoreModeMem), 0, testFollowerRead,
		testAutoRepair, testVolWriteMutex, testForceROW, false, false, true, testZoneName, testMpLyout, "", 0, "default", 4, 2, false)
	if err == nil {
		t.Errorf("expect error is failed, inner data can not be enabled when volume default store mode is memory")
		t.FailNow()
	}

	err = testMc.AdminAPI().CreateVolume(volName, testOwner, testMpcount, testDpSize, testCapacity,
		testReplicas, testMpReplicas, testTrashDays, int(proto.StoreModeMem), 4096, testFollowerRead,
		testAutoRepair, testVolWriteMutex, testForceROW, false, false, false, testZoneName, testMpLyout, "", 0, "default", 4, 2, false)
	if err == nil {
		t.Errorf("expect error is failed, inner size can not be set when inner data disbaled")
		t.FailNow()
	}

	err = testMc.AdminAPI().CreateVolume(volName, testOwner, testMpcount, testDpSize, testCapacity,
		testReplicas, testMpReplicas, testTrashDays, int(proto.StoreModeRocksDb), 4096, testFollowerRead,
		testAutoRepair, testVolWriteMutex, testForceROW, false, false, false, testZoneName, testMpLyout, "", 0, "default", 4, 2, false)
	if err == nil {
		t.Errorf("expect error is failed, inner size can not be set when inner data disbaled")
		t.FailNow()
	}

	err = testMc.AdminAPI().CreateVolume(volName, testOwner, testMpcount, testDpSize, testCapacity,
		testReplicas, testMpReplicas, testTrashDays, int(proto.StoreModeMem), 0, testFollowerRead,
		testAutoRepair, testVolWriteMutex, testForceROW, false, false, false, testZoneName, testMpLyout, "", 0, "default", 4, 2, false)
	if err != nil {
		t.Errorf("create volume failed, err(%v), vol(%v)", err, testVolName)
		t.FailNow()
	}

	err = testMc.AdminAPI().UpdateVolume(volName, extentCap, testReplicas, testMpReplicas, trashDays, int(proto.StoreModeMem), 0,
		updateFollowerRead, false, false, false, false, false, false, false, true, authKey, testZoneName,
		testMpLyout, "", 0, 0, 0, "default")
	if err == nil {
		t.Errorf("expect error is failed, inner data can not be enabled when volume default store mode is memory")
		t.FailNow()
	}

	err = testMc.AdminAPI().UpdateVolume(volName, extentCap, testReplicas, testMpReplicas, trashDays, int(proto.StoreModeMem), 4096,
		updateFollowerRead, false, false, false, false, false, false, false, false, authKey, testZoneName,
		testMpLyout, "", 0, 0, 0, "default")
	if err == nil {
		t.Errorf("expect error is failed, inner size can not be set when inner data flag disabled")
		t.FailNow()
	}

	err = testMc.AdminAPI().UpdateVolume(volName, extentCap, testReplicas, testMpReplicas, trashDays, int(proto.StoreModeRocksDb), 0,
		updateFollowerRead, false, false, false, false, false, false, false, false, authKey, testZoneName,
		testMpLyout, "", 0, 0, 0, "default")
	if err != nil {
		t.Errorf("update volume default store mode failed, err(%v), vol(%v)", err, volName)
		t.FailNow()
	}

	err = testMc.AdminAPI().UpdateVolume(volName, extentCap, testReplicas, testMpReplicas, trashDays, int(proto.StoreModeRocksDb), 0,
		updateFollowerRead, false, false, false, false, false, false, false, true, authKey, testZoneName,
		testMpLyout, "", 0, 0, 0, "default")
	if err == nil {
		t.Errorf("expect error is failed, volume container mem mode mp")
		t.FailNow()
	}

	var vv *proto.SimpleVolView
	vv, err = testMc.adminAPI.GetVolumeSimpleInfo(volName)
	if err != nil {
		t.Errorf("get volume simple info failed, err(%v), volume(%s)", err, volName)
		t.FailNow()
	}

	if vv.DefaultStoreMode != proto.StoreModeRocksDb || vv.EnableInnerData || vv.InnerSize != 0 {
		t.Errorf("volume config mismatch, expect(defaultStoreMode:%v, enableInnerData:true, innerSize:4096)," +
			" actual(defaultStoreMode:%v, enableInnerData:%v, innerSize:%v)", proto.StoreModeRocksDb, vv.DefaultStoreMode,
			vv.EnableInnerData, vv.InnerSize)
		t.FailNow()
	}

	err = testMc.AdminAPI().DeleteVolume(volName, authKey)
	if err != nil {
		t.Errorf("delete vols failed: err(%v) vol(%v)", err, volName)
		t.FailNow()
	}
}

func TestVolCreateAndUpdate_InnerDataCase02(t *testing.T) {
	volName := "testInnerDataCase02"
	authKey := calcAuthKey(testOwner)
	extentCap := uint64(2)
	updateFollowerRead := false
	trashDays := 15
	err := testMc.AdminAPI().CreateVolume(volName, testOwner, testMpcount, testDpSize, testCapacity,
		testReplicas, testMpReplicas, testTrashDays, int(proto.StoreModeRocksDb), 4096, testFollowerRead,
		testAutoRepair, testVolWriteMutex, testForceROW, false, false, false, testZoneName, testMpLyout, "", 0, "default", 4, 2, false)
	if err == nil {
		t.Errorf("expect error is failed, inner size can not be set when inner data flag disabled")
		t.FailNow()
	}

	err = testMc.AdminAPI().CreateVolume(volName, testOwner, testMpcount, testDpSize, testCapacity,
		testReplicas, testMpReplicas, testTrashDays, int(proto.StoreModeRocksDb), 0, testFollowerRead,
		testAutoRepair, testVolWriteMutex, testForceROW, false, false, false, testZoneName, testMpLyout, "", 0, "default", 4, 2, false)
	if err != nil {
		t.Errorf("create volume failed, err(%v), vol(%s)", err, volName)
		t.FailNow()
	}

	err = testMc.AdminAPI().UpdateVolume(volName, extentCap, testReplicas, testMpReplicas, trashDays, int(proto.StoreModeRocksDb), 4096,
		updateFollowerRead, false, false, false, false, false, false, false, true, authKey, testZoneName,
		testMpLyout, "", 0, 0, 0, "default")
	if err != nil {
		t.Errorf("update volume failed, err(%v), vol(%s)", err, volName)
		t.FailNow()
	}

	err = testMc.AdminAPI().UpdateVolume(volName, extentCap, testReplicas, testMpReplicas, trashDays, int(proto.StoreModeRocksDb), 1024,
		updateFollowerRead, false, false, false, false, false, false, false,true, authKey, testZoneName,
		testMpLyout, "", 0, 0, 0, "default")
	if err != nil {
		t.Errorf("update volume failed, err(%v), vol(%s)", err, volName)
		t.FailNow()
	}

	err = testMc.AdminAPI().UpdateVolume(volName, extentCap, testReplicas, testMpReplicas, trashDays, int(proto.StoreModeMem), 0,
		updateFollowerRead, false, false, false, false, false, false, false, false, authKey, testZoneName,
		testMpLyout, "", 0, 0, 0, "default")
	if err == nil {
		t.Errorf("expect error is failed, if inner data has been enabled, it is not allowed to disable")
		t.FailNow()
	}

	var vv *proto.SimpleVolView
	vv, err = testMc.adminAPI.GetVolumeSimpleInfo(volName)
	if err != nil {
		t.Errorf("get volume simple info failed, err(%v), volume(%s)", err, volName)
		t.FailNow()
	}
	if vv.DefaultStoreMode != proto.StoreModeRocksDb || !vv.EnableInnerData || vv.InnerSize != 1024 {
		t.Errorf("volume config mismatch, expect(defaultStoreMode:%v, enableInnerData:true, innerSize:4096)," +
			" actual(defaultStoreMode:%v, enableInnerData:%v, innerSize:%v)", proto.StoreModeRocksDb, vv.DefaultStoreMode,
			vv.EnableInnerData, vv.InnerSize)
		t.FailNow()
	}

	err = testMc.AdminAPI().DeleteVolume(volName, authKey)
	if err != nil {
		t.Errorf("delete vols failed: err(%v) vol(%v)", err, volName)
		t.FailNow()
	}
}
