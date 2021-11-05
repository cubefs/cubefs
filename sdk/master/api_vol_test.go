package master

import (
	"crypto/md5"
	"encoding/hex"
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
	testMpReplicas           = 3
	testFollowerRead         = true
	testForceROW			 = false
	testAutoRepair           = false
	testVolWriteMutex        = false
	testZoneName             = "default"
	testMc                   = NewMasterClient([]string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"}, false)
)

func TestVolCreate(t *testing.T) {
	err := testMc.AdminAPI().CreateVolume(testVolName, testOwner, testMpcount, testDpSize, testCapacity,
		testReplicas, testMpReplicas, testFollowerRead, testAutoRepair, testVolWriteMutex, testForceROW, testZoneName, 0)
	if err != nil {
		t.Errorf("create vol failed: err(%v) vol(%v)", err, testVolName)
	}
	defaultVol := "defaultVol"
	err = testMc.AdminAPI().CreateDefaultVolume(defaultVol, testOwner)
	if err != nil {
		t.Errorf("create vol failed: err(%v) vol(%v)", err, defaultVol)
	}
}

func TestUpdateVol(t *testing.T) {
	authKey := calcAuthKey(testOwner)
	extentCap := uint64(2)
	updateFollowerRead := false
	err := testMc.AdminAPI().UpdateVolume(testVolName, extentCap, testReplicas, testMpReplicas, updateFollowerRead, false, false, false, false, authKey, testZoneName, 0, 0)
	if err != nil {
		t.Errorf("update vol failed: err(%v) vol(%v)", err, testVolName)
	}
	vol, err := testMc.ClientAPI().GetVolume(testVolName, authKey)
	if err != nil {
		t.Errorf("GetVolume failed: err(%v) vol(%v) expectSet(%v) actualSet(%v)", err, testVolName, vol, updateFollowerRead)
	}
	volInfo, err := testMc.AdminAPI().GetVolumeSimpleInfo(testVolName)
	if err != nil || volInfo.Name != testVolName {
		t.Errorf("get vol info failed: err(%v) vol(%v)", err, testVolName)
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
