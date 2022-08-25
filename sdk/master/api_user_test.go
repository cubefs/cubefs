package master

import (
	"github.com/chubaofs/chubaofs/proto"
	"testing"
)

var (
	testUser     = "test_01"
	testTransVol = "unittest_transferVol"

	paramC = &proto.UserCreateParam{ID: testUser, Type: proto.UserTypeNormal}
	paramU = &proto.UserUpdateParam{UserID: testUser}
	paramT = &proto.UserTransferVolParam{
		Volume:  testTransVol,
		UserSrc: testOwner,
		UserDst: testUser,
		Force:   false,
	}
	param1 = proto.NewUserPermUpdateParam(testUser, testTransVol)
	param2 = proto.NewUserPermRemoveParam(testUser, testTransVol)
)

func TestUserAPI(t *testing.T) {
	// create new user
	userInfo, err := testMc.UserAPI().CreateUser(paramC)
	if err != nil {
		t.Errorf("Creat user failed: err(%v), user info(%v)", err, userInfo)
	}
	userInfoArr, err := testMc.UserAPI().ListUsers(testUser)
	if userInfoArr == nil || err != nil {
		t.Fatalf("List users failed: err(%v)", err)
	}
	// create new vol for transfer、perm
	err = testMc.AdminAPI().CreateVolume(testTransVol, testOwner, testMpcount, testDpSize, testCapacity, testReplicas, testMpReplicas,
		testTrashDays, testStoreMode, testFollowerRead, testAutoRepair, testVolWriteMutex, testForceROW, false, testEnableWriteCache, testZoneName, testMpLyout, "", 0, proto.CompactDefaultName, 4, 2, false)
	if err != nil {
		t.Fatalf("create vol failed: err(%v) vol(%v)", err, testTransVol)
	}
	// add perm for the user who doesn't own the vol
	if _, err = testMc.UserAPI().UpdatePolicy(param1); err != nil {
		t.Errorf("Update policy failed: err(%v), user(%v), vol(%v)", err, param1.UserID, param1.Volume)
	}
	if _, err = testMc.UserAPI().UpdateUser(paramU); err != nil {
		t.Errorf("Update user(%v) failed: err(%v)", paramU.UserID, err)
	}
	if _, err = testMc.UserAPI().RemovePolicy(param2); err != nil {
		t.Errorf("Remove policy failed: err(%v), user(%v), vol(%v)", err, param2.UserID, param2.Volume)
	}
	// transfer the vol from owner to others
	if userInfo, err = testMc.UserAPI().TransferVol(paramT); err != nil {
		t.Fatalf("Transfer vol from srcUser(%v) to dstUser(%v) failed: err(%v)", paramT.UserSrc, paramT.UserDst, err)
	}
	if _, err = testMc.UserAPI().GetUserInfo(paramT.UserDst); err != nil {
		t.Errorf("Get user(%v) info failed: err(%v)", paramT.UserDst, err)
	}
	// transfer back
	if userInfo, err = testMc.UserAPI().TransferVol(&proto.UserTransferVolParam{Volume: testTransVol,
		UserSrc: testUser, UserDst: testOwner}); err != nil {
		t.Errorf("Transfer back failed: err(%v)", err)
	}
	// delete the newly created user
	if err = testMc.UserAPI().DeleteUser(testUser); err != nil {
		t.Errorf("Delete user(%v) failed: err(%v)", testUser, err)
	}
	userInfoArr, err = testMc.UserAPI().ListUsers(testUser)
	if len(userInfoArr) > 0 || err != nil {
		t.Errorf("After delete user, List users failed: arr(%v), err(%v)", userInfoArr, err)
	}
	// Delete the assisting vol in unittest
	authKey := calcAuthKey(testOwner)
	if err = testMc.AdminAPI().DeleteVolume(testTransVol, authKey); err != nil {
		t.Errorf("Delete assisting vol(%v) failed: err(%v)", testTransVol, err)
	}
}
