// Copyright 2018 The ChubaoFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"testing"

	"github.com/chubaofs/chubaofs/objectnode"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/oss"
)

var masters = []string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"}
var mc = master.NewMasterClient(masters, false)

func TestOSSAPI(t *testing.T) {
	var (
		akPolicy1 *oss.AKPolicy
		akPolicy2 *oss.AKPolicy
		err       error
	)

	// create user
	if akPolicy1, err = mc.OSSAPI().CreateUser("testuser1"); err != nil {
		panic(err)
	}
	if akPolicy1, err = mc.OSSAPI().CreateUser("testuser1"); err != nil {
		//fmt.Printf(err.Error())
	}
	if akPolicy2, err = mc.OSSAPI().CreateUserWithKey("testUser2", akPolicy1.AccessKey, akPolicy1.SecretKey); err != nil {
		//fmt.Printf(err.Error())
	}
	if akPolicy2, err = mc.OSSAPI().CreateUserWithKey("testUser2", "0123456789acsdef", "0123456789acsdefwewe"); err != nil {
		//fmt.Printf(err.Error())
	}
	if akPolicy2, err = mc.OSSAPI().CreateUserWithKey("testUser2", "0123456789acsdefwewe", "0123456789acsdefwewe123456ygfdwe"); err != nil {
		//fmt.Printf(err.Error())
	}
	if akPolicy2, err = mc.OSSAPI().CreateUserWithKey("testUser2", "0123456789acsdef", "0123456789acsdefwewe123456ygfdwe"); err != nil {
		panic(err.Error())
	}

	// add policy
	userPolicy1 := &oss.UserPolicy{OwnVol: []string{"testvol1"}}
	if akPolicy1, err = mc.OSSAPI().AddPolicy(akPolicy1.AccessKey, userPolicy1); err != nil {
		panic(err.Error())
	}
	//fmt.Printf("after add policy: [%v]", akPolicy1)
	userPolicy2 := &oss.UserPolicy{NoneOwnVol: map[string][]string{"testvol2": {objectnode.GetObjectAclAction}}}
	if akPolicy1, err = mc.OSSAPI().AddPolicy(akPolicy1.AccessKey, userPolicy2); err != nil {
		panic(err.Error())
	}
	//fmt.Printf("after add policy: [%v]", akPolicy1)
	userPolicy3 := &oss.UserPolicy{
		OwnVol:     []string{"testvol3", "testvol5", "testvol1"},
		NoneOwnVol: map[string][]string{"testvol2": {objectnode.PutBucketAclAction, objectnode.ListBucketAction, objectnode.GetObjectAclAction}, "testvol4": {objectnode.DeleteBucketAction}},
	}
	if akPolicy1, err = mc.OSSAPI().AddPolicy(akPolicy1.AccessKey, userPolicy3); err != nil {
		panic(err.Error())
	}
	//fmt.Printf("after add policy: [%v]", akPolicy1)
	userPolicy4 := &oss.UserPolicy{NoneOwnVol: map[string][]string{"testvol1": {objectnode.DeleteBucketAction, objectnode.PutBucketTaggingAction}}}
	if akPolicy2, err = mc.OSSAPI().AddPolicy(akPolicy2.AccessKey, userPolicy4); err != nil {
		panic(err.Error())
	}
	//fmt.Printf("after add policy: [%v]", akPolicy2)

	//transfer vol
	if akPolicy2, err = mc.OSSAPI().TransferVol("testvol3", akPolicy1.AccessKey, akPolicy2.AccessKey); err != nil {
		panic(err)
	}
	//fmt.Printf("after transfer vol[testvol3]: user[%v], target user[%v]", akPolicy1, akPolicy2)
	if akPolicy1, err = mc.OSSAPI().TransferVol("testvol1", akPolicy2.AccessKey, akPolicy1.AccessKey); err != nil {
		panic(err)
	}
	//fmt.Printf("after transfer vol[testvol1]: user[%v], target user[%v]", akPolicy2, akPolicy1)

	// delete vol policy
	if err = mc.OSSAPI().DeleteVolPolicy("testvol1"); err != nil {
		panic(err)
	}
	//fmt.Printf("after delete vol policy[testvol1]: user1[%v], user2[%v]", akPolicy1, akPolicy2)
	if err = mc.OSSAPI().DeleteVolPolicy("testvol2"); err != nil {
		panic(err)
	}
	//fmt.Printf("after delete vol policy[testvol2]: user1[%v], user2[%v]", akPolicy1, akPolicy2)
	if err = mc.OSSAPI().DeleteVolPolicy("testvol3"); err != nil {
		panic(err)
	}
	//fmt.Printf("after delete vol policy[testvol3]: user1[%v], user2[%v]", akPolicy1, akPolicy2)
	if err = mc.OSSAPI().DeleteVolPolicy("testvol10"); err != nil {
		panic(err)
	}
	//fmt.Printf("after delete vol policy[testvol10]: user1[%v], user2[%v]", akPolicy1, akPolicy2)

	// delete policy
	if akPolicy1, err = mc.OSSAPI().DeletePolicy(akPolicy1.AccessKey, userPolicy1); err != nil {
		panic(err.Error())
	}
	//fmt.Printf("after delete policy: [%v]", akPolicy1)
	if akPolicy1, err = mc.OSSAPI().DeletePolicy(akPolicy1.AccessKey, userPolicy2); err != nil {
		panic(err.Error())
	}
	//fmt.Printf("after delete policy: [%v]", akPolicy1)
	if akPolicy1, err = mc.OSSAPI().DeletePolicy(akPolicy1.AccessKey, userPolicy3); err != nil {
		panic(err.Error())
	}
	//fmt.Printf("after delete policy: [%v]", akPolicy1)
	if akPolicy1, err = mc.OSSAPI().DeletePolicy(akPolicy1.AccessKey, userPolicy1); err != nil {
		//fmt.Printf(err.Error())
	}
	//fmt.Printf("after delete policy: [%v]", akPolicy1)

	// delete user
	if err = mc.OSSAPI().DeleteUser("testUser2"); err != nil {
		panic(err.Error())
	}
	if err = mc.OSSAPI().DeleteUser("testUser2"); err != nil {
		//fmt.Printf(err.Error())
	}

	// get info
	if akPolicy1, err = mc.OSSAPI().GetUserInfo("testuser1"); err != nil {
		panic(err)
	}
	if akPolicy2, err = mc.OSSAPI().GetUserInfo("testuser2"); err != nil {
		//fmt.Printf(err.Error())
	}
	if akPolicy2, err = mc.OSSAPI().GetAKInfo("0123456789acsdef"); err != nil {
		//fmt.Printf(err.Error())
	}
	if akPolicy1, err = mc.OSSAPI().GetAKInfo(akPolicy1.AccessKey); err != nil {
		panic(err)
	}
}
