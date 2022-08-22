// Copyright 2018 The CubeFS Authors.
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
	"encoding/json"
	"fmt"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
)

func (u *User) submit(metadata *RaftCmd) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return errors.New(err.Error())
	}
	if _, err = u.partition.Submit(cmd); err != nil {
		msg := fmt.Sprintf("action[user_submit] err:%v", err.Error())
		return errors.New(msg)
	}
	return
}

// key = #ak#accesskey, value = userInfo
func (u *User) syncAddUserInfo(userInfo *proto.UserInfo) (err error) {
	return u.syncPutUserInfo(opSyncAddUserInfo, userInfo)
}

func (u *User) syncDeleteUserInfo(userInfo *proto.UserInfo) (err error) {
	return u.syncPutUserInfo(opSyncDeleteUserInfo, userInfo)
}

func (u *User) syncUpdateUserInfo(userInfo *proto.UserInfo) (err error) {
	return u.syncPutUserInfo(opSyncUpdateUserInfo, userInfo)
}

func (u *User) syncPutUserInfo(opType uint32, userInfo *proto.UserInfo) (err error) {
	raftCmd := new(RaftCmd)
	raftCmd.Op = opType
	raftCmd.K = userPrefix + userInfo.UserID
	raftCmd.V, err = json.Marshal(userInfo)
	if err != nil {
		return errors.New(err.Error())
	}
	return u.submit(raftCmd)
}

// key = #user#userid, value = userInfo
func (u *User) syncAddAKUser(akUser *proto.AKUser) (err error) {
	return u.syncPutAKUser(opSyncAddAKUser, akUser)
}

func (u *User) syncDeleteAKUser(akUser *proto.AKUser) (err error) {
	return u.syncPutAKUser(opSyncDeleteAKUser, akUser)
}

func (u *User) syncPutAKUser(opType uint32, akUser *proto.AKUser) (err error) {
	userInfo := new(RaftCmd)
	userInfo.Op = opType
	userInfo.K = akPrefix + akUser.AccessKey
	userInfo.V, err = json.Marshal(akUser)
	if err != nil {
		return errors.New(err.Error())
	}
	return u.submit(userInfo)
}

// key = #voluser#volname, value = userIDs
func (u *User) syncAddVolUser(volUser *proto.VolUser) (err error) {
	return u.syncPutVolUser(opSyncAddVolUser, volUser)
}

func (u *User) syncDeleteVolUser(volUser *proto.VolUser) (err error) {
	return u.syncPutVolUser(opSyncDeleteVolUser, volUser)
}

func (u *User) syncUpdateVolUser(volUser *proto.VolUser) (err error) {
	return u.syncPutVolUser(opSyncUpdateVolUser, volUser)
}

func (u *User) syncPutVolUser(opType uint32, volUser *proto.VolUser) (err error) {
	userInfo := new(RaftCmd)
	userInfo.Op = opType
	userInfo.K = volUserPrefix + volUser.Vol
	userInfo.V, err = json.Marshal(volUser)
	if err != nil {
		return errors.New(err.Error())
	}
	return u.submit(userInfo)
}

func (u *User) loadUserStore() (err error) {
	result, err := u.fsm.store.SeekForPrefix([]byte(userPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadUserKeyInfo], err: %v", err.Error())
		return err
	}
	for _, value := range result {
		userInfo := &proto.UserInfo{}
		if err = json.Unmarshal(value, userInfo); err != nil {
			err = fmt.Errorf("action[loadUserKeyInfo], unmarshal err: %v", err.Error())
			return err
		}
		u.userStore.Store(userInfo.UserID, userInfo)
		log.LogInfof("action[loadUserKeyInfo], userID[%v]", userInfo.UserID)
	}
	return
}

func (u *User) loadAKStore() (err error) {
	result, err := u.fsm.store.SeekForPrefix([]byte(akPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadAKStore], err: %v", err.Error())
		return err
	}
	for _, value := range result {
		akUser := &proto.AKUser{}
		if err = json.Unmarshal(value, akUser); err != nil {
			err = fmt.Errorf("action[loadAKStore], unmarshal err: %v", err.Error())
			return err
		}
		u.AKStore.Store(akUser.AccessKey, akUser)
		log.LogInfof("action[loadAKStore], ak[%v], userID[%v]", akUser.AccessKey, akUser.UserID)
	}
	return
}

func (u *User) loadVolUsers() (err error) {
	result, err := u.fsm.store.SeekForPrefix([]byte(volUserPrefix))
	if err != nil {
		err = fmt.Errorf("action[loadVolUsers], err: %v", err.Error())
		return err
	}
	for _, value := range result {
		volUser := &proto.VolUser{}
		if err = json.Unmarshal(value, volUser); err != nil {
			err = fmt.Errorf("action[loadVolUsers], unmarshal err: %v", err.Error())
			return err
		}
		u.volUser.Store(volUser.Vol, volUser)
		log.LogInfof("action[loadVolUsers], vol[%v]", volUser.Vol)
	}
	return
}
