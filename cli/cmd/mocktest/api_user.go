// Copyright 2023 The CubeFS Authors.
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

package mocktest

import (
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
)

type mockUserAPI struct {
	master.UserAPI
}

func (m *mockUserAPI) AclOperation(volName string, localIP string, op uint32) (aclInfo *proto.AclRsp, err error) {
	if volName != CommonVolName {
		return nil, proto.ErrVolNotExists
	}
	return &proto.AclRsp{
		OK: true,
		List: []*proto.AclIpInfo{
			{
				Ip:    "192.168.0.1",
				CTime: 1689091200,
			},
		},
	}, nil
}

func (m *mockUserAPI) UidOperation(volName string, uid string, op uint32, val string) (uidInfo *proto.UidSpaceRsp, err error) {
	if volName != CommonVolName {
		return nil, proto.ErrVolNotExists
	}
	return &proto.UidSpaceRsp{
		Info: "",
		OK:   true,
		UidSpaceArr: []*proto.UidSpaceInfo{
			{
				VolName:   "vol1",
				Uid:       1,
				CTime:     0,
				Enabled:   true,
				Limited:   false,
				UsedSize:  0,
				LimitSize: 0,
				Rsv:       "",
			},
			{
				VolName:   "vol2",
				Uid:       1,
				CTime:     0,
				Enabled:   false,
				Limited:   false,
				UsedSize:  0,
				LimitSize: 0,
				Rsv:       "",
			},
		},
		Reserve: "",
	}, nil
}

func (m *mockUserAPI) CreateUser(param *proto.UserCreateParam) (userInfo *proto.UserInfo, err error) {
	return &proto.UserInfo{
		UserID:      "user1",
		AccessKey:   "",
		SecretKey:   "",
		Policy:      nil,
		UserType:    0,
		CreateTime:  "",
		Description: "",
		Mu:          sync.RWMutex{},
		EMPTY:       false,
	}, nil
}

func (m *mockUserAPI) DeleteUser(userID string) (err error) {
	return nil
}

func (m *mockUserAPI) UpdateUser(param *proto.UserUpdateParam) (userInfo *proto.UserInfo, err error) {
	return &proto.UserInfo{
		UserID:      "user1",
		AccessKey:   "",
		SecretKey:   "",
		Policy:      nil,
		UserType:    0,
		CreateTime:  "",
		Description: "",
		Mu:          sync.RWMutex{},
		EMPTY:       false,
	}, nil
}

func (m *mockUserAPI) GetUserInfo(userID string) (userInfo *proto.UserInfo, err error) {
	return &proto.UserInfo{
		UserID:    "user1",
		AccessKey: "key1",
		SecretKey: "key2",
		Policy: &proto.UserPolicy{
			OwnVols:        []string{"vol1", "vol2"},
			AuthorizedVols: map[string][]string{"vol1": {"vol1"}},
		},
		UserType:    0,
		CreateTime:  "",
		Description: "",
		Mu:          sync.RWMutex{},
		EMPTY:       false,
	}, nil
}

func (m *mockUserAPI) UpdatePolicy(param *proto.UserPermUpdateParam) (userInfo *proto.UserInfo, err error) {
	return &proto.UserInfo{
		UserID:    "user1",
		AccessKey: "",
		SecretKey: "",
		Policy: &proto.UserPolicy{
			OwnVols:        []string{"vol1", "vol2"},
			AuthorizedVols: map[string][]string{"vol1": {"vol1"}},
		},
		UserType:    0,
		CreateTime:  "",
		Description: "",
		Mu:          sync.RWMutex{},
		EMPTY:       false,
	}, nil
}

func (m *mockUserAPI) RemovePolicy(param *proto.UserPermRemoveParam) (userInfo *proto.UserInfo, err error) {
	return &proto.UserInfo{
		UserID:    "user1",
		AccessKey: "",
		SecretKey: "",
		Policy: &proto.UserPolicy{
			OwnVols:        []string{"vol1", "vol2"},
			AuthorizedVols: map[string][]string{"vol1": {"vol1"}},
		},
		UserType:    0,
		CreateTime:  "",
		Description: "",
		Mu:          sync.RWMutex{},
		EMPTY:       false,
	}, nil
}

func (m *mockUserAPI) ListUsers(keywords string) (users []*proto.UserInfo, err error) {
	return []*proto.UserInfo{
		{
			UserID:    "user1",
			AccessKey: "",
			SecretKey: "",
			Policy: &proto.UserPolicy{
				OwnVols:        []string{"vol1", "vol2"},
				AuthorizedVols: map[string][]string{"vol1": {"vol1"}},
			},
			UserType:    0,
			CreateTime:  "",
			Description: "",
			Mu:          sync.RWMutex{},
			EMPTY:       false,
		},
	}, nil
}

func (m *mockUserAPI) TransferVol(param *proto.UserTransferVolParam) (userInfo *proto.UserInfo, err error) {
	return &proto.UserInfo{
		UserID:    "user1",
		AccessKey: "",
		SecretKey: "",
		Policy: &proto.UserPolicy{
			OwnVols:        []string{"vol1", "vol2"},
			AuthorizedVols: map[string][]string{"vol1": {"vol1"}},
		},
		UserType:    0,
		CreateTime:  "",
		Description: "",
		Mu:          sync.RWMutex{},
		EMPTY:       false,
	}, nil
}
