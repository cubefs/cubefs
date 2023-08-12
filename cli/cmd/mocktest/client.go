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

import "github.com/cubefs/cubefs/sdk/master"

var (
	CommonVolName = "commonVol"
)

type mockMasterClient struct {
	master.MasterClient
	adminAPI  *mockAdminAPI
	clientAPI *mockClientAPI
	nodeAPI   *mockNodeAPI
	userAPI   *mockUserAPI
}

func NewMockMasterClient() master.IMasterClient {
	return &mockMasterClient{
		adminAPI:  &mockAdminAPI{},
		clientAPI: &mockClientAPI{},
		nodeAPI:   &mockNodeAPI{},
		userAPI:   &mockUserAPI{},
	}
}

func (m *mockMasterClient) AdminAPI() master.IAdminAPI {
	return m.adminAPI
}

func (m *mockMasterClient) ClientAPI() master.IClientAPI {
	return m.clientAPI
}

func (m *mockMasterClient) NodeAPI() master.INodeAPI {
	return m.nodeAPI
}

func (m *mockMasterClient) UserAPI() master.IUserAPI {
	return m.userAPI
}
