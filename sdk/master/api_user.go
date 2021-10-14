package master

import (
	"encoding/json"
	"net/http"

	"github.com/chubaofs/chubaofs/proto"
)

type UserAPI struct {
	mc *MasterClient
}

func (api *UserAPI) CreateUser(param *proto.UserCreateParam) (userInfo *proto.UserInfo, err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserCreate)
	var reqBody []byte
	if reqBody, err = json.Marshal(param); err != nil {
		return
	}
	request.addBody(reqBody)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	userInfo = &proto.UserInfo{}
	if err = json.Unmarshal(data, userInfo); err != nil {
		return
	}
	return
}

func (api *UserAPI) DeleteUser(userID string) (err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserDelete)
	request.addParam("user", userID)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *UserAPI) UpdateUser(param *proto.UserUpdateParam) (userInfo *proto.UserInfo, err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserUpdate)
	var reqBody []byte
	if reqBody, err = json.Marshal(param); err != nil {
		return
	}
	request.addBody(reqBody)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	userInfo = &proto.UserInfo{}
	if err = json.Unmarshal(data, userInfo); err != nil {
		return
	}
	return
}

func (api *UserAPI) GetAKInfo(accesskey string) (userInfo *proto.UserInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.UserGetAKInfo)
	request.addParam("ak", accesskey)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	userInfo = &proto.UserInfo{}
	if err = json.Unmarshal(data, userInfo); err != nil {
		return
	}
	return
}

func (api *UserAPI) GetUserInfo(userID string) (userInfo *proto.UserInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.UserGetInfo)
	request.addParam("user", userID)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	userInfo = &proto.UserInfo{}
	if err = json.Unmarshal(data, userInfo); err != nil {
		return
	}
	return
}

func (api *UserAPI) UpdatePolicy(param *proto.UserPermUpdateParam) (userInfo *proto.UserInfo, err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserUpdatePolicy)
	var reqBody []byte
	if reqBody, err = json.Marshal(param); err != nil {
		return
	}
	request.addBody(reqBody)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	userInfo = &proto.UserInfo{}
	if err = json.Unmarshal(data, userInfo); err != nil {
		return
	}
	return
}

func (api *UserAPI) RemovePolicy(param *proto.UserPermRemoveParam) (userInfo *proto.UserInfo, err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserRemovePolicy)
	var reqBody []byte
	if reqBody, err = json.Marshal(param); err != nil {
		return
	}
	request.addBody(reqBody)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	userInfo = &proto.UserInfo{}
	if err = json.Unmarshal(data, userInfo); err != nil {
		return
	}
	return
}

func (api *UserAPI) DeleteVolPolicy(vol string) (err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserDeleteVolPolicy)
	request.addParam("name", vol)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *UserAPI) TransferVol(param *proto.UserTransferVolParam) (userInfo *proto.UserInfo, err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserTransferVol)
	var reqBody []byte
	if reqBody, err = json.Marshal(param); err != nil {
		return
	}
	request.addBody(reqBody)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	userInfo = &proto.UserInfo{}
	if err = json.Unmarshal(data, userInfo); err != nil {
		return
	}
	return
}

func (api *UserAPI) ListUsers(keywords string) (users []*proto.UserInfo, err error) {
	var request = newAPIRequest(http.MethodGet, proto.UserList)
	request.addParam("keywords", keywords)
	if err = api.mc.generateSignature(request); err != nil {
		return
	}
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	users = make([]*proto.UserInfo, 0)
	if err = json.Unmarshal(data, &users); err != nil {
		return
	}
	return
}

func (api *UserAPI) ListUsersOfVol(vol string) (users []string, err error) {
	var request = newAPIRequest(http.MethodGet, proto.UsersOfVol)
	request.addParam("name", vol)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	users = make([]string, 0)
	if err = json.Unmarshal(data, &users); err != nil {
		return
	}
	return
}
