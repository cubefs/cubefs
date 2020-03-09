package master

import (
	"encoding/json"
	"net/http"

	"github.com/chubaofs/chubaofs/proto"
)

type UserAPI struct {
	mc *MasterClient
}

func (api *UserAPI) CreateUser(userID string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodPut, proto.UserCreate)
	request.addParam("owner", userID)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	akPolicy = &proto.AKPolicy{}
	if err = json.Unmarshal(data, akPolicy); err != nil {
		return
	}
	return
}

func (api *UserAPI) CreateUserWithKey(userID, ak, sk string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodPut, proto.UserCreateWithKey)
	request.addParam("owner", userID)
	request.addParam("ak", ak)
	request.addParam("sk", sk)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	akPolicy = &proto.AKPolicy{}
	if err = json.Unmarshal(data, akPolicy); err != nil {
		return
	}
	return
}

func (api *UserAPI) DeleteUser(userID string) (err error) {
	var request = newAPIRequest(http.MethodDelete, proto.UserDelete)
	request.addParam("owner", userID)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *UserAPI) GetAKInfo(accesskey string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodGet, proto.UserGetAKInfo)
	request.addParam("ak", accesskey)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	akPolicy = &proto.AKPolicy{}
	if err = json.Unmarshal(data, akPolicy); err != nil {
		return
	}
	return
}

func (api *UserAPI) GetUserInfo(userID string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodGet, proto.UserGetInfo)
	request.addParam("owner", userID)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	akPolicy = &proto.AKPolicy{}
	if err = json.Unmarshal(data, akPolicy); err != nil {
		return
	}
	return
}

func (api *UserAPI) AddPolicy(accesskey string, policy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	var body []byte
	if body, err = json.Marshal(policy); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.UserAddPolicy)
	request.addParam("ak", accesskey)
	request.addBody(body)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	akPolicy = &proto.AKPolicy{}
	if err = json.Unmarshal(data, akPolicy); err != nil {
		return
	}
	return
}

func (api *UserAPI) DeletePolicy(accesskey string, policy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	var body []byte
	if body, err = json.Marshal(policy); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.UserDeletePolicy)
	request.addParam("ak", accesskey)
	request.addBody(body)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	akPolicy = &proto.AKPolicy{}
	if err = json.Unmarshal(data, akPolicy); err != nil {
		return
	}
	return
}

func (api *UserAPI) DeleteVolPolicy(vol string) (err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserDeleteVolPolicy)
	request.addParam("name", vol)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *UserAPI) TransferVol(vol, ak, targetAK string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodPost, proto.UserTransferVol)
	request.addParam("name", vol)
	request.addParam("ak", ak)
	request.addParam("targetak", targetAK)
	var data []byte
	if data, err = api.mc.serveRequest(request); err != nil {
		return
	}
	akPolicy = &proto.AKPolicy{}
	if err = json.Unmarshal(data, akPolicy); err != nil {
		return
	}
	return
}
