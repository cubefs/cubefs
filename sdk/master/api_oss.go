package master

import (
	"encoding/json"
	"net/http"

	"github.com/chubaofs/chubaofs/proto"
)

type OSSAPI struct {
	mc *MasterClient
}

func (api *OSSAPI) CreateUser(userID string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodPut, proto.OSSCreateUser)
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

func (api *OSSAPI) CreateUserWithKey(userID, ak, sk string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodPut, proto.OSSCreateUserWithKey)
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

func (api *OSSAPI) DeleteUser(userID string) (err error) {
	var request = newAPIRequest(http.MethodDelete, proto.OSSDeleteUser)
	request.addParam("owner", userID)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *OSSAPI) GetAKInfo(accesskey string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodGet, proto.OSSGetAKInfo)
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

func (api *OSSAPI) GetUserInfo(userID string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodGet, proto.OSSGetUserInfo)
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

func (api *OSSAPI) AddPolicy(accesskey string, policy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	var body []byte
	if body, err = json.Marshal(policy); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.OSSAddPolicy)
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

func (api *OSSAPI) DeletePolicy(accesskey string, policy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	var body []byte
	if body, err = json.Marshal(policy); err != nil {
		return
	}
	var request = newAPIRequest(http.MethodPost, proto.OSSDeletePolicy)
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

func (api *OSSAPI) DeleteVolPolicy(vol string) (err error) {
	var request = newAPIRequest(http.MethodPost, proto.OSSDeleteVolPolicy)
	request.addParam("name", vol)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *OSSAPI) TransferVol(vol, ak, targetAK string) (akPolicy *proto.AKPolicy, err error) {
	var request = newAPIRequest(http.MethodPost, proto.OSSTransferVol)
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
