package master

import (
	"fmt"
	"os"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/ump"
)

type UserAPI struct {
	mc *MasterClient
	h  map[string]string // extra headers
}

func (api *UserAPI) WithHeader(key, val string) *UserAPI {
	return &UserAPI{mc: api.mc, h: mergeHeader(api.h, key, val)}
}

func (api *UserAPI) EncodingWith(encoding string) *UserAPI {
	return api.WithHeader(headerAcceptEncoding, encoding)
}

func (api *UserAPI) EncodingGzip() *UserAPI {
	return api.EncodingWith(encodingGzip)
}

func (api *UserAPI) CreateUser(param *proto.UserCreateParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(post, proto.UserCreate).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) DeleteUser(userID string, clientIDKey string) (err error) {
	request := newRequest(post, proto.UserDelete).Header(api.h)
	request.addParam("user", userID)
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *UserAPI) UpdateUser(param *proto.UserUpdateParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(post, proto.UserUpdate).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) GetAKInfo(accesskey string) (userInfo *proto.UserInfo, err error) {
	localIP, _ := ump.GetLocalIpAddr()
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(get, proto.UserGetAKInfo).
		Header(api.h).Param(anyParam{"ak", accesskey}, anyParam{"ip", localIP}))
	return
}

func (api *UserAPI) AclOperation(volName string, localIP string, op uint32) (aclInfo *proto.AclRsp, err error) {
	aclInfo = &proto.AclRsp{}
	if err = api.mc.requestWith(aclInfo, newRequest(get, proto.AdminACL).Header(api.h).Param(
		anyParam{"name", volName},
		anyParam{"ip", localIP},
		anyParam{"op", op},
	)); err != nil {
		fmt.Fprintf(os.Stdout, "AclOperation err %v\n", err)
		return
	}
	return
}

func (api *UserAPI) UidOperation(volName string, uid string, op uint32, val string) (uidInfo *proto.UidSpaceRsp, err error) {
	uidInfo = &proto.UidSpaceRsp{}
	if err = api.mc.requestWith(uidInfo, newRequest(get, proto.AdminUid).Header(api.h).Param(
		anyParam{"name", volName},
		anyParam{"uid", uid},
		anyParam{"op", op},
		anyParam{"capacity", val},
	)); err != nil {
		fmt.Fprintf(os.Stdout, "UidOperation err %v\n", err)
		return
	}
	return
}

func (api *UserAPI) GetUserInfo(userID string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(get, proto.UserGetInfo).Header(api.h).addParam("user", userID))
	return
}

func (api *UserAPI) UpdatePolicy(param *proto.UserPermUpdateParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(post, proto.UserUpdatePolicy).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) RemovePolicy(param *proto.UserPermRemoveParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(post, proto.UserRemovePolicy).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) DeleteVolPolicy(vol, clientIDKey string) (err error) {
	return api.mc.request(newRequest(post, proto.UserDeleteVolPolicy).Header(api.h).
		addParam("name", vol).addParam("clientIDKey", clientIDKey))
}

func (api *UserAPI) TransferVol(param *proto.UserTransferVolParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(post, proto.UserTransferVol).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) ListUsers(keywords string) (users []*proto.UserInfo, err error) {
	users = make([]*proto.UserInfo, 0)
	err = api.mc.requestWith(&users, newRequest(get, proto.UserList).Header(api.h).addParam("keywords", keywords))
	return
}

func (api *UserAPI) ListUsersOfVol(vol string) (users []string, err error) {
	users = make([]string, 0)
	err = api.mc.requestWith(&users, newRequest(get, proto.UsersOfVol).Header(api.h).addParam("name", vol))
	return
}
