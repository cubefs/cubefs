package master

import (
	"context"
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

func (api *UserAPI) CreateUser(ctx context.Context, param *proto.UserCreateParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(ctx, post, proto.UserCreate).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) DeleteUser(ctx context.Context, userID string, clientIDKey string) (err error) {
	request := newRequest(ctx, post, proto.UserDelete).Header(api.h)
	request.addParam("user", userID)
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *UserAPI) UpdateUser(ctx context.Context, param *proto.UserUpdateParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(ctx, post, proto.UserUpdate).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) GetAKInfo(ctx context.Context, accesskey string) (userInfo *proto.UserInfo, err error) {
	localIP, _ := ump.GetLocalIpAddr()
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(ctx, get, proto.UserGetAKInfo).
		Header(api.h).Param(anyParam{"ak", accesskey}, anyParam{"ip", localIP}))
	return
}

func (api *UserAPI) AclOperation(ctx context.Context, volName string, localIP string, op uint32) (aclInfo *proto.AclRsp, err error) {
	aclInfo = &proto.AclRsp{}
	if err = api.mc.requestWith(aclInfo, newRequest(ctx, get, proto.AdminACL).Header(api.h).Param(
		anyParam{"name", volName},
		anyParam{"ip", localIP},
		anyParam{"op", op},
	)); err != nil {
		fmt.Fprintf(os.Stdout, "AclOperation err %v\n", err)
		return
	}
	return
}

func (api *UserAPI) UidOperation(ctx context.Context, volName string, uid string, op uint32, val string) (uidInfo *proto.UidSpaceRsp, err error) {
	uidInfo = &proto.UidSpaceRsp{}
	if err = api.mc.requestWith(uidInfo, newRequest(ctx, get, proto.AdminUid).Header(api.h).Param(
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

func (api *UserAPI) GetUserInfo(ctx context.Context, userID string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(ctx, get, proto.UserGetInfo).Header(api.h).addParam("user", userID))
	return
}

func (api *UserAPI) UpdatePolicy(ctx context.Context, param *proto.UserPermUpdateParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(ctx, post, proto.UserUpdatePolicy).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) RemovePolicy(ctx context.Context, param *proto.UserPermRemoveParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(ctx, post, proto.UserRemovePolicy).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) DeleteVolPolicy(ctx context.Context, vol, clientIDKey string) (err error) {
	return api.mc.request(newRequest(ctx, post, proto.UserDeleteVolPolicy).Header(api.h).
		addParam("name", vol).addParam("clientIDKey", clientIDKey))
}

func (api *UserAPI) TransferVol(ctx context.Context, param *proto.UserTransferVolParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	err = api.mc.requestWith(userInfo, newRequest(ctx, post, proto.UserTransferVol).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) ListUsers(ctx context.Context, keywords string) (users []*proto.UserInfo, err error) {
	users = make([]*proto.UserInfo, 0)
	err = api.mc.requestWith(&users, newRequest(ctx, get, proto.UserList).Header(api.h).addParam("keywords", keywords))
	return
}

func (api *UserAPI) ListUsersOfVol(ctx context.Context, vol string) (users []string, err error) {
	users = make([]string, 0)
	err = api.mc.requestWith(&users, newRequest(ctx, get, proto.UsersOfVol).Header(api.h).addParam("name", vol))
	return
}
