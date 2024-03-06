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
	ctxChild := proto.ContextWithOperation(ctx, "CreateUser")
	err = api.mc.requestWith(userInfo, newRequest(ctxChild, post, proto.UserCreate).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) DeleteUser(ctx context.Context, userID string, clientIDKey string) (err error) {
	ctxChild := proto.ContextWithOperation(ctx, "DeleteUser")
	request := newRequest(ctxChild, post, proto.UserDelete).Header(api.h)
	request.addParam("user", userID)
	request.addParam("clientIDKey", clientIDKey)
	if _, err = api.mc.serveRequest(request); err != nil {
		return
	}
	return
}

func (api *UserAPI) UpdateUser(ctx context.Context, param *proto.UserUpdateParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	ctxChild := proto.ContextWithOperation(ctx, "UpdateUser")
	err = api.mc.requestWith(userInfo, newRequest(ctxChild, post, proto.UserUpdate).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) GetAKInfo(ctx context.Context, accesskey string) (userInfo *proto.UserInfo, err error) {
	localIP, _ := ump.GetLocalIpAddr()
	userInfo = &proto.UserInfo{}
	ctxChild := proto.ContextWithOperation(ctx, "GetAKInfo")
	err = api.mc.requestWith(userInfo, newRequest(ctxChild, get, proto.UserGetAKInfo).
		Header(api.h).Param(anyParam{"ak", accesskey}, anyParam{"ip", localIP}))
	return
}

func (api *UserAPI) AclOperation(ctx context.Context, volName string, localIP string, op uint32) (aclInfo *proto.AclRsp, err error) {
	aclInfo = &proto.AclRsp{}

	ctxChild := proto.ContextWithOperation(ctx, "AclOperation")
	if err = api.mc.requestWith(aclInfo, newRequest(ctxChild, get, proto.AdminACL).Header(api.h).Param(
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
	ctxChild := proto.ContextWithOperation(ctx, "UidOperation")
	if err = api.mc.requestWith(uidInfo, newRequest(ctxChild, get, proto.AdminUid).Header(api.h).Param(
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
	ctxChild := proto.ContextWithOperation(ctx, "GetUserInfo")
	err = api.mc.requestWith(userInfo, newRequest(ctxChild, get, proto.UserGetInfo).Header(api.h).addParam("user", userID))
	return
}

func (api *UserAPI) UpdatePolicy(ctx context.Context, param *proto.UserPermUpdateParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	ctxChild := proto.ContextWithOperation(ctx, "UpdatePolicy")
	err = api.mc.requestWith(userInfo, newRequest(ctxChild, post, proto.UserUpdatePolicy).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) RemovePolicy(ctx context.Context, param *proto.UserPermRemoveParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	ctxChild := proto.ContextWithOperation(ctx, "RemovePolicy")
	err = api.mc.requestWith(userInfo, newRequest(ctxChild, post, proto.UserRemovePolicy).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) DeleteVolPolicy(ctx context.Context, vol, clientIDKey string) (err error) {
	ctxChild := proto.ContextWithOperation(ctx, "DeleteVolPolicy")
	return api.mc.request(newRequest(ctxChild, post, proto.UserDeleteVolPolicy).Header(api.h).
		addParam("name", vol).addParam("clientIDKey", clientIDKey))
}

func (api *UserAPI) TransferVol(ctx context.Context, param *proto.UserTransferVolParam, clientIDKey string) (userInfo *proto.UserInfo, err error) {
	userInfo = &proto.UserInfo{}
	ctxChild := proto.ContextWithOperation(ctx, "TransferVol")
	err = api.mc.requestWith(userInfo, newRequest(ctxChild, post, proto.UserTransferVol).
		Header(api.h).Body(param).addParam("clientIDKey", clientIDKey))
	return
}

func (api *UserAPI) ListUsers(ctx context.Context, keywords string) (users []*proto.UserInfo, err error) {
	users = make([]*proto.UserInfo, 0)
	ctxChild := proto.ContextWithOperation(ctx, "ListUsers")
	err = api.mc.requestWith(&users, newRequest(ctxChild, get, proto.UserList).Header(api.h).addParam("keywords", keywords))
	return
}

func (api *UserAPI) ListUsersOfVol(ctx context.Context, vol string) (users []string, err error) {
	users = make([]string, 0)
	ctxChild := proto.ContextWithOperation(ctx, "ListUsersOfVol")
	err = api.mc.requestWith(&users, newRequest(ctxChild, get, proto.UsersOfVol).Header(api.h).addParam("name", vol))
	return
}
