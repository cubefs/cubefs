package service

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/graphql/client"
	"github.com/cubefs/cubefs/sdk/graphql/client/user"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

type LoginService struct {
	client     *client.MasterGClient
	userClient *user.UserClient
}

func NewLoginService(client *client.MasterGClient) *LoginService {
	return &LoginService{
		client:     client,
		userClient: user.NewUserClient(client),
	}
}

type UserToken struct {
	UserID   string `json:"userID"`
	Token    string `json:"token"`
	Status   bool   `json:"status"`
	UserInfo *user.UserInfo
}

func (ls *LoginService) login(ctx context.Context, args struct {
	UserID   string
	Password string
	empty    bool
}) (*UserToken, error) {
	_, err := ls.client.ValidatePassword(ctx, args.UserID, args.Password)
	if err != nil {
		return nil, err
	}

	ctx = context.WithValue(ctx, proto.UserKey, args.UserID)

	userInfo, err := ls.userClient.GetUserInfoForLogin(ctx, args.UserID)
	if err != nil {
		return nil, err
	}

	return &UserToken{
		UserID: userInfo.User_id,
		Token:  cutil.TokenRegister(userInfo),
		Status: true,
	}, nil
}

func (ls *LoginService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()
	query := schema.Query()
	query.FieldFunc("login", ls.login)
	return schema.MustBuild()
}

type permissionMode int

const ADMIN permissionMode = permissionMode(1)
const USER permissionMode = permissionMode(2)

func permissions(ctx context.Context, mode permissionMode) (userInfo *user.UserInfo, perm permissionMode, err error) {
	userInfo = ctx.Value(proto.UserInfoKey).(*user.UserInfo)

	perm = USER
	if userInfo.User_type == uint8(proto.UserTypeRoot) || userInfo.User_type == uint8(proto.UserTypeAdmin) {
		perm = ADMIN
	}

	if ADMIN&mode == ADMIN {
		if perm == ADMIN {
			return
		}
	}

	if USER&mode == USER {
		if perm == USER {
			return
		}
	}
	perm = permissionMode(0)
	err = fmt.Errorf("user:[%s] permissions has err:[%d] your:[%d]", userInfo.User_id, mode, perm)
	userInfo = nil
	return
}
