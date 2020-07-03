package user_test

import (
	"context"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/graphql/client"
	"github.com/chubaofs/chubaofs/sdk/graphql/client/user"
	"testing"
)

func TestUserClient_GetUserInfo(t *testing.T) {
	c := client.NewMasterGClient([]string{"127.0.0.1:8989"})
	tokenInfo, err := c.Login(context.Background(), "root", "ANSJ")
	if err != nil {
		panic(err)
	}

	ctx := context.WithValue(context.Background(), proto.HeadAuthorized, tokenInfo.Token)

	userClient := user.NewUserClient(c)

	userInfo, err := userClient.GetUserInfo(ctx, "root")
	if err != nil {
		panic(err)
	}

	if userInfo.User_id != "root" {
		panic("get value has err")
	}

	if userInfo.Policy == nil {
		panic("get policy has err")
	}

	if userInfo.Create_time == "" {
		panic("userInfo create time err ")
	}

}

func TestUserClient_DeleteUser_CreateUser(t *testing.T) {
	c := client.NewMasterGClient([]string{"127.0.0.1:8989"})
	tokenInfo, err := c.Login(context.Background(), "root", "ANSJ")
	if err != nil {
		panic(err)
	}

	ctx := context.WithValue(context.Background(), proto.HeadAuthorized, tokenInfo.Token)

	userClient := user.NewUserClient(c)

	userID := "userID"

	_, err = userClient.DeleteUser(ctx, userID)

	if err != nil {
		panic(err)
	}

	userInfo, err := userClient.CreateUser(ctx, "accessKey", userID, "password", "secretKey", 2)
	if err != nil {
		panic(err)
	}

	if userInfo.User_id != userID {
		panic("get value has err")
	}

	if userInfo.Policy == nil {
		panic("get policy has err")
	}

	if userInfo.Create_time == "" {
		panic("userInfo create time err ")
	}
}
