package auth

import (
	"errors"
	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/console/backend/helper/codes"
	"github.com/cubefs/cubefs/console/backend/helper/ginutils"
	"github.com/cubefs/cubefs/console/backend/model"
	"github.com/cubefs/cubefs/console/backend/service/auth"
)

func getSession(c *gin.Context) (interface{}, error) {
	session := sessions.Default(c)
	sessionId, err := c.Cookie("sessionId")
	if err != nil {
		log.Errorf("get session err: %+v", err)
		return nil, err
	}
	sessionData := session.Get(sessionId)
	if sessionData == nil {
		log.Error("get session err")
		err = errors.New("get session err")
		return nil, err
	}
	return sessionData, nil
}

func isUpdateUserAuth(sessionData interface{}, userName string, authCode string) bool {
	sessionUserName := sessionData.(map[string]interface{})["UserName"].(string)
	if sessionUserName == userName {
		return true
	}
	sessionUserId := sessionData.(map[string]interface{})["Id"].(int)
	permissions, err := auth.GetUserPermission(sessionUserId, nil)
	if err != nil {
		log.Errorf("get user permission err: %+v", err)
		return false
	}
	for _, item := range permissions {
		if item.AuthCode == authCode {
			return true
		}
	}
	return false
}

func LoginHandler(c *gin.Context) {
	input := &model.AuthUser{}
	if !ginutils.Check(c, input) {
		return
	}
	err := auth.LoginUser(input, c)
	if err != nil {
		ginutils.Send(c, codes.Forbidden.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
	return
}

func LogoutHandler(c *gin.Context) {
	session := sessions.Default(c)
	sessionId, err := c.Cookie("sessionId")
	if err != nil {
		log.Errorf("get session err: %+v", err)
		ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), "get session err: "+err.Error())
		return
	}
	session.Delete(sessionId)
	_ = session.Save()
	c.SetCookie("sessionId", sessionId, -1, "/", c.Request.Host, false, true)
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), "")
}

func GetPermissionHandler(c *gin.Context) {
	input := &model.FindAuthPermissionParam{}
	if !ginutils.Check(c, input) {
		return
	}
	data, err := auth.GetPermissionList(input)
	if err != nil {
		log.Errorf("get permission list err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), data)
}

func CreatePermissionHandler(c *gin.Context) {
	input := &model.AuthPermission{}
	if !ginutils.Check(c, input) {
		return
	}
	err := input.Create()
	if err != nil {
		log.Errorf("create the permission err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func UpdatePermissionHandler(c *gin.Context) {
	input := &model.AuthPermission{}
	if !ginutils.Check(c, input) {
		return
	}
	err := input.Update()
	if err != nil {
		log.Errorf("update the permission err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func DeletePermissionHandler(c *gin.Context) {
	type PermissionIds struct {
		Ids []int `json:"ids"`
	}

	input := &PermissionIds{}
	if !ginutils.Check(c, input) {
		return
	}
	permission := &model.AuthPermission{}
	err := permission.Delete(input.Ids)
	if err != nil {
		log.Errorf("delete the permission err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func GetRoleHandler(c *gin.Context) {
	input := &model.FindAuthRoleParam{}
	if !ginutils.Check(c, input) {
		return
	}
	data, err := auth.GetRoleList(input)
	if err != nil {
		log.Errorf("get role list err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), data)
}

func CreateRoleHandler(c *gin.Context) {
	input := &auth.RoleInput{}
	if !ginutils.Check(c, input) {
		return
	}
	role := input.AuthRole
	ids := input.PermissionIds
	err := role.Create(ids)
	if err != nil {
		log.Errorf("create the role err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func UpdateRoleHandler(c *gin.Context) {
	input := &auth.RoleInput{}
	if !ginutils.Check(c, input) {
		return
	}
	role := input.AuthRole
	ids := input.PermissionIds
	err := role.Update(ids)
	if err != nil {
		log.Errorf("update the role err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func DeleteRoleHandler(c *gin.Context) {
	type RoleIds struct {
		Ids []int `json:"ids"`
	}

	input := &RoleIds{}
	if !ginutils.Check(c, input) {
		return
	}
	role := &model.AuthRole{}
	err := role.Delete(input.Ids)
	if err != nil {
		log.Errorf("delete the role err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func GetUserHandler(c *gin.Context) {
	input := &model.FindAuthUserParam{}
	if !ginutils.Check(c, input) {
		return
	}
	data, err := auth.GetUserList(input)
	if err != nil {
		log.Errorf("get user list err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), data)
}

func CreateUserHandler(c *gin.Context) {
	input := &auth.UserInput{}
	if !ginutils.Check(c, input) {
		return
	}
	user := input.AuthUser
	ids := input.RoleIds
	err := user.Create(ids)
	if err != nil {
		log.Errorf("create the user err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func UpdateUserHandler(c *gin.Context) {
	input := &auth.UserInput{}
	if !ginutils.Check(c, input) {
		return
	}
	user := input.AuthUser
	ids := input.RoleIds
	err := user.Update(ids)
	if err != nil {
		log.Errorf("update the user err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func UpdateSelfUserHandler(c *gin.Context) {
	input := &auth.UserInput{}
	if !ginutils.Check(c, input) {
		return
	}
	sessionData, err := getSession(c)
	if err != nil {
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), "")
		return
	}
	sessionUserName := sessionData.(map[string]interface{})["UserName"].(string)
	if sessionUserName != input.UserName {
		ginutils.Send(c, codes.ResultError.Code(), "No permission to modify user info", "")
		return
	}
	user := input.AuthUser
	ids := input.RoleIds
	err = user.Update(ids)
	if err != nil {
		log.Errorf("update the user err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func DeleteUserHandler(c *gin.Context) {
	type UserIds struct {
		Ids []int `json:"ids"`
	}
	input := &UserIds{}
	if !ginutils.Check(c, input) {
		return
	}
	user := &model.AuthUser{}
	err := user.Delete(input.Ids)
	if err != nil {
		log.Errorf("delete the user err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func UpdateUserPasswordHandler(c *gin.Context) {
	//input := &auth.PasswordInput{}
	input := &model.AuthUser{}
	if !ginutils.Check(c, input) {
		return
	}
	user := new(model.AuthUser)
	user.UserName = input.UserName
	user.Password = input.Password
	err := user.UpdatePassword()
	if err != nil {
		log.Errorf("update the user password err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), "")
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func UpdateSelfUserPasswordHandler(c *gin.Context) {
	input := &auth.PasswordInput{}
	if !ginutils.Check(c, input) {
		return
	}
	err := auth.ChangeUserPassword(input)
	if err != nil {
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), "")
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), nil)
}

func GetUserPermissionHandler(c *gin.Context) {
	type AuthType struct {
		AuthType *int `form:"auth_type"`
	}
	input := &AuthType{}
	if !ginutils.Check(c, input) {
		return
	}

	sessionData, err := getSession(c)
	if err != nil {
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), "")
	}
	userId := sessionData.(map[string]interface{})["Id"].(int)
	permissions, err := auth.GetUserPermission(userId, input.AuthType)
	if err != nil {
		log.Errorf("get user permission err: %+v", err)
		ginutils.Send(c, codes.ResultError.Code(), err.Error(), nil)
		return
	}
	ginutils.Send(c, codes.OK.Code(), codes.OK.Msg(), permissions)
}
