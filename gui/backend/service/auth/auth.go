package auth

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/console/backend/helper/crypt"
	"github.com/cubefs/cubefs/console/backend/model"
	"github.com/cubefs/cubefs/console/backend/model/mysql"
)

type RoleInput struct {
	model.AuthRole
	PermissionIds []int `json:"permission_ids"`
}

type UserInput struct {
	model.AuthUser
	RoleIds []int `json:"role_ids"`
}

type PasswordInput struct {
	UserName    string `json:"user_name"`
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

func initPermission() error {
	db := mysql.GetDB()
	tx := db.Begin()

	// create the permission by the router
	permissionList := make([]model.AuthPermission, 0)
	//permissionMap := make(map[string]string, 0)
	for _, v := range DefaultPermission {
		temps := make([]model.AuthPermission, 0)
		query := tx.Model(&model.AuthPermission{}).Where("auth_code = ?", v.AuthCode).Find(&temps)
		if query.Error != nil {
			tx.Rollback()
			return query.Error
		}
		if len(temps) == 0 {
			permissionList = append(permissionList, model.AuthPermission{
				AuthCode: v.AuthCode,
				AuthName: v.AuthName,
				AuthType: v.AuthType,
				URI:      v.URI,
				Method:   v.Method,
				IsLogin:  v.IsLogin,
				IsCheck:  v.IsCheck,
			})
		}
	}
	if len(permissionList) > 0 {
		query := tx.Create(&permissionList)
		if query.Error != nil {
			tx.Rollback()
			return query.Error
		}
	}
	tx.Commit()
	return nil
}

func initRole() error {
	db := mysql.GetDB()
	tx := db.Begin()
	// create the default role
	roleList := make([]model.AuthRole, 0)
	for _, v := range DefaultRole {
		temps := make([]model.AuthRole, 0)
		query := tx.Model(&model.AuthRole{}).Where("role_code = ? or role_name = ?", v.RoleCode, v.RoleName).Find(&temps)
		if query.Error != nil {
			tx.Rollback()
			return query.Error
		}
		if len(temps) == 0 {
			roleList = append(roleList, model.AuthRole{
				RoleCode:    v.RoleCode,
				RoleName:    v.RoleName,
				MaxVolCount: v.MaxVolCount,
				MaxVolSize:  v.MaxVolSize,
			})
		}
	}
	if len(roleList) > 0 {
		query := tx.Create(&roleList)
		if query.Error != nil {
			tx.Rollback()
			return query.Error
		}
	} else {
		tx.Rollback()
		return nil
	}

	// create the relationship between the role and the permission
	type PermissionId struct {
		Id int `json:"id"`
	}

	for k, v := range DefaultRolePermission {
		role := new(model.AuthRole)
		query := tx.Model(&model.AuthRole{}).Where("role_code = ?", k).First(&role)
		if query.Error != nil {
			tx.Rollback()
			return query.Error
		}
		if role == nil || role == (&model.AuthRole{}) {
			tx.Rollback()
			return errors.New("the role is empty")
		}

		perList := make([]PermissionId, 0)
		query = tx.Model(&model.AuthPermission{}).Select("id")
		if k == "Root" {
			query = query.Where("1 = 1")
		} else {
			query = query.Where("auth_code in ?", v)
		}
		query = query.Find(&perList)
		if query.Error != nil {
			tx.Rollback()
			return query.Error
		}

		if len(perList) > 0 {
			rolePerList := make([]model.AuthRolePermission, 0)
			for _, item := range perList {
				rolePerList = append(rolePerList, model.AuthRolePermission{RoleId: role.Id, PermissionId: item.Id})
			}

			query = tx.Delete(&model.AuthRolePermission{}, "role_id = ?", role.Id)
			if query.Error != nil {
				tx.Rollback()
				return query.Error
			}
			query = tx.Model(&model.AuthRolePermission{}).Create(&rolePerList)
			if query.Error != nil {
				tx.Rollback()
				return query.Error
			}
		}
	}
	tx.Commit()
	return nil
}

func initUser() error {
	db := mysql.GetDB()
	tx := db.Begin()
	users := make([]model.AuthUser, 0)
	// check whether the user is exists
	query := tx.Model(&model.AuthUser{}).
		Where("user_name = ? or email = ? or phone = ?", DefaultRoot.UserName, DefaultRoot.Email, DefaultRoot.Phone).Find(&users)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	if len(users) != 0 {
		tx.Rollback()
		return nil
	}

	// create the user
	user := new(model.AuthUser)
	user.UserName = DefaultRoot.UserName
	user.Email = DefaultRoot.Email
	user.Password = DefaultRoot.Password
	user.Phone = DefaultRoot.Phone
	query = tx.Create(&user)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}

	// create the relationship between the user and the role
	roles := make([]int, 0)
	query = tx.Model(&model.AuthRole{}).Select("id").Where("role_code = 'Root'").Find(&roles)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	if len(roles) > 0 {
		userRoleList := make([]model.AuthUserRole, 0)
		for _, item := range roles {
			userRoleList = append(userRoleList, model.AuthUserRole{UserId: user.Id, RoleId: item})
		}
		query = tx.Delete(&model.AuthUserRole{}, "user_id = ?", user.Id)
		if query.Error != nil {
			tx.Rollback()
			return query.Error
		}
		query = tx.Model(&model.AuthUserRole{}).Create(&userRoleList)
		if query.Error != nil {
			tx.Rollback()
			return query.Error
		}
	}
	tx.Commit()
	return nil
}

func InitAuth() error {
	_ = CreateDb()
	err := initPermission()
	if err != nil {
		return err
	}
	err = initRole()
	if err != nil {
		return err
	}
	err = initUser()
	if err != nil {
		return err
	}
	return nil
}

func CreateDb() map[string]string {
	db := mysql.GetDB()
	errorList := make(map[string]string)
	modelMap := make(map[string]interface{})
	modelMap["AuthUser"] = &model.AuthUser{}
	modelMap["AuthRole"] = &model.AuthRole{}
	modelMap["AuthPermission"] = &model.AuthPermission{}
	modelMap["AuthUserRole"] = &model.AuthUserRole{}
	modelMap["AuthRolePermission"] = &model.AuthRolePermission{}

	for k, v := range modelMap {
		err := db.AutoMigrate(v)
		if err != nil {
			errorList[k] = fmt.Sprintf("%s", err)
		}
	}
	return errorList
}

func GetSessionId() string {
	b := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(b)
}

func LoginUser(input *model.AuthUser, c *gin.Context) error {
	if input == nil {
		return errors.New("the user info is empty")
	}
	if input.UserName == DefaultRoot.UserName && input.Password == DefaultRoot.Password {
		return errors.New("the default user must change password")
	}
	db := mysql.GetDB()
	user := &model.AuthUser{}
	query := db.Model(&model.AuthUser{}).Where("user_name = ?", input.UserName).First(user)
	if query.Error != nil {
		return query.Error
	}
	if user == nil || user == (&model.AuthUser{}) {
		return errors.New("the password is incorrect")
	}
	if user.Password != input.Password {
		return errors.New("the password is incorrect")
	}
	session := sessions.Default(c)
	sessionId := GetSessionId()
	gob.Register(map[string]interface{}{})
	userSession := make(map[string]interface{})
	userSession["Id"] = user.Id
	userSession["UserName"] = user.UserName
	userSession["Email"] = user.Email
	userSession["Phone"] = user.Phone
	session.Set(sessionId, userSession)
	err := session.Save()
	if err != nil {
		return err
	}
	c.SetCookie("sessionId", sessionId, 8*3600, "/", c.Request.Host, false, true)
	return nil
}

func GetPermissionList(input *model.FindAuthPermissionParam) (*map[string]interface{}, error) {
	permission := &model.AuthPermission{}
	permissionList, count, err := permission.Find(input)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	result["page"] = input.Page
	result["pageSize"] = input.PageSize
	result["count"] = count
	result["permissions"] = permissionList
	return &result, nil
}

func GetRoleList(input *model.FindAuthRoleParam) (*map[string]interface{}, error) {
	role := &model.AuthRole{}
	roleList, count, err := role.Find(input)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	result["page"] = input.Page
	result["pageSize"] = input.PageSize
	result["count"] = count
	result["roles"] = roleList
	return &result, nil
}

func GetUserList(input *model.FindAuthUserParam) (*map[string]interface{}, error) {
	user := &model.AuthUser{}
	userList, count, err := user.Find(input)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	result["page"] = input.Page
	result["pageSize"] = input.PageSize
	result["count"] = count
	result["users"] = userList
	return &result, nil
}

func GetUserRole(userId int) ([]model.AuthRole, error) {
	db := mysql.GetDB()
	roles := make([]model.AuthRole, 0)
	query := db.Model(&model.AuthUserRole{}).Select("auth_roles.*").
		Joins("left join auth_roles on auth_user_roles.role_id = auth_roles.id").
		Where("auth_user_roles.user_id = ?", userId).Find(&roles)
	if query.Error != nil {
		return nil, query.Error
	}
	return roles, nil
}

func GetUserPermission(userId int, authType *int) ([]model.AuthPermission, error) {
	db := mysql.GetDB()
	roleIds := make([]int, 0)
	query := db.Model(&model.AuthUserRole{}).Select("role_id").Where("user_id = ?", userId).Find(&roleIds)
	if query.Error != nil {
		return nil, query.Error
	}

	permissions := make([]model.AuthPermission, 0)
	db = db.Model(&model.AuthRolePermission{}).Select("auth_permissions.*").
		Joins("left join auth_permissions on auth_role_permissions.permission_id = auth_permissions.id").
		Where("auth_role_permissions.role_id in ?", roleIds)
	if authType != nil {
		db = db.Where("auth_permissions.auth_type = ?", *authType)
	}
	query = db.Find(&permissions)
	if query.Error != nil {
		return nil, query.Error
	}
	return permissions, nil
}

func ChangeUserPassword(input *PasswordInput) error {
	db := mysql.GetDB()
	user := new(model.AuthUser)
	query := db.Model(&model.AuthUser{}).Where("user_name = ?", input.UserName).First(&user)
	if query.Error != nil {
		return query.Error
	}
	if user == nil || user == (&model.AuthUser{}) {
		return errors.New("user name or password incorrect")
	}
	if user.Password != input.OldPassword {
		return errors.New("user name or password incorrect")
	}

	// check new password complies with the rule
	password, err := crypt.Decrypt(input.NewPassword)
	if err != nil {
		log.Errorf("password decrypt failed :%v", err.Error())
		return errors.New("user name or password incorrect")
	}
	if len(password) < model.PasswordMinLength || len(password) > model.PasswordMaxLength {
		return errors.New("the password length exceeds the limit")
	}
	re, _ := regexp.Compile(`^[a-zA-Z0-9~!@#$%^&*_.?]+$`)
	match := re.MatchString(password)
	if !match {
		return errors.New("the password can contain only digits, uppercase letters, lowercase letters and special characters(~!@#$%^&*_.?)")
	}
	var level = 0
	regList := []string{`[0-9]+`, `[a-z]+`, `[A-Z]+`, `[~!@#$%^&*.?]+`}
	for _, reg := range regList {
		match, _ := regexp.MatchString(reg, password)
		if match {
			level++
		}
	}
	if level < 2 {
		return errors.New("the password must contain at least two types of digits, uppercase letters, lowercase letters and special characters(~!@#$%^&*.?)")
	}

	user.Password = input.NewPassword
	query = db.Save(&user)
	if query.Error != nil {
		return query.Error
	}
	return nil
}
