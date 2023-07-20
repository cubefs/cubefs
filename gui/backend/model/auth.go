package model

import (
	"errors"
	"regexp"
	"time"

	"github.com/cubefs/cubefs/console/backend/helper/crypt"
	"github.com/cubefs/cubefs/console/backend/model/mysql"
)

const (
	PasswordMinLength = 8
	PasswordMaxLength = 16
)

type AuthUser struct {
	Id         int       `gorm:"primaryKey;AUTO_INCREMENT;column:id;" json:"id"`
	UserName   string    `gorm:"index:idx_name,unique;column:user_name;type:varchar(32);not null;" json:"user_name"`
	Password   string    `gorm:"column:password;type:varchar(128);not null" json:"password"`
	Email      string    `gorm:"index:idx_email,unique;column:email;type:varchar(128);not null" json:"email"`
	Phone      string    `gorm:"index:idx_phone,unique;column:phone;type:varchar(11);not null" json:"phone"`
	CreateTime time.Time `gorm:"column:create_time;not null;autoCreateTime;type:datetime" json:"createTime"`
	UpdateTime time.Time `gorm:"column:update_time;not null;autoUpdateTime;type:datetime" json:"updateTime"`
}

type AuthRole struct {
	Id          int       `gorm:"primaryKey;AUTO_INCREMENT;column:id;" json:"id"`
	RoleCode    string    `gorm:"index:idx_code,unique;column:role_code;type:varchar(128);not null;" json:"role_code"`
	RoleName    string    `gorm:"index:idx_name,unique;column:role_name;type:varchar(128);not null" json:"role_name"`
	MaxVolCount int       `gorm:"column:max_vol_count;type:int;" json:"max_vol_count"`
	MaxVolSize  int       `gorm:"column:max_vol_size;type:int;" json:"max_vol_size"`
	CreateTime  time.Time `gorm:"column:create_time;not null;autoCreateTime;type:datetime" json:"createTime"`
	UpdateTime  time.Time `gorm:"column:update_time;not null;autoUpdateTime;type:datetime" json:"updateTime"`
}

type AuthPermission struct {
	Id         int       `gorm:"primaryKey;AUTO_INCREMENT;column:id;" json:"id"`
	AuthCode   string    `gorm:"index:idx_code,unique;column:auth_code;type:varchar(128);not null;" json:"auth_code"`
	AuthName   string    `gorm:"column:auth_name;type:varchar(128);not null;" json:"auth_name"`
	AuthType   *int      `gorm:"column:auth_type;type:int(5);default:0;not null;" json:"auth_type"` // 0. backend api auth 1. front auth
	URI        string    `gorm:"column:uri;type:varchar(200);" json:"uri"`
	Method     string    `gorm:"column:method;type:varchar(10);" json:"method"`
	Group      string    `gorm:"column:group;type:varchar(128);" json:"group"`
	IsLogin    bool      `gorm:"column:is_login;type:tinyint(1);default:false;not null" json:"is_login"`
	IsCheck    bool      `gorm:"column:is_check;type:tinyint(1);default:false;not null" json:"is_check"`
	CreateTime time.Time `gorm:"column:create_time;not null;autoCreateTime;type:datetime" json:"createTime"`
	UpdateTime time.Time `gorm:"column:update_time;not null;autoUpdateTime;type:datetime" json:"updateTime"`
}

type AuthUserRole struct {
	Id     int `gorm:"primaryKey;AUTO_INCREMENT;column:id;" json:"id"`
	UserId int `gorm:"index:idx_user_role,unique;column:user_id;type:int;not null;" json:"user_id"`
	RoleId int `gorm:"index:idx_user_role,unique;column:role_id;type:int;not null;" json:"role_id"`
}

type AuthRolePermission struct {
	Id           int `gorm:"primaryKey;AUTO_INCREMENT;column:id;" json:"id"`
	RoleId       int `gorm:"index:idx_role_permission,unique;column:role_id;type:int;not null;" json:"role_id"`
	PermissionId int `gorm:"index:idx_role_permission,unique;column:permission_id;type:int;not null;" json:"permission_id"`
}

type AuthUserVo struct {
	AuthUser
	Password *struct{}  `json:"password,omitempty"`
	Roles    []AuthRole `json:"roles"`
}

type AuthRoleVo struct {
	AuthRole
	Permissions []AuthPermission `json:"permissions"`
}

type FindAuthPermissionParam struct {
	Page         int    `form:"page"`
	PageSize     int    `form:"page_size"`
	Ids          []int  `form:"ids"`
	AuthCodeLike string `form:"auth_code_like"`
	AuthNameLike string `form:"auth_name_like"`
}

type FindAuthRoleParam struct {
	Page         int    `form:"page"`
	PageSize     int    `form:"page_size"`
	Ids          []int  `form:"ids"`
	RoleCodeLike string `form:"role_code_like"`
	RoleNameLike string `form:"role_name_like"`
}

type FindAuthUserParam struct {
	IsAll        bool   `form:"is_all"`
	Page         int    `form:"page"`
	PageSize     int    `form:"page_size"`
	UserNameLike string `form:"user_name_like"`
	EmailLike    string `form:"email_like"`
	PhoneLike    string `form:"phone_like"`
}

func (p *AuthPermission) Find(param *FindAuthPermissionParam) ([]AuthPermission, int, error) {
	db := mysql.GetDB()
	db = db.Where("1 = 1")
	if p.Id != 0 {
		db = db.Where("id = ?", p.Id)
	}
	if p.AuthCode != "" {
		db = db.Where("auth_code = ?", p.AuthCode)
	}
	if p.AuthName != "" {
		db = db.Where("auth_name = ?", p.AuthName)
	}
	if p.AuthType != nil {
		db = db.Where("auth_type = ?", *p.AuthType)
	}
	if param != nil {
		if len(param.Ids) > 0 {
			db = db.Where("id in ?", param.Ids)
		}
		if param.AuthCodeLike != "" {
			db = db.Where("auth_code like ?", "%"+param.AuthCodeLike+"%")
		}
		if param.AuthNameLike != "" {
			db = db.Where("auth_name like ?", "%"+param.AuthNameLike+"%")
		}
		if param.PageSize == 0 {
			param.PageSize = 10
		}
		if param.Page == 0 {
			param.Page = 1
		}
	} else {
		param = &FindAuthPermissionParam{}
		param.Page = 1
		param.PageSize = 10
	}
	var count int64
	db = db.Model(&AuthPermission{}).Count(&count)
	db = db.Scopes(mysql.Paginate(param.PageSize, param.Page))
	permissions := make([]AuthPermission, 0)
	query := db.Find(&permissions)
	if query.Error != nil {
		return nil, 0, query.Error
	}
	return permissions, int(count), nil
}

func (p *AuthPermission) IsValid() error {
	if p == nil {
		return errors.New("the permission param is nil")
	}
	// check whether the parameter has a null value
	if p.AuthCode == "" || p.AuthName == "" || p.AuthType == nil || p.Group == "" {
		return errors.New("the permission param has empty value")
	}
	if *p.AuthType == 0 && (p.Method == "" || p.URI == "") {
		return errors.New("the permission param has empty value")
	}

	// check the permission code complies with the rules：can contain only digits, uppercase letters, lowercase letters and underline(_)
	re, _ := regexp.Compile(`^[a-zA-Z0-9_]+$`)
	match := re.MatchString(p.AuthCode)
	if !match {
		return errors.New("the auth code can contain only digits, uppercase letters, lowercase letters and underline")
	}
	return nil
}

func (p *AuthPermission) Create() error {
	err := p.IsValid()
	if err != nil {
		return err
	}
	// check whether the permission exist
	db := mysql.GetDB()
	temps := make([]AuthPermission, 0)
	query := db.Model(&AuthPermission{}).Where("auth_code = ?", p.AuthCode).Find(&temps)
	if query.Error != nil {
		return query.Error
	}
	if len(temps) != 0 {
		return errors.New("the permission already exists")
	}
	if *p.AuthType == 0 {
		query = db.Model(&AuthPermission{}).Where("uri = ? and method = ?", p.URI, p.Method).Find(&temps)
		if query.Error != nil {
			return query.Error
		}
		if len(temps) != 0 {
			return errors.New("the permission already exists")
		}
	}
	permission := new(AuthPermission)
	permission.AuthCode = p.AuthCode
	permission.AuthName = p.AuthName
	permission.AuthType = p.AuthType
	permission.URI = p.URI
	permission.Method = p.Method
	permission.Group = p.Group
	permission.IsLogin = p.IsLogin
	permission.IsCheck = p.IsCheck
	query = db.Create(&permission)
	if query.Error != nil {
		return query.Error
	}
	return nil
}

func (p *AuthPermission) Update() error {
	err := p.IsValid()
	if err != nil {
		return err
	}
	if p.Id == 0 {
		return errors.New("id is required")
	}

	db := mysql.GetDB()
	temps := make([]AuthPermission, 0)
	query := db.Model(&AuthPermission{}).Where("id != ? and (auth_code = ? )", p.Id, p.AuthCode).Find(&temps)
	if query.Error != nil {
		return query.Error
	}
	if len(temps) > 0 {
		return errors.New("the permission already exists")
	}
	if *p.AuthType == 0 {
		query = db.Model(&AuthPermission{}).Where("id != ? and uri = ? and method = ?", p.Id, p.URI, p.Method).Find(&temps)
		if query.Error != nil {
			return query.Error
		}
		if len(temps) != 0 {
			return errors.New("the permission already exists")
		}
	}

	permission := new(AuthPermission)
	query = db.Model(&AuthPermission{}).Where("id = ?", p.Id).First(&permission)
	if query.Error != nil {
		return query.Error
	}
	if permission == nil || permission == (&AuthPermission{}) {
		return errors.New("the permission is not exists")
	}
	permission.AuthCode = p.AuthCode
	permission.AuthName = p.AuthName
	permission.AuthType = p.AuthType
	permission.URI = p.URI
	permission.Method = p.Method
	permission.Group = p.Group
	permission.IsLogin = p.IsLogin
	permission.IsCheck = p.IsCheck
	query = db.Updates(&permission)
	if query.Error != nil {
		return query.Error
	}
	return nil
}

func (p *AuthPermission) Delete(ids []int) error {
	if len(ids) == 0 {
		return errors.New("delete the permission ids is empty")
	}
	db := mysql.GetDB()
	tx := db.Begin()
	query := tx.Delete(&AuthPermission{}, "id in ?", ids)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	query = tx.Delete(&AuthRolePermission{}, "permission_id in ?", ids)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	tx.Commit()
	return nil
}

func (r *AuthRole) Find(param *FindAuthRoleParam) ([]AuthRoleVo, int, error) {
	db := mysql.GetDB()
	db = db.Where("1 = 1")
	if r.Id != 0 {
		db = db.Where("id = ?", r.Id)
	}
	if r.RoleCode != "" {
		db = db.Where("role_code = ?", r.RoleCode)
	}
	if r.RoleName != "" {
		db = db.Where("role_name = ?", r.RoleName)
	}
	if param != nil {
		if len(param.Ids) > 0 {
			db = db.Where("id in ?", param.Ids)
		}
		if param.RoleCodeLike != "" {
			db = db.Where("role_code like ?", "%"+param.RoleCodeLike+"%")
		}
		if param.RoleNameLike != "" {
			db = db.Where("role_name like ?", "%"+param.RoleNameLike+"%")
		}
		if param.PageSize == 0 {
			param.PageSize = 10
		}
		if param.Page == 0 {
			param.Page = 1
		}
	} else {
		param = &FindAuthRoleParam{}
		param.Page = 1
		param.PageSize = 10
	}
	var count int64
	db = db.Model(&AuthRole{}).Count(&count)
	db = db.Scopes(mysql.Paginate(param.PageSize, param.Page))
	roles := make([]AuthRole, 0)
	query := db.Find(&roles)
	if query.Error != nil {
		return nil, 0, query.Error
	}
	roleIds := make([]int, 0)
	for _, role := range roles {
		roleIds = append(roleIds, role.Id)
	}

	// get the relationship between the role and the permission
	type rolePermissionRelation struct {
		RoleId int `json:"role_id"`
		AuthPermission
	}
	roleRes := make([]rolePermissionRelation, 0)
	db2 := mysql.GetDB()
	query = db2.Model(&AuthRolePermission{}).Select("auth_role_permissions.role_id, auth_permissions.*").
		Where("auth_role_permissions.role_id in ?", roleIds).
		Joins("left join auth_permissions on auth_role_permissions.permission_id = auth_permissions.id").Find(&roleRes)
	if query.Error != nil {
		return nil, 0, query.Error
	}
	roleReMap := make(map[int][]AuthPermission)
	for _, item := range roleRes {
		if _, ok := roleReMap[item.RoleId]; !ok {
			roleReMap[item.RoleId] = make([]AuthPermission, 0)
		}
		roleReMap[item.RoleId] = append(roleReMap[item.RoleId], item.AuthPermission)
	}

	roleVos := make([]AuthRoleVo, 0)
	for _, role := range roles {
		temp := AuthRoleVo{}
		temp.AuthRole = role
		temp.Permissions = []AuthPermission{}
		if value, ok := roleReMap[role.Id]; ok {
			temp.Permissions = value
		}
		roleVos = append(roleVos, temp)
	}
	return roleVos, int(count), nil
}

func (r *AuthRole) IsValid() error {
	// check whether the parameter has a null value
	if r.RoleCode == "" || r.RoleName == "" {
		return errors.New("the role param has empty value")
	}
	// check the role code complies with the rules：can contain only digits, uppercase letters, lowercase letters and underline(_)
	re, _ := regexp.Compile(`^[a-zA-Z0-9_]+$`)
	match := re.MatchString(r.RoleCode)
	if !match {
		return errors.New("the role code can contain only digits, uppercase letters, lowercase letters and underline")
	}
	return nil
}

func (r *AuthRole) Create(permissionIds []int) error {
	err := r.IsValid()
	if err != nil {
		return err
	}
	// check whether the role exist
	db := mysql.GetDB()
	temps := make([]AuthRole, 0)
	query := db.Model(&AuthRole{}).Where("role_code = ? or role_name = ?", r.RoleCode, r.RoleName).Find(&temps)
	if query.Error != nil {
		return query.Error
	}
	if len(temps) != 0 {
		return errors.New("the role already exists")
	}
	tx := db.Begin()
	role := new(AuthRole)
	role.RoleCode = r.RoleCode
	role.RoleName = r.RoleName
	role.MaxVolSize = r.MaxVolSize
	role.MaxVolCount = r.MaxVolCount
	query = tx.Create(&role)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}

	// save the relationship between the role and the permission
	query = tx.Delete(&AuthRolePermission{}, "role_id = ?", role.Id)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	perIds := make([]int, 0)
	query = tx.Model(&AuthPermission{}).Select("id").Where("id in ?", permissionIds).Find(&perIds)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	rolePerList := make([]AuthRolePermission, 0)
	for _, item := range perIds {
		rolePerList = append(rolePerList, AuthRolePermission{RoleId: role.Id, PermissionId: item})
	}
	query = tx.Model(&AuthRolePermission{}).Create(&rolePerList)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	tx.Commit()
	return nil
}

func (r *AuthRole) Update(permissionIds []int) error {
	err := r.IsValid()
	if err != nil {
		return err
	}
	if r.Id == 0 {
		return errors.New("update role id is empty")
	}

	// check whether the role exist
	db := mysql.GetDB()
	temps := make([]AuthRole, 0)
	query := db.Model(&AuthRole{}).Where("id != ? and (role_code = ? or role_name = ?)", r.Id, r.RoleCode, r.RoleName).Find(&temps)
	if query.Error != nil {
		return query.Error
	}
	if len(temps) != 0 {
		return errors.New("the role already exists")
	}

	role := new(AuthRole)
	query = db.Model(&AuthRole{}).Where("id = ?", r.Id).First(&role)
	if query.Error != nil {
		return query.Error
	}
	if role == nil || role == (&AuthRole{}) {
		return errors.New("the role is not exists")
	}

	tx := db.Begin()
	role.RoleCode = r.RoleCode
	role.RoleName = r.RoleName
	role.MaxVolSize = r.MaxVolSize
	role.MaxVolCount = r.MaxVolCount
	query = tx.Updates(&role)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}

	// save the relationship between the role and the permission
	query = tx.Delete(&AuthRolePermission{}, "role_id = ?", role.Id)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	perIds := make([]int, 0)
	query = tx.Model(&AuthPermission{}).Select("id").Where("id in ?", permissionIds).Find(&perIds)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	rolePerList := make([]AuthRolePermission, 0)
	for _, item := range perIds {
		rolePerList = append(rolePerList, AuthRolePermission{RoleId: role.Id, PermissionId: item})
	}
	query = tx.Model(&AuthRolePermission{}).Create(&rolePerList)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	tx.Commit()
	return nil
}

func (r *AuthRole) Delete(ids []int) error {
	if len(ids) == 0 {
		return errors.New("delete the role ids is empty")
	}
	db := mysql.GetDB()
	tx := db.Begin()
	query := tx.Delete(&AuthRole{}, "id in ?", ids)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	query = tx.Delete(&AuthRolePermission{}, "role_id in ?", ids)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	query = tx.Delete(&AuthUserRole{}, "role_id in ?", ids)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	tx.Commit()
	return nil
}

func (u *AuthUser) Find(param *FindAuthUserParam) ([]AuthUserVo, int, error) {
	db := mysql.GetDB()
	db = db.Where("1 = 1")
	if u.Id != 0 {
		db = db.Where("id = ?", u.Id)
	}
	if u.UserName != "" {
		db = db.Where("user_name = ?", u.UserName)
	}
	if u.Email != "" {
		db = db.Where("email = ?", u.Email)
	}
	if u.Phone != "" {
		db = db.Where("phone = ?", u.Phone)
	}
	if param != nil {
		if param.UserNameLike != "" {
			db = db.Where("user_name like ?", "%"+param.UserNameLike+"%")
		}
		if param.EmailLike != "" {
			db = db.Where("email like ?", "%"+param.EmailLike+"%")
		}
		if param.PhoneLike != "" {
			db = db.Where("phone like ?", "%"+param.PhoneLike+"%")
		}
		if param.PageSize == 0 {
			param.PageSize = 10
		}
		if param.Page == 0 {
			param.Page = 1
		}
	} else {
		param = &FindAuthUserParam{}
		param.Page = 1
		param.PageSize = 10
	}
	var count int64
	db = db.Model(&AuthUser{}).Count(&count)
	db = db.Scopes(mysql.Paginate(param.PageSize, param.Page))
	users := make([]AuthUser, 0)
	query := db.Find(&users)
	if query.Error != nil {
		return nil, 0, query.Error
	}
	userIds := make([]int, 0)
	for _, user := range users {
		userIds = append(userIds, user.Id)
	}

	// get the relationship between the user and the role
	type userRoleRelation struct {
		UserId int `json:"user_id"`
		AuthRole
	}
	userRes := make([]userRoleRelation, 0)
	db2 := mysql.GetDB()
	query = db2.Model(&AuthUserRole{}).Select("auth_user_roles.user_id, auth_roles.*").
		Where("auth_user_roles.user_id in ?", userIds).
		Joins("left join auth_roles on auth_user_roles.role_id = auth_roles.id").Find(&userRes)
	if query.Error != nil {
		return nil, 0, query.Error
	}
	userReMap := make(map[int][]AuthRole)
	for _, item := range userRes {
		if _, ok := userReMap[item.UserId]; !ok {
			userReMap[item.UserId] = make([]AuthRole, 0)
		}
		userReMap[item.UserId] = append(userReMap[item.UserId], item.AuthRole)
	}

	userVos := make([]AuthUserVo, 0)
	for _, user := range users {
		temp := AuthUserVo{}
		temp.AuthUser = user
		temp.Roles = []AuthRole{}
		if value, ok := userReMap[user.Id]; ok {
			temp.Roles = value
		}
		userVos = append(userVos, temp)
	}
	return userVos, int(count), nil
}

func (u *AuthUser) IsValid() error {
	if u == nil {
		return errors.New("the user param is nil")
	}
	// check whether the parameter has a null value
	if u.UserName == "" || u.Email == "" || u.Password == "" || u.Phone == "" {
		return errors.New("the user param has empty value")
	}
	// check the username complies with the rule: can contain only digits, uppercase letters, lowercase letters and underline(_)
	re, _ := regexp.Compile(`^[a-zA-Z0-9_]+$`)
	match := re.MatchString(u.UserName)
	if !match {
		return errors.New("the user_name can contain only digits, uppercase letters, lowercase letters and underline")
	}

	// check the password complies with the rule: must contain at least two types of digits, uppercase letters, lowercase letters and special characters(~!@#$%^&*.?)
	password, err := crypt.Decrypt(u.Password)
	if err != nil {
		return errors.New("the password decrypt fail: " + err.Error())
	}
	if len(password) < PasswordMinLength || len(password) > PasswordMaxLength {
		return errors.New("the password length exceeds the limit")
	}
	re, _ = regexp.Compile(`^[a-zA-Z0-9~!@#$%^&*_.?]+$`)
	match = re.MatchString(password)
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
	return nil
}

func (u *AuthUser) Create(roleIds []int) error {
	err := u.IsValid()
	if err != nil {
		return err
	}
	// check whether the user exist
	db := mysql.GetDB()
	temps := make([]AuthUser, 0)
	query := db.Model(&AuthUser{}).Where("user_name = ? or email = ? or phone = ?", u.UserName, u.Email, u.Phone).Find(&temps)
	if query.Error != nil {
		return query.Error
	}
	if len(temps) != 0 {
		return errors.New("the user already exists")
	}
	tx := db.Begin()
	user := new(AuthUser)
	user.UserName = u.UserName
	user.Email = u.Email
	user.Password = u.Password
	user.Phone = u.Phone
	query = tx.Create(&user)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}

	// save the relationship between the role and the permission
	query = tx.Delete(&AuthUserRole{}, "user_id = ?", user.Id)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	roles := make([]int, 0)
	query = tx.Model(&AuthRole{}).Select("id").Where("id in ?", roleIds).Find(&roles)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	userRoleList := make([]AuthUserRole, 0)
	for _, item := range roles {
		userRoleList = append(userRoleList, AuthUserRole{UserId: user.Id, RoleId: item})
	}
	query = tx.Model(&AuthUserRole{}).Create(&userRoleList)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	tx.Commit()
	return nil
}

func (u *AuthUser) Update(roleIds []int) error {
	if u == nil {
		return errors.New("the user param is nil")
	}
	// check whether the parameter has a null value
	if u.Id == 0 || u.UserName == "" || u.Email == "" || u.Phone == "" {
		return errors.New("the user param has empty value")
	}
	// check the username complies with the rule: can contain only digits, uppercase letters, lowercase letters and underline(_)
	re, _ := regexp.Compile(`^[a-zA-Z0-9_]+$`)
	match := re.MatchString(u.UserName)
	if !match {
		return errors.New("the user_name can contain only digits, uppercase letters, lowercase letters and underline")
	}

	// check whether the new user exist
	db := mysql.GetDB()
	temps := make([]AuthUser, 0)
	query := db.Model(&AuthUser{}).
		Where("id != ? and (user_name = ? or email = ? or phone = ?)", u.Id, u.UserName, u.Email, u.Phone).Find(&temps)
	if query.Error != nil {
		return query.Error
	}
	if len(temps) != 0 {
		return errors.New("the user already exists")
	}

	// check whether the old user exist
	user := new(AuthUser)
	query = db.Model(&AuthUser{}).Where("id = ?", u.Id).First(&user)
	if query.Error != nil {
		return query.Error
	}
	if user == nil || user == (&AuthUser{}) {
		return errors.New("the user is not exists")
	}

	tx := db.Begin()
	user.UserName = u.UserName
	user.Email = u.Email
	user.Phone = u.Phone
	query = tx.Updates(&user)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}

	// save the relationship between the user and the role
	query = tx.Delete(&AuthUserRole{}, "user_id = ?", user.Id)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	roles := make([]int, 0)
	query = tx.Model(&AuthRole{}).Select("id").Where("id in ?", roleIds).Find(&roles)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	userRoleList := make([]AuthUserRole, 0)
	for _, item := range roles {
		userRoleList = append(userRoleList, AuthUserRole{UserId: user.Id, RoleId: item})
	}
	query = tx.Model(&AuthUserRole{}).Create(&userRoleList)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	tx.Commit()
	return nil
}

func (u *AuthUser) UpdatePassword() error {
	if u == nil {
		return errors.New("the user param is nil")
	}
	// check whether the parameter has a null value
	if u.UserName == "" || u.Password == "" {
		return errors.New("the user param has empty value")
	}

	user := new(AuthUser)
	db := mysql.GetDB()
	query := db.Model(&AuthUser{}).Where("user_name = ?", u.UserName).First(&user)
	if query.Error != nil {
		return query.Error
	}
	if user == nil || user == (&AuthUser{}) {
		return errors.New("the user is not exists")
	}

	// check new password complies with the rule
	password, err := crypt.Decrypt(u.Password)
	if err != nil {
		return errors.New("the password decrypt fail: " + err.Error())
	}
	if len(password) < PasswordMinLength || len(password) > PasswordMaxLength {
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

	user.Password = u.Password
	query = db.Save(&user)
	if query.Error != nil {
		return query.Error
	}
	return nil

}

func (u *AuthUser) Delete(ids []int) error {
	if len(ids) == 0 {
		return errors.New("delete the user ids is empty")
	}
	db := mysql.GetDB()
	tx := db.Begin()
	query := tx.Delete(&AuthUser{}, "id in ?", ids)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	query = tx.Delete(&AuthUserRole{}, "user_id in ?", ids)
	if query.Error != nil {
		tx.Rollback()
		return query.Error
	}
	tx.Commit()
	return nil
}
