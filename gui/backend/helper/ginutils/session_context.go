package ginutils

import (
	"errors"

	"github.com/gin-gonic/gin"

	"github.com/cubefs/cubefs/console/backend/helper/enums"
)

type LoginUser struct {
	Id       int
	UserName string
	Email    string
	Phone    string
}

func GetLoginUser(c *gin.Context) (*LoginUser, error) {
	sessionData, exists := c.Get(enums.LoginUser)
	if !exists {
		return nil, errors.New("session not exists")
	}
	user, ok := sessionData.(map[string]interface{})
	if !ok {
		return nil, errors.New("invalid session data")
	}
	return &LoginUser{
		Id:       user["Id"].(int),
		UserName: user["UserName"].(string),
		Email:    user["Email"].(string),
		Phone:    user["Phone"].(string),
	}, nil
}
