package router

import (
	"github.com/cubefs/cubefs/console/backend/config"
	"github.com/cubefs/cubefs/console/backend/handler/auth"
	"github.com/gin-gonic/gin"
)

type authRouter struct{}

func (r *authRouter) Register(engine *gin.Engine) {
	group := engine.Group(config.Conf.Prefix.Api + "/console/auth")

	group.POST("/login", auth.LoginHandler)
	group.POST("/logout", auth.LogoutHandler)

	user := group.Group("/user")
	{
		user.GET("/list", auth.GetUserHandler)
		user.POST("/create", auth.CreateUserHandler)
		user.PUT("/update", auth.UpdateUserHandler)
		user.PUT("/self/update", auth.UpdateSelfUserHandler)
		user.DELETE("/delete", auth.DeleteUserHandler)
		user.PUT("/password/update", auth.UpdateUserPasswordHandler)
		user.PUT("/password/self/update/", auth.UpdateSelfUserPasswordHandler)
		user.GET("/permission", auth.GetUserPermissionHandler)
	}
	role := group.Group("/role")
	{
		role.GET("/list", auth.GetRoleHandler)
		role.POST("/create", auth.CreateRoleHandler)
		role.PUT("/update", auth.UpdateRoleHandler)
		role.DELETE("/delete", auth.DeleteRoleHandler)
	}
	permission := group.Group("/permission")
	{
		permission.GET("/list", auth.GetPermissionHandler)
		permission.POST("/create", auth.CreatePermissionHandler)
		permission.PUT("/update", auth.UpdatePermissionHandler)
		permission.DELETE("/delete", auth.DeletePermissionHandler)
	}
}
