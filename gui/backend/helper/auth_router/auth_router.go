package auth_router

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
)

var (
	PathAuthCodeMap    = make(map[string]string)
	PathAuthRoleMap    = make(map[string]string)
	PathAuthFuncMap    = make(map[string]AuthHandlerFunc)
	PathAuthNoLoginMap = make(map[string]bool)
)

type AuthHandlerFunc func(*gin.Context) bool

type RouterGroup struct {
	RouterGroup *gin.RouterGroup
}

type IRoutes interface {
	Handle(string, string, ...gin.HandlerFunc) IRoutes
	HandleAuthCode(string, string, string, ...gin.HandlerFunc) IRoutes
	HandleAuthRole(string, string, string, ...gin.HandlerFunc) IRoutes
	HandleAuthFunc(string, string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	HandleAuthNoLogin(string, string, ...gin.HandlerFunc) IRoutes
	Any(string, ...gin.HandlerFunc) IRoutes
	AnyAuthCode(string, string, ...gin.HandlerFunc) IRoutes
	AnyAuthRole(string, string, ...gin.HandlerFunc) IRoutes
	AnyAuthFunc(string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	AnyAuthNoLogin(string, ...gin.HandlerFunc) IRoutes
	GET(string, ...gin.HandlerFunc) IRoutes
	GetAuthCode(string, string, ...gin.HandlerFunc) IRoutes
	GetAuthRole(string, string, ...gin.HandlerFunc) IRoutes
	GetAuthFunc(string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	GetAuthNoLogin(string, ...gin.HandlerFunc) IRoutes
	POST(string, ...gin.HandlerFunc) IRoutes
	PostAuthCode(string, string, ...gin.HandlerFunc) IRoutes
	PostAuthRole(string, string, ...gin.HandlerFunc) IRoutes
	PostAuthFunc(string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	PostAuthNoLogin(string, ...gin.HandlerFunc) IRoutes
	DELETE(string, ...gin.HandlerFunc) IRoutes
	DeleteAuthCode(string, string, ...gin.HandlerFunc) IRoutes
	DeleteAuthRole(string, string, ...gin.HandlerFunc) IRoutes
	DeleteAuthFunc(string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	DeleteAuthNoLogin(string, ...gin.HandlerFunc) IRoutes
	PATCH(string, ...gin.HandlerFunc) IRoutes
	PatchAuthCode(string, string, ...gin.HandlerFunc) IRoutes
	PatchAuthRole(string, string, ...gin.HandlerFunc) IRoutes
	PatchAuthFunc(string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	PatchAuthNoLogin(string, ...gin.HandlerFunc) IRoutes
	PUT(string, ...gin.HandlerFunc) IRoutes
	PutAuthCode(string, string, ...gin.HandlerFunc) IRoutes
	PutAuthRole(string, string, ...gin.HandlerFunc) IRoutes
	PutAuthFunc(string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	PutAuthNoLogin(string, ...gin.HandlerFunc) IRoutes
	OPTIONS(string, ...gin.HandlerFunc) IRoutes
	OptionsAuthCode(string, string, ...gin.HandlerFunc) IRoutes
	OptionsAuthRole(string, string, ...gin.HandlerFunc) IRoutes
	OptionsAuthFunc(string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	OptionsAuthNoLogin(string, ...gin.HandlerFunc) IRoutes
	HEAD(string, ...gin.HandlerFunc) IRoutes
	HeadAuthCode(string, string, ...gin.HandlerFunc) IRoutes
	HeadAuthRole(string, string, ...gin.HandlerFunc) IRoutes
	HeadAuthFunc(string, AuthHandlerFunc, ...gin.HandlerFunc) IRoutes
	HeadAuthNoLogin(string, ...gin.HandlerFunc) IRoutes
}

func (group *RouterGroup) getPathKey(httpMethod, relativePath string) string {
	path := strings.TrimSuffix(group.RouterGroup.BasePath(), "/") + "/" + strings.TrimPrefix(relativePath, "/")
	return httpMethod + strings.ReplaceAll(path, "/", "_")
}

func (group *RouterGroup) addPathAuthCodeMap(httpMethod, relativePath, authCode string) {
	path := group.getPathKey(httpMethod, relativePath)
	PathAuthCodeMap[path] = authCode
}

func (group *RouterGroup) addPathAuthRoleMap(httpMethod, relativePath string, authRole string) {
	path := group.getPathKey(httpMethod, relativePath)
	PathAuthRoleMap[path] = authRole
}

func (group *RouterGroup) addPathAuthFuncMap(httpMethod, relativePath string, authHandleFunc AuthHandlerFunc) {
	path := group.getPathKey(httpMethod, relativePath)
	PathAuthFuncMap[path] = authHandleFunc
}

func (group *RouterGroup) addPathAuthNoLoginMap(httpMethod, relativePath string) {
	path := group.getPathKey(httpMethod, relativePath)
	PathAuthNoLoginMap[path] = true
}

func (group *RouterGroup) Group(relativePath string, handlers ...gin.HandlerFunc) *RouterGroup {
	return &RouterGroup{
		RouterGroup: group.RouterGroup.Group(relativePath, handlers...),
	}
}

func (group *RouterGroup) Handle(httpMethod, relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.Handle(httpMethod, relativePath, handlers...)
	return group
}

func (group *RouterGroup) HandleAuthCode(httpMethod, relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(httpMethod, relativePath, authCode)
	return group.Handle(httpMethod, relativePath, handlers...)
}

func (group *RouterGroup) HandleAuthRole(httpMethod, relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(httpMethod, relativePath, authRole)
	return group.Handle(httpMethod, relativePath, handlers...)
}

func (group *RouterGroup) HandleAuthFunc(httpMethod, relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(httpMethod, relativePath, authHandleFunc)
	return group.Handle(httpMethod, relativePath, handlers...)
}

func (group *RouterGroup) HandleAuthNoLogin(httpMethod, relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(httpMethod, relativePath)
	return group.Handle(httpMethod, relativePath, handlers...)
}

func (group *RouterGroup) GET(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.GET(relativePath, handlers...)
	return group
}

func (group *RouterGroup) GetAuthCode(relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(http.MethodGet, relativePath, authCode)
	return group.GET(relativePath, handlers...)
}

func (group *RouterGroup) GetAuthRole(relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(http.MethodGet, relativePath, authRole)
	return group.GET(relativePath, handlers...)
}

func (group *RouterGroup) GetAuthFunc(relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(http.MethodGet, relativePath, authHandleFunc)
	return group.GET(relativePath, handlers...)
}

func (group *RouterGroup) GetAuthNoLogin(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(http.MethodGet, relativePath)
	return group.GET(relativePath, handlers...)
}

func (group *RouterGroup) POST(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.POST(relativePath, handlers...)
	return group
}

func (group *RouterGroup) PostAuthCode(relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(http.MethodPost, relativePath, authCode)
	return group.POST(relativePath, handlers...)
}

func (group *RouterGroup) PostAuthRole(relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(http.MethodPost, relativePath, authRole)
	return group.POST(relativePath, handlers...)
}

func (group *RouterGroup) PostAuthFunc(relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(http.MethodPost, relativePath, authHandleFunc)
	return group.POST(relativePath, handlers...)
}

func (group *RouterGroup) PostAuthNoLogin(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(http.MethodPost, relativePath)
	return group.POST(relativePath, handlers...)
}

func (group *RouterGroup) PUT(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.PUT(relativePath, handlers...)
	return group
}

func (group *RouterGroup) PutAuthCode(relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(http.MethodPut, relativePath, authCode)
	return group.PUT(relativePath, handlers...)
}

func (group *RouterGroup) PutAuthRole(relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(http.MethodPut, relativePath, authRole)
	return group.PUT(relativePath, handlers...)
}

func (group *RouterGroup) PutAuthFunc(relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(http.MethodPut, relativePath, authHandleFunc)
	return group.PUT(relativePath, handlers...)
}

func (group *RouterGroup) PutAuthNoLogin(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(http.MethodPut, relativePath)
	return group.PUT(relativePath, handlers...)
}

func (group *RouterGroup) PATCH(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.PATCH(relativePath, handlers...)
	return group
}

func (group *RouterGroup) PatchAuthCode(relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(http.MethodPatch, relativePath, authCode)
	return group.PATCH(relativePath, handlers...)
}

func (group *RouterGroup) PatchAuthRole(relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(http.MethodPatch, relativePath, authRole)
	return group.PATCH(relativePath, handlers...)
}

func (group *RouterGroup) PatchAuthFunc(relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(http.MethodPatch, relativePath, authHandleFunc)
	return group.PATCH(relativePath, handlers...)
}

func (group *RouterGroup) PatchAuthNoLogin(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(http.MethodPatch, relativePath)
	return group.PATCH(relativePath, handlers...)
}

func (group *RouterGroup) DELETE(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.DELETE(relativePath, handlers...)
	return group
}

func (group *RouterGroup) DeleteAuthCode(relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(http.MethodDelete, relativePath, authCode)
	return group.DELETE(relativePath, handlers...)
}

func (group *RouterGroup) DeleteAuthRole(relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(http.MethodDelete, relativePath, authRole)
	return group.DELETE(relativePath, handlers...)
}

func (group *RouterGroup) DeleteAuthFunc(relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(http.MethodDelete, relativePath, authHandleFunc)
	return group.DELETE(relativePath, handlers...)
}

func (group *RouterGroup) DeleteAuthNoLogin(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(http.MethodDelete, relativePath)
	return group.DELETE(relativePath, handlers...)
}

func (group *RouterGroup) OPTIONS(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.OPTIONS(relativePath, handlers...)
	return group
}

func (group *RouterGroup) OptionsAuthCode(relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(http.MethodOptions, relativePath, authCode)
	return group.OPTIONS(relativePath, handlers...)
}

func (group *RouterGroup) OptionsAuthRole(relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(http.MethodOptions, relativePath, authRole)
	return group.OPTIONS(relativePath, handlers...)
}

func (group *RouterGroup) OptionsAuthFunc(relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(http.MethodOptions, relativePath, authHandleFunc)
	return group.OPTIONS(relativePath, handlers...)
}

func (group *RouterGroup) OptionsAuthNoLogin(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(http.MethodOptions, relativePath)
	return group.OPTIONS(relativePath, handlers...)
}

func (group *RouterGroup) HEAD(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.HEAD(relativePath, handlers...)
	return group
}

func (group *RouterGroup) HeadAuthCode(relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(http.MethodHead, relativePath, authCode)
	return group.HEAD(relativePath, handlers...)
}

func (group *RouterGroup) HeadAuthRole(relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(http.MethodHead, relativePath, authRole)
	return group.HEAD(relativePath, handlers...)
}

func (group *RouterGroup) HeadAuthFunc(relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(http.MethodHead, relativePath, authHandleFunc)
	return group.HEAD(relativePath, handlers...)
}

func (group *RouterGroup) HeadAuthNoLogin(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(http.MethodHead, relativePath)
	return group.HEAD(relativePath, handlers...)
}

func (group *RouterGroup) Any(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.RouterGroup.Any(relativePath, handlers...)
	return group
}

func (group *RouterGroup) AnyAuthCode(relativePath, authCode string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthCodeMap(http.MethodGet, relativePath, authCode)
	group.addPathAuthCodeMap(http.MethodPost, relativePath, authCode)
	group.addPathAuthCodeMap(http.MethodPut, relativePath, authCode)
	group.addPathAuthCodeMap(http.MethodPatch, relativePath, authCode)
	group.addPathAuthCodeMap(http.MethodHead, relativePath, authCode)
	group.addPathAuthCodeMap(http.MethodOptions, relativePath, authCode)
	group.addPathAuthCodeMap(http.MethodDelete, relativePath, authCode)
	group.addPathAuthCodeMap(http.MethodConnect, relativePath, authCode)
	group.addPathAuthCodeMap(http.MethodTrace, relativePath, authCode)
	return group.Any(relativePath, handlers...)
}

func (group *RouterGroup) AnyAuthRole(relativePath, authRole string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthRoleMap(http.MethodGet, relativePath, authRole)
	group.addPathAuthRoleMap(http.MethodPost, relativePath, authRole)
	group.addPathAuthRoleMap(http.MethodPut, relativePath, authRole)
	group.addPathAuthRoleMap(http.MethodPatch, relativePath, authRole)
	group.addPathAuthRoleMap(http.MethodHead, relativePath, authRole)
	group.addPathAuthRoleMap(http.MethodOptions, relativePath, authRole)
	group.addPathAuthRoleMap(http.MethodDelete, relativePath, authRole)
	group.addPathAuthRoleMap(http.MethodConnect, relativePath, authRole)
	group.addPathAuthRoleMap(http.MethodTrace, relativePath, authRole)
	return group.Any(relativePath, handlers...)
}

func (group *RouterGroup) AnyAuthFunc(relativePath string, authHandleFunc AuthHandlerFunc, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthFuncMap(http.MethodGet, relativePath, authHandleFunc)
	group.addPathAuthFuncMap(http.MethodPost, relativePath, authHandleFunc)
	group.addPathAuthFuncMap(http.MethodPut, relativePath, authHandleFunc)
	group.addPathAuthFuncMap(http.MethodPatch, relativePath, authHandleFunc)
	group.addPathAuthFuncMap(http.MethodHead, relativePath, authHandleFunc)
	group.addPathAuthFuncMap(http.MethodOptions, relativePath, authHandleFunc)
	group.addPathAuthFuncMap(http.MethodDelete, relativePath, authHandleFunc)
	group.addPathAuthFuncMap(http.MethodConnect, relativePath, authHandleFunc)
	group.addPathAuthFuncMap(http.MethodTrace, relativePath, authHandleFunc)
	return group.Any(relativePath, handlers...)
}

func (group *RouterGroup) AnyAuthNoLogin(relativePath string, handlers ...gin.HandlerFunc) IRoutes {
	group.addPathAuthNoLoginMap(http.MethodGet, relativePath)
	group.addPathAuthNoLoginMap(http.MethodPost, relativePath)
	group.addPathAuthNoLoginMap(http.MethodPut, relativePath)
	group.addPathAuthNoLoginMap(http.MethodPatch, relativePath)
	group.addPathAuthNoLoginMap(http.MethodHead, relativePath)
	group.addPathAuthNoLoginMap(http.MethodOptions, relativePath)
	group.addPathAuthNoLoginMap(http.MethodDelete, relativePath)
	group.addPathAuthNoLoginMap(http.MethodConnect, relativePath)
	group.addPathAuthNoLoginMap(http.MethodTrace, relativePath)
	return group.Any(relativePath, handlers...)
}
