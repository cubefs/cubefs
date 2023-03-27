package cutil

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/graphql/client/user"
	"github.com/google/uuid"
	"sync"
	"time"
)

var userTimeout int64 = 1200

var userCache = sync.Map{}

type cacheEntity struct {
	ui       *user.UserInfo
	lastTime int64
}

func TokenValidate(token string) (*user.UserInfo, error) {

	if token == "" {
		return nil, fmt.Errorf("the token not found in head:[%s] or url param:[%s]", proto.HeadAuthorized, proto.ParamAuthorized)
	}

	v, found := userCache.Load(token)

	if !found {
		return nil, fmt.Errorf("the token:[%s] is invalidate", token)
	}

	ce := v.(*cacheEntity)

	now := time.Now().Unix()
	if now-ce.lastTime > userTimeout {
		return nil, fmt.Errorf("token is timeoout, please register new")
	}
	ce.lastTime = now
	return ce.ui, nil
}

func TokenRegister(ui *user.UserInfo) string {
	token := uuid.New().String()
	userCache.Store(token, &cacheEntity{
		ui:       ui,
		lastTime: time.Now().Unix(),
	})
	return token
}
