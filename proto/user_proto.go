// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package proto

import (
	"fmt"
	"regexp"
	"sync"
)

var (
	AKRegexp = regexp.MustCompile("^[a-zA-Z0-9]{16}$")
	SKRegexp = regexp.MustCompile("^[a-zA-Z0-9]{32}$")
)

type UserType uint8

const (
	UserTypeInvalid UserType = 0x0
	UserTypeRoot    UserType = 0x1
	UserTypeAdmin   UserType = 0x2
	UserTypeNormal  UserType = 0x3
)

func (u UserType) Valid() bool {
	switch u {
	case UserTypeRoot,
		UserTypeAdmin,
		UserTypeNormal:
		return true
	default:
	}
	return false
}

func (u UserType) String() string {
	switch u {
	case UserTypeRoot:
		return "root"
	case UserTypeAdmin:
		return "admin"
	case UserTypeNormal:
		return "normal"
	default:
	}
	return "invalid"
}

func UserTypeFromString(name string) UserType {
	switch name {
	case "root":
		return UserTypeRoot
	case "admin":
		return UserTypeAdmin
	case "normal":
		return UserTypeNormal
	default:
	}
	return UserTypeInvalid
}

func IsValidAK(ak string) bool {
	if AKRegexp.MatchString(ak) {
		return true
	} else {
		return false
	}
}

func IsValidSK(sk string) bool {
	if SKRegexp.MatchString(sk) {
		return true
	} else {
		return false
	}
}

type AKUser struct {
	AccessKey string `json:"access_key" graphql:"access_key"`
	UserID    string `json:"user_id" graphql:"user_id"`
	Password  string `json:"password" graphql:"password"`
}

type UserInfo struct {
	UserID      string       `json:"user_id" graphql:"user_id"`
	AccessKey   string       `json:"access_key" graphql:"access_key"`
	SecretKey   string       `json:"secret_key" graphql:"secret_key"`
	Policy      *UserPolicy  `json:"policy" graphql:"policy"`
	UserType    UserType     `json:"user_type" graphql:"user_type"`
	CreateTime  string       `json:"create_time" graphql:"create_time"`
	Description string       `json:"description" graphql:"description"`
	Mu          sync.RWMutex `json:"-" graphql:"-"`
	EMPTY       bool         //graphql need ???
}

func (i *UserInfo) String() string {
	if i == nil {
		return "nil"
	}
	return fmt.Sprintf("%v_%v_%v_%v",
		i.UserID, i.AccessKey, i.SecretKey, i.UserType)
}

func NewUserInfo() *UserInfo {
	return &UserInfo{Policy: NewUserPolicy()}
}

type VolUser struct {
	Vol     string       `json:"vol"`
	UserIDs []string     `json:"user_id"`
	Mu      sync.RWMutex `json:"-" graphql:"-"`
}

type UserPolicy struct {
	OwnVols        []string            `json:"own_vols" graphql:"own_vols"`
	AuthorizedVols map[string][]string `json:"authorized_vols" graphql:"-"` // mapping: volume -> actions
	mu             sync.RWMutex
}

func NewUserPolicy() *UserPolicy {
	return &UserPolicy{
		OwnVols:        make([]string, 0),
		AuthorizedVols: make(map[string][]string),
	}
}

func (policy *UserPolicy) IsOwn(volume string) bool {
	policy.mu.RLock()
	defer policy.mu.RUnlock()
	for _, vol := range policy.OwnVols {
		if vol == volume {
			return true
		}
	}
	return false
}

func (policy *UserPolicy) IsAuthorized(volume, subdir string, action Action) bool {
	policy.mu.RLock()
	defer policy.mu.RUnlock()
	if len(policy.OwnVols) > 0 {
		for _, v := range policy.OwnVols {
			if v == volume {
				return true
			}
		}
	}
	values, exist := policy.AuthorizedVols[volume]
	if !exist {
		return false
	}
	for _, value := range values {
		if perm := ParsePermission(value); !perm.IsNone() && perm.IsBuiltin() && perm.MatchSubdir(subdir) && BuiltinPermissionActions(perm).Contains(action) {
			return true
		}
		if act := ParseAction(value); act == action {
			return true
		}
	}
	return false
}

func (policy *UserPolicy) AddOwnVol(volume string) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	for _, ownVol := range policy.OwnVols {
		if ownVol == volume {
			return
		}
	}
	policy.OwnVols = append(policy.OwnVols, volume)
}

func (policy *UserPolicy) RemoveOwnVol(volume string) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	for i, ownVol := range policy.OwnVols {
		if ownVol == volume {
			if i == len(policy.OwnVols)-1 {
				policy.OwnVols = policy.OwnVols[:i]
				return
			}
			policy.OwnVols = append(policy.OwnVols[:i], policy.OwnVols[i+1:]...)
			return
		}
	}
}

func (policy *UserPolicy) AddAuthorizedVol(volume string, policies []string) { //todo check duplicate
	policy.mu.Lock()
	defer policy.mu.Unlock()
	newPolicies := make([]string, 0)
	for _, policy := range policies {
		if perm := ParsePermission(policy); !perm.IsNone() {
			newPolicies = append(newPolicies, perm.String())
		}
		if act := ParseAction(policy); !act.IsNone() {
			newPolicies = append(newPolicies, act.String())
		}
	}
	policy.AuthorizedVols[volume] = newPolicies
}

func (policy *UserPolicy) RemoveAuthorizedVol(volume string) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	delete(policy.AuthorizedVols, volume)
}

func (policy *UserPolicy) SetPerm(volume string, perm Permission) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	policy.AuthorizedVols[volume] = []string{perm.String()}
}

func (policy *UserPolicy) SetActions(volume string, actions Actions) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	var values = make([]string, actions.Len())
	for i, action := range actions {
		values[i] = action.String()
	}
	policy.AuthorizedVols[volume] = values
}

func (policy *UserPolicy) Add(addPolicy *UserPolicy) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	policy.OwnVols = append(policy.OwnVols, addPolicy.OwnVols...)
	for k, v := range addPolicy.AuthorizedVols {
		if apis, ok := policy.AuthorizedVols[k]; ok {
			policy.AuthorizedVols[k] = append(apis, addPolicy.AuthorizedVols[k]...)
		} else {
			policy.AuthorizedVols[k] = v
		}
	}
}

func (policy *UserPolicy) Delete(deletePolicy *UserPolicy) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	policy.OwnVols = removeSlice(policy.OwnVols, deletePolicy.OwnVols)
	for k, v := range deletePolicy.AuthorizedVols {
		if apis, ok := policy.AuthorizedVols[k]; ok {
			policy.AuthorizedVols[k] = removeSlice(apis, v)
		}
	}
}

func removeSlice(s []string, removeSlice []string) []string {
	if len(s) == 0 {
		return s
	}
	for _, elem := range removeSlice {
		for i, v := range s {
			if v == elem {
				s = append(s[:i], s[i+1:]...)
				break
			}
		}
	}
	return s
}

func CleanPolicy(policy *UserPolicy) (newUserPolicy *UserPolicy) {
	m := make(map[string]bool)
	newUserPolicy = NewUserPolicy()
	policy.mu.Lock()
	defer policy.mu.Unlock()
	for _, vol := range policy.OwnVols {
		if _, exist := m[vol]; !exist {
			m[vol] = true
			newUserPolicy.OwnVols = append(newUserPolicy.OwnVols, vol)
		}
	}
	for vol, apis := range policy.AuthorizedVols {
		checkMap := make(map[string]bool)
		newAPI := make([]string, 0)
		for _, api := range apis {
			if _, exist := checkMap[api]; !exist {
				checkMap[api] = true
				newAPI = append(newAPI, api)
			}
		}
		newUserPolicy.AuthorizedVols[vol] = newAPI
	}
	return
}

type UserCreateParam struct {
	ID          string   `json:"id"`
	Password    string   `json:"pwd"`
	AccessKey   string   `json:"ak"`
	SecretKey   string   `json:"sk"`
	Type        UserType `json:"type"`
	Description string   `json:"description"`
}

type UserPermUpdateParam struct {
	UserID string   `json:"user_id"`
	Volume string   `json:"volume"`
	Subdir string   `json:"subdir"`
	Policy []string `json:"policy"`
}

func NewUserPermUpdateParam(userID, volmue string) *UserPermUpdateParam {
	return &UserPermUpdateParam{UserID: userID, Volume: volmue, Policy: make([]string, 0)}
}

func (param *UserPermUpdateParam) SetPolicy(policy string) {
	param.Policy = append(param.Policy, policy)
}

type UserPermRemoveParam struct {
	UserID string `json:"user_id"`
	Volume string `json:"volume"`
}

func NewUserPermRemoveParam(userID, volmue string) *UserPermRemoveParam {
	return &UserPermRemoveParam{UserID: userID, Volume: volmue}
}

type UserTransferVolParam struct {
	Volume  string `json:"volume"`
	UserSrc string `json:"user_src"`
	UserDst string `json:"user_dst"`
	Force   bool   `json:"force"`
}

type UserUpdateParam struct {
	UserID      string   `json:"user_id"`
	AccessKey   string   `json:"access_key"`
	SecretKey   string   `json:"secret_key"`
	Type        UserType `json:"type"`
	Password    string   `json:"password"`
	Description string   `json:"description"`
}
