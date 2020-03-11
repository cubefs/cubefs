package proto

import "sync"

type UserType string

const (
	SuperAdmin UserType = "super_admin"
	Admin      UserType = "admin"
	User       UserType = "user"
)

type UserAK struct {
	UserID    string `json:"user_id"`
	AccessKey string `json:"access_key"`
	Password  string `json:"password"`
}

type AKPolicy struct {
	AccessKey string      `json:"access_key"`
	SecretKey string      `json:"secret_key"`
	Policy    *UserPolicy `json:"policy"`
	UserID    string      `json:"user_id"`
	UserType  UserType    `json:"user_type"`
}

type UserPolicy struct {
	OwnVols    []string
	NoneOwnVol map[string][]string // mapping: volume -> actions
	mu         sync.RWMutex
}

func NewUserPolicy() *UserPolicy {
	return &UserPolicy{
		OwnVols:    make([]string, 0),
		NoneOwnVol: make(map[string][]string),
	}
}

type VolAK struct {
	Vol          string              `json:"vol"`
	AKAndActions map[string][]string // k: ak, v: actions
	sync.RWMutex
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

func (policy *UserPolicy) Add(addPolicy *UserPolicy) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	policy.OwnVols = append(policy.OwnVols, addPolicy.OwnVols...)
	for k, v := range addPolicy.NoneOwnVol {
		if apis, ok := policy.NoneOwnVol[k]; ok {
			policy.NoneOwnVol[k] = append(apis, addPolicy.NoneOwnVol[k]...)
		} else {
			policy.NoneOwnVol[k] = v
		}
	}
}

func (policy *UserPolicy) Delete(deletePolicy *UserPolicy) {
	policy.mu.Lock()
	defer policy.mu.Unlock()
	policy.OwnVols = removeSlice(policy.OwnVols, deletePolicy.OwnVols)
	for k, v := range deletePolicy.NoneOwnVol {
		if apis, ok := policy.NoneOwnVol[k]; ok {
			policy.NoneOwnVol[k] = removeSlice(apis, v)
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
	newUserPolicy = &UserPolicy{OwnVols: make([]string, 0), NoneOwnVol: make(map[string][]string)}
	policy.mu.Lock()
	defer policy.mu.Unlock()
	for _, vol := range policy.OwnVols {
		if _, exist := m[vol]; !exist {
			m[vol] = true
			newUserPolicy.OwnVols = append(newUserPolicy.OwnVols, vol)
		}
	}
	for vol, apis := range policy.NoneOwnVol {
		checkMap := make(map[string]bool)
		newAPI := make([]string, 0)
		for _, api := range apis {
			if _, exist := checkMap[api]; !exist {
				checkMap[api] = true
				newAPI = append(newAPI, api)
			}
		}
		newUserPolicy.NoneOwnVol[vol] = newAPI
	}
	return
}

func IsUserType(userType UserType) bool {
	if userType == SuperAdmin || userType == Admin || userType == User {
		return true
	} else {
		return false
	}
}
