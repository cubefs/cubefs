package master

import (
	"crypto/sha1"
	"io"
	"sync"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	accessKeyLength = 16
	secretKeyLength = 32
	separator       = "_"
	ALL             = "all"
	DefaultPassword = "MxMxMxMxMxMx"
)

type User struct {
	fsm             *MetadataFsm
	partition       raftstore.Partition
	akStore         sync.Map //K: ak, V: AKPolicy
	userAk          sync.Map //K: user, V: ak
	volAKs          sync.Map //K: vol, V: aks
	akStoreMutex    sync.RWMutex
	userAKMutex     sync.RWMutex
	volAKsMutex     sync.RWMutex
	SuperAdminExist bool
}

func newUser(fsm *MetadataFsm, partition raftstore.Partition) (u *User) {
	u = new(User)
	u.fsm = fsm
	u.partition = partition
	return
}

func (u *User) createKey(userID, password string, userType proto.UserType) (akPolicy *proto.AKPolicy, err error) {
	var (
		userAK     *proto.UserAK
		userPolicy *proto.UserPolicy
		exist      bool
	)
	if !proto.IsUserType(userType) {
		err = proto.ErrUserType
		return
	}
	if userType == proto.SuperAdmin && u.SuperAdminExist {
		err = proto.ErrSuperAdminExists
		return
	}
	accessKey := util.RandomString(accessKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
	secretKey := util.RandomString(secretKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
	u.akStoreMutex.Lock()
	defer u.akStoreMutex.Unlock()
	u.userAKMutex.Lock()
	defer u.userAKMutex.Unlock()
	//check duplicate
	if _, exist = u.userAk.Load(userID); exist {
		err = proto.ErrDuplicateUserID
		return
	}
	_, exist = u.akStore.Load(accessKey)
	for exist {
		accessKey = util.RandomString(accessKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
		_, exist = u.akStore.Load(accessKey)
	}
	userPolicy = &proto.UserPolicy{OwnVols: make([]string, 0), NoneOwnVol: make(map[string][]string)}
	akPolicy = &proto.AKPolicy{AccessKey: accessKey, SecretKey: secretKey, Policy: userPolicy,
		UserID: userID, Password: sha1String(password), UserType: userType}
	userAK = &proto.UserAK{UserID: userID, AccessKey: accessKey}
	if err = u.syncAddAKPolicy(akPolicy); err != nil {
		return
	}
	if err = u.syncAddUserAK(userAK); err != nil {
		return
	}
	u.akStore.Store(accessKey, akPolicy)
	u.userAk.Store(userID, userAK)
	log.LogInfof("action[createUser], userID: %v, accesskey[%v], secretkey[%v]", userID, accessKey, secretKey)
	return
}

func (u *User) createUserWithKey(userID, password, accessKey, secretKey string, userType proto.UserType) (akPolicy *proto.AKPolicy, err error) {
	var (
		userAK     *proto.UserAK
		userPolicy *proto.UserPolicy
		exist      bool
	)
	if !proto.IsUserType(userType) {
		err = proto.ErrUserType
		return
	}
	if userType == proto.SuperAdmin && u.SuperAdminExist {
		err = proto.ErrSuperAdminExists
		return
	}
	u.akStoreMutex.Lock()
	defer u.akStoreMutex.Unlock()
	u.userAKMutex.Lock()
	defer u.userAKMutex.Unlock()
	//check duplicate
	if _, exist = u.userAk.Load(userID); exist {
		err = proto.ErrDuplicateUserID
		return
	}
	if _, exist = u.akStore.Load(accessKey); exist {
		err = proto.ErrDuplicateAccessKey
		return
	}
	userPolicy = &proto.UserPolicy{OwnVols: make([]string, 0), NoneOwnVol: make(map[string][]string)}
	akPolicy = &proto.AKPolicy{AccessKey: accessKey, SecretKey: secretKey, Policy: userPolicy,
		UserID: userID, Password: sha1String(password), UserType: userType}
	userAK = &proto.UserAK{UserID: userID, AccessKey: accessKey}
	if err = u.syncAddAKPolicy(akPolicy); err != nil {
		return
	}
	if err = u.syncAddUserAK(userAK); err != nil {
		return
	}
	u.akStore.Store(accessKey, akPolicy)
	u.userAk.Store(userID, userAK)
	log.LogInfof("action[createUserWithKey], userID: %v, accesskey[%v], secretkey[%v]", userID, accessKey, secretKey)
	return
}

func (u *User) deleteKey(userID string) (err error) {
	var (
		userAK   *proto.UserAK
		akPolicy *proto.AKPolicy
	)
	if value, exist := u.userAk.Load(userID); !exist {
		err = proto.ErrOSSUserNotExists
		return
	} else {
		userAK = value.(*proto.UserAK)
	}
	if akPolicy, err = u.loadAKInfo(userAK.AccessKey); err != nil {
		return
	}
	if len(akPolicy.Policy.OwnVols) > 0 {
		err = proto.ErrOwnVolExists
		return
	}
	if err = u.syncDeleteAKPolicy(akPolicy); err != nil {
		return
	}
	if err = u.syncDeleteUserAK(userAK); err != nil {
		return
	}
	u.akStore.Delete(userAK.AccessKey)
	u.userAk.Delete(userID)
	log.LogInfof("action[deleteUser], userID: %v, accesskey[%v]", userID, userAK.AccessKey)
	return
}

func (u *User) getKeyInfo(ak string) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.loadAKInfo(ak); err != nil {
		return
	}
	log.LogInfof("action[getKeyInfo], accesskey[%v]", ak)
	return
}

func (u *User) getUserInfo(userID string) (akPolicy *proto.AKPolicy, err error) {
	var (
		ak string
	)
	if value, exist := u.userAk.Load(userID); exist {
		ak = value.(*proto.UserAK).AccessKey
	} else {
		err = proto.ErrOSSUserNotExists
		return
	}
	if akPolicy, err = u.loadAKInfo(ak); err != nil {
		return
	}
	log.LogInfof("action[getUserInfo], userID: %v", userID)
	return
}

func (u *User) addPolicy(ak string, userPolicy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.loadAKInfo(ak); err != nil {
		return
	}
	akPolicy.Policy.Add(userPolicy)
	akPolicy.Policy = proto.CleanPolicy(akPolicy.Policy)
	if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.addVolAKs(ak, userPolicy); err != nil {
		return
	}
	log.LogInfof("action[addPolicy], accessKey: %v", ak)
	return
}

func (u *User) deletePolicy(ak string, userPolicy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.loadAKInfo(ak); err != nil {
		return
	}
	akPolicy.Policy.Delete(userPolicy)
	if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.deleteVolAKs(ak, userPolicy); err != nil {
		return
	}
	log.LogInfof("action[deletePolicy], accessKey: %v", ak)
	return
}

func (u *User) deleteVolPolicy(volName string) (err error) {
	var (
		volAK    *proto.VolAK
		akPolicy *proto.AKPolicy
	)
	//get related ak
	if value, exist := u.volAKs.Load(volName); exist {
		volAK = value.(*proto.VolAK)
	} else {
		err = proto.ErrVolPolicyNotExists
		return
	}
	//delete policy
	for _, akAndAction := range volAK.AKAndActions {
		ak := akAndAction[:accessKeyLength]
		action := akAndAction[accessKeyLength+1:]
		if akPolicy, err = u.loadAKInfo(ak); err != nil {
			return
		}
		var userPolicy *proto.UserPolicy
		if action == ALL {
			userPolicy = &proto.UserPolicy{OwnVols: []string{volName}}
		} else {
			userPolicy = &proto.UserPolicy{NoneOwnVol: map[string][]string{volName: {action}}}
		}
		akPolicy.Policy.Delete(userPolicy)
		if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
			err = proto.ErrPersistenceByRaft
			return
		}
	}
	//delete volName index
	if err = u.syncDeleteVolAK(volAK); err != nil {
		return
	}
	u.volAKs.Delete(volAK.Vol)
	log.LogInfof("action[deleteVolPolicy], volName: %v", volName)
	return
}

func (u *User) transferVol(volName, ak, targetKey string) (targetAKPolicy *proto.AKPolicy, err error) {
	var akPolicy *proto.AKPolicy
	userPolicy := &proto.UserPolicy{OwnVols: []string{volName}}
	if akPolicy, err = u.loadAKInfo(ak); err != nil {
		return
	}
	if !contains(akPolicy.Policy.OwnVols, volName) {
		err = proto.ErrHaveNoPolicy
		return
	}
	if _, err = u.deletePolicy(ak, userPolicy); err != nil {
		return
	}
	if targetAKPolicy, err = u.addPolicy(targetKey, userPolicy); err != nil {
		return
	}
	log.LogInfof("action[transferVol], volName: %v, ak: %v, targetKey: %v", volName, ak, targetKey)
	return
}

func (u *User) loadAKInfo(ak string) (akPolicy *proto.AKPolicy, err error) {
	if value, exist := u.akStore.Load(ak); exist {
		akPolicy = value.(*proto.AKPolicy)
	} else {
		err = proto.ErrAccessKeyNotExists
	}
	return
}

func (u *User) addVolAKs(ak string, policy *proto.UserPolicy) (err error) {
	u.volAKsMutex.Lock()
	defer u.volAKsMutex.Unlock()
	for _, vol := range policy.OwnVols {
		if err = u.addAKToVol(ak+separator+ALL, vol); err != nil {
			return
		}
	}
	for vol, apis := range policy.NoneOwnVol {
		for _, api := range apis {
			if err = u.addAKToVol(ak+separator+api, vol); err != nil {
				return
			}
		}
	}
	return
}

func (u *User) addAKToVol(akAndAction string, volName string) (err error) {
	var volAK *proto.VolAK
	if value, ok := u.volAKs.Load(volName); ok {
		volAK = value.(*proto.VolAK)
		volAK.Lock()
		defer volAK.Unlock()
		volAK.AKAndActions = append(volAK.AKAndActions, akAndAction)
	} else {
		aks := make([]string, 0)
		aks = append(aks, akAndAction)
		volAK = &proto.VolAK{Vol: volName, AKAndActions: aks}
		u.volAKs.Store(volName, volAK)
	}
	if err = u.syncAddVolAK(volAK); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (u *User) deleteVolAKs(ak string, policy *proto.UserPolicy) (err error) {
	for _, vol := range policy.OwnVols {
		if err = u.deleteAKFromVol(ak+separator+ALL, vol); err != nil {
			return
		}
	}
	for vol, apis := range policy.NoneOwnVol {
		for _, api := range apis {
			if err = u.deleteAKFromVol(ak+separator+api, vol); err != nil {
				return
			}
		}
	}
	return
}

func (u *User) deleteAKFromVol(akAndAction string, volName string) (err error) {
	var volAK *proto.VolAK
	if value, ok := u.volAKs.Load(volName); ok {
		volAK = value.(*proto.VolAK)
		volAK.Lock()
		defer volAK.Unlock()
		volAK.AKAndActions = removeAK(volAK.AKAndActions, akAndAction)
	} else {
		err = proto.ErrHaveNoPolicy
	}
	if err = u.syncUpdateVolAK(volAK); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func removeAK(array []string, element string) []string {
	for k, v := range array {
		if v == element {
			return append(array[:k], array[k+1:]...)
		}
	}
	log.LogErrorf("Delete user policy failed: remove accesskey [%v] form vol", element)
	return array
}

func sha1String(s string) string {
	t := sha1.New()
	io.WriteString(t, s)
	return string(t.Sum(nil))
}

func (u *User) clearAKStore() {
	u.akStore.Range(func(key, value interface{}) bool {
		u.akStore.Delete(key)
		return true
	})
}

func (u *User) clearUserAK() {
	u.userAk.Range(func(key, value interface{}) bool {
		u.userAk.Delete(key)
		return true
	})
}

func (u *User) clearVolAKs() {
	u.volAKs.Range(func(key, value interface{}) bool {
		u.volAKs.Delete(key)
		return true
	})
}
