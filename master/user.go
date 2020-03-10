package master

import (
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
)

type User struct {
	fsm          *MetadataFsm
	partition    raftstore.Partition
	akStore      sync.Map //K: ak, V: AKPolicy
	userAk       sync.Map //K: user, V: ak
	volAKs       sync.Map //K: vol, V: aks
	akStoreMutex sync.RWMutex
	userAKMutex  sync.RWMutex
	volAKsMutex  sync.RWMutex
}

func newUser(fsm *MetadataFsm, partition raftstore.Partition) (u *User) {
	u = new(User)
	u.fsm = fsm
	u.partition = partition
	return
}

func (u *User) createKey(owner string) (akPolicy *proto.AKPolicy, err error) {
	var (
		userAK     *proto.UserAK
		userPolicy *proto.UserPolicy
		exit       bool
	)
	accessKey := util.RandomString(accessKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
	secretKey := util.RandomString(secretKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
	u.akStoreMutex.Lock()
	defer u.akStoreMutex.Unlock()
	u.userAKMutex.Lock()
	defer u.userAKMutex.Unlock()
	//check duplicate
	if _, exit = u.userAk.Load(owner); exit {
		err = proto.ErrDuplicateUserID
		return
	}
	_, exit = u.akStore.Load(accessKey)
	for exit {
		accessKey = util.RandomString(accessKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
		_, exit = u.akStore.Load(accessKey)
	}
	userPolicy = &proto.UserPolicy{OwnVols: make([]string, 0), NoneOwnVol: make(map[string][]string)}
	akPolicy = &proto.AKPolicy{AccessKey: accessKey, SecretKey: secretKey, Policy: userPolicy, UserID: owner}
	userAK = &proto.UserAK{UserID: owner, AccessKey: accessKey}
	if err = u.syncAddAKPolicy(akPolicy); err != nil {
		return
	}
	if err = u.syncAddUserAK(userAK); err != nil {
		return
	}
	u.akStore.Store(accessKey, akPolicy)
	u.userAk.Store(owner, userAK)
	log.LogInfof("action[createUser], user: %v, accesskey[%v], secretkey[%v]", owner, accessKey, secretKey)
	return
}

func (u *User) createUserWithKey(owner, accessKey, secretKey string) (akPolicy *proto.AKPolicy, err error) {
	var (
		userAK     *proto.UserAK
		userPolicy *proto.UserPolicy
		exit       bool
	)
	u.akStoreMutex.Lock()
	defer u.akStoreMutex.Unlock()
	u.userAKMutex.Lock()
	defer u.userAKMutex.Unlock()
	//check duplicate
	if _, exit = u.userAk.Load(owner); exit {
		err = proto.ErrDuplicateUserID
		return
	}
	if _, exit = u.akStore.Load(accessKey); exit {
		err = proto.ErrDuplicateAccessKey
		return
	}
	userPolicy = &proto.UserPolicy{OwnVols: make([]string, 0), NoneOwnVol: make(map[string][]string)}
	akPolicy = &proto.AKPolicy{AccessKey: accessKey, SecretKey: secretKey, Policy: userPolicy, UserID: owner}
	userAK = &proto.UserAK{UserID: owner, AccessKey: accessKey}
	if err = u.syncAddAKPolicy(akPolicy); err != nil {
		return
	}
	if err = u.syncAddUserAK(userAK); err != nil {
		return
	}
	u.akStore.Store(accessKey, akPolicy)
	u.userAk.Store(owner, userAK)
	log.LogInfof("action[createUserWithKey], user: %v, accesskey[%v], secretkey[%v]", owner, accessKey, secretKey)
	return
}

func (u *User) deleteKey(owner string) (err error) {
	var (
		userAK   *proto.UserAK
		akPolicy *proto.AKPolicy
	)
	if value, exit := u.userAk.Load(owner); !exit {
		err = proto.ErrOSSUserNotExists
		return
	} else {
		userAK = value.(*proto.UserAK)
	}
	if akPolicy, err = u.getAKInfo(userAK.AccessKey); err != nil {
		return
	}
	if len(akPolicy.Policy.OwnVols) > 0 {
		err = proto.ErrOwnVolExits
		return
	}
	if err = u.syncDeleteAKPolicy(akPolicy); err != nil {
		return
	}
	if err = u.syncDeleteUserAK(userAK); err != nil {
		return
	}
	u.akStore.Delete(userAK.AccessKey)
	u.userAk.Delete(owner)
	log.LogInfof("action[deleteUser], user: %v, accesskey[%v]", owner, userAK.AccessKey)
	return
}

func (u *User) getKeyInfo(ak string) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.getAKInfo(ak); err != nil {
		return
	}
	log.LogInfof("action[getKeyInfo], accesskey[%v]", ak)
	return
}

func (u *User) getUserInfo(owner string) (akPolicy *proto.AKPolicy, err error) {
	var (
		ak string
	)
	if value, exit := u.userAk.Load(owner); exit {
		ak = value.(*proto.UserAK).AccessKey
	} else {
		err = proto.ErrOSSUserNotExists
		return
	}
	if akPolicy, err = u.getAKInfo(ak); err != nil {
		return
	}
	log.LogInfof("action[getUserInfo], user: %v", owner)
	return
}

func (u *User) addPolicy(ak string, userPolicy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.getAKInfo(ak); err != nil {
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
	if akPolicy, err = u.getAKInfo(ak); err != nil {
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

func (u *User) deleteVolPolicy(vol string) (err error) {
	var (
		volAK    *proto.VolAK
		akPolicy *proto.AKPolicy
	)
	//get related ak
	if value, exit := u.volAKs.Load(vol); exit {
		volAK = value.(*proto.VolAK)
	} else {
		err = proto.ErrVolPolicyNotExists
		return
	}
	//delete policy
	for _, akAndAction := range volAK.AKAndActions {
		ak := akAndAction[:accessKeyLength]
		action := akAndAction[accessKeyLength+1:]
		if akPolicy, err = u.getAKInfo(ak); err != nil {
			return
		}
		var userPolicy *proto.UserPolicy
		if action == ALL {
			userPolicy = &proto.UserPolicy{OwnVols: []string{vol}}
		} else {
			userPolicy = &proto.UserPolicy{NoneOwnVol: map[string][]string{vol: {action}}}
		}
		akPolicy.Policy.Delete(userPolicy)
		if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
			err = proto.ErrPersistenceByRaft
			return
		}
	}
	//delete vol index
	if err = u.syncDeleteVolAK(volAK); err != nil {
		return
	}
	u.volAKs.Delete(volAK.Vol)
	log.LogInfof("action[deleteVolPolicy], volName: %v", vol)
	return
}

func (u *User) transferVol(vol, ak, targetKey string) (targetAKPolicy *proto.AKPolicy, err error) {
	var akPolicy *proto.AKPolicy
	userPolicy := &proto.UserPolicy{OwnVols: []string{vol}}
	if akPolicy, err = u.getAKInfo(ak); err != nil {
		return
	}
	if !contains(akPolicy.Policy.OwnVols, vol) {
		err = proto.ErrHaveNoPolicy
		return
	}
	if _, err = u.deletePolicy(ak, userPolicy); err != nil {
		return
	}
	if targetAKPolicy, err = u.addPolicy(targetKey, userPolicy); err != nil {
		return
	}
	log.LogInfof("action[transferVol], volName: %v, ak: %v, targetKey: %v", vol, ak, targetKey)
	return
}

func (u *User) getAKInfo(ak string) (akPolicy *proto.AKPolicy, err error) {
	if value, exit := u.akStore.Load(ak); exit {
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

func (u *User) addAKToVol(akAndAction string, vol string) (err error) {
	var volAK *proto.VolAK
	if value, ok := u.volAKs.Load(vol); ok {
		volAK = value.(*proto.VolAK)
		volAK.Lock()
		defer volAK.Unlock()
		volAK.AKAndActions = append(volAK.AKAndActions, akAndAction)
	} else {
		aks := make([]string, 0)
		aks = append(aks, akAndAction)
		volAK = &proto.VolAK{Vol: vol, AKAndActions: aks}
		u.volAKs.Store(vol, volAK)
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

func (u *User) deleteAKFromVol(akAndAction string, vol string) (err error) {
	var volAK *proto.VolAK
	if value, ok := u.volAKs.Load(vol); ok {
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
