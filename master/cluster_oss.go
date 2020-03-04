package master

import (
	"fmt"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	accessKeyLength = 16
	secretKeyLength = 32
	separator       = "_"
	ALL             = "all"
)

func (c *Cluster) createKey(owner string) (akPolicy *proto.AKPolicy, err error) {
	var (
		userAK     *proto.UserAK
		userPolicy *proto.UserPolicy
		exit       bool
	)
	accessKey := util.RandomString(accessKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
	secretKey := util.RandomString(secretKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
	c.akStoreMutex.Lock()
	defer c.akStoreMutex.Unlock()
	c.userAKMutex.Lock()
	defer c.userAKMutex.Unlock()
	//check duplicate
	if _, exit = c.userAk.Load(owner); exit {
		err = proto.ErrDuplicateUserID
		goto errHandler
	}
	_, exit = c.akStore.Load(accessKey)
	for exit {
		accessKey = util.RandomString(accessKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
		_, exit = c.akStore.Load(accessKey)
	}
	userPolicy = &proto.UserPolicy{OwnVol: make([]string, 0), NoneOwnVol: make(map[string][]string)}
	akPolicy = &proto.AKPolicy{AccessKey: accessKey, SecretKey: secretKey, Policy: userPolicy, UserID: owner}
	userAK = &proto.UserAK{UserID: owner, AccessKey: accessKey}
	if err = c.syncAddAKPolicy(akPolicy); err != nil {
		goto errHandler
	}
	if err = c.syncAddUserAK(userAK); err != nil {
		goto errHandler
	}
	c.akStore.Store(accessKey, akPolicy)
	c.userAk.Store(owner, userAK)
	log.LogInfof("action[createUser], clusterID[%v] user: %v, accesskey[%v], secretkey[%v]",
		c.Name, owner, accessKey, secretKey)
	return
errHandler:
	err = fmt.Errorf("action[createUser], clusterID[%v] user: %v err: %v ", c.Name, owner, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) createUserWithKey(owner, accessKey, secretKey string) (akPolicy *proto.AKPolicy, err error) {
	var (
		userAK     *proto.UserAK
		userPolicy *proto.UserPolicy
		exit       bool
	)
	c.akStoreMutex.Lock()
	defer c.akStoreMutex.Unlock()
	c.userAKMutex.Lock()
	defer c.userAKMutex.Unlock()
	//check duplicate
	if _, exit = c.userAk.Load(owner); exit {
		err = proto.ErrDuplicateUserID
		goto errHandler
	}
	if _, exit = c.akStore.Load(accessKey); exit {
		err = proto.ErrDuplicateAccessKey
		goto errHandler
	}
	userPolicy = &proto.UserPolicy{OwnVol: make([]string, 0), NoneOwnVol: make(map[string][]string)}
	akPolicy = &proto.AKPolicy{AccessKey: accessKey, SecretKey: secretKey, Policy: userPolicy, UserID: owner}
	userAK = &proto.UserAK{UserID: owner, AccessKey: accessKey}
	if err = c.syncAddAKPolicy(akPolicy); err != nil {
		goto errHandler
	}
	if err = c.syncAddUserAK(userAK); err != nil {
		goto errHandler
	}
	c.akStore.Store(accessKey, akPolicy)
	c.userAk.Store(owner, userAK)
	log.LogInfof("action[createUserWithKey], clusterID[%v] user: %v, accesskey[%v], secretkey[%v]",
		c.Name, owner, accessKey, secretKey)
	return
errHandler:
	err = fmt.Errorf("action[createUserWithKey], clusterID[%v] user: %v, ak: %v, sk: %v, err: %v ", c.Name, owner, accessKey, secretKey, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) deleteKey(owner string) (err error) {
	var (
		userAK   *proto.UserAK
		akPolicy *proto.AKPolicy
	)
	if value, exit := c.userAk.Load(owner); !exit {
		err = proto.ErrOSSUserNotExists
		goto errHandler
	} else {
		userAK = value.(*proto.UserAK)
	}
	akPolicy = &proto.AKPolicy{AccessKey: userAK.AccessKey, UserID: owner}
	userAK = &proto.UserAK{UserID: owner, AccessKey: userAK.AccessKey}
	if err = c.syncDeleteAKPolicy(akPolicy); err != nil {
		goto errHandler
	}
	if err = c.syncDeleteUserAK(userAK); err != nil {
		goto errHandler
	}
	c.akStore.Delete(userAK.AccessKey)
	c.userAk.Delete(owner)
	log.LogInfof("action[deleteUser], clusterID[%v] user: %v, accesskey[%v]", c.Name, owner, userAK.AccessKey)
	return
errHandler:
	err = fmt.Errorf("action[deleteUser], clusterID[%v] user: %v err: %v ", c.Name, owner, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) getKeyInfo(ak string) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = c.getAKInfo(ak); err != nil {
		goto errHandler
	}
	log.LogInfof("action[getOSSAKInfo], clusterID[%v] accesskey[%v]", c.Name, ak)
	return
errHandler:
	err = fmt.Errorf("action[getOSSAKInfo], clusterID[%v] ak: %v err: %v ", c.Name, ak, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) getUserInfo(owner string) (akPolicy *proto.AKPolicy, err error) {
	var (
		ak string
	)
	if value, exit := c.userAk.Load(owner); exit {
		ak = value.(*proto.UserAK).AccessKey
	} else {
		err = proto.ErrOSSUserNotExists
		goto errHandler
	}
	if akPolicy, err = c.getAKInfo(ak); err != nil {
		goto errHandler
	}
	log.LogInfof("action[getOSSUserInfo], clusterID[%v] user: %v", c.Name, owner)
	return
errHandler:
	err = fmt.Errorf("action[getOSSUserInfo], clusterID[%v] user: %v err: %v ", c.Name, owner, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) addPolicy(ak string, userPolicy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = c.getAKInfo(ak); err != nil {
		goto errHandler
	}
	akPolicy.Policy.Add(userPolicy)
	if err = c.syncUpdateAKPolicy(akPolicy); err != nil {
		err = proto.ErrPersistenceByRaft
		goto errHandler
	}
	if err = c.addVolAKs(ak, userPolicy); err != nil {
		goto errHandler
	}
	log.LogInfof("action[addOSSPolicy], clusterID[%v] accessKey: %v", c.Name, ak)
	return
errHandler:
	err = fmt.Errorf("action[addOSSPolicy], clusterID[%v] accessKey: %v err: %v", c.Name, ak, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) deletePolicy(ak string, userPolicy *proto.UserPolicy) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = c.getAKInfo(ak); err != nil {
		goto errHandler
	}
	akPolicy.Policy.Delete(userPolicy)
	if err = c.syncUpdateAKPolicy(akPolicy); err != nil {
		err = proto.ErrPersistenceByRaft
		goto errHandler
	}
	if err = c.deleteVolAKs(ak, userPolicy); err != nil {
		goto errHandler
	}
	log.LogInfof("action[deleteOSSPolicy], clusterID[%v] accessKey: %v", c.Name, ak)
	return
errHandler:
	err = fmt.Errorf("action[deleteOSSPolicy], clusterID[%v] accessKey: %v err: %v", c.Name, ak, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) deleteVolPolicy(vol string) (err error) {
	var (
		volAK    *proto.VolAK
		akPolicy *proto.AKPolicy
	)
	//get related ak
	if value, exit := c.volAKs.Load(vol); exit {
		volAK = value.(*proto.VolAK)
	} else {
		err = proto.ErrVolPolicyNotExists
		goto errHandler
	}
	//delete policy
	for _, akAndAction := range volAK.AKAndActions {
		ak := akAndAction[:accessKeyLength]
		action := akAndAction[accessKeyLength+1:]
		if akPolicy, err = c.getAKInfo(ak); err != nil {
			goto errHandler
		}
		var userPolicy *proto.UserPolicy
		if action == ALL {
			userPolicy = &proto.UserPolicy{OwnVol: []string{vol}}
		} else {
			userPolicy = &proto.UserPolicy{NoneOwnVol: map[string][]string{vol: {action}}}
		}
		akPolicy.Policy.Delete(userPolicy)
		if err = c.syncUpdateAKPolicy(akPolicy); err != nil {
			err = proto.ErrPersistenceByRaft
			goto errHandler
		}
	}
	//delete vol index
	if err = c.syncDeleteVolAK(volAK); err != nil {
		goto errHandler
	}
	c.volAKs.Delete(volAK.Vol)
	log.LogInfof("action[deleteOSSVolPolicy], clusterID[%v] volName: %v", c.Name, vol)
	return
errHandler:
	err = fmt.Errorf("action[deleteOSSVolPolicy], clusterID[%v] volName: %v err: %v", c.Name, vol, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) transferVol(vol, ak, targetKey string) (targetAKPolicy *proto.AKPolicy, err error) {
	var akPolicy *proto.AKPolicy
	userPolicy := &proto.UserPolicy{OwnVol: []string{vol}}
	if akPolicy, err = c.getAKInfo(ak); err != nil {
		goto errHandler
	}
	if !contains(akPolicy.Policy.OwnVol, vol) {
		err = proto.ErrHaveNoPolicy
		goto errHandler
	}
	if _, err = c.deletePolicy(ak, userPolicy); err != nil {
		goto errHandler
	}
	if targetAKPolicy, err = c.addPolicy(targetKey, userPolicy); err != nil {
		goto errHandler
	}
	log.LogInfof("action[transferOSSVol], clusterID[%v] volName: %v, ak: %v, targetKey: %v", c.Name, vol, ak, targetKey)
	return
errHandler:
	err = fmt.Errorf("action[transferOSSVol], clusterID[%v] volName: %v, ak: %v, targetKey: %v, err: %v", c.Name, vol, ak, targetKey, err.Error())
	log.LogError(errors.Stack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) getAKInfo(ak string) (akPolicy *proto.AKPolicy, err error) {
	if value, exit := c.akStore.Load(ak); exit {
		akPolicy = value.(*proto.AKPolicy)
	} else {
		err = proto.ErrAccessKeyNotExists
	}
	return
}

func (c *Cluster) addVolAKs(ak string, policy *proto.UserPolicy) (err error) {
	c.volAKsMutex.Lock()
	defer c.volAKsMutex.Unlock()
	for _, vol := range policy.OwnVol {
		if err = c.addAKToVol(ak+separator+ALL, vol); err != nil {
			return
		}
	}
	for vol, apis := range policy.NoneOwnVol {
		for _, api := range apis {
			if err = c.addAKToVol(ak+separator+api, vol); err != nil {
				return
			}
		}
	}
	return
}

func (c *Cluster) addAKToVol(akAndAction string, vol string) (err error) {
	var volAK *proto.VolAK
	if value, ok := c.volAKs.Load(vol); ok {
		volAK = value.(*proto.VolAK)
		volAK.Lock()
		defer volAK.Unlock()
		volAK.AKAndActions = append(volAK.AKAndActions, akAndAction)
	} else {
		aks := make([]string, 0)
		aks = append(aks, akAndAction)
		volAK = &proto.VolAK{Vol: vol, AKAndActions: aks}
		c.volAKs.Store(vol, volAK)
	}
	if err = c.syncAddVolAK(volAK); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (c *Cluster) deleteVolAKs(ak string, policy *proto.UserPolicy) (err error) {
	for _, vol := range policy.OwnVol {
		if err = c.deleteAKFromVol(ak+separator+ALL, vol); err != nil {
			return
		}
	}
	for vol, apis := range policy.NoneOwnVol {
		for _, api := range apis {
			if err = c.deleteAKFromVol(ak+separator+api, vol); err != nil {
				return
			}
		}
	}
	return
}

func (c *Cluster) deleteAKFromVol(akAndAction string, vol string) (err error) {
	var volAK *proto.VolAK
	if value, ok := c.volAKs.Load(vol); ok {
		volAK = value.(*proto.VolAK)
		volAK.Lock()
		defer volAK.Unlock()
		volAK.AKAndActions = removeAK(volAK.AKAndActions, akAndAction)
	} else {
		err = proto.ErrHaveNoPolicy
	}
	if err = c.syncUpdateVolAK(volAK); err != nil {
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

func (c *Cluster) clearAKStore() {
	c.akStore.Range(func(key, value interface{}) bool {
		c.akStore.Delete(key)
		return true
	})
}

func (c *Cluster) clearUserAK() {
	c.userAk.Range(func(key, value interface{}) bool {
		c.userAk.Delete(key)
		return true
	})
}

func (c *Cluster) clearVolAKs() {
	c.volAKs.Range(func(key, value interface{}) bool {
		c.volAKs.Delete(key)
		return true
	})
}
