package master

import (
	"crypto/sha1"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	accessKeyLength     = 16
	secretKeyLength     = 32
	ALL                 = "all"
	RootUserID          = "root"
	DefaultRootPasswd   = "ChubaoFSRoot"
	DefaultUserPassword = "ChubaoFSUser"
)

type User struct {
	fsm          *MetadataFsm
	partition    raftstore.Partition
	akStore      sync.Map //K: ak, V: AKPolicy
	userAk       sync.Map //K: user, V: ak
	volUser      sync.Map //K: vol, V: userIDs
	akStoreMutex sync.RWMutex
	userAKMutex  sync.RWMutex
	volUserMutex sync.RWMutex
	rootExist    bool
}

func newUser(fsm *MetadataFsm, partition raftstore.Partition) (u *User) {
	u = new(User)
	u.fsm = fsm
	u.partition = partition
	return
}

func (u *User) createKey(param *proto.UserCreateParam) (akPolicy *proto.AKPolicy, err error) {
	var (
		userAK     *proto.UserAK
		userPolicy *proto.UserPolicy
		exist      bool
	)
	if param.ID == "" {
		err = proto.ErrInvalidUserID
		return
	}
	if !param.Type.Valid() {
		err = proto.ErrInvalidUserType
		return
	}

	var userID = param.ID
	var password = param.Password
	if password == "" {
		password = DefaultUserPassword
	}
	var accessKey = param.AccessKey
	if accessKey == "" {
		accessKey = util.RandomString(accessKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
	}
	var secretKey = param.SecretKey
	if secretKey == "" {
		secretKey = util.RandomString(secretKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
	}
	var userType = param.Type
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
	userPolicy = proto.NewUserPolicy()
	akPolicy = &proto.AKPolicy{AccessKey: accessKey, SecretKey: secretKey, Policy: userPolicy,
		UserID: userID, UserType: userType, CreateTime: time.Unix(time.Now().Unix(), 0).Format(proto.TimeFormat)}
	userAK = &proto.UserAK{UserID: userID, AccessKey: accessKey, Password: sha1String(password)}
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

func (u *User) deleteKey(userID string) (err error) {
	var (
		userAK   *proto.UserAK
		akPolicy *proto.AKPolicy
	)
	if value, exist := u.userAk.Load(userID); !exist {
		err = proto.ErrUserNotExists
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
	if akPolicy.UserType == proto.UserTypeRoot {
		err = proto.ErrNoPermission
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
	// delete userID from related policy in volUserStore
	u.removeUserFromAllVol(userID)
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
		err = proto.ErrUserNotExists
		return
	}
	if akPolicy, err = u.loadAKInfo(ak); err != nil {
		return
	}
	log.LogInfof("action[getUserInfo], userID: %v", userID)
	return
}

func (u *User) updatePolicy(params *proto.UserPermUpdateParam) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.getUserInfo(params.UserID); err != nil {
		return
	}
	akPolicy.Policy.AddAuthorizedVol(params.Volume, params.Policy)
	if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.addUserToVol(params.UserID, params.Volume); err != nil {
		return
	}
	log.LogInfof("action[updatePolicy], userID: %v, volume: %v", params.UserID, params.Volume)
	return
}

func (u *User) removePolicy(params *proto.UserPermRemoveParam) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.getUserInfo(params.UserID); err != nil {
		return
	}
	akPolicy.Policy.RemoveAuthorizedVol(params.Volume)
	if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.removeUserFromVol(params.UserID, params.Volume); err != nil {
		return
	}
	log.LogInfof("action[removePolicy], userID: %v, volume: %v", params.UserID, params.Volume)
	return
}

func (u *User) addOwnVol(userID, volName string) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.getUserInfo(userID); err != nil {
		return
	}
	akPolicy.Policy.AddOwnVol(volName)
	if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.addUserToVol(userID, volName); err != nil {
		return
	}
	log.LogInfof("action[addOwnVol], userID: %v, volume: %v", userID, volName)
	return
}

func (u *User) removeOwnVol(userID, volName string) (akPolicy *proto.AKPolicy, err error) {
	if akPolicy, err = u.getUserInfo(userID); err != nil {
		return
	}
	akPolicy.Policy.RemoveOwnVol(volName)
	if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.removeUserFromVol(userID, volName); err != nil {
		return
	}
	log.LogInfof("action[removeOwnVol], userID: %v, volume: %v", userID, volName)
	return
}

func (u *User) deleteVolPolicy(volName string) (err error) {
	var (
		volUser  *proto.VolUser
		akPolicy *proto.AKPolicy
	)
	//get related userIDs
	if value, exist := u.volUser.Load(volName); exist {
		volUser = value.(*proto.VolUser)
	} else {
		return nil
	}
	//delete policy
	for _, userID := range volUser.UserIDs {
		if akPolicy, err = u.getUserInfo(userID); err != nil {
			return
		}
		akPolicy.Policy.RemoveOwnVol(volName)
		akPolicy.Policy.RemoveAuthorizedVol(volName)
		if err = u.syncUpdateAKPolicy(akPolicy); err != nil {
			err = proto.ErrPersistenceByRaft
			return
		}
	}
	//delete volName index
	if err = u.syncDeleteVolUser(volUser); err != nil {
		return
	}
	u.volUser.Delete(volUser.Vol)
	log.LogInfof("action[deleteVolPolicy], volName: %v", volName)
	return
}

func (u *User) transferVol(params *proto.UserTransferVolParam) (targetAKPolicy *proto.AKPolicy, err error) {
	var akPolicy *proto.AKPolicy
	if akPolicy, err = u.getUserInfo(params.UserSrc); err != nil {
		return
	}
	if !contains(akPolicy.Policy.OwnVols, params.Volume) {
		err = proto.ErrHaveNoPolicy
		return
	}
	if _, err = u.removeOwnVol(params.UserSrc, params.Volume); err != nil {
		return
	}
	if targetAKPolicy, err = u.addOwnVol(params.UserDst, params.Volume); err != nil {
		return
	}
	log.LogInfof("action[transferVol], volName: %v, userSrc: %v, userDst: %v", params.Volume, params.UserSrc, params.UserDst)
	return
}

func (u *User) getAllUserInfo(keywords string) (akPolicies []*proto.AKPolicy) {
	akPolicies = make([]*proto.AKPolicy, 0)
	u.akStore.Range(func(key, value interface{}) bool { //todo mutex
		akPolicy := value.(*proto.AKPolicy)
		if strings.Contains(akPolicy.UserID, keywords) {
			akPolicies = append(akPolicies, akPolicy)
		}
		return true
	})
	log.LogInfof("action[getAllUserInfo], keywords: %v, total numbers: %v", keywords, len(akPolicies))
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

func (u *User) addUserToVol(userID, volName string) (err error) {
	u.volUserMutex.Lock()
	defer u.volUserMutex.Unlock()
	var (
		volUser *proto.VolUser
	)
	if value, ok := u.volUser.Load(volName); ok {
		volUser = value.(*proto.VolUser)
		volUser.Lock()
		defer volUser.Unlock()
		volUser.UserIDs = append(volUser.UserIDs, userID)
	} else {
		volUser = &proto.VolUser{Vol: volName, UserIDs: []string{userID}}
		u.volUser.Store(volName, volUser)
	}
	if err = u.syncAddVolUser(volUser); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}
func (u *User) removeUserFromVol(userID, volName string) (err error) {
	var (
		volUser *proto.VolUser
	)
	if value, ok := u.volUser.Load(volName); ok {
		volUser = value.(*proto.VolUser)
		volUser.Lock()
		defer volUser.Unlock()
		volUser.UserIDs = removeString(volUser.UserIDs, userID)
	} else {
		err = proto.ErrHaveNoPolicy
	}
	if err = u.syncUpdateVolUser(volUser); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	return
}

func (u *User) removeUserFromAllVol(userID string) {
	u.volUser.Range(func(key, value interface{}) bool {
		volUser := value.(*proto.VolUser)
		volUser.Lock()
		volUser.UserIDs = removeString(volUser.UserIDs, userID)
		volUser.Unlock()
		return true
	})
}

func removeString(array []string, element string) []string {
	for k, v := range array {
		if v == element {
			return append(array[:k], array[k+1:]...)
		}
	}
	log.LogErrorf("Delete user policy failed: remove userID[%v] form vol", element)
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

func (u *User) clearVolUsers() {
	u.volUser.Range(func(key, value interface{}) bool {
		u.volUser.Delete(key)
		return true
	})
}
