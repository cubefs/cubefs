package master

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

const (
	accessKeyLength     = 16
	secretKeyLength     = 32
	RootUserID          = "root"
	DefaultRootPasswd   = "ChubaoFSRoot"
	DefaultUserPassword = "ChubaoFSUser"
)

type User struct {
	fsm            *MetadataFsm
	partition      raftstore.Partition
	userStore      sync.Map //K: userID, V: UserInfo
	AKStore        sync.Map //K: ak, V: userID
	volUser        sync.Map //K: vol, V: userIDs
	userStoreMutex sync.RWMutex
	AKStoreMutex   sync.RWMutex
	volUserMutex   sync.RWMutex
}

func newUser(fsm *MetadataFsm, partition raftstore.Partition) (u *User) {
	u = new(User)
	u.fsm = fsm
	u.partition = partition
	return
}

func (u *User) createKey(param *proto.UserCreateParam) (userInfo *proto.UserInfo, err error) {
	var (
		AKUser     *proto.AKUser
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
	var description = param.Description
	u.userStoreMutex.Lock()
	defer u.userStoreMutex.Unlock()
	u.AKStoreMutex.Lock()
	defer u.AKStoreMutex.Unlock()
	//check duplicate
	if _, exist = u.userStore.Load(userID); exist {
		err = proto.ErrDuplicateUserID
		return
	}
	_, exist = u.AKStore.Load(accessKey)
	for exist {
		accessKey = util.RandomString(accessKeyLength, util.Numeric|util.LowerLetter|util.UpperLetter)
		_, exist = u.AKStore.Load(accessKey)
	}
	userPolicy = proto.NewUserPolicy()
	userInfo = &proto.UserInfo{UserID: userID, AccessKey: accessKey, SecretKey: secretKey, Policy: userPolicy,
		UserType: userType, CreateTime: time.Unix(time.Now().Unix(), 0).Format(proto.TimeFormat), Description: description}
	AKUser = &proto.AKUser{AccessKey: accessKey, UserID: userID, Password: encodingPassword(password)}
	if err = u.syncAddUserInfo(userInfo); err != nil {
		return
	}
	if err = u.syncAddAKUser(AKUser); err != nil {
		return
	}
	u.userStore.Store(userID, userInfo)
	u.AKStore.Store(accessKey, AKUser)
	log.LogInfof("action[createUser], userID: %v, accesskey[%v], secretkey[%v]", userID, accessKey, secretKey)
	return
}

func (u *User) deleteKey(userID string) (err error) {
	var (
		akUser   *proto.AKUser
		userInfo *proto.UserInfo
	)

	u.userStoreMutex.Lock()
	defer u.userStoreMutex.Unlock()
	u.AKStoreMutex.Lock()
	defer u.AKStoreMutex.Unlock()

	if value, exist := u.userStore.Load(userID); !exist {
		err = proto.ErrUserNotExists
		return
	} else {
		userInfo = value.(*proto.UserInfo)
	}
	userInfo.Mu.Lock()
	defer userInfo.Mu.Unlock()
	if len(userInfo.Policy.OwnVols) > 0 {
		err = proto.ErrOwnVolExists
		return
	}
	if userInfo.UserType == proto.UserTypeRoot {
		err = proto.ErrNoPermission
		return
	}
	if akUser, err = u.getAKUser(userInfo.AccessKey); err != nil {
		return
	}
	if err = u.syncDeleteUserInfo(userInfo); err != nil {
		return
	}
	if err = u.syncDeleteAKUser(akUser); err != nil {
		return
	}
	u.userStore.Delete(userID)
	u.AKStore.Delete(akUser.AccessKey)
	// delete userID from related policy in volUserStore
	u.removeUserFromAllVol(userID)
	log.LogInfof("action[deleteUser], userID: %v, accesskey[%v]", userID, userInfo.AccessKey)
	return
}

func (u *User) updateKey(param *proto.UserUpdateParam) (userInfo *proto.UserInfo, err error) {
	if param.UserID == "" {
		err = proto.ErrInvalidUserID
		return
	}

	u.userStoreMutex.Lock()
	defer u.userStoreMutex.Unlock()
	u.AKStoreMutex.Lock()
	defer u.AKStoreMutex.Unlock()

	if value, exist := u.userStore.Load(param.UserID); !exist {
		err = proto.ErrUserNotExists
		return
	} else {
		userInfo = value.(*proto.UserInfo)
	}
	userInfo.Mu.Lock()
	defer userInfo.Mu.Unlock()
	if userInfo.UserType == proto.UserTypeRoot {
		err = proto.ErrNoPermission
		return
	}
	var formerAK = userInfo.AccessKey
	var akMark, skMark, typeMark, describeMark int
	if param.AccessKey != "" {
		if !proto.IsValidAK(param.AccessKey) {
			err = proto.ErrInvalidAccessKey
			return
		}
		if _, exist := u.AKStore.Load(param.AccessKey); exist {
			err = proto.ErrDuplicateAccessKey
			return
		}
		akMark = 1
	}
	if param.SecretKey != "" {
		if !proto.IsValidSK(param.SecretKey) {
			err = proto.ErrInvalidSecretKey
			return
		}
		skMark = 1
	}
	//Type == 0,do not modify type
	if param.Type != 0 {
		if param.Type.Valid() {
			typeMark = 1
		} else {
			err = proto.ErrInvalidUserType
			return
		}
	}
	if param.Description != "" {
		describeMark = 1
	}

	var akUserBef *proto.AKUser
	var akUserAft *proto.AKUser

	if value, exist := u.AKStore.Load(formerAK); exist {
		akUserBef = value.(*proto.AKUser)
	} else {
		err = proto.ErrAccessKeyNotExists
		return
	}
	if akMark == 1 {
		userInfo.AccessKey = param.AccessKey
	}
	if skMark == 1 {
		userInfo.SecretKey = param.SecretKey
	}
	if typeMark == 1 {
		userInfo.UserType = param.Type
	}
	if describeMark == 1 {
		userInfo.Description = param.Description
	}

	if len(strings.TrimSpace(param.Password)) != 0 {
		akUserBef.Password = encodingPassword(param.Password)
	}

	akUserAft = &proto.AKUser{AccessKey: userInfo.AccessKey, UserID: param.UserID, Password: akUserBef.Password}

	if err = u.syncUpdateUserInfo(userInfo); err != nil {
		return
	}
	if err = u.syncDeleteAKUser(akUserBef); err != nil {
		return
	}
	if err = u.syncAddAKUser(akUserAft); err != nil {
		return
	}
	u.AKStore.Delete(formerAK)
	u.AKStore.Store(akUserAft.AccessKey, akUserAft)

	log.LogInfof("action[updateUser], userID: %v, accesskey[%v], secretkey[%v]", userInfo.UserID, userInfo.AccessKey, userInfo.SecretKey)
	return
}

func (u *User) getKeyInfo(ak string) (userInfo *proto.UserInfo, err error) {
	var akUser *proto.AKUser
	if akUser, err = u.getAKUser(ak); err != nil {
		return
	}
	if userInfo, err = u.getUserInfo(akUser.UserID); err != nil {
		return
	}
	log.LogInfof("action[getKeyInfo], accesskey[%v]", ak)
	return
}

func (u *User) getUserInfo(userID string) (userInfo *proto.UserInfo, err error) {
	if value, exist := u.userStore.Load(userID); exist {
		userInfo = value.(*proto.UserInfo)
	} else {
		err = proto.ErrUserNotExists
		return
	}
	log.LogInfof("action[getUserInfo], userID: %v", userID)
	return
}

func (u *User) updatePolicy(params *proto.UserPermUpdateParam) (userInfo *proto.UserInfo, err error) {
	if userInfo, err = u.getUserInfo(params.UserID); err != nil {
		return
	}
	userInfo.Mu.Lock()
	defer userInfo.Mu.Unlock()
	if userInfo.Policy.IsOwn(params.Volume) {
		err = proto.ErrIsOwner
		return
	}
	userInfo.Policy.AddAuthorizedVol(params.Volume, params.Policy)
	if err = u.syncUpdateUserInfo(userInfo); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.addUserToVol(params.UserID, params.Volume); err != nil {
		return
	}
	log.LogInfof("action[updatePolicy], userID: %v, volume: %v", params.UserID, params.Volume)
	return
}

func (u *User) removePolicy(params *proto.UserPermRemoveParam) (userInfo *proto.UserInfo, err error) {
	if userInfo, err = u.getUserInfo(params.UserID); err != nil {
		return
	}
	userInfo.Mu.Lock()
	defer userInfo.Mu.Unlock()
	if userInfo.Policy.IsOwn(params.Volume) {
		err = proto.ErrIsOwner
		return
	}
	userInfo.Policy.RemoveAuthorizedVol(params.Volume)
	if err = u.syncUpdateUserInfo(userInfo); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.removeUserFromVol(params.UserID, params.Volume); err != nil {
		return
	}
	log.LogInfof("action[removePolicy], userID: %v, volume: %v", params.UserID, params.Volume)
	return
}

func (u *User) addOwnVol(userID, volName string) (userInfo *proto.UserInfo, err error) {
	if userInfo, err = u.getUserInfo(userID); err != nil {
		return
	}
	userInfo.Mu.Lock()
	defer userInfo.Mu.Unlock()
	userInfo.Policy.AddOwnVol(volName)
	userInfo.Policy.RemoveAuthorizedVol(volName)
	if err = u.syncUpdateUserInfo(userInfo); err != nil {
		err = proto.ErrPersistenceByRaft
		return
	}
	if err = u.addUserToVol(userID, volName); err != nil {
		return
	}
	log.LogInfof("action[addOwnVol], userID: %v, volume: %v", userID, volName)
	return
}

func (u *User) removeOwnVol(userID, volName string) (userInfo *proto.UserInfo, err error) {
	if userInfo, err = u.getUserInfo(userID); err != nil {
		return
	}
	userInfo.Mu.Lock()
	defer userInfo.Mu.Unlock()
	userInfo.Policy.RemoveOwnVol(volName)
	if err = u.syncUpdateUserInfo(userInfo); err != nil {
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
		userInfo *proto.UserInfo
	)
	//delete policy
	var deletedUsers = make([]string, 0)
	var userIDs []string
	if userIDs, err = u.getUsersOfVol(volName); err != nil {
		return
	}
	for _, userID := range userIDs {
		if userInfo, err = u.getUserInfo(userID); err != nil {
			if err == proto.ErrUserNotExists {
				deletedUsers = append(deletedUsers, userID)
				log.LogWarnf("action[deleteVolPolicy], userID: %v does not exist", userID)
				continue
			}
			return
		}
		userInfo.Mu.Lock()
		userInfo.Policy.RemoveOwnVol(volName)
		userInfo.Policy.RemoveAuthorizedVol(volName)
		if err = u.syncUpdateUserInfo(userInfo); err != nil {
			err = proto.ErrPersistenceByRaft
			userInfo.Mu.Unlock()
			return
		}
		userInfo.Mu.Unlock()
	}
	//delete volName index
	if value, exist := u.volUser.Load(volName); exist {
		volUser = value.(*proto.VolUser)
	} else {
		return nil
	}
	volUser.Mu.Lock()
	if err = u.syncDeleteVolUser(volUser); err != nil {
		volUser.Mu.Unlock()
		return
	}
	u.volUser.Delete(volUser.Vol)
	volUser.Mu.Unlock()
	for _, deletedUser := range deletedUsers {
		u.removeUserFromAllVol(deletedUser)
	}
	log.LogInfof("action[deleteVolPolicy], volName: %v", volName)
	return
}

func (u *User) transferVol(params *proto.UserTransferVolParam) (targetUserInfo *proto.UserInfo, err error) {
	var userInfo *proto.UserInfo
	userInfo, err = u.getUserInfo(params.UserSrc)
	if (err != nil && err != proto.ErrUserNotExists) || (!params.Force && err == proto.ErrUserNotExists) {
		return
	}
	if err == nil {
		var isOwned = userInfo.Policy.IsOwn(params.Volume)
		if !isOwned && !params.Force && params.UserSrc != params.UserDst {
			err = proto.ErrHaveNoPolicy
			return
		}
		if isOwned {
			if _, err = u.removeOwnVol(params.UserSrc, params.Volume); err != nil {
				return
			}
		}
	}

	if targetUserInfo, err = u.addOwnVol(params.UserDst, params.Volume); err != nil {
		return
	}
	log.LogInfof("action[transferVol], volName: %v, userSrc: %v, userDst: %v", params.Volume, params.UserSrc, params.UserDst)
	return
}

func (u *User) getAllUserInfo(keywords string) (users []*proto.UserInfo) {
	users = make([]*proto.UserInfo, 0)
	u.userStore.Range(func(key, value interface{}) bool {
		userInfo := value.(*proto.UserInfo)
		if strings.Contains(userInfo.UserID, keywords) {
			users = append(users, userInfo)
		}
		return true
	})
	log.LogInfof("action[getAllUserInfo], keywords: %v, total numbers: %v", keywords, len(users))
	return
}

func (u *User) getUsersOfVol(volName string) (userIDs []string, err error) {
	var volUser *proto.VolUser
	userIDs = make([]string, 0)
	if value, exist := u.volUser.Load(volName); exist {
		volUser = value.(*proto.VolUser)
	} else {
		err = proto.ErrHaveNoPolicy
		return
	}
	volUser.Mu.RLock()
	defer volUser.Mu.RUnlock()
	for _, userID := range volUser.UserIDs {
		userIDs = append(userIDs, userID)
	}
	log.LogInfof("action[getUsersOfVol], vol: %v, user numbers: %v", volName, len(userIDs))
	return
}

func (u *User) getAKUser(ak string) (akUser *proto.AKUser, err error) {
	if value, exist := u.AKStore.Load(ak); exist {
		akUser = value.(*proto.AKUser)
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
		volUser.Mu.Lock()
		defer volUser.Mu.Unlock()
		if contains(volUser.UserIDs, userID) {
			return
		}
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
		volUser.Mu.Lock()
		defer volUser.Mu.Unlock()
		volUser.UserIDs, _ = removeString(volUser.UserIDs, userID)
	} else {
		err = proto.ErrHaveNoPolicy
		return
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
		volUser.Mu.Lock()
		var exist bool
		volUser.UserIDs, exist = removeString(volUser.UserIDs, userID)
		if exist {
			if err := u.syncUpdateVolUser(volUser); err != nil {
				err = proto.ErrPersistenceByRaft
				log.LogErrorf("action[deleteUser], userID: %v, volUser: %v, err: %v", userID, volUser, err)
			}
		}
		volUser.Mu.Unlock()
		return true
	})
}

func removeString(array []string, element string) ([]string, bool) {
	for k, v := range array {
		if v == element {
			return append(array[:k], array[k+1:]...), true
		}
	}
	return array, false
}

func encodingPassword(s string) string {
	t := sha1.New()
	io.WriteString(t, s)
	return hex.EncodeToString(t.Sum(nil))
}

func (u *User) clearUserStore() {
	u.userStore.Range(func(key, value interface{}) bool {
		u.userStore.Delete(key)
		return true
	})
}

func (u *User) clearAKStore() {
	u.AKStore.Range(func(key, value interface{}) bool {
		u.AKStore.Delete(key)
		return true
	})
}

func (u *User) clearVolUsers() {
	u.volUser.Range(func(key, value interface{}) bool {
		u.volUser.Delete(key)
		return true
	})
}
