package authnode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/keystore"
)

// PutKey change keyInfo in keystore cache
func (mf *KeystoreFsm) PutKey(k *keystore.KeyInfo) {
	mf.ksMutex.Lock()
	defer mf.ksMutex.Unlock()
	if _, ok := (mf.keystore)[k.ID]; !ok {
		(mf.keystore)[k.ID] = k
	}
}

// GetKey Get keyInfo from keystore cache
func (mf *KeystoreFsm) GetKey(id string) (u *keystore.KeyInfo, err error) {
	mf.ksMutex.RLock()
	defer mf.ksMutex.RUnlock()
	u, ok := (mf.keystore)[id]
	if !ok {
		err = proto.ErrKeyNotExists
	}
	return
}

// DeleteKey Delete keyInfo in keystore cache
func (mf *KeystoreFsm) DeleteKey(id string) {
	mf.ksMutex.Lock()
	defer mf.ksMutex.Unlock()
	delete(mf.keystore, id)
	return
}

func (mf *KeystoreFsm) PutAKInfo(akInfo *keystore.AccessKeyInfo) {
	mf.aksMutex.Lock()
	defer mf.aksMutex.Unlock()
	if _, ok := (mf.accessKeystore)[akInfo.AccessKey]; !ok {
		(mf.accessKeystore)[akInfo.AccessKey] = akInfo
	}
}

func (mf *KeystoreFsm) GetAKInfo(accessKey string) (akInfo *keystore.AccessKeyInfo, err error) {
	mf.aksMutex.RLock()
	defer mf.aksMutex.RUnlock()
	akInfo, ok := (mf.accessKeystore)[accessKey]
	if !ok {
		err = proto.ErrAccessKeyNotExists
	}
	return
}

func (mf *KeystoreFsm) DeleteAKInfo(accessKey string) {
	mf.aksMutex.Lock()
	defer mf.aksMutex.Unlock()
	delete(mf.accessKeystore, accessKey)
	return
}
