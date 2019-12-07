package authnode

import (
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/keystore"
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
	delete((mf.keystore), id)
	return
}
