package objectnode

import (
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	META_OSS_VOLUME = ".oss_meta"
)

type objectStore struct {
	vm *volumeManager
}

func (s *objectStore) Init(vm *volumeManager) {
	s.vm = vm
	//TODO: init meta dir
}

func (s *objectStore) Put(vol, obj, key string, data []byte) (err error) {
	log.LogInfo("put object store")

	return
}

func (s *objectStore) Get(vol, obj, key string) (data []byte, err error) {
	return
}

func (s *objectStore) Delete(vol, obj, key string) (err error) {
	return
}

func (s *objectStore) List(vol, obj string) (data [][]byte, err error) {
	return
}
