package objectnode

import (
	"strings"

	"github.com/chubaofs/chubaofs/proto"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	volumeRootInode = uint64(1)
)

type xattrStore struct {
	vm *volumeManager //vol *volume
}

func (s *xattrStore) Init(vm *volumeManager) {
	s.vm = vm
}

func (s *xattrStore) getInode(vol, path string) (*volume, uint64, error) {
	v, err := s.vm.loadVolume(vol)
	if err != nil {
		return nil, 0, err
	}
	inode := volumeRootInode
	if path != "" && path != "/" {
		items := strings.Split(path, "/")
		for _, item := range items {
			if item == "" {
				continue
			}
			inode, _, err = v.mw.Lookup_ll(inode, item)
			if err != nil {
				return v, inode, err
			}
		}
	}
	return v, inode, nil
}

func (s *xattrStore) Put(vol, path, key string, data []byte) (err error) {
	v, err1 := s.vm.loadVolume(vol)
	if err1 != nil {
		err = err1
		return
	}
	err = v.SetXAttr(path, key, data)
	if err != nil {
		log.LogErrorf("policy: %v, %v", key, data)
	}
	return
}

func (s *xattrStore) Get(vol, path, key string) (val []byte, err error) {
	var v *volume
	v, err = s.vm.loadVolume(vol)
	if err != nil {
		return
	}

	var xattrInfo *proto.XAttrInfo
	if xattrInfo, err = v.GetXAttr(path, key); err != nil {
		return
	}
	if xattrInfo == nil {
		return
	}

	var strVal string
	strVal = xattrInfo.XAttrs[key]
	if len(strVal) > 0 {
		val = []byte(strVal)
		return
	}
	return
}

func (s *xattrStore) Delete(vol, obj, key string) (err error) {

	return
}

func (s *xattrStore) List(vol, obj string) (data [][]byte, err error) {

	return
}
