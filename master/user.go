package master

import (
	"sync"

	"github.com/chubaofs/chubaofs/raftstore"
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
