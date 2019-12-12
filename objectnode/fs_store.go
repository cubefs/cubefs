package objectnode

type MetaStore interface {
}

// MetaStore
type Store interface {
	Init(vm *volumeManager)
	Put(ns, obj, key string, data []byte) error
	Get(ns, obj, key string) (data []byte, err error)
	List(ns, obj string) (data [][]byte, err error)
	Delete(ns, obj, key string) error
}
