package locstore

import "github.com/cubefs/cubefs/blobstore/common/proto"

// Defines metadata operations
type LocationStore interface {
	Put(key string, location proto.Location) error
	Get(key string) (location proto.Location, err error)
	Del(key string) error
	Close()
}
