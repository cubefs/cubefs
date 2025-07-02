package locstore

import (
	"github.com/cubefs/cubefs/blobstore/api/access"
)

// Defines metadata operations
type LocationStore interface {
	Put(key string, location access.Location) error
	Get(key string) (location access.Location, err error)
	Del(key string) error
	Close()
}
