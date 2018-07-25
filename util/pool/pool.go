package pool

import "github.com/juju/errors"

var (
	ErrClosed = errors.New("closed pool")
)

// Pool is an interface wraps necessary methods for resource pool implement.
type Pool interface {
	// Get a resource from resource pool.
	Get() (interface{}, error)

	// Put a resource to resource pool.
	Put(interface{}) error

	// Close tries close specified resource.
	Close(interface{}) error

	// Release all resource entity stored in pool.
	Release()

	// Len returns number of resource stored in pool.
	Len() int
}
