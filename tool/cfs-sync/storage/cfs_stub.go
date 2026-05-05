//go:build !linux

package storage

import "fmt"

// NewCFS is not supported on non-Linux platforms because the CubeFS SDK
// depends on Linux-only syscalls (O_DIRECT, stat_t.Ctim).
func NewCFS(_ CFSConfig, _ string) (Storage, error) {
	return nil, fmt.Errorf("CubeFS storage backend is only supported on Linux")
}
