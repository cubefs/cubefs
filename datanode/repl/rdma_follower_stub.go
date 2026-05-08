//go:build !(linux && rdma)

package repl

import "github.com/cubefs/cubefs/util/rdma"

// EnableFollowerRDMA is a no-op in non-RDMA builds. Signature mirrors the
// rdma-tagged version so callers can drop conditional build-tag guards.
func EnableFollowerRDMA(_ rdma.RDMAPoolConfig) error { return nil }
