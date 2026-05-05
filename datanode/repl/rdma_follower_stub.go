//go:build !(linux && rdma)

package repl

// EnableFollowerRDMA is a no-op in non-RDMA builds.
func EnableFollowerRDMA(_, _ int) error { return nil }
