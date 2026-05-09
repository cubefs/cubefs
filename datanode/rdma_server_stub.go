//go:build !(linux && rdma)

package datanode

import (
	"errors"

	"github.com/cubefs/cubefs/util/rdma"
)

// RDMAServerConfig is a no-op stub for non-rdma builds. Mirrors the field
// set of the rdma-tagged variant so callers can populate it unconditionally.
type RDMAServerConfig struct {
	Port     int
	NumSlots int
	SlotSize int
	Poll     rdma.PollConfig
	Role     string
}

// DataNodeRDMACtx is a no-op stub for non-rdma builds.
type DataNodeRDMACtx struct{}

// NewDataNodeRDMACtx returns an error on non-RDMA builds. Signature
// mirrors the rdma-tagged version: takes the DataNode reference so the
// dispatch logic can directly call s.Prepare / s.OperatePacket.
func NewDataNodeRDMACtx(_ RDMAServerConfig, _ *DataNode) (*DataNodeRDMACtx, error) {
	return nil, errors.New("rdma: not supported in this build")
}

func (ctx *DataNodeRDMACtx) Start() error { return nil }
func (ctx *DataNodeRDMACtx) Stop()        {}
