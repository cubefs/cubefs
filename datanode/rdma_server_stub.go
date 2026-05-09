//go:build !(linux && rdma)

package datanode

import (
	"errors"
	"net"

	"github.com/cubefs/cubefs/datanode/repl"
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

func NewDataNodeRDMACtx(_ RDMAServerConfig, _ func(*repl.Packet, net.Conn) error) (*DataNodeRDMACtx, error) {
	return nil, errors.New("rdma: not supported in this build")
}

func (ctx *DataNodeRDMACtx) Start() error { return nil }
func (ctx *DataNodeRDMACtx) Stop()        {}
