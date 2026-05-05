//go:build !(linux && rdma)

package datanode

import (
	"errors"
	"net"

	"github.com/cubefs/cubefs/datanode/repl"
)

// RDMAServerConfig is a no-op stub for non-rdma builds.
type RDMAServerConfig struct {
	Port          int
	NumSlots      int
	SlotSize      int
	SpinThreshold int
}

// DataNodeRDMACtx is a no-op stub for non-rdma builds.
type DataNodeRDMACtx struct{}

func NewDataNodeRDMACtx(_ RDMAServerConfig, _ func(*repl.Packet, net.Conn) error) (*DataNodeRDMACtx, error) {
	return nil, errors.New("rdma: not supported in this build")
}

func (ctx *DataNodeRDMACtx) Start() error { return nil }
func (ctx *DataNodeRDMACtx) Stop()        {}
