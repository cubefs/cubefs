// Copyright (C) 2019  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

type snap struct {
	name         string
	table        string
	snapshotType *pb.SnapshotDescription_Type
	version      int32
	owner        string
}

func (s *snap) ToProto() *pb.SnapshotDescription {
	return &pb.SnapshotDescription{
		Type:    s.snapshotType,
		Table:   proto.String(s.table),
		Name:    proto.String(s.name),
		Version: proto.Int32(s.version),
		Owner:   proto.String(s.owner),
	}
}

func (s *snap) Version(v int32) {
	s.version = v
}

func (s *snap) Owner(o string) {
	s.owner = o
}

func (s *snap) Type(t pb.SnapshotDescription_Type) {
	s.snapshotType = &t
}

type snapshotSettable interface {
	Version(int32)
	Owner(string)
	Type(pb.SnapshotDescription_Type)
}

// SnapshotVersion sets the version of the snapshot.
func SnapshotVersion(v int32) func(Call) error {
	return func(g Call) error {
		sn, ok := g.(snapshotSettable)
		if !ok {
			return errors.New("'SnapshotVersion' option can only be used with Snapshot queries")
		}
		sn.Version(v)
		return nil
	}
}

// SnapshotOwner sets the owner of the snapshot.
func SnapshotOwner(o string) func(Call) error {
	return func(g Call) error {
		sn, ok := g.(snapshotSettable)
		if !ok {
			return errors.New("'SnapshotOwner' option can only be used with Snapshot queries")
		}
		sn.Owner(o)
		return nil
	}
}

// SnapshotSkipFlush disables hbase flushing when creating the snapshot.
func SnapshotSkipFlush() func(Call) error {
	return func(g Call) error {
		sn, ok := g.(snapshotSettable)
		if !ok {
			return errors.New("'SnapshotSkipFlush' option can only be used with Snapshot queries")
		}
		sn.Type(pb.SnapshotDescription_SKIPFLUSH)
		return nil
	}
}

// Snapshot represents a Snapshot HBase call
type Snapshot struct {
	base
	snap
}

// NewSnapshot creates a new Snapshot request that will request a
// new snapshot in HBase.
func NewSnapshot(ctx context.Context, name string, table string,
	opts ...func(Call) error) (*Snapshot, error) {
	sn := &Snapshot{
		base{
			table:    []byte(table),
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		snap{
			name: name, table: table,
		},
	}
	if err := applyOptions(sn, opts...); err != nil {
		return nil, err
	}
	return sn, nil
}

// Name returns the name of this RPC call.
func (sr *Snapshot) Name() string {
	return "Snapshot"
}

// ToProto converts the RPC into a protobuf message.
func (sr *Snapshot) ToProto() proto.Message {
	return &pb.SnapshotRequest{Snapshot: sr.snap.ToProto()}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (sr *Snapshot) NewResponse() proto.Message {
	return &pb.SnapshotResponse{}
}

// SnapshotDone represents an IsSnapshotDone HBase call.
type SnapshotDone struct {
	*Snapshot
}

// NewSnapshotDone creates a new SnapshotDone request that will check if
// the given snapshot has been complete.
func NewSnapshotDone(t *Snapshot) *SnapshotDone {
	return &SnapshotDone{t}
}

// Name returns the name of this RPC call.
func (sr *SnapshotDone) Name() string {
	return "IsSnapshotDone"
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (sr *SnapshotDone) NewResponse() proto.Message {
	return &pb.IsSnapshotDoneResponse{}
}

// DeleteSnapshot represents a DeleteSnapshot HBase call.
type DeleteSnapshot struct {
	*Snapshot
}

// NewDeleteSnapshot creates a new DeleteSnapshot request that will delete
// the given snapshot.
func NewDeleteSnapshot(t *Snapshot) *DeleteSnapshot {
	return &DeleteSnapshot{t}
}

// Name returns the name of this RPC call.
func (sr *DeleteSnapshot) Name() string {
	return "DeleteSnapshot"
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (sr *DeleteSnapshot) NewResponse() proto.Message {
	return &pb.DeleteSnapshotResponse{}
}

// ListSnapshots represents a new GetCompletedSnapshots request that will
// list all snapshots.
type ListSnapshots struct {
	base
}

// NewListSnapshots creates a new GetCompletedSnapshots request that will
// list all snapshots.
func NewListSnapshots(ctx context.Context) *ListSnapshots {
	return &ListSnapshots{
		base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
	}
}

// Name returns the name of this RPC call.
func (sr *ListSnapshots) Name() string {
	return "GetCompletedSnapshots"
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (sr *ListSnapshots) NewResponse() proto.Message {
	return &pb.GetCompletedSnapshotsResponse{}
}

// ToProto converts the RPC into a protobuf message.
func (sr *ListSnapshots) ToProto() proto.Message {
	return &pb.GetCompletedSnapshotsRequest{}
}

// RestoreSnapshot represents a RestoreSnapshot HBase call.
type RestoreSnapshot struct {
	*Snapshot
}

// NewRestoreSnapshot creates a new RestoreSnapshot request that will delete
// the given snapshot.
func NewRestoreSnapshot(t *Snapshot) *RestoreSnapshot {
	return &RestoreSnapshot{t}
}

// Name returns the name of this RPC call.
func (sr *RestoreSnapshot) Name() string {
	return "RestoreSnapshot"
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (sr *RestoreSnapshot) NewResponse() proto.Message {
	return &pb.RestoreSnapshotResponse{}
}

// RestoreSnapshotDone represents an IsRestoreSnapshotDone HBase call.
type RestoreSnapshotDone struct {
	*Snapshot
}

// NewRestoreSnapshotDone creates a new RestoreSnapshotDone request that will check if
// the given snapshot has been complete.
func NewRestoreSnapshotDone(t *Snapshot) *RestoreSnapshotDone {
	return &RestoreSnapshotDone{t}
}

// Name returns the name of this RPC call.
func (sr *RestoreSnapshotDone) Name() string {
	return "IsRestoreSnapshotDone"
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (sr *RestoreSnapshotDone) NewResponse() proto.Message {
	return &pb.IsRestoreSnapshotDoneResponse{}
}
