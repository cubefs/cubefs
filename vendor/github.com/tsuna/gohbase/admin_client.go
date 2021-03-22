// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package gohbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase/hrpc"
	"github.com/tsuna/gohbase/pb"
	"github.com/tsuna/gohbase/region"
	"github.com/tsuna/gohbase/zk"
)

const (
	// snapshotValidateInterval specifies the amount of time to wait before
	// polling the hbase server about the status of a snapshot operation.
	snaphotValidateInterval time.Duration = time.Second / 2
)

// AdminClient to perform admistrative operations with HMaster
type AdminClient interface {
	CreateTable(t *hrpc.CreateTable) error
	DeleteTable(t *hrpc.DeleteTable) error
	EnableTable(t *hrpc.EnableTable) error
	DisableTable(t *hrpc.DisableTable) error
	CreateSnapshot(t *hrpc.Snapshot) error
	DeleteSnapshot(t *hrpc.Snapshot) error
	ListSnapshots(t *hrpc.ListSnapshots) ([]*pb.SnapshotDescription, error)
	RestoreSnapshot(t *hrpc.Snapshot) error
	ClusterStatus() (*pb.ClusterStatus, error)
	ListTableNames(t *hrpc.ListTableNames) ([]*pb.TableName, error)
	// SetBalancer sets balancer state and returns previous state
	SetBalancer(sb *hrpc.SetBalancer) (bool, error)
	// MoveRegion moves a region to a different RegionServer
	MoveRegion(mr *hrpc.MoveRegion) error
}

// NewAdminClient creates an admin HBase client.
func NewAdminClient(zkquorum string, options ...Option) AdminClient {
	return newAdminClient(zkquorum, options...)
}

func newAdminClient(zkquorum string, options ...Option) AdminClient {
	log.WithFields(log.Fields{
		"Host": zkquorum,
	}).Debug("Creating new admin client.")
	c := &client{
		clientType:    region.MasterClient,
		rpcQueueSize:  defaultRPCQueueSize,
		flushInterval: defaultFlushInterval,
		// empty region in order to be able to set client to it
		adminRegionInfo:     region.NewInfo(0, nil, nil, nil, nil, nil),
		zkTimeout:           defaultZkTimeout,
		zkRoot:              defaultZkRoot,
		effectiveUser:       defaultEffectiveUser,
		regionLookupTimeout: region.DefaultLookupTimeout,
		regionReadTimeout:   region.DefaultReadTimeout,
		newRegionClientFn:   region.NewClient,
	}
	for _, option := range options {
		option(c)
	}
	c.zkClient = zk.NewClient(zkquorum, c.zkTimeout)
	return c
}

//Get the status of the cluster
func (c *client) ClusterStatus() (*pb.ClusterStatus, error) {
	pbmsg, err := c.SendRPC(hrpc.NewClusterStatus())
	if err != nil {
		return nil, err
	}

	r, ok := pbmsg.(*pb.GetClusterStatusResponse)
	if !ok {
		return nil, fmt.Errorf("sendRPC returned not a ClusterStatusResponse")
	}

	return r.GetClusterStatus(), nil
}

func (c *client) CreateTable(t *hrpc.CreateTable) error {
	pbmsg, err := c.SendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.CreateTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a CreateTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) DeleteTable(t *hrpc.DeleteTable) error {
	pbmsg, err := c.SendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.DeleteTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a DeleteTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) EnableTable(t *hrpc.EnableTable) error {
	pbmsg, err := c.SendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.EnableTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a EnableTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) DisableTable(t *hrpc.DisableTable) error {
	pbmsg, err := c.SendRPC(t)
	if err != nil {
		return err
	}

	r, ok := pbmsg.(*pb.DisableTableResponse)
	if !ok {
		return fmt.Errorf("sendRPC returned not a DisableTableResponse")
	}

	return c.checkProcedureWithBackoff(t.Context(), r.GetProcId())
}

func (c *client) checkProcedureWithBackoff(ctx context.Context, procID uint64) error {
	backoff := backoffStart
	for {
		pbmsg, err := c.SendRPC(hrpc.NewGetProcedureState(ctx, procID))
		if err != nil {
			return err
		}

		res := pbmsg.(*pb.GetProcedureResultResponse)
		switch res.GetState() {
		case pb.GetProcedureResultResponse_NOT_FOUND:
			return fmt.Errorf("procedure not found")
		case pb.GetProcedureResultResponse_FINISHED:
			if fe := res.Exception; fe != nil {
				ge := fe.GenericException
				if ge == nil {
					return errors.New("got unexpected empty exception")
				}
				return fmt.Errorf("procedure exception: %s: %s", ge.GetClassName(), ge.GetMessage())
			}
			return nil
		default:
			backoff, err = sleepAndIncreaseBackoff(ctx, backoff)
			if err != nil {
				return err
			}
		}
	}
}

// CreateSnapshot creates a snapshot in HBase.
//
// If a context happens during creation, no cleanup is done.
func (c *client) CreateSnapshot(t *hrpc.Snapshot) error {

	pbmsg, err := c.SendRPC(t)
	if err != nil {
		return err
	}

	_, ok := pbmsg.(*pb.SnapshotResponse)
	if !ok {
		return errors.New("sendPRC returned not a SnapshotResponse")
	}

	ticker := time.NewTicker(snaphotValidateInterval)
	defer ticker.Stop()
	check := hrpc.NewSnapshotDone(t)
	ctx := t.Context()

	for {
		select {
		case <-ticker.C:
			pbmsgs, err := c.SendRPC(check)
			if err != nil {
				return err
			}

			r, ok := pbmsgs.(*pb.IsSnapshotDoneResponse)
			if !ok {
				return errors.New("sendPRC returned not a IsSnapshotDoneResponse")
			}

			if r.GetDone() {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// DeleteSnapshot deletes a snapshot in HBase.
func (c *client) DeleteSnapshot(t *hrpc.Snapshot) error {
	rt := hrpc.NewDeleteSnapshot(t)
	pbmsg, err := c.SendRPC(rt)
	if err != nil {
		return err
	}

	_, ok := pbmsg.(*pb.DeleteSnapshotResponse)
	if !ok {
		return errors.New("sendPRC returned not a DeleteSnapshotResponse")
	}

	return nil
}

func (c *client) ListSnapshots(t *hrpc.ListSnapshots) ([]*pb.SnapshotDescription, error) {
	pbmsg, err := c.SendRPC(t)
	if err != nil {
		return nil, err
	}

	r, ok := pbmsg.(*pb.GetCompletedSnapshotsResponse)
	if !ok {
		return nil, errors.New("sendPRC returned not a GetCompletedSnapshotsResponse")
	}

	return r.GetSnapshots(), nil

}

func (c *client) RestoreSnapshot(t *hrpc.Snapshot) error {
	rt := hrpc.NewRestoreSnapshot(t)
	pbmsg, err := c.SendRPC(rt)
	if err != nil {
		return err
	}

	_, ok := pbmsg.(*pb.RestoreSnapshotResponse)
	if !ok {
		return errors.New("sendPRC returned not a RestoreSnapshotResponse")
	}
	return nil
}

func (c *client) ListTableNames(t *hrpc.ListTableNames) ([]*pb.TableName, error) {
	pbmsg, err := c.SendRPC(t)
	if err != nil {
		return nil, err
	}

	res, ok := pbmsg.(*pb.GetTableNamesResponse)
	if !ok {
		return nil, errors.New("sendPRC returned not a GetTableNamesResponse")
	}

	return res.GetTableNames(), nil
}

func (c *client) SetBalancer(sb *hrpc.SetBalancer) (bool, error) {
	pbmsg, err := c.SendRPC(sb)
	if err != nil {
		return false, err
	}
	res, ok := pbmsg.(*pb.SetBalancerRunningResponse)
	if !ok {
		return false, errors.New("SendPRC returned not a SetBalancerRunningResponse")
	}
	return res.GetPrevBalanceValue(), nil
}

func (c *client) MoveRegion(mr *hrpc.MoveRegion) error {
	pbmsg, err := c.SendRPC(mr)
	if err != nil {
		return err
	}
	_, ok := pbmsg.(*pb.MoveRegionResponse)
	if !ok {
		return errors.New("SendPRC returned not a MoveRegionResponse")
	}
	return nil
}
