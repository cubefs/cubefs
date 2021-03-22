// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// SetBalancer allows to enable or disable balancer
type SetBalancer struct {
	base
	req *pb.SetBalancerRunningRequest
}

// NewListTableNames creates a new SetBalancer request that will set balancer state.
func NewSetBalancer(ctx context.Context, enabled bool) (*SetBalancer, error) {
	return &SetBalancer{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		req: &pb.SetBalancerRunningRequest{On: &enabled},
	}, nil
}

// Name returns the name of this RPC call.
func (sb *SetBalancer) Name() string {
	return "SetBalancerRunning"
}

// ToProto converts the RPC into a protobuf message.
func (sb *SetBalancer) ToProto() proto.Message {
	return sb.req
}

// NewResponse creates an empty protobuf message to read the response of this RPC.
func (sb *SetBalancer) NewResponse() proto.Message {
	return &pb.SetBalancerRunningResponse{}
}
