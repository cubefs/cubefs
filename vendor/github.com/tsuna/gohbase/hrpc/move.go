// Copyright (C) 2020  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// MoveRegion allows to move region to a different RegionServer.
type MoveRegion struct {
	base
	req *pb.MoveRegionRequest
}

// WithDestinationRegionServer specifies destination RegionServer for MoveReqion request
// A server name is its host, port plus startcode: host187.example.com,60020,1289493121758
func WithDestinationRegionServer(serverName string) func(Call) error {
	return func(c Call) error {
		mr, ok := c.(*MoveRegion)
		if !ok {
			return errors.New("WithDestinationRegionServer option can only be used with MoveRegion")
		}
		out := strings.SplitN(serverName, ",", 3)
		if len(out) != 3 {
			return errors.New(
				"invalid server name, needs to be of format <host>,<port>,<startcode>")
		}

		// parse port
		port, err := strconv.ParseUint(out[1], 10, 32)
		if err != nil {
			return fmt.Errorf("failed to parse port: %w", err)
		}

		// parse startCode
		startCode, err := strconv.ParseUint(out[2], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse startcode: %w", err)
		}
		mr.req.DestServerName = &pb.ServerName{
			HostName:  proto.String(out[0]),
			Port:      proto.Uint32(uint32(port)),
			StartCode: proto.Uint64(uint64(startCode)),
		}
		return nil
	}
}

// NewMoveRegion creates an hrpc to move region to a different RegionServer.
// Specify encoded region name.
func NewMoveRegion(ctx context.Context, regionName []byte,
	opts ...func(Call) error) (*MoveRegion, error) {
	mr := &MoveRegion{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		req: &pb.MoveRegionRequest{
			Region: &pb.RegionSpecifier{
				Type:  pb.RegionSpecifier_ENCODED_REGION_NAME.Enum(),
				Value: regionName,
			},
		},
	}
	if err := applyOptions(mr, opts...); err != nil {
		return nil, err
	}
	return mr, nil
}

// Name returns the name of this RPC call.
func (mr *MoveRegion) Name() string {
	return "MoveRegion"
}

// ToProto converts the RPC into a protobuf message.
func (mr *MoveRegion) ToProto() proto.Message {
	return mr.req
}

// NewResponse creates an empty protobuf message to read the response of this RPC.
func (mr *MoveRegion) NewResponse() proto.Message {
	return &pb.MoveRegionResponse{}
}
