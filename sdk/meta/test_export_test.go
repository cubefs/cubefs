// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package meta

import (
	"encoding/json"
	"net"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

func TestNewTestMetaWrapperWithLeaderInodeGet(t *testing.T) {
	t.Parallel()
	const ino = uint64(70001)
	info := &proto.InodeInfo{Inode: ino, Mode: 0o644, Nlink: 1}

	addr, cleanup := startMockMetaPacketListener(t, func(conn net.Conn) error {
		pkt := proto.NewPacket()
		if err := pkt.ReadFromConnWithVer(conn, proto.ReadDeadlineTime); err != nil {
			return err
		}
		resp := proto.NewPacketReqID()
		resp.ReqID = pkt.ReqID
		resp.Opcode = pkt.Opcode
		resp.PartitionID = pkt.PartitionID
		resp.ResultCode = proto.OpOk
		body, err := json.Marshal(&proto.InodeGetResponse{Info: info})
		if err != nil {
			return err
		}
		resp.Data = body
		resp.Size = uint32(len(body))
		return resp.WriteToConn(conn)
	})
	t.Cleanup(cleanup)

	mw := NewTestMetaWrapperWithLeader(t, addr)
	got, err := mw.InodeGet_ll(ino, false)
	require.NoError(t, err)
	require.Equal(t, ino, got.Inode)
}
