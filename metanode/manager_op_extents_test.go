// Copyright 2026 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package metanode

import (
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestHandleMetadataOperation_AsyncExtentsListOpcode(t *testing.T) {
	proto.InitBufferPool(int64(32768))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mp := mockPartitionRaftForFsmInodeTest(t, ctrl, proto.StoreModeMem)
	mp.config.NodeId = 1
	mp.config.Peers = []proto.Peer{{ID: 1, Addr: "127.0.0.1:1"}}
	const ino = uint64(5001)
	prepareInodeForFsmInodeTest(t, mp, ino)

	mm := &metadataManager{
		partitions: map[uint64]MetaPartition{mp.config.PartitionId: mp},
		metaNode:   &MetaNode{opLimiter: newOpLimiter()},
	}

	for _, opcode := range []uint8{proto.OpMetaExtentsList, proto.OpMetaAsyncExtentsList} {
		opcode := opcode
		t.Run((&proto.Packet{Opcode: opcode}).GetOpMsg(), func(t *testing.T) {
			req := &proto.GetExtentsRequest{
				VolName:     mp.config.VolName,
				PartitionID: mp.config.PartitionId,
				Inode:       ino,
			}
			body, err := json.Marshal(req)
			require.NoError(t, err)

			client, srv := net.Pipe()
			defer func() { _ = client.Close() }()

			p := &Packet{}
			p.Magic = proto.ProtoMagic
			p.Opcode = opcode
			p.PartitionID = mp.config.PartitionId
			p.Data = body
			p.Size = uint32(len(body))

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer srv.Close()
				err := mm.HandleMetadataOperation(srv, p, "127.0.0.1:1")
				require.NoError(t, err)
			}()

			out := readAllAvailable(client, 2*time.Second)
			wg.Wait()
			require.NotEmpty(t, out)
			require.Equal(t, proto.OpOk, p.ResultCode)
		})
	}
}
