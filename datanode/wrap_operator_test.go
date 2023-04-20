package datanode

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/datanode/mock"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUpstreamRead(t *testing.T) {
	tcp := mock.NewMockTcp(mockDataTcpPort1)
	err := tcp.Start()
	assert.Nil(t, err)
	defer tcp.Stop()

	dataReceiver := make([]byte, 0)
	var callbackWriter = func(packet *repl.Packet) error {
		dataReceiver = append(dataReceiver, packet.Data...)
		return nil
	}
	reqPacket := newReadPacket(context.Background(), &proto.ExtentKey{
		PartitionId: 1,
		ExtentId:    1,
	}, 0, mock.UpstreamReadSize, 0, false)

	err = upstreamStreamRead(reqPacket, callbackWriter, fmt.Sprintf("127.0.0.1:%d", mockDataTcpPort1))
	assert.Nil(t, err)
	assert.Equal(t, len(dataReceiver), mock.UpstreamReadSize)
	realData := mock.RandTestData(mock.UpstreamReadSize, mock.UpstreamReadSeed)
	assert.Equal(t, realData, dataReceiver)
}

func TestUpstreamConsistentReadWithOneBadHost(t *testing.T) {
	//node 3 is not started
	tcp := mock.NewMockTcp(mockDataTcpPort2)
	err := tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()

	dataReceiver := make([]byte, 0)
	var callbackWriter = func(packet *repl.Packet) error {
		dataReceiver = append(dataReceiver, packet.Data...)
		return nil
	}
	//remote applyID=2
	partition := &DataPartition{
		partitionID: 1,
		applyStatus: &WALApplyStatus{
			applied: 1,
		},
		replicas: []string{
			fmt.Sprintf("127.0.0.1:%d", mockDataTcpPort1),
			fmt.Sprintf("127.0.0.1:%d", mockDataTcpPort2),
			fmt.Sprintf("127.0.0.1:%d", mockDataTcpPort3),
		},
		config: &dataPartitionCfg{
			NodeID: 1,
			Peers: []proto.Peer{
				{
					ID:   1,
					Addr: fmt.Sprintf("127.0.0.1:%d", mockDataTcpPort1),
				},
				{
					ID:   2,
					Addr: fmt.Sprintf("127.0.0.1:%d", mockDataTcpPort2),
				},
				{
					ID:   3,
					Addr: fmt.Sprintf("127.0.0.1:%d", mockDataTcpPort3),
				},
			},
		},
	}

	reqPacket := newReadPacket(context.Background(), &proto.ExtentKey{
		PartitionId: 1,
		ExtentId:    1,
	}, 0, mock.UpstreamReadSize, 0, false)

	allAppliedIDMap, replyNum := partition.getAllReplicaAppliedID(context.Background(), proto.UpstreamRequestDeadLineTimeNs, proto.UpstreamRequestDeadLineTimeNs)
	assert.Greater(t, replyNum, uint8(len(partition.replicas)/2))

	_, maxAplHost := partition.findMaxID(allAppliedIDMap)
	//turn to follower read and avoid recycling
	reqPacket.Opcode = proto.OpStreamFollowerRead
	assert.False(t, partition.IsLocalAddress(maxAplHost))
	err = upstreamStreamRead(reqPacket, callbackWriter, maxAplHost)
	log.LogWarnf("action[handleStreamReadPacket] upstream req(%v) to max apply id host:%v", reqPacket.LogMessage(reqPacket.GetOpMsg(), "local req", reqPacket.StartT, err), maxAplHost)
	assert.Equal(t, len(dataReceiver), mock.UpstreamReadSize)
	realData := mock.RandTestData(mock.UpstreamReadSize, mock.UpstreamReadSeed)
	assert.Equal(t, realData, dataReceiver)
}

// newReadPacket returns a new read packet.
func newReadPacket(ctx context.Context, key *proto.ExtentKey, extentOffset, size int, fileOffset uint64, followerRead bool) *repl.Packet {
	p := new(repl.Packet)
	p.ExtentID = key.ExtentId
	p.PartitionID = key.PartitionId
	p.Magic = proto.ProtoMagic
	p.ExtentOffset = int64(extentOffset)
	p.Size = uint32(size)
	if followerRead {
		p.Opcode = proto.OpStreamFollowerRead
	} else {
		p.Opcode = proto.OpStreamRead
	}
	p.ExtentType = proto.NormalExtentType
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = 0
	p.KernelOffset = uint64(fileOffset)
	p.SetCtx(ctx)
	return p
}
