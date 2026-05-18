package stream

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/wrapper"
)

func TestAllocateExtentRemoveDataPartitionForWrite(t *testing.T) {
	// Use a short retry budget so the allocateExtent loop exits quickly after failures
	SetExentRetryArgs(0, 0, 1, false)

	dps := []*wrapper.DataPartition{
		wrapper.NewTestDP(1, 1),
		wrapper.NewTestDP(2, 1),
		wrapper.NewTestDP(3, 1),
	}
	dataWrapper := wrapper.NewTestWrapperWithDps(dps)

	client := &ExtentClient{
		dataWrapper: dataWrapper,
	}

	stream := &Streamer{
		client: client,
	}

	eh := &ExtentHandler{
		stream:       stream,
		id:           GetExtentHandlerID(),
		inode:        1,
		fileOffset:   0,
		storeMode:    proto.NormalExtentType,
		key:          nil,
		poolId:       1,
		empty:        make(chan struct{}, 1024),
		request:      make(chan *Packet, 10240),
		reply:        make(chan *Packet, 1024),
		doneSender:   make(chan struct{}),
		doneReceiver: make(chan struct{}),
		stop:         make(chan struct{}),
		verUpdate:    make(chan uint64),
	}

	// allocateExtent will:
	// 1. Call GetDataPartitionForWrite -> returns a dp with unreachable host (127.0.0.1:1)
	// 2. Call createExtent -> connection refused (no "Again"/"LimitedIoErr" in error)
	// 3. Call RemoveDataPartitionForWrite(dp.PartitionID, eh.poolId) -> LINE 824 COVERED
	// 4. Loop retries until all dps exhausted or retry limit hit
	err := eh.allocateExtent()
	if err == nil {
		t.Fatal("expected allocateExtent to fail, but it succeeded")
	}
	t.Logf("allocateExtent failed as expected: %v", err)
}
