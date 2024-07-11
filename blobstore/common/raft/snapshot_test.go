package raft

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type MockIncomingSnapshotStorage struct{}

func (m *MockIncomingSnapshotStorage) NewBatch() Batch {
	return &mockBatch{}
}

// mockSnapshot mocks Snapshot interface for testing purposes.
type mockSnapshot struct {
	Sequence uint64
}

// ReadBatch mock implementation for Snapshot interface method.
func (ms *mockSnapshot) ReadBatch() (Batch, error) {
	if ms.Sequence < 3 {
		ms.Sequence++
		return &mockBatch{}, nil
	}
	return nil, io.EOF
}

// Term mock implementation for Snapshot interface method.
func (ms *mockSnapshot) Term() uint64 {
	return 1
}

// Index mock implementation for Snapshot interface method.
func (ms *mockSnapshot) Index() uint64 {
	return 10
}

// Close mock implementation for Snapshot interface method.
func (ms *mockSnapshot) Close() error {
	return nil
}

// mockBatch mocks Batch interface for testing purposes.
type mockBatch struct{ data []byte }

// Put mock implementation for Batch interface method.
func (mb *mockBatch) Put(key, value []byte) {}

// Delete mock implementation for Batch interface method.
func (mb *mockBatch) Delete(key []byte) {}

// DeleteRange mock implementation for Batch interface method.
func (mb *mockBatch) DeleteRange(start []byte, end []byte) {}

// data mock implementation for Batch interface method.
func (mb *mockBatch) Data() []byte { return mb.data }

// From mock implementation for Batch interface method.
func (mb *mockBatch) From(data []byte) {}

// Close mock implementation for Batch interface method.
func (mb *mockBatch) Close() {}

// MockOutgoingSnapshotStream mocks outgoingSnapshotStream interface for testing purposes.
type MockOutgoingSnapshotStream struct {
	times         int
	mockCheckFunc func(request *RaftSnapshotRequest) error
}

// Send mock implementation for outgoingSnapshotStream interface method.
func (mos *MockOutgoingSnapshotStream) Send(request *RaftSnapshotRequest) error {
	if mos.mockCheckFunc == nil {
		return nil
	}

	return mos.mockCheckFunc(request)
}

// Recv mock implementation for outgoingSnapshotStream interface method.
func (mos *MockOutgoingSnapshotStream) Recv() (*RaftSnapshotResponse, error) {
	defer func() { mos.times++ }()

	switch mos.times {
	case 0:
		return &RaftSnapshotResponse{Status: RaftSnapshotResponse_ACCEPTED}, nil
	case 1:
		return &RaftSnapshotResponse{Status: RaftSnapshotResponse_APPLIED}, nil
	}

	return &RaftSnapshotResponse{Status: RaftSnapshotResponse_ACCEPTED}, nil
}

// Replay the Recv behavior
func mockSnapshotResponseStreamRecv() (*RaftSnapshotRequest, error) {
	snapshotBatch := &mockBatch{data: []byte("unittestbatchdata")}
	snapshot := &RaftSnapshotRequest{
		Header: &RaftSnapshotHeader{
			ID:       "snapshot-test",
			Type:     RaftSnapshotHeader_BALANCE,
			Strategy: RaftSnapshotHeader_KV_BATCH,
		},
		Seq: 0,
	}
	snapshot.Data = snapshotBatch.Data()
	return snapshot, nil
}

// MockSnapshotResponseStream mocks SnapshotResponseStream interface for testing purposes.
type MockSnapshotResponseStream struct {
	times int
}

// Context mock implementation for SnapshotResponseStream interface method.
func (msrs *MockSnapshotResponseStream) Context() context.Context { return nil }

// Send mock implementation for SnapshotResponseStream interface method.
func (msrs *MockSnapshotResponseStream) Send(response *RaftSnapshotResponse) error { return nil }

// Recv mock implementation for SnapshotResponseStream interface method.
func (msrs *MockSnapshotResponseStream) Recv() (*RaftSnapshotRequest, error) {
	defer func() { msrs.times++ }()

	switch msrs.times {
	case 0:
		return &RaftSnapshotRequest{Final: true}, io.EOF
	case 1:
		return mockSnapshotResponseStreamRecv()
	case 2:
		return &RaftSnapshotRequest{Seq: 2}, nil
	}

	return nil, nil
}

// TestIncomingSnapshot_ReadBatch tests the incomingSnapshot.ReadBatch() method.
/*func TestIncomingSnapshot_ReadBatch(t *testing.T) {
	// Mock data
	snapshotHeader := &RaftSnapshotHeader{ID: "snapshot-test", Type: RaftSnapshotHeader_BALANCE}
	mockSnapshotResponseStream := &MockSnapshotResponseStream{}

	// Test successful EOF ReadBatch call
	incomingSnapshot := newIncomingSnapshot(snapshotHeader, &MockIncomingSnapshotStorage{}, mockSnapshotResponseStream)
	batch, err := incomingSnapshot.ReadBatch()
	require.Nil(t, batch)
	require.Equal(t, io.EOF, err)

	// Test successful ReadBatch call with available Batch
	batch, err = incomingSnapshot.ReadBatch()
	require.NotNil(t, batch.(*mockBatch))
	require.NoError(t, err)

	// Test unexpected sequence number error
	batch, err = incomingSnapshot.ReadBatch()
	require.Nil(t, batch)
	require.Error(t, err)
}*/

// TestSnapshotRecorder_Set tests the snapshotRecorder.Set() method.
func TestSnapshotRecorder_Set(t *testing.T) {
	// Mock data
	snapshotHeader := &RaftSnapshotHeader{ID: "snapshot-test", Type: RaftSnapshotHeader_BALANCE}
	mockSnapshot := &mockSnapshot{}
	snapshotRecorder := newSnapshotRecorder(1, time.Second)
	outgoingSnapshot := newOutgoingSnapshot(snapshotHeader.ID, mockSnapshot)

	// Test adding snapshot to empty recorder
	err := snapshotRecorder.Set(outgoingSnapshot)
	require.NoError(t, err)

	// Test adding snapshot with maximum limit reached
	err = snapshotRecorder.Set(outgoingSnapshot)
	require.Error(t, err)
}

// TestSnapshotRecorder_Get tests the snapshotRecorder.Get() method.
func TestSnapshotRecorder_Get(t *testing.T) {
	// Mock data
	snapshotHeader := &RaftSnapshotHeader{ID: "snapshot-test", Type: RaftSnapshotHeader_BALANCE}
	mockSnapshot := &mockSnapshot{}
	snapshotRecorder := newSnapshotRecorder(1, time.Second)

	// Test getting existing snapshot
	outgoingSnapshot := newOutgoingSnapshot(snapshotHeader.ID, mockSnapshot)
	snapshotRecorder.Set(outgoingSnapshot)
	result := snapshotRecorder.Get("snapshot-test")
	require.NotNil(t, result)

	// Test getting non-existing snapshot
	result = snapshotRecorder.Get("non-existing-snapshot")
	require.Nil(t, result)
}

// TestSnapshotRecorder_Delete tests the snapshotRecorder.Delete() method.
func TestSnapshotRecorder_Delete(t *testing.T) {
	// Mock data
	snapshotHeader := &RaftSnapshotHeader{ID: "snapshot-test", Type: RaftSnapshotHeader_BALANCE}
	mockSnapshot := &mockSnapshot{}
	snapshotRecorder := newSnapshotRecorder(1, time.Second)
	outgoingSnapshot := newOutgoingSnapshot(snapshotHeader.ID, mockSnapshot)
	snapshotRecorder.Set(outgoingSnapshot)

	// Test deleting from recorder
	snapshotRecorder.Delete("snapshot-test")
	result := snapshotRecorder.Get("snapshot-test")
	require.Nil(t, result)
}

// TestSnapshotRecorder_Close tests the snapshotRecorder.Close() method.
func TestSnapshotRecorder_Close(t *testing.T) {
	// Mock data
	snapshotHeader := &RaftSnapshotHeader{ID: "snapshot-test", Type: RaftSnapshotHeader_BALANCE}
	mockSnapshot := &mockSnapshot{}
	snapshotRecorder := newSnapshotRecorder(1, time.Second)
	outgoingSnapshot := newOutgoingSnapshot(snapshotHeader.ID, mockSnapshot)
	snapshotRecorder.Set(outgoingSnapshot)

	// Test closing snapshots
	snapshotRecorder.Close()
	result := snapshotRecorder.Get("snapshot-test")
	require.Nil(t, result)
}

// TestBatch_Data tests the Batch.Data() method.
func TestBatch_Data(t *testing.T) {
	// Mock data
	mockBatch := &mockBatch{data: []byte("unittestbatchdata")}

	// Test data retrieval
	result := mockBatch.Data()
	require.True(t, bytes.Equal([]byte("unittestbatchdata"), result))
}
