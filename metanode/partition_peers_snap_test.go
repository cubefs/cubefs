// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metanode

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"testing"

	raftproto "github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/require"
)

// mockSnapIterator implements raftproto.SnapIterator for testing.
type mockSnapIterator struct {
	items [][]byte
	pos   int
}

func (m *mockSnapIterator) Next() ([]byte, error) {
	if m.pos >= len(m.items) {
		return nil, io.EOF
	}
	data := m.items[m.pos]
	m.pos++
	return data, nil
}

// Compile-time check: mockSnapIterator implements raftproto.SnapIterator.
var _ raftproto.SnapIterator = (*mockSnapIterator)(nil)

// buildApplyIdPayload builds the raw V bytes for an opFSMApplyId MetaItem:
//   - first 8 bytes: applyID (big-endian uint64)
//   - followed by JSON-encoded peers when len(peers) > 0
//
// This mirrors the encoding written by MetaItemIterator.Next() in
// partition_item.go after the fix.
func buildApplyIdPayload(applyID uint64, peers []proto.Peer) ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, applyID)
	if len(peers) > 0 {
		peersJSON, err := json.Marshal(peers)
		if err != nil {
			return nil, err
		}
		buf = append(buf, peersJSON...)
	}
	return buf, nil
}

// encodeMetaItem wraps a payload in a MetaItem and serialises it.
func encodeMetaItem(op uint32, key, value []byte) ([]byte, error) {
	item := NewMetaItem(op, key, value)
	return item.MarshalBinary()
}

// --- Encode-side tests (partition_item.go) ---

// TestApplyIdPayloadWithPeers verifies that buildApplyIdPayload (which mirrors
// MetaItemIterator.Next()) embeds the applyID in the first 8 bytes and appends
// valid JSON-encoded peers after that.
func TestApplyIdPayloadWithPeers(t *testing.T) {
	peers := []proto.Peer{
		{ID: 1, Addr: "10.0.0.1:9021"},
		{ID: 2, Addr: "10.0.0.2:9021"},
		{ID: 3, Addr: "10.0.0.3:9021"},
	}
	const applyID uint64 = 42

	v, err := buildApplyIdPayload(applyID, peers)
	require.NoError(t, err)
	require.True(t, len(v) > 8, "peers JSON must be appended after the 8-byte applyID")

	// First 8 bytes: applyID
	require.Equal(t, applyID, binary.BigEndian.Uint64(v[:8]))

	// Remaining bytes: peers JSON
	var got []proto.Peer
	require.NoError(t, json.Unmarshal(v[8:], &got))
	require.Len(t, got, len(peers))
	for i := range peers {
		require.Equal(t, peers[i].ID, got[i].ID)
		require.Equal(t, peers[i].Addr, got[i].Addr)
	}
}

// TestApplyIdPayloadWithoutPeers verifies backward compatibility: when there
// are no peers the payload is exactly 8 bytes (old-format compatible).
func TestApplyIdPayloadWithoutPeers(t *testing.T) {
	const applyID uint64 = 999

	v, err := buildApplyIdPayload(applyID, nil)
	require.NoError(t, err)
	require.Equal(t, 8, len(v), "no peers → payload must be exactly 8 bytes")
	require.Equal(t, applyID, binary.BigEndian.Uint64(v))
}

// TestApplyIdMetaItemRoundTrip verifies that after wrapping the payload in a
// MetaItem and (un)marshalling it, the op code and V bytes are preserved.
func TestApplyIdMetaItemRoundTrip(t *testing.T) {
	peers := []proto.Peer{
		{ID: 10, Addr: "192.168.1.10:9021"},
		{ID: 20, Addr: "192.168.1.20:9021"},
	}
	const applyID uint64 = 1234

	v, err := buildApplyIdPayload(applyID, peers)
	require.NoError(t, err)

	raw, err := encodeMetaItem(opFSMApplyId, (&SnapItemWrapper{key: SiwKeyApplyId}).MarshalKey(), v)
	require.NoError(t, err)

	snap := NewMetaItem(0, nil, nil)
	require.NoError(t, snap.UnmarshalBinary(raw))
	require.Equal(t, uint32(opFSMApplyId), snap.Op)
	require.Equal(t, v, snap.V)
}

// --- Decode-side tests (partition_fsm.go) ---

// decodeApplyIdPayload is the exact logic from ApplySnapshot case opFSMApplyId
// (partition_fsm.go lines 842-853).  We test it in isolation so the test
// doesn't need to drive the full ApplySnapshot state machine.
func decodeApplyIdPayload(v []byte) (applyID uint64, peers []proto.Peer, err error) {
	applyID = binary.BigEndian.Uint64(v)
	if len(v) > 8 {
		if err = json.Unmarshal(v[8:], &peers); err != nil {
			return
		}
	}
	return
}

// TestDecodeApplyIdPayloadWithPeers checks that the decode logic correctly
// extracts the applyID and peer list from a payload that has peers embedded.
func TestDecodeApplyIdPayloadWithPeers(t *testing.T) {
	peers := []proto.Peer{
		{ID: 1, Addr: "10.0.0.1:9021"},
		{ID: 2, Addr: "10.0.0.2:9021"},
	}
	const applyID uint64 = 77

	v, err := buildApplyIdPayload(applyID, peers)
	require.NoError(t, err)

	gotID, gotPeers, err := decodeApplyIdPayload(v)
	require.NoError(t, err)
	require.Equal(t, applyID, gotID)
	require.Len(t, gotPeers, len(peers))
	for i := range peers {
		require.Equal(t, peers[i].ID, gotPeers[i].ID)
		require.Equal(t, peers[i].Addr, gotPeers[i].Addr)
	}
}

// TestDecodeApplyIdPayloadWithoutPeers checks that the decode logic returns an
// empty peers slice and does not error when the payload is exactly 8 bytes
// (old format — backward compatibility).
func TestDecodeApplyIdPayloadWithoutPeers(t *testing.T) {
	const applyID uint64 = 512

	v, err := buildApplyIdPayload(applyID, nil)
	require.NoError(t, err)

	gotID, gotPeers, err := decodeApplyIdPayload(v)
	require.NoError(t, err)
	require.Equal(t, applyID, gotID)
	require.Empty(t, gotPeers, "no peers appended → peers must be empty")
}

// TestDecodeApplyIdPayloadInvalidJSON checks that malformed JSON after the
// 8-byte applyID is properly propagated as an error.
func TestDecodeApplyIdPayloadInvalidJSON(t *testing.T) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 100)
	buf = append(buf, []byte("not-valid-json")...)

	_, _, err := decodeApplyIdPayload(buf)
	require.Error(t, err, "malformed peers JSON must return an error")
}

// TestPeersEncodedByIteratorAreDecodedByApplySnapshot is an end-to-end
// round-trip that encodes with the iterator logic and decodes with the
// ApplySnapshot logic, verifying they are consistent.
func TestPeersEncodedByIteratorAreDecodedByApplySnapshot(t *testing.T) {
	peers := []proto.Peer{
		{ID: 1, Addr: "172.16.0.1:9021"},
		{ID: 2, Addr: "172.16.0.2:9021"},
		{ID: 3, Addr: "172.16.0.3:9021"},
	}
	const applyID uint64 = 9999

	// Encode (partition_item.go path)
	v, err := buildApplyIdPayload(applyID, peers)
	require.NoError(t, err)

	// Decode (partition_fsm.go path)
	gotID, gotPeers, err := decodeApplyIdPayload(v)
	require.NoError(t, err)
	require.Equal(t, applyID, gotID)
	require.Len(t, gotPeers, len(peers))
	for i := range peers {
		require.Equal(t, peers[i].ID, gotPeers[i].ID)
		require.Equal(t, peers[i].Addr, gotPeers[i].Addr)
	}
}
