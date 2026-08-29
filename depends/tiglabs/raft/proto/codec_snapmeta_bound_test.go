package proto

import (
	"encoding/binary"
	"runtime"
	"testing"
)

// A 20 byte snapshot message declaring size=0xFFFFFFFF must not size the
// allocation from that count alone. Peer is 24 bytes in memory while occupying
// peer_size (11) bytes on the wire, so the count implies 96 GiB, and the decode
// loop then indexes datas out of range.
//
// The assertion is on allocated bytes, not on "does it return an error":
// a test that only checks for an error would still pass on the unfixed code
// once the out-of-range index panics, which is the wrong reason.
func TestSnapshotMetaDecodeBoundsPeerCount(t *testing.T) {
	datas := make([]byte, snapmeta_header)
	binary.BigEndian.PutUint64(datas[0:], 7)           // Index
	binary.BigEndian.PutUint64(datas[8:], 3)           // Term
	binary.BigEndian.PutUint32(datas[16:], ^uint32(0)) // size = 4294967295

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	m := &SnapshotMeta{}
	m.Decode(datas)

	runtime.ReadMemStats(&after)
	grew := after.TotalAlloc - before.TotalAlloc

	// 1 MiB is generous: the honest path allocates nothing here, because 20
	// bytes cannot carry a single 11 byte peer beyond the 20 byte header.
	if grew > 1<<20 {
		t.Fatalf("decoding a 20 byte message allocated %d bytes (%.2f MiB); "+
			"the peer count must be bounded by the buffer", grew, float64(grew)/(1<<20))
	}
	if len(m.Peers) != 0 {
		t.Fatalf("Peers = %d, want 0: a 20 byte message carries no peers", len(m.Peers))
	}
}

// A count the buffer really can supply must still decode, so the bound cannot be
// a blanket rejection.
func TestSnapshotMetaDecodeAcceptsGenuinePeers(t *testing.T) {
	const n = 3
	datas := make([]byte, snapmeta_header+n*peer_size)
	binary.BigEndian.PutUint64(datas[0:], 11)
	binary.BigEndian.PutUint64(datas[8:], 2)
	binary.BigEndian.PutUint32(datas[16:], n)
	// Give each peer a distinct ID so a silent truncation would be visible.
	for i := 0; i < n; i++ {
		off := snapmeta_header + uint64(i)*peer_size
		datas[off] = byte(1)
		binary.BigEndian.PutUint16(datas[off+1:], uint16(i))
		binary.BigEndian.PutUint64(datas[off+3:], uint64(100+i))
	}

	m := &SnapshotMeta{}
	m.Decode(datas)

	if len(m.Peers) != n {
		t.Fatalf("Peers = %d, want %d: a well-formed message must still decode", len(m.Peers), n)
	}
	for i := 0; i < n; i++ {
		if got := m.Peers[i].ID; got != uint64(100+i) {
			t.Errorf("Peers[%d].ID = %d, want %d", i, got, 100+i)
		}
	}
}

// Exactly-fitting and one-short counts sit on the boundary the fix computes.
func TestSnapshotMetaDecodeBoundaryIsExact(t *testing.T) {
	build := func(bufPeers int, declared uint32) []byte {
		d := make([]byte, snapmeta_header+uint64(bufPeers)*peer_size)
		binary.BigEndian.PutUint32(d[16:], declared)
		return d
	}
	// Room for exactly 2 peers, declaring 2: must decode.
	m := &SnapshotMeta{}
	m.Decode(build(2, 2))
	if len(m.Peers) != 2 {
		t.Errorf("exact fit: Peers = %d, want 2", len(m.Peers))
	}
	// Room for 2, declaring 3: must be refused rather than read past the end.
	m2 := &SnapshotMeta{}
	m2.Decode(build(2, 3))
	if len(m2.Peers) != 0 {
		t.Errorf("one too many: Peers = %d, want 0", len(m2.Peers))
	}
}

// A buffer shorter than the fixed header must not underflow the available-bytes
// subtraction. This is the case that turns a bound into a new bug if written as
// len(datas)-snapmeta_header on unsigned types.
func TestSnapshotMetaDecodeShortBufferDoesNotUnderflow(t *testing.T) {
	datas := make([]byte, snapmeta_header) // header only, no peers
	binary.BigEndian.PutUint32(datas[16:], 1)
	m := &SnapshotMeta{}
	m.Decode(datas)
	if len(m.Peers) != 0 {
		t.Fatalf("Peers = %d, want 0", len(m.Peers))
	}
}
