// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rdma

import (
	"encoding/binary"
	"fmt"
)

// MRLookupReplySize is the wire size of an MRLookupReply when packed
// into a packet's Arg field. Fixed-size so the client can parse it
// without a length prefix.
const MRLookupReplySize = 24

// MRLookupReply is the server's response to OpReadMRLookup. Encoded
// big-endian into the response packet's Arg field; the response
// packet's CRC field carries the read-side data CRC separately so
// the client can verify after the RDMA Read pulls the bytes.
//
// Layout:
//
//	offset 0:  Rkey       (uint32, 4 bytes)
//	offset 4:  PoolIndex  (uint32, 4 bytes)  // for the subsequent Release
//	offset 8:  VA         (uint64, 8 bytes)
//	offset 16: Length     (uint64, 8 bytes)  // bytes filled into the buffer
//
// Total = 24 bytes.
type MRLookupReply struct {
	Rkey      uint32
	PoolIndex uint32
	VA        uint64
	Length    uint64
}

// Marshal writes the reply into a 24-byte big-endian buffer. Returns
// an error if out is shorter than MRLookupReplySize.
func (r *MRLookupReply) Marshal(out []byte) error {
	if len(out) < MRLookupReplySize {
		return fmt.Errorf("rdma: MRLookupReply.Marshal: buffer %d < %d", len(out), MRLookupReplySize)
	}
	binary.BigEndian.PutUint32(out[0:4], r.Rkey)
	binary.BigEndian.PutUint32(out[4:8], r.PoolIndex)
	binary.BigEndian.PutUint64(out[8:16], r.VA)
	binary.BigEndian.PutUint64(out[16:24], r.Length)
	return nil
}

// Unmarshal reads the reply from a big-endian buffer.
func (r *MRLookupReply) Unmarshal(in []byte) error {
	if len(in) < MRLookupReplySize {
		return fmt.Errorf("rdma: MRLookupReply.Unmarshal: buffer %d < %d", len(in), MRLookupReplySize)
	}
	r.Rkey = binary.BigEndian.Uint32(in[0:4])
	r.PoolIndex = binary.BigEndian.Uint32(in[4:8])
	r.VA = binary.BigEndian.Uint64(in[8:16])
	r.Length = binary.BigEndian.Uint64(in[16:24])
	return nil
}

// MRReleaseArgSize is the wire size of an MRReleaseArg. Fixed at
// 4 bytes carrying the PoolIndex the server handed out in the
// matching MRLookupReply.
const MRReleaseArgSize = 4

// MRReleaseArg is the client's payload for OpReadMRRelease. Sent in
// the request packet's Arg field so the server can locate the
// MRBuffer in its pool without a per-conn lookup table.
type MRReleaseArg struct {
	PoolIndex uint32
}

// Marshal writes the arg into a 4-byte big-endian buffer.
func (r *MRReleaseArg) Marshal(out []byte) error {
	if len(out) < MRReleaseArgSize {
		return fmt.Errorf("rdma: MRReleaseArg.Marshal: buffer %d < %d", len(out), MRReleaseArgSize)
	}
	binary.BigEndian.PutUint32(out[0:4], r.PoolIndex)
	return nil
}

// Unmarshal reads the arg from a big-endian buffer.
func (r *MRReleaseArg) Unmarshal(in []byte) error {
	if len(in) < MRReleaseArgSize {
		return fmt.Errorf("rdma: MRReleaseArg.Unmarshal: buffer %d < %d", len(in), MRReleaseArgSize)
	}
	r.PoolIndex = binary.BigEndian.Uint32(in[0:4])
	return nil
}
