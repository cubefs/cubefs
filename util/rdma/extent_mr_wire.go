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

// Wire encodings for the persistent-MR (Phase A) protocol. All
// fixed-size big-endian payloads carried in proto.Packet.Arg. Pinned
// layouts so future field additions are explicit version bumps
// rather than silent breaks.

// ExtentMRLookupRequest is the client→server payload of
// OpExtentMRLookup. PartitionID + ExtentID identify the target
// extent; LeaseSecondsHint suggests how long the client wants the
// lease to last (server caps at its own maximum).
const ExtentMRLookupRequestSize = 20

type ExtentMRLookupRequest struct {
	PartitionID      uint64
	ExtentID         uint64
	LeaseSecondsHint uint32
}

func (r *ExtentMRLookupRequest) Marshal(out []byte) error {
	if len(out) < ExtentMRLookupRequestSize {
		return fmt.Errorf("rdma: ExtentMRLookupRequest.Marshal: buffer %d < %d", len(out), ExtentMRLookupRequestSize)
	}
	binary.BigEndian.PutUint64(out[0:8], r.PartitionID)
	binary.BigEndian.PutUint64(out[8:16], r.ExtentID)
	binary.BigEndian.PutUint32(out[16:20], r.LeaseSecondsHint)
	return nil
}

func (r *ExtentMRLookupRequest) Unmarshal(in []byte) error {
	if len(in) < ExtentMRLookupRequestSize {
		return fmt.Errorf("rdma: ExtentMRLookupRequest.Unmarshal: buffer %d < %d", len(in), ExtentMRLookupRequestSize)
	}
	r.PartitionID = binary.BigEndian.Uint64(in[0:8])
	r.ExtentID = binary.BigEndian.Uint64(in[8:16])
	r.LeaseSecondsHint = binary.BigEndian.Uint32(in[16:20])
	return nil
}

// ExtentMRLookupReply is the server→client payload of
// OpExtentMRLookup on success. Carries the MR credentials the
// client needs to issue an RDMA Read plus a LeaseID the client uses
// to renew / release.
//
// GrantedSeconds is the actual lease duration the server granted
// (≤ the client's hint). The client should issue OpExtentMRRenew
// before this expires to keep the lease alive; missing the renewal
// window means the next RDMA Read may fail at the NIC layer once
// the MR has been deregistered server-side.
const ExtentMRLookupReplySize = 40

type ExtentMRLookupReply struct {
	LeaseID        uint64
	Rkey           uint32
	GrantedSeconds uint32
	VA             uint64
	Size           uint64
	// 8 bytes reserved for future expansion (e.g., MR version /
	// generation) without breaking the fixed wire size.
	_reserved uint64
}

func (r *ExtentMRLookupReply) Marshal(out []byte) error {
	if len(out) < ExtentMRLookupReplySize {
		return fmt.Errorf("rdma: ExtentMRLookupReply.Marshal: buffer %d < %d", len(out), ExtentMRLookupReplySize)
	}
	binary.BigEndian.PutUint64(out[0:8], r.LeaseID)
	binary.BigEndian.PutUint32(out[8:12], r.Rkey)
	binary.BigEndian.PutUint32(out[12:16], r.GrantedSeconds)
	binary.BigEndian.PutUint64(out[16:24], r.VA)
	binary.BigEndian.PutUint64(out[24:32], r.Size)
	binary.BigEndian.PutUint64(out[32:40], r._reserved)
	return nil
}

func (r *ExtentMRLookupReply) Unmarshal(in []byte) error {
	if len(in) < ExtentMRLookupReplySize {
		return fmt.Errorf("rdma: ExtentMRLookupReply.Unmarshal: buffer %d < %d", len(in), ExtentMRLookupReplySize)
	}
	r.LeaseID = binary.BigEndian.Uint64(in[0:8])
	r.Rkey = binary.BigEndian.Uint32(in[8:12])
	r.GrantedSeconds = binary.BigEndian.Uint32(in[12:16])
	r.VA = binary.BigEndian.Uint64(in[16:24])
	r.Size = binary.BigEndian.Uint64(in[24:32])
	r._reserved = binary.BigEndian.Uint64(in[32:40])
	return nil
}

// ExtentMRRenewRequest is client→server for OpExtentMRRenew. The
// LeaseID was returned by a prior OpExtentMRLookup. LeaseSecondsHint
// is the desired new TTL (server still caps).
const ExtentMRRenewRequestSize = 12

type ExtentMRRenewRequest struct {
	LeaseID          uint64
	LeaseSecondsHint uint32
}

func (r *ExtentMRRenewRequest) Marshal(out []byte) error {
	if len(out) < ExtentMRRenewRequestSize {
		return fmt.Errorf("rdma: ExtentMRRenewRequest.Marshal: buffer %d < %d", len(out), ExtentMRRenewRequestSize)
	}
	binary.BigEndian.PutUint64(out[0:8], r.LeaseID)
	binary.BigEndian.PutUint32(out[8:12], r.LeaseSecondsHint)
	return nil
}

func (r *ExtentMRRenewRequest) Unmarshal(in []byte) error {
	if len(in) < ExtentMRRenewRequestSize {
		return fmt.Errorf("rdma: ExtentMRRenewRequest.Unmarshal: buffer %d < %d", len(in), ExtentMRRenewRequestSize)
	}
	r.LeaseID = binary.BigEndian.Uint64(in[0:8])
	r.LeaseSecondsHint = binary.BigEndian.Uint32(in[8:12])
	return nil
}

// ExtentMRRenewReply confirms the new TTL. On unknown / expired
// lease the server replies via the packet's ResultCode (OpNotExistErr
// or similar) and this struct is not serialized at all — clients see
// the error and fall back to a fresh lookup.
const ExtentMRRenewReplySize = 4

type ExtentMRRenewReply struct {
	GrantedSeconds uint32
}

func (r *ExtentMRRenewReply) Marshal(out []byte) error {
	if len(out) < ExtentMRRenewReplySize {
		return fmt.Errorf("rdma: ExtentMRRenewReply.Marshal: buffer %d < %d", len(out), ExtentMRRenewReplySize)
	}
	binary.BigEndian.PutUint32(out[0:4], r.GrantedSeconds)
	return nil
}

func (r *ExtentMRRenewReply) Unmarshal(in []byte) error {
	if len(in) < ExtentMRRenewReplySize {
		return fmt.Errorf("rdma: ExtentMRRenewReply.Unmarshal: buffer %d < %d", len(in), ExtentMRRenewReplySize)
	}
	r.GrantedSeconds = binary.BigEndian.Uint32(in[0:4])
	return nil
}
