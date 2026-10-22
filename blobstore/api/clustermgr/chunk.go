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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package clustermgr

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

type ChunkInfo struct {
	Id         ChunkID      `json:"id"`
	Vuid       proto.Vuid   `json:"vuid"`
	DiskID     proto.DiskID `json:"diskid"`
	Total      uint64       `json:"total"`  // ChunkSize
	Used       uint64       `json:"used"`   // user data size
	Free       uint64       `json:"free"`   // ChunkSize - Used
	Size       uint64       `json:"size"`   // Chunk File Size (logic size)
	Status     ChunkStatus  `json:"status"` // normal, readonly
	Compacting bool         `json:"compacting"`
}

type ChunkReported struct {
	Vuid  proto.Vuid
	Total uint64
	Used  uint64
	Free  uint64
	Size  uint64
}

func (ci *ChunkInfo) Reported() ChunkReported {
	return ChunkReported{
		Vuid:  ci.Vuid,
		Total: ci.Total,
		Used:  ci.Used,
		Free:  ci.Free,
		Size:  ci.Size,
	}
}

const (
	ChunkStatusDefault  ChunkStatus = iota // 0
	ChunkStatusNormal                      // 1
	ChunkStatusReadOnly                    // 2
	ChunkStatusRelease                     // 3
	ChunkNumStatus                         // 4
)

const (
	ReleaseForUser    = "release for user"
	ReleaseForCompact = "release for compact"
)

// Chunk ID
// vuid + timestamp
const (
	chunkVuidLen      = 8
	chunkTimestmapLen = 8
	ChunkIDLength     = chunkVuidLen + chunkTimestmapLen
)

var InvalidChunkID ChunkID = [ChunkIDLength]byte{}

var (
	_vuidHexLen      = hex.EncodedLen(chunkVuidLen)
	_timestampHexLen = hex.EncodedLen(chunkTimestmapLen)

	// ${vuid_hex}-${tiemstamp_hex}
	// |-- 8 Bytes --|-- 1 Bytes --|-- 8 Bytes --|
	delimiter        = []byte("-")
	ChunkIDEncodeLen = _vuidHexLen + _timestampHexLen + len(delimiter)
)

type (
	ChunkID     [ChunkIDLength]byte
	ChunkStatus uint8
)

func (c ChunkID) UnixTime() uint64 {
	return binary.BigEndian.Uint64(c[chunkVuidLen:ChunkIDLength])
}

func (c ChunkID) VolumeUnitId() proto.Vuid {
	return proto.Vuid(binary.BigEndian.Uint64(c[:chunkVuidLen]))
}

func (c *ChunkID) Marshal() ([]byte, error) {
	buf := make([]byte, ChunkIDEncodeLen)
	var i int

	hex.Encode(buf[i:_vuidHexLen], c[:chunkVuidLen])
	i += _vuidHexLen

	copy(buf[i:i+len(delimiter)], delimiter)
	i += len(delimiter)

	hex.Encode(buf[i:], c[chunkVuidLen:ChunkIDLength])

	return buf, nil
}

func (c *ChunkID) Unmarshal(data []byte) error {
	if len(data) != ChunkIDEncodeLen {
		panic(errors.New("chunk buf size not match"))
	}

	var i int

	_, err := hex.Decode(c[:chunkVuidLen], data[i:_vuidHexLen])
	if err != nil {
		return err
	}

	i += _vuidHexLen
	i += len(delimiter)

	_, err = hex.Decode(c[chunkVuidLen:], data[i:])
	if err != nil {
		return err
	}

	return nil
}

func (c ChunkID) String() string {
	buf, _ := c.Marshal()
	return string(buf[:])
}

func (c ChunkID) MarshalJSON() ([]byte, error) {
	b := make([]byte, ChunkIDEncodeLen+2)
	b[0], b[ChunkIDEncodeLen+1] = '"', '"'

	buf, _ := c.Marshal()
	copy(b[1:], buf)

	return b, nil
}

func (c *ChunkID) UnmarshalJSON(data []byte) (err error) {
	if len(data) != ChunkIDEncodeLen+2 {
		return errors.New("failed unmarshal json")
	}

	return c.Unmarshal(data[1 : ChunkIDEncodeLen+1])
}

func EncodeChunk(id ChunkID) string {
	return id.String()
}

func NewChunkID(vuid proto.Vuid) (chunkId ChunkID) {
	binary.BigEndian.PutUint64(chunkId[:chunkVuidLen], uint64(vuid))
	binary.BigEndian.PutUint64(chunkId[chunkVuidLen:ChunkIDLength], uint64(time.Now().UnixNano()))
	return
}

func DecodeChunk(name string) (id ChunkID, err error) {
	buf := []byte(name)
	if len(buf) != ChunkIDEncodeLen {
		return InvalidChunkID, errors.New("invalid chunk name")
	}

	if err = id.Unmarshal(buf); err != nil {
		return InvalidChunkID, errors.New("chunk unmarshal failed")
	}

	return
}

func (s *ChunkStatus) String() string {
	switch *s {
	case ChunkStatusDefault:
		return "default"
	case ChunkStatusNormal:
		return "normal"
	case ChunkStatusReadOnly:
		return "readOnly"
	case ChunkStatusRelease:
		return "release"
	default:
		return "unkown"
	}
}

const (
	// magic = base + version. base is above the legacy count limit (1<<20).
	chunkReportMagicBase   uint32 = 0xfffefff9
	chunkReportVersionSpan uint32 = 1 << 16
	chunkReportHeaderSize         = 8
	maxChunkReportCount           = 1 << 20

	chunkReportVersion1     uint32 = 1
	chunkReportV1RecordSize        = 8 + 8 + 8 + 8 + 8 // vuid, total, used, free, size
)

func chunkReportVersion(magic uint32) (uint32, bool) {
	if magic <= chunkReportMagicBase {
		return 0, false
	}
	ver := magic - chunkReportMagicBase
	if ver >= chunkReportVersionSpan {
		return 0, false
	}
	return ver, true
}

type ReportChunkArgs struct {
	ChunkInfos []ChunkInfo `json:"chunk_infos"`
}

func (r *ReportChunkArgs) Size() int {
	return r.chunkReportSizeV1()
}

func (r *ReportChunkArgs) Encode(buf []byte) error {
	return r.encodeChunkReportV1(buf)
}

func (r *ReportChunkArgs) chunkReportSizeV1() int {
	return chunkReportHeaderSize + len(r.ChunkInfos)*chunkReportV1RecordSize
}

func (r *ReportChunkArgs) encodeChunkReportV1(buf []byte) error {
	count, err := r.chunkReportCount()
	if err != nil {
		return err
	}
	if len(buf) < r.chunkReportSizeV1() {
		return fmt.Errorf("chunk_report: buffer too short %d", len(buf))
	}
	binary.BigEndian.PutUint32(buf[0:4], chunkReportMagicBase+chunkReportVersion1)
	binary.BigEndian.PutUint32(buf[4:8], count)
	off := chunkReportHeaderSize
	for i := range r.ChunkInfos {
		binary.BigEndian.PutUint64(buf[off:], uint64(r.ChunkInfos[i].Vuid))
		binary.BigEndian.PutUint64(buf[off+8:], r.ChunkInfos[i].Total)
		binary.BigEndian.PutUint64(buf[off+16:], r.ChunkInfos[i].Used)
		binary.BigEndian.PutUint64(buf[off+24:], r.ChunkInfos[i].Free)
		binary.BigEndian.PutUint64(buf[off+32:], r.ChunkInfos[i].Size)
		off += chunkReportV1RecordSize
	}
	return nil
}

func (r *ReportChunkArgs) legacyChunkReportSize() int {
	return chunkReportLegacySize(len(r.ChunkInfos))
}

func chunkReportLegacySize(count int) int {
	const recordSize = 8 + 4 + 8 + 8 + 8 + 8 + 1 + 1 // vuid, diskID, total, used, free, size, status, compacting
	return 4 + count*(ChunkIDEncodeLen+recordSize)
}

func (r *ReportChunkArgs) encodeLegacyChunkReport(buf []byte) error {
	count, err := r.chunkReportCount()
	if err != nil {
		return err
	}
	if len(buf) < r.legacyChunkReportSize() {
		return fmt.Errorf("chunk_report: buffer too short %d", len(buf))
	}
	binary.BigEndian.PutUint32(buf[0:4], count)
	off := 4
	for i := range r.ChunkInfos {
		info := &r.ChunkInfos[i]
		raw, err := info.Id.Marshal()
		if err != nil {
			return err
		}
		off += copy(buf[off:], raw)
		binary.BigEndian.PutUint64(buf[off:], uint64(info.Vuid))
		off += 8
		binary.BigEndian.PutUint32(buf[off:], uint32(info.DiskID))
		off += 4
		binary.BigEndian.PutUint64(buf[off:], info.Total)
		off += 8
		binary.BigEndian.PutUint64(buf[off:], info.Used)
		off += 8
		binary.BigEndian.PutUint64(buf[off:], info.Free)
		off += 8
		binary.BigEndian.PutUint64(buf[off:], info.Size)
		off += 8
		buf[off] = byte(info.Status)
		off++
		if info.Compacting {
			buf[off] = 1
		} else {
			buf[off] = 0
		}
		off++
	}
	return nil
}

func (r *ReportChunkArgs) chunkReportCount() (uint32, error) {
	count := uint32(len(r.ChunkInfos))
	if count == 0 {
		return 0, errors.New("chunk_report: chunk infos is empty")
	}
	if count > maxChunkReportCount {
		return 0, fmt.Errorf("chunk_report: chunks is too much %d, limit %d", count, maxChunkReportCount)
	}
	return count, nil
}

func (r *ReportChunkArgs) Decode(buf []byte) error {
	if len(buf) < 4 {
		return errors.New("chunk_report: buffer too short")
	}
	magic := binary.BigEndian.Uint32(buf[:4])
	if ver, ok := chunkReportVersion(magic); ok {
		switch ver {
		case chunkReportVersion1:
			return r.decodeChunkReportV1(buf)
		default:
			return fmt.Errorf("chunk_report: unsupported version %d", ver)
		}
	}
	return r.decodeLegacyChunkReport(buf)
}

func (r *ReportChunkArgs) decodeChunkReportV1(buf []byte) error {
	if len(buf) < chunkReportHeaderSize {
		return errors.New("chunk_report: buffer too short")
	}
	count := binary.BigEndian.Uint32(buf[4:8])
	if count == 0 {
		return errors.New("chunk_report: chunk infos is empty")
	}
	if count > maxChunkReportCount {
		return fmt.Errorf("chunk_report: chunks is too much %d, limit %d", count, maxChunkReportCount)
	}
	expect := chunkReportHeaderSize + int(count)*chunkReportV1RecordSize
	if len(buf) != expect {
		return fmt.Errorf("chunk_report: length mismatch, got %d expect %d", len(buf), expect)
	}
	r.ChunkInfos = make([]ChunkInfo, count)
	off := chunkReportHeaderSize
	for i := range r.ChunkInfos {
		r.ChunkInfos[i].Vuid = proto.Vuid(binary.BigEndian.Uint64(buf[off:]))
		r.ChunkInfos[i].Total = binary.BigEndian.Uint64(buf[off+8:])
		r.ChunkInfos[i].Used = binary.BigEndian.Uint64(buf[off+16:])
		r.ChunkInfos[i].Free = binary.BigEndian.Uint64(buf[off+24:])
		r.ChunkInfos[i].Size = binary.BigEndian.Uint64(buf[off+32:])
		off += chunkReportV1RecordSize
	}
	return nil
}

func (r *ReportChunkArgs) decodeLegacyChunkReport(buf []byte) error {
	count := binary.BigEndian.Uint32(buf[:4])
	if count > maxChunkReportCount {
		return fmt.Errorf("chunk_report: chunks is too much %d, limit %d", count, maxChunkReportCount)
	}
	expect := chunkReportLegacySize(int(count))
	if len(buf) < expect {
		return fmt.Errorf("chunk_report: length mismatch, got %d expect %d", len(buf), expect)
	}
	r.ChunkInfos = make([]ChunkInfo, count)
	off := 4
	for i := range r.ChunkInfos {
		if err := r.ChunkInfos[i].Id.Unmarshal(buf[off : off+ChunkIDEncodeLen]); err != nil {
			return err
		}
		off += ChunkIDEncodeLen
		r.ChunkInfos[i].Vuid = proto.Vuid(binary.BigEndian.Uint64(buf[off:]))
		off += 8
		r.ChunkInfos[i].DiskID = proto.DiskID(binary.BigEndian.Uint32(buf[off:]))
		off += 4
		r.ChunkInfos[i].Total = binary.BigEndian.Uint64(buf[off:])
		off += 8
		r.ChunkInfos[i].Used = binary.BigEndian.Uint64(buf[off:])
		off += 8
		r.ChunkInfos[i].Free = binary.BigEndian.Uint64(buf[off:])
		off += 8
		r.ChunkInfos[i].Size = binary.BigEndian.Uint64(buf[off:])
		off += 8
		r.ChunkInfos[i].Status = ChunkStatus(buf[off])
		off++
		r.ChunkInfos[i].Compacting = buf[off] != 0
		off++
	}
	return nil
}

func (c *Client) ReportChunk(ctx context.Context, args *ReportChunkArgs) (err error) {
	b := bytespool.Alloc(args.Size())
	defer bytespool.Free(b)
	if err = args.Encode(b); err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, "/chunk/report", bytes.NewReader(b))
	if err != nil {
		return err
	}
	request.ContentLength = int64(len(b))
	request.Header.Set(rpc.HeaderContentType, rpc.MIMEStream)
	resp, err := c.Do(ctx, request)
	if err != nil {
		return
	}
	if resp.StatusCode/100 != 2 {
		err = rpc.ParseResponseErr(resp)
	}
	resp.Body.Close()
	return
}

type SetCompactChunkArgs struct {
	Vuid       proto.Vuid `json:"vuid"`
	Compacting bool       `json:"compact"`
}

func (c *Client) SetCompactChunk(ctx context.Context, args *SetCompactChunkArgs) (err error) {
	err = c.PostWith(ctx, "/chunk/set/compact", nil, args)
	return
}
