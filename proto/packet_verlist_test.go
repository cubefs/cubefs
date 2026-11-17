// Copyright 2026 The CubeFS Authors.
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

package proto

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
)

// buildVersionListPacket returns the bytes a peer would send for a packet carrying
// a version list of cnt entries: a header with VersionListFlag set, the uint16
// count, then cnt entries of verInfoCnt bytes each.
func buildVersionListPacket(cnt uint16) []byte {
	header := make([]byte, util.PacketHeaderSize)
	header[0] = ProtoMagic
	header[1] = VersionListFlag // ExtentType; no MultiVersion/ProtocolVersion extras

	body := make([]byte, 0, 2+int(cnt)*verInfoCnt)
	body = binary.BigEndian.AppendUint16(body, cnt)
	for i := 0; i < int(cnt); i++ {
		entry := make([]byte, verInfoCnt)
		binary.BigEndian.PutUint64(entry[0:8], uint64(i+1)) // Ver
		binary.BigEndian.PutUint64(entry[8:16], 0)          // DelTime
		entry[16] = 0                                       // Status
		body = append(body, entry...)
	}
	return append(header, body...)
}

// TestReadFromConnWithVerCount drives ReadFromConnWithVer over a real conn with
// version-list counts either side of the uint16 boundary.
//
// verInfoCnt is 17, so a count above 3855 overflows uint16 arithmetic when it is
// used to size the buffer: 3856*17 is 65552, which wraps to 16. The packet is then
// under-read and rejected, so a peer cannot send a version list longer than 3855
// entries at all.
//
// The assertion is that the packet parses and yields the entries that were sent.
// It fails on the unwidened multiplication, where the short buffer makes
// UnmarshalVersionSlice return EOF.
func TestReadFromConnWithVerCount(t *testing.T) {
	InitBufferPool(32 * 1024)

	for _, cnt := range []uint16{0, 1, 100, 3855, 3856, 5000} {
		t.Run("", func(t *testing.T) {
			wire := buildVersionListPacket(cnt)

			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()

			go func() {
				_, _ = client.Write(wire)
			}()

			p := &Packet{}
			err := p.ReadFromConnWithVer(server, NoReadDeadlineTime)
			require.NoError(t, err, "cnt=%d must parse", cnt)
			require.Len(t, p.VerList, int(cnt), "cnt=%d", cnt)
			for i, v := range p.VerList {
				require.Equal(t, uint64(i+1), v.Ver, "cnt=%d entry %d", cnt, i)
			}
		})
	}
}

// TestVersionListBufferSizeDoesNotWrap states the arithmetic directly, so the
// boundary is documented even if the wire format changes.
func TestVersionListBufferSizeDoesNotWrap(t *testing.T) {
	for _, cnt := range []uint16{3856, 30000, 65535} {
		widened := int(cnt) * verInfoCnt
		wrapped := int(cnt * verInfoCnt) // what uint16 arithmetic yields
		require.Greater(t, widened, wrapped,
			"cnt=%d: uint16 arithmetic wraps here, which is the case under test", cnt)
		require.Equal(t, int(cnt)*17, widened, "cnt=%d", cnt)
	}
}
