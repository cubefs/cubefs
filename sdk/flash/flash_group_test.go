package flash

import (
	"context"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/util/unit"
	"math/rand"
	"testing"
	"time"
)

var (
	testRemoteCache = new(RemoteCache)
	ctx             = context.Background()
)

func Test_getFlashHost(t *testing.T) {
	rankedHosts1 := make(map[ZoneRankType][]string, 0)
	rankedHosts1[SameZoneRank] = append(rankedHosts1[SameZoneRank], "1.1.1.1")
	rankedHosts1[UnknownZoneRank] = append(rankedHosts1[UnknownZoneRank], "3.3.3.3")

	rankedHosts2 := make(map[ZoneRankType][]string, 0)
	rankedHosts2[SameRegionRank] = append(rankedHosts2[SameRegionRank], "2.2.2.2")
	rankedHosts2[UnknownZoneRank] = append(rankedHosts2[UnknownZoneRank], "3.3.3.3")

	rankedHosts3 := make(map[ZoneRankType][]string, 0)
	rankedHosts3[UnknownZoneRank] = append(rankedHosts3[UnknownZoneRank], "3.3.3.3")

	testRemoteCache.sameZoneWeight = sameZoneWeight

	fgInfo1 := &proto.FlashGroupInfo{}
	fg1 := NewFlashGroup(fgInfo1, rankedHosts1)
	fg2 := NewFlashGroup(fgInfo1, rankedHosts2)
	fg3 := NewFlashGroup(fgInfo1, rankedHosts3)

	testCases := []struct {
		fg   *FlashGroup
		want string
	}{
		{
			fg:   fg1,
			want: "1.1.1.1",
		},
		{
			fg:   fg2,
			want: "2.2.2.2",
		},
		{
			fg:   fg3,
			want: "",
		},
	}

	for i, tt := range testCases {
		got := tt.fg.getFlashHost()
		if got != tt.want {
			t.Errorf("testCase(%d) failed: got(%v) want(%v)", i, got, tt.want)
		}
	}
}

type testSlot struct {
	volume          string
	inode           uint64
	fixedFileOffset uint64
}

func BenchmarkPbMarshal(b *testing.B) {
	data := make([]byte, 4*proto.CACHE_BLOCK_SIZE, 4*proto.CACHE_BLOCK_SIZE)
	source := &proto.DataSource{}
	cReadReq1 := &proto.CacheReadRequest{
		CacheRequest: &proto.CacheRequest{FixedFileOffset: proto.CACHE_BLOCK_SIZE, Sources: []*proto.DataSource{source}},
		Offset:       0,
		Size_:        4 * unit.MB,
		Data:         data[proto.CACHE_BLOCK_SIZE : 2*proto.CACHE_BLOCK_SIZE],
	}
	var testF = func() {
		packet := common.NewCachePacket(ctx, 1, proto.OpCacheRead)
		packet.MarshalDataPb(cReadReq1)
	}
	for i := 0; i < b.N; i++ {
		testF()
	}
}

func BenchmarkComputeCacheBlockSlot(b *testing.B) {
	t := generator()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ComputeCacheBlockSlot(t.volume, t.inode, t.fixedFileOffset)
	}
	b.ReportAllocs()
}

func generator() *testSlot {
	var r = rand.New(rand.NewSource(time.Now().UnixNano()))
	return &testSlot{
		volume:          "tone-client",
		inode:           r.Uint64(),
		fixedFileOffset: r.Uint64(),
	}
}