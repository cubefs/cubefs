package meta

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strings"
	"testing"
	"time"
)

var (
	mw, _ = NewMetaWrapper(&MetaConfig{
		Volume:        ltptestVolume,
		Masters:       strings.Split(ltptestMaster, ","),
		ValidateOwner: true,
		Owner:         ltptestVolume,
	})
)

func Test_updateMetaPartitionsWithNoCache(t *testing.T) {
	require.NotNil(t, mw)
	if err := mw.updateMetaPartitionsWithNoCache(); err != nil {
		t.Fatalf("Test_updateMetaPartitionsWithNoCache: update err(%v)", err)
	}
	if len(mw.partitions) == 0 || len(mw.rwPartitions) == 0 || mw.ranges.Len() == 0 {
		t.Fatalf("Test_updateMetaPartitionsWithNoCache: no mp, mp count(%v) rwMP count(%v) btree count(%v)",
			len(mw.partitions), len(mw.rwPartitions), mw.ranges.Len())
	}
}

func Test_isCredibleMetaPartitionView(t *testing.T) {
	metaWrapper := new(MetaWrapper)
	metaWrapper.partitions = make(map[uint64]*MetaPartition)
	mp0 := &proto.MetaPartitionView{PartitionID: 1, Start: 1, End: 3}
	mp1 := &proto.MetaPartitionView{PartitionID: 2, Start: 4, End: 6}
	mp2 := &proto.MetaPartitionView{PartitionID: 2, Start: 2, End: 5}
	mp3 := &proto.MetaPartitionView{PartitionID: 3, Start: 7, End: 9}
	mp4 := &proto.MetaPartitionView{PartitionID: 4, Start: 8, End: 10}

	testCases := []struct {
		mps    []*proto.MetaPartitionView
		expect bool
	}{
		{
			mps:    []*proto.MetaPartitionView{mp0, mp1},
			expect: true,
		},
		{
			mps:    []*proto.MetaPartitionView{mp0, mp2},
			expect: false,
		},
		{
			mps:    []*proto.MetaPartitionView{mp1, mp2},
			expect: false,
		},
		{
			mps:    []*proto.MetaPartitionView{mp0, mp1, mp3},
			expect: true,
		},
		{
			mps:    []*proto.MetaPartitionView{mp0, mp1, mp2},
			expect: false,
		},
		{
			mps:    []*proto.MetaPartitionView{mp0, mp1, mp3, mp4},
			expect: false,
		},
	}

	for i, tt := range testCases {
		actual := metaWrapper.isCredibleMetaPartitionView(tt.mps)
		assert.Equal(t, tt.expect, actual, fmt.Sprintf("case: %v failed", i))
		if actual {
			updatePartitions(metaWrapper, tt.mps)
		}
	}
}

func updatePartitions(mw *MetaWrapper, mps []*proto.MetaPartitionView) {
	for _, view := range mps {
		mp := &MetaPartition{
			PartitionID: view.PartitionID,
			Start:       view.Start,
			End:         view.End,
		}
		mw.partitions[view.PartitionID] = mp
	}
}

func Benchmark_isCredibleMetaPartitionView(b *testing.B) {
	t := generateTestMps()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mw.isCredibleMetaPartitionView(t)
	}
	b.ReportAllocs()
}

func generateTestMps() []*proto.MetaPartitionView {
	length := 10000
	mps := make([]*proto.MetaPartitionView, length)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		mps[i] = &proto.MetaPartitionView{
			Start: r.Uint64(),
			End:   r.Uint64(),
		}
	}
	return mps
}
