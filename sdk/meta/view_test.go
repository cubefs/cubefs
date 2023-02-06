package meta

import (
	"strings"
	"testing"
)

func Test_updateMetaPartitionsWithNoCache(t *testing.T)  {
	mw, err := NewMetaWrapper(&MetaConfig{
		Volume:        ltptestVolume,
		Masters:       strings.Split(ltptestMaster, ","),
		ValidateOwner: true,
		Owner:         ltptestVolume,
	})
	if err != nil {
		t.Fatalf("Test_updateMetaPartitionsWithNoCache: new err(%v)", err)
	}
	if err = mw.updateMetaPartitionsWithNoCache(); err != nil {
		t.Fatalf("Test_updateMetaPartitionsWithNoCache: update err(%v)", err)
	}
	if len(mw.partitions) == 0 || len(mw.rwPartitions) == 0 || mw.ranges.Len() == 0 {
		t.Fatalf("Test_updateMetaPartitionsWithNoCache: no mp, mp count(%v) rwMP count(%v) btree count(%v)",
			len(mw.partitions), len(mw.rwPartitions), mw.ranges.Len())
	}
}