package gorocksdb

import (
	"bytes"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestCompactionFilter(t *testing.T) {
	var (
		changeKey    = []byte("change")
		changeValOld = []byte("old")
		changeValNew = []byte("new")
		deleteKey    = []byte("delete")
	)
	db := newTestDB(t, "TestCompactionFilter", func(opts *Options) {
		opts.SetCompactionFilter(&mockCompactionFilter{
			filter: func(level int, key, val []byte) (remove bool, newVal []byte) {
				if bytes.Equal(key, changeKey) {
					return false, changeValNew
				}
				if bytes.Equal(key, deleteKey) {
					return true, val
				}
				t.Errorf("key %q not expected during compaction", key)
				return false, nil
			},
		})
	})
	defer db.Close()

	// insert the test keys
	wo := NewDefaultWriteOptions()
	ensure.Nil(t, db.Put(wo, changeKey, changeValOld))
	ensure.Nil(t, db.Put(wo, deleteKey, changeValNew))

	// trigger a compaction
	db.CompactRange(Range{nil, nil})

	// ensure that the value is changed after compaction
	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, changeKey)
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), changeValNew)

	// ensure that the key is deleted after compaction
	v2, err := db.Get(ro, deleteKey)
	ensure.Nil(t, err)
	ensure.True(t, v2.Data() == nil)
}

type mockCompactionFilter struct {
	filter func(level int, key, val []byte) (remove bool, newVal []byte)
}

func (m *mockCompactionFilter) Name() string { return "gorocksdb.test" }
func (m *mockCompactionFilter) Filter(level int, key, val []byte) (bool, []byte) {
	return m.filter(level, key, val)
}
