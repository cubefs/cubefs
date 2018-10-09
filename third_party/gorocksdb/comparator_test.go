package gorocksdb

import (
	"bytes"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestComparator(t *testing.T) {
	db := newTestDB(t, "TestComparator", func(opts *Options) {
		opts.SetComparator(&bytesReverseComparator{})
	})
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	// create a iterator to collect the keys
	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	// we seek to the last key and iterate in reverse order
	// to match given keys
	var actualKeys [][]byte
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		key := make([]byte, 4)
		copy(key, iter.Key().Data())
		actualKeys = append(actualKeys, key)
	}
	ensure.Nil(t, iter.Err())

	// ensure that the order is correct
	ensure.DeepEqual(t, actualKeys, givenKeys)
}

type bytesReverseComparator struct{}

func (cmp *bytesReverseComparator) Name() string { return "gorocksdb.bytes-reverse" }
func (cmp *bytesReverseComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b) * -1
}
