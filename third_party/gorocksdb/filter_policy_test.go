package gorocksdb

import (
	"testing"

	"github.com/facebookgo/ensure"
)

// fatalAsError is used as a wrapper to make it possible to use ensure
// also if C calls Go otherwise it will throw a internal lockOSThread error.
type fatalAsError struct {
	t *testing.T
}

func (f *fatalAsError) Fatal(a ...interface{}) {
	f.t.Error(a...)
}

func TestFilterPolicy(t *testing.T) {
	var (
		givenKeys          = [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
		givenFilter        = []byte("key")
		createFilterCalled = false
		keyMayMatchCalled  = false
	)
	policy := &mockFilterPolicy{
		createFilter: func(keys [][]byte) []byte {
			createFilterCalled = true
			ensure.DeepEqual(&fatalAsError{t}, keys, givenKeys)
			return givenFilter
		},
		keyMayMatch: func(key, filter []byte) bool {
			keyMayMatchCalled = true
			ensure.DeepEqual(&fatalAsError{t}, key, givenKeys[0])
			ensure.DeepEqual(&fatalAsError{t}, filter, givenFilter)
			return true
		},
	}

	db := newTestDB(t, "TestFilterPolicy", func(opts *Options) {
		blockOpts := NewDefaultBlockBasedTableOptions()
		blockOpts.SetFilterPolicy(policy)
		opts.SetBlockBasedTableFactory(blockOpts)
	})
	defer db.Close()

	// insert keys
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, []byte("val")))
	}

	// flush to trigger the filter creation
	ensure.Nil(t, db.Flush(NewDefaultFlushOptions()))
	ensure.True(t, createFilterCalled)

	// test key may match call
	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, givenKeys[0])
	defer v1.Free()
	ensure.Nil(t, err)
	ensure.True(t, keyMayMatchCalled)
}

type mockFilterPolicy struct {
	createFilter func(keys [][]byte) []byte
	keyMayMatch  func(key, filter []byte) bool
}

func (m *mockFilterPolicy) Name() string { return "gorocksdb.test" }
func (m *mockFilterPolicy) CreateFilter(keys [][]byte) []byte {
	return m.createFilter(keys)
}
func (m *mockFilterPolicy) KeyMayMatch(key, filter []byte) bool {
	return m.keyMayMatch(key, filter)
}
