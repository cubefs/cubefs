package gorocksdb

import (
	"github.com/facebookgo/ensure"
	"io/ioutil"
	"os"
	"testing"
)

func TestCheckpoint(t *testing.T) {

	suffix := "checkpoint"
	dir, err := ioutil.TempDir("", "gorocksdb-"+suffix)
	ensure.Nil(t, err)
	err = os.RemoveAll(dir)
	ensure.Nil(t, err)

	db := newTestDB(t, "TestCheckpoint", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	givenVal := []byte("val")
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		ensure.Nil(t, db.Put(wo, k, givenVal))
	}

	var dbCheck *DB
	var checkpoint *Checkpoint

	checkpoint, err = db.NewCheckpoint()
	defer checkpoint.Destroy()
	ensure.NotNil(t, checkpoint)
	ensure.Nil(t, err)

	err = checkpoint.CreateCheckpoint(dir, 0)
	ensure.Nil(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	dbCheck, err = OpenDb(opts, dir)
	defer dbCheck.Close()
	ensure.Nil(t, err)

	// test keys
	var value *Slice
	ro := NewDefaultReadOptions()
	for _, k := range givenKeys {
		value, err = dbCheck.Get(ro, k)
		defer value.Free()
		ensure.Nil(t, err)
		ensure.DeepEqual(t, value.Data(), givenVal)
	}

}
