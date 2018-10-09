package gorocksdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/facebookgo/ensure"
)

func TestExternalFile(t *testing.T) {
	db := newTestDB(t, "TestDBExternalFile", nil)
	defer db.Close()

	envOpts := NewDefaultEnvOptions()
	opts := NewDefaultOptions()
	w := NewSSTFileWriter(envOpts, opts)
	defer w.Destroy()

	filePath, err := ioutil.TempFile("", "sst-file-test")
	ensure.Nil(t, err)
	defer os.Remove(filePath.Name())

	err = w.Open(filePath.Name())
	ensure.Nil(t, err)

	err = w.Add([]byte("aaa"), []byte("aaaValue"))
	ensure.Nil(t, err)
	err = w.Add([]byte("bbb"), []byte("bbbValue"))
	ensure.Nil(t, err)
	err = w.Add([]byte("ccc"), []byte("cccValue"))
	ensure.Nil(t, err)
	err = w.Add([]byte("ddd"), []byte("dddValue"))
	ensure.Nil(t, err)

	err = w.Finish()
	ensure.Nil(t, err)

	ingestOpts := NewDefaultIngestExternalFileOptions()
	err = db.IngestExternalFile([]string{filePath.Name()}, ingestOpts)
	ensure.Nil(t, err)

	readOpts := NewDefaultReadOptions()

	v1, err := db.Get(readOpts, []byte("aaa"))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v1.Data(), []byte("aaaValue"))
	v2, err := db.Get(readOpts, []byte("bbb"))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v2.Data(), []byte("bbbValue"))
	v3, err := db.Get(readOpts, []byte("ccc"))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v3.Data(), []byte("cccValue"))
	v4, err := db.Get(readOpts, []byte("ddd"))
	ensure.Nil(t, err)
	ensure.DeepEqual(t, v4.Data(), []byte("dddValue"))
}
