// Copyright 2022 The CubeFS Authors.
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

package kvstore

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDb(t *testing.T) {
	path, err := ioutil.TempDir("", "testrocksdbkvdb12233222")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	k1 := []byte("t1k1")
	v1 := []byte("t1v1")

	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	db, err := OpenDB(path, false)
	assert.NoError(t, err)
	defer db.Close()

	ins, ok := db.(*instance)
	assert.Equal(t, true, ok)
	assert.Equal(t, path, ins.Name())
	assert.NotNil(t, ins.GetDB())

	err = db.Put(KV{Key: k1, Value: v1})
	assert.NoError(t, err)

	v, err := db.Get(k1)
	assert.NoError(t, err)
	assert.Equal(t, v1, v)

	s := db.NewSnapshot()
	assert.NotNil(t, s)
	i := db.NewIterator(s)
	assert.NotNil(t, i)
	defer db.ReleaseSnapshot(s)
	defer i.Close()

	i.Seek(nil)
	if i.Valid() {
		ik := i.Key().Data()
		assert.Equal(t, k1, ik)
		i.Key().Free()

		iv := i.Value().Data()
		assert.Equal(t, v1, iv)
		i.Value().Free()
	}
}

func TestTable(t *testing.T) {
	path, err := ioutil.TempDir("", "testrocksdbkvdb12345")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	tableName1 := "table1"
	tableName2 := "table2"
	k1 := []byte("t1k1")
	k2 := []byte("t1k2")
	v1 := []byte("t1v1")
	v2 := []byte("t1v2")

	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	{
		db, err := OpenDBWithCF(path, false, []string{tableName1, tableName2})
		assert.NoError(t, err)

		t1 := db.Table(tableName1)
		assert.NoError(t, err)

		tab, ok := t1.(*table)
		assert.Equal(t, true, ok)
		assert.NotNil(t, tab)
		assert.Equal(t, tableName1, tab.Name())
		assert.NoError(t, tab.Flush())

		assert.NotNil(t, tab.GetDB())
		assert.NotNil(t, tab.GetCf())
		writeBatch := tab.NewWriteBatch()
		assert.NotNil(t, writeBatch)
		assert.NoError(t, tab.DoBatch(writeBatch))

		// not found
		_, err = t1.Get(k1)
		assert.Equal(t, ErrNotFound, err)

		err = t1.Put(KV{Key: k1, Value: v1})
		assert.NoError(t, err)

		err = t1.Put(KV{Key: k2, Value: v2})
		assert.NoError(t, err)

		v, err := t1.Get(k1)
		assert.NoError(t, err)
		assert.Equal(t, v1, v)

		snapshot := tab.NewSnapshot()
		assert.NotNil(t, snapshot)
		tab.ReleaseSnapshot(snapshot)

		db.Close()
	}

	{
		db, err := OpenDBWithCF(path, false, []string{tableName1, tableName2})
		assert.NoError(t, err)

		t1 := db.Table(tableName1)
		assert.NoError(t, err)
		v, err := t1.Get(k1)
		assert.NoError(t, err)
		assert.Equal(t, v1, v)

		v, err = t1.Get(k2)
		assert.NoError(t, err)
		assert.Equal(t, v2, v)

		t2 := db.Table(tableName2)
		err = t2.Put(KV{Key: k2, Value: v2})
		assert.NoError(t, err)
		v, err = t2.Get(k2)
		assert.NoError(t, err)
		assert.Equal(t, v2, v)

		i := t1.NewIterator(nil)
		assert.NotNil(t, i)

		kvs := make([]KV, 0)
		for i.SeekToFirst(); i.Valid(); i.Next() {
			assert.NoError(t, i.Err())
			ik := i.Key().Data()
			key := make([]byte, len(ik))
			copy(key, ik)
			i.Key().Free()
			iv := i.Value().Data()
			value := make([]byte, len(iv))
			copy(value, iv)
			i.Value().Free()
			kvs = append(kvs, KV{Key: key, Value: value})
		}
		for i := range kvs {
			t.Log(string(kvs[i].Key), ": ", string(kvs[i].Value))
		}

		assert.Equal(t, 2, len(kvs))
		assert.Equal(t, string(k1), string(kvs[0].Key))
		assert.Equal(t, string(v1), string(kvs[0].Value))
		assert.Equal(t, string(k2), string(kvs[1].Key))
		assert.Equal(t, string(v2), string(kvs[1].Value))

		i.Close()
		db.Close()
	}
}

func TestTableBatchOP(t *testing.T) {
	path, err := ioutil.TempDir("", "testrocksdbkvdb12345")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	tableName1 := "table1"
	tableName2 := "table2"
	k1 := []byte("t1k1")
	v1 := []byte("t1v1")

	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	db, err := OpenDBWithCF(path, false, []string{tableName1, tableName2})
	assert.NoError(t, err)

	t1 := db.Table(tableName1)
	assert.NoError(t, err)

	kvs := []KV{
		{Key: k1, Value: v1},
	}

	err = t1.WriteBatch(kvs, false)
	assert.NoError(t, err)

	v, err := t1.Get(k1)
	assert.NoError(t, err)
	assert.Equal(t, v1, v)

	err = t1.DeleteBatch([][]byte{k1}, true)
	assert.NoError(t, err)
}

func TestDbBatchOP(t *testing.T) {
	path, err := ioutil.TempDir("", "testrocksdbkvdb121212323")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	db, err := OpenDB(path, false)
	assert.NoError(t, err)
	defer db.Close()

	keyPrefix := "testkey"

	{
		var kvs []KV
		for i := 0; i < 10; i++ {
			var value [8]byte

			key := []byte(fmt.Sprintf("%s/%d", keyPrefix, i))
			binary.BigEndian.PutUint64(value[:], uint64(i))

			kv := KV{
				Key:   key,
				Value: value[:],
			}
			kvs = append(kvs, kv)
		}

		err = db.WriteBatch(kvs, true)
		require.NoError(t, err)

		// valid
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("%s/%d", keyPrefix, i))
			expValue := i
			valueBytes, err := db.Get([]byte(key))
			require.NoError(t, err)

			value := binary.BigEndian.Uint64(valueBytes)
			require.Equal(t, int64(expValue), int64(value))
		}
	}

	{ // delete batch
		var ks [][]byte
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("%s/%d", keyPrefix, i))
			ks = append(ks, key)
		}
		err = db.DeleteBatch(ks, true)
		require.NoError(t, err)

		// valid
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("%s/%d", keyPrefix, i))
			_, err = db.Get(key)
			require.Error(t, err)
		}
	}

	{ // test Delete
		err = db.Delete([]byte(fmt.Sprintf("%s/%d", keyPrefix, 1)))
		require.NoError(t, err)
	}
}
