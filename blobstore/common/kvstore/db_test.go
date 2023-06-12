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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDb(t *testing.T) {
	path, err := os.MkdirTemp("", "testrocksdbkvdb12233222")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	k1 := []byte("t1k1")
	v1 := []byte("t1v1")

	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	db, err := OpenDB(path, false)
	require.NoError(t, err)
	defer db.Close()

	ins, ok := db.(*instance)
	require.Equal(t, true, ok)
	require.Equal(t, path, ins.Name())
	require.NotNil(t, ins.GetDB())

	err = db.Put(KV{Key: k1, Value: v1})
	require.NoError(t, err)

	v, err := db.Get(k1)
	require.NoError(t, err)
	require.Equal(t, v1, v)

	s := db.NewSnapshot()
	require.NotNil(t, s)
	i := db.NewIterator(s)
	require.NotNil(t, i)
	defer db.ReleaseSnapshot(s)
	defer i.Close()

	i.Seek(nil)
	if i.Valid() {
		ik := i.Key().Data()
		require.Equal(t, k1, ik)
		i.Key().Free()

		iv := i.Value().Data()
		require.Equal(t, v1, iv)
		i.Value().Free()
	}
}

func TestTable(t *testing.T) {
	path, err := os.MkdirTemp("", "testrocksdbkvdb12345")
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
		require.NoError(t, err)

		t1 := db.Table(tableName1)
		require.NoError(t, err)

		tab, ok := t1.(*table)
		require.Equal(t, true, ok)
		require.NotNil(t, tab)
		require.Equal(t, tableName1, tab.Name())
		require.NoError(t, tab.Flush())

		require.NotNil(t, tab.GetDB())
		require.NotNil(t, tab.GetCf())
		writeBatch := tab.NewWriteBatch()
		require.NotNil(t, writeBatch)
		require.NoError(t, tab.DoBatch(writeBatch))

		// not found
		_, err = t1.Get(k1)
		require.Equal(t, ErrNotFound, err)

		err = t1.Put(KV{Key: k1, Value: v1})
		require.NoError(t, err)

		err = t1.Put(KV{Key: k2, Value: v2})
		require.NoError(t, err)

		v, err := t1.Get(k1)
		require.NoError(t, err)
		require.Equal(t, v1, v)

		snapshot := tab.NewSnapshot()
		require.NotNil(t, snapshot)
		tab.ReleaseSnapshot(snapshot)

		db.Close()
	}

	{
		db, err := OpenDBWithCF(path, false, []string{tableName1, tableName2})
		require.NoError(t, err)

		t1 := db.Table(tableName1)
		require.NoError(t, err)
		v, err := t1.Get(k1)
		require.NoError(t, err)
		require.Equal(t, v1, v)

		v, err = t1.Get(k2)
		require.NoError(t, err)
		require.Equal(t, v2, v)

		t2 := db.Table(tableName2)
		err = t2.Put(KV{Key: k2, Value: v2})
		require.NoError(t, err)
		v, err = t2.Get(k2)
		require.NoError(t, err)
		require.Equal(t, v2, v)

		i := t1.NewIterator(nil)
		require.NotNil(t, i)

		kvs := make([]KV, 0)
		for i.SeekToFirst(); i.Valid(); i.Next() {
			require.NoError(t, i.Err())
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

		require.Equal(t, 2, len(kvs))
		require.Equal(t, string(k1), string(kvs[0].Key))
		require.Equal(t, string(v1), string(kvs[0].Value))
		require.Equal(t, string(k2), string(kvs[1].Key))
		require.Equal(t, string(v2), string(kvs[1].Value))

		i.Close()
		db.Close()
	}
}

func TestTableBatchOP(t *testing.T) {
	path, err := os.MkdirTemp("", "testrocksdbkvdb12345")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	tableName1 := "table1"
	tableName2 := "table2"
	k1 := []byte("t1k1")
	v1 := []byte("t1v1")

	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	db, err := OpenDBWithCF(path, false, []string{tableName1, tableName2})
	require.NoError(t, err)

	t1 := db.Table(tableName1)
	require.NoError(t, err)

	kvs := []KV{
		{Key: k1, Value: v1},
	}

	err = t1.WriteBatch(kvs, false)
	require.NoError(t, err)

	v, err := t1.Get(k1)
	require.NoError(t, err)
	require.Equal(t, v1, v)

	err = t1.DeleteBatch([][]byte{k1}, true)
	require.NoError(t, err)
}

func TestDbBatchOP(t *testing.T) {
	path, err := os.MkdirTemp("", "testrocksdbkvdb121212323")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	err = os.MkdirAll(path, 0o755)
	require.NoError(t, err)

	db, err := OpenDB(path, false)
	require.NoError(t, err)
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
