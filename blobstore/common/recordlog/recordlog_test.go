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

package recordlog

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type Doc struct {
	ID    string `json:"id"`
	Fsize int64  `json:"fsize"`
	Hash  string `json:"hash"`
	Fh    []byte `json:"fh"`
}

func TestLogger(t *testing.T) {
	var conf *Config
	doc := Doc{
		ID:    "id",
		Fsize: 1000,
		Hash:  "hash",
		Fh:    []byte("fh"),
	}
	// test nopEncoder
	enc, err := NewEncoder(conf)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				err := enc.Encode(doc)
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, enc.Close())

	tmpDir := path.Join(os.TempDir(), fmt.Sprintf("recordlog_tmp_%d", rand.Intn(10000000)))
	err = os.Mkdir(tmpDir, 0o755)
	t.Log(tmpDir)
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	conf = &Config{
		Dir:       tmpDir,
		ChunkBits: 20,
	}

	enc, err = NewEncoder(conf)
	require.NoError(t, err)
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				err := enc.Encode(doc)
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	// test read
	dec := json.NewDecoder(enc.(io.Reader))
	count := 0
	var m Doc
	for {
		err = dec.Decode(&m)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		require.NoError(t, err)
		require.Equal(t, doc, m)
		count += 1
	}
	require.Equal(t, 50000, count)
	require.NoError(t, enc.Close())
}
