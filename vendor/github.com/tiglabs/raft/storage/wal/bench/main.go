// Copyright 2018 The TigLabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"fmt"

	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
)

var n = flag.Int("n", 100000, "requests")
var l = flag.Int("l", 1024, "entry data length")

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomEntry(rnd *rand.Rand, index uint64) *proto.Entry {
	data := make([]byte, *l)
	for i := 0; i < *l; i++ {
		data[i] = letters[rnd.Int()%len(letters)]
	}

	ent := &proto.Entry{
		Index: index,
		Type:  proto.EntryNormal,
		Term:  uint64(rnd.Uint32()),
		Data:  data,
	}

	return ent
}

func main() {
	flag.Parse()

	dir, err := ioutil.TempDir(os.TempDir(), "db_bench_")
	if err != nil {
		panic(err)
	}

	s, err := wal.NewStorage(dir, nil)
	if err != nil {
		panic(err)
	}

	start := time.Now()
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	reqs := *n
	for i := 0; i < reqs; i++ {
		ent := randomEntry(rnd, uint64(i))
		if err := s.StoreEntries([]*proto.Entry{ent}); err != nil {
			panic(err)
		}
	}
	spend := time.Since(start)
	ops := (int64(reqs) * 1000) / (spend.Nanoseconds() / 1000000)
	fmt.Printf("write %d entries spend: %v, ops: %v\n", reqs, spend, ops)
}
