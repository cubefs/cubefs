// Copyright 2018 The CFS Authors.
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

package ump

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestUmp(t *testing.T) {

	InitUmp("ebs")
	for i := 0; i < 100; i++ {

		go func() {
			for {
				up := BeforeTP("wocao" + strconv.FormatInt(rand.Int63(), 16))
				AfterTP(up, nil)
				Alive("nimei")
				Alarm("baojingle", "weishenmene")
			}
		}()
	}

	for i := 0; i < 100; i++ {
		time.Sleep(time.Second)
	}
}
