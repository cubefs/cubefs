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

package blobnode

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCleanExpiredStatFile(t *testing.T) {
	s := &Service{
		ctx:     context.TODO(),
		closeCh: make(chan struct{}),
		Conf: &Config{
			CleanExpiredStatIntervalSec: 1,
		},
	}
	//
	s.cleanExpiredStatFile()

	//
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.loopCleanExpiredStatFile()
	}()

	time.Sleep(2 * time.Second)

	close(s.closeCh)
	wg.Wait()
}
