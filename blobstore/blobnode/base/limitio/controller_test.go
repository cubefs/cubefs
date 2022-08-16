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

package limitio

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewController(t *testing.T) {
	// scene
	c := NewController(100000)
	defer c.Close()
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Assign(1)
		}(i)
	}
	wg.Wait()
	require.Equal(t, 0, int(c.Hit()))

	// scene: iops 100
	c = NewController(100)
	defer c.Close()
	time.Sleep(1 * time.Second)
	for i := 0; i < 200; i++ {
		// iops: 66
		time.Sleep(15 * time.Millisecond)
		c.Assign(1)
	}
	wg.Wait()
	require.Equal(t, 0, int(c.Hit()))

	// scene: iops 100
	c = NewController(100)
	defer c.Close()
	for i := 0; i < 200; i++ {
		// iops: 200
		time.Sleep(5 * time.Millisecond)
		c.Assign(1)
	}
	wg.Wait()
	require.Equal(t, true, int(c.Hit()) > 0)
}
