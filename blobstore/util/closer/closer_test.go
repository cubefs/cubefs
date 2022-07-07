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

package closer_test

import (
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/util/closer"
)

func TestCloserClose(t *testing.T) {
	{
		closer.Close(nil)
		closer.Close(closer.New())
		closer.Close(0b10)
		closer.Close("0x19")
	}
	{
		c := closer.New()
		for range [1 << 10]struct{}{} {
			c.Close()
		}
	}
	{
		c := closer.New()
		wg := sync.WaitGroup{}
		wg.Add(1 << 10)
		for range [1 << 10]struct{}{} {
			go func() {
				c.Close()
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func TestCloserDone(t *testing.T) {
	{
		c := closer.New()
		c.Close()
		for range [1 << 10]struct{}{} {
			c.Done()
		}
	}
	{
		c := closer.New()
		time.AfterFunc(200*time.Millisecond, c.Close)
		wg := sync.WaitGroup{}
		wg.Add(1 << 10)
		for range [1 << 10]struct{}{} {
			go func() {
				<-c.Done()
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
