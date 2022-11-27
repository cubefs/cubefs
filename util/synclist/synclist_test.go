// Copyright 2018 The CubeFS Authors.
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

package synclist

import (
	"sync"
	"testing"
)

/*
func TestListPushBach(t *testing.T) {
	l := list.New()

	wg := sync.WaitGroup{}
	for j := 0; j < 50; j += 1 {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i += 1 {
				println("pushback: ", i)
				l.PushBack(i)
			}
			wg.Done()
		}()
	}
	for j := 0; j < 60; j += 1 {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i += 1 {
				if f := l.Front(); f != nil {
					println("remove: ", f.Value)
					l.Remove(f)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
*/

func TestSyncPushBach(t *testing.T) {
	l := New()

	wg := sync.WaitGroup{}
	for j := 0; j < 50; j += 1 {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i += 1 {
				println("pushback: ", i)
				l.PushBack(i)
			}
			wg.Done()
		}()
	}
	for j := 0; j < 60; j += 1 {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i += 1 {
				if f := l.Front(); f != nil {
					println("remove: ", f.Value)
					l.Remove(f)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
