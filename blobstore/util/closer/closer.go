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

package closer

import (
	"sync"
)

// Closer is the interface for object that can release its resources.
type Closer interface {
	// Close release all resources holded by the object.
	Close()
	// Done returns a channel that's closed when object was closed.
	Done() <-chan struct{}
}

// Close release all resources holded by the object.
func Close(obj interface{}) {
	if obj == nil {
		return
	}
	if c, ok := obj.(Closer); ok {
		c.Close()
	}
}

// New returns a closer.
func New() Closer {
	return &closer{ch: make(chan struct{})}
}

type closer struct {
	once sync.Once
	ch   chan struct{}
}

func (c *closer) Close() {
	c.once.Do(func() {
		close(c.ch)
	})
}

func (c *closer) Done() <-chan struct{} {
	return c.ch
}
