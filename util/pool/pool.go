// Copyright 2018 The ChuBao Authors.
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

package pool

import "github.com/juju/errors"

var (
	ErrClosed = errors.New("closed pool")
)

// Pool is an interface wraps necessary methods for resource pool implement.
type Pool interface {
	// Get a resource from resource pool.
	Get() (interface{}, error)

	// Put a resource to resource pool.
	Put(interface{}) error

	// Close tries close specified resource.
	Close(interface{}) error

	// Release all resource entity stored in pool.
	Release()

	// Len returns number of resource stored in pool.
	Len() int

	AutoRelease()
}
