// Copyright 2019 The CubeFS Authors.
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

package objectnode

type MetaStore interface {
}

// MetaStore
type Store interface {
	Init(vm *VolumeManager)
	Put(ns, obj, key string, data []byte) error
	Get(ns, obj, key string) (data []byte, err error)
	List(ns, obj string) (data [][]byte, err error)
	Delete(ns, obj, key string) error
}
