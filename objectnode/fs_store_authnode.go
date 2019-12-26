// Copyright 2018 The ChubaoFS Authors.
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

type authnodeStore struct {
	vm *volumeManager //vol *volume
}

func (s *authnodeStore) Init(vm *volumeManager) {
	s.vm = vm
	//TODO: init authnode store
}

func (s *authnodeStore) Get(vol, path, key string) (val []byte, err error) {

	return
}

func (s *authnodeStore) Put(vol, path, key string, data []byte) (err error) {
	//TODO: implement authonode store put method

	return nil
}

func (s *authnodeStore) Delete(vol, path, key string) (err error) {
	//TODO: implement authonode store put method

	return
}

func (s *authnodeStore) List(vol, path string) (data [][]byte, err error) {
	//TODO: implement authonode store list method

	return
}
