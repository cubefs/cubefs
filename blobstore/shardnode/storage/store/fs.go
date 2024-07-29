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

package store

import (
	"os"
	"path/filepath"
)

type (
	RawFS interface {
		CreateRawFile(name string) (RawFile, error)
		OpenRawFile(name string) (RawFile, error)
		ReadDir(dir string) ([]string, error)
	}
	RawFile interface {
		Read(p []byte) (n int, err error)
		Write(p []byte) (n int, err error)
		Close() error
	}
)

type posixRawFS struct {
	path        string
	handleError func(err error)
}

func (r *posixRawFS) CreateRawFile(name string) (RawFile, error) {
	filePath := r.path + "/" + name

	f, err := os.OpenFile(r.path+"/"+name, os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		if !os.IsNotExist(err) {
			r.handleError(err)
			return nil, err
		}

		dir := filepath.Dir(filePath)
		if err = os.MkdirAll(dir, 0o755); err != nil {
			r.handleError(err)
			return nil, err
		}

		f, err = os.OpenFile(r.path+"/"+name, os.O_CREATE|os.O_RDWR, 0o755)
		if err != nil {
			r.handleError(err)
			return nil, err
		}
	}

	return &posixRawFile{f: f, handleError: r.handleError}, nil
}

func (r *posixRawFS) OpenRawFile(name string) (RawFile, error) {
	f, err := os.OpenFile(r.path+"/"+name, os.O_RDONLY, 0o755)
	if err != nil {
		r.handleError(err)
		return nil, err
	}
	return &posixRawFile{f: f, handleError: r.handleError}, nil
}

func (r *posixRawFS) ReadDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(r.path + "/" + dir)
	if err != nil {
		r.handleError(err)
		return nil, err
	}

	ret := make([]string, len(entries))
	for i := range entries {
		ret[i] = entries[i].Name()
	}

	return ret, nil
}

type posixRawFile struct {
	f           *os.File
	handleError func(err error)
}

func (pf *posixRawFile) Read(p []byte) (n int, err error) {
	n, err = pf.f.Read(p)
	if err != nil {
		pf.handleError(err)
	}
	return n, err
}

func (pf *posixRawFile) Write(p []byte) (n int, err error) {
	n, err = pf.f.Write(p)
	if err != nil {
		pf.handleError(err)
	}
	return n, err
}

func (pf *posixRawFile) Close() error {
	return pf.f.Close()
}
