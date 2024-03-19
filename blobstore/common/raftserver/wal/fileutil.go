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

package wal

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
)

const (
	TrashPath = ".trash"
)

func InitPath(dir string) error {
	dir = path.Join(dir, TrashPath)
	info, err := os.Stat(dir)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			if os.IsNotExist(pathErr) {
				return os.MkdirAll(dir, 0o755)
			}
		}
		return err
	}

	if !info.IsDir() {
		return errors.New("path is not directory")
	}

	return nil
}

type logName struct {
	sequence uint64
	index    uint64
}

func (l *logName) String() string {
	return fmt.Sprintf("%016x-%016x.log", l.sequence, l.index)
}

func (l *logName) parse(s string) error {
	_, err := fmt.Sscanf(s, "%016x-%016x.log", &l.sequence, &l.index)
	return err
}

type logNameSlice []logName

func (s logNameSlice) Len() int           { return len(s) }
func (s logNameSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s logNameSlice) Less(i, j int) bool { return s[i].sequence < s[j].sequence }

func listLogFiles(path string) (fnames []logName, err error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		var n logName
		if err := n.parse(file.Name()); err == nil {
			fnames = append(fnames, n)
		}
	}
	sort.Sort(logNameSlice(fnames))
	return
}
