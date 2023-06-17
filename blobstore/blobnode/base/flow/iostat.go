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

package flow

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
)

const (
	IOStatFileSuffix = "iostat"
)

var (
	_mux          = sync.RWMutex{}
	defaultIOStat = initDefaultStat()
)

var ErrInvalidStatName = errors.New("flow: invalid stat name")

type StatGetter interface {
	GetStatMgr(ioType bnapi.IOType) iostat.StatMgrAPI
}

type IOFlowStat [bnapi.IOTypeMax]*iostat.StatMgr

func (flow *IOFlowStat) GetStatMgr(iot bnapi.IOType) iostat.StatMgrAPI {
	if flow == nil {
		_mux.RLock()
		defer _mux.RUnlock()
		return defaultIOStat[iot]
	}
	return flow[iot]
}

func initDefaultStat() *IOFlowStat {
	stat := new(IOFlowStat)
	for i := range stat {
		stat[i] = &iostat.StatMgr{}
	}
	return stat
}

func SetupDefaultIOStat(s *IOFlowStat) {
	_mux.Lock()
	defer _mux.Unlock()
	defaultIOStat = s
}

// GenerateStatName ${pid}.${key}_${value}.${suffix}
func GenerateStatName(pid int, key string, value string, suffix string) string {
	return fmt.Sprintf("%d.%s_%s.%s", pid, key, value, suffix)
}

func ParseStatName(fname string) (pid int, suffix string, err error) {
	var pidstr string
	arr := strings.SplitN(path.Base(fname), ".", 3)
	switch len(arr) {
	case 3:
		pidstr, suffix = arr[0], arr[2]
	default:
		return pid, suffix, ErrInvalidStatName
	}

	pid, err = strconv.Atoi(pidstr)
	if err != nil {
		return pid, suffix, err
	}

	return pid, suffix, nil
}

func NewIOFlowStat(key string, dryRun bool) (*IOFlowStat, error) {
	iom := IOFlowStat{}

	buildPath := func(key string, value string) (path string) {
		path = fmt.Sprintf("%s/%s", iostat.IOSTAT_DIRECTORY, GenerateStatName(os.Getpid(), key, value, IOStatFileSuffix))
		return path
	}

	for k, v := range bnapi.IOtypemap {
		iostatPath := buildPath(key, v)
		dgIOStat, err := iostat.StatInit(iostatPath, 0, dryRun)
		if err != nil {
			return nil, err
		}
		iom[k] = dgIOStat
	}

	return &iom, nil
}
