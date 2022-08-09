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

package blobnode

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/cubefs/cubefs/blobstore/blobnode/base"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/flow"
	"github.com/cubefs/cubefs/blobstore/common/fileutil"
	"github.com/cubefs/cubefs/blobstore/common/iostat"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	statExpiredTimeoutM = 180
)

func (s *Service) loopCleanExpiredStatFile() {
	span, _ := trace.StartSpanFromContextWithTraceID(context.Background(), "", "CleanExpiredStat")
	span.Infof("loop clean expired stat file")

	ticker := time.NewTicker(time.Duration(s.Conf.CleanExpiredStatIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.closeCh:
			span.Warnf("loop clean expired stat file")
			return
		case <-ticker.C:
			s.cleanExpiredStatFile()
		}
	}
}

func (s *Service) cleanExpiredStatFile() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", base.BackgroudReqID("ExpiredStat"))

	purgeFunc := func(path string) error {
		// if the format is incorrect, skip
		pid, suffix, err := flow.ParseStatName(path)
		if err != nil {
			span.Errorf("path:%v, err:%v", path, err)
			return err
		}

		if suffix != flow.IOStatFileSuffix {
			return errors.New("invalid suffix")
		}

		// alive processe, skip
		pidpath := fmt.Sprintf("/proc/%d/", pid)

		if _, err = os.Stat(pidpath); err == nil {
			return errors.New("progress still alive")
		}
		if !os.IsNotExist(err) {
			span.Errorf("path:%v, err:%v", path, err)
			return err
		}
		data, err := ioutil.ReadFile(pidpath + "comm")
		if err == nil {
			return errors.New("progress still alive")
		}

		// lock the stat file
		lk, err := fileutil.TryLockFile(path)
		if err != nil {
			return err
		}
		defer lk.Unlock()

		info, err := os.Stat(path)
		if err != nil {
			span.Errorf("path:%v, err:%v", path, err)
			return err
		}

		// protect time
		if time.Since(info.ModTime()) < time.Minute*time.Duration(statExpiredTimeoutM) {
			return errors.New("still in protection")
		}

		span.Warnf("file %s(%s) modfiy time:%v, will be deleted",
			path, strings.TrimSpace(string(data)), info.ModTime())

		// finally, delete it
		if err = os.Remove(path); err != nil {
			span.Errorf("remove %s failed, err:%v", path, err)
			return err
		}

		return nil
	}

	names, err := iostat.PurgeStatFile(ctx, iostat.IOSTAT_DIRECTORY, flow.IOStatFileSuffix, purgeFunc)
	if err != nil {
		span.Errorf("purege stat file occur err, fnames:%v, err:%v", names, err)
		return
	}

	span.Debugf("purge stat file:%v", names)
}
