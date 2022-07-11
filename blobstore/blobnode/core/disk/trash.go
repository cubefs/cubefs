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

package disk

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func (ds *DiskStorage) loopCleanTrash() {
	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "Trash "+ds.Conf.Path)

	span.Infof("loop clean trash.")

	timer := initTimer(ds.Conf.DiskCleanTrashIntervalSec)
	defer timer.Stop()

	for {
		select {
		case <-ds.closeCh:
			span.Infof("loop clean trash done")
			return
		case <-timer.C:
			if err := ds.cleanTrash(ctx); err != nil {
				span.Errorf("Failed exec clean trash. err:%v", err)
			}
			resetTimer(ds.Conf.DiskCleanTrashIntervalSec, timer)
		}
	}
}

func (ds *DiskStorage) cleanTrash(ctx context.Context) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Infof("clean trash start.")
	defer span.Infof("clean trash stop.")

	trashPath := core.SysTrashPath(ds.Conf.Path)
	trashProtection := time.Duration(ds.Conf.DiskTrashProtectionM) * time.Minute

	fis, err := ioutil.ReadDir(trashPath)
	if err != nil {
		span.Debugf("failed readdir, err:%v", err)
		return
	}
	for _, fi := range fis {
		path := filepath.Join(trashPath, fi.Name())
		mtime := fi.ModTime()

		if time.Since(mtime) < trashProtection {
			span.Debugf("%s mtime:%v, trashProtection:%v. skip", path, mtime, trashProtection)
			continue
		}

		// todo: report to ums
		if !ds.Conf.AllowCleanTrash {
			span.Debugf("skip clean rubbish %s", fi.Name())
			continue
		}

		span.Warnf("will remove %s, mtime:%s, protection:%v", fi.Name(), mtime.Format(time.RFC3339), trashProtection)
		err := os.Remove(path)
		if err != nil {
			span.Errorf("failed remove %s, err:%v", path, err)
			continue
		}
		span.Infof("%s removed", path)
	}

	return
}

func (ds *DiskStorage) moveToTrash(ctx context.Context, path string) (err error) {
	span := trace.SpanFromContextSafe(ctx)

	fi, err := os.Stat(path)
	if err != nil {
		span.Errorf("failed stat %s, err:%v", path, err)
		return err
	}

	if fi.IsDir() {
		return fmt.Errorf("illegal operation, %s is dir", path)
	}

	now := time.Now().UnixNano()
	filename := fmt.Sprintf("%s.%v", filepath.Base(path), now)

	trashPath := core.SysTrashPath(ds.Conf.Path)
	dstPath := filepath.Join(trashPath, filename)

	span.Warnf("mv %s to trash", path)

	err = os.Rename(path, dstPath)
	if err != nil {
		span.Errorf("failed rename, err:%v", err)
		return err
	}

	return nil
}
