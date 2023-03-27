// Copyright 2020 The CubeFS Authors.
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

package cmd

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	_ "github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
	"math"
	"path"
	"strings"
	"syscall"
)

var (
	cleanCount uint32
)

type CleanResp struct {
	Code uint32
	Msg string
	Num uint32
}

func newFSCleanCmd(client *master.MasterClient) *cobra.Command {
	var vol string
	var c = &cobra.Command{
		Use:   "clean [path]",
		Short: "clean the deleted directory or file",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				rsp CleanResp
				err error
			)
			defer func() {
				if !isFormatAsJSON {
					return
				}
				if err != nil {
					rsp.Code = 1
					rsp.Msg = err.Error()
				} else {
					rsp.Code = 0
					rsp.Num = cleanCount
				}
				printAsJson(&rsp)
			}()

			err = newTrashEnv(client, vol)
			if err != nil {
				return
			}
			if err = cleanPath(args[0]); err != nil {
				if !isFormatAsJSON {
					fmt.Println(err)
				}
			}
		},
	}
	c.Flags().StringVarP(&vol, "vol", "v", "", "volume Name")
	c.Flags().BoolVarP(&forceFlag, "force", "f", false,
		"Force to clean a directory or file, which has more deleted records, that have Name Name, just different timestamps")
	c.Flags().BoolVarP(&isFormatAsJSON, "json", "j", false, "output as json ")
	c.MarkFlagRequired("vol")
	return c
}

func cleanPath(pathStr string) (err error) {
	defer func() {
		if err != nil {
			msg := fmt.Sprintf("Failed to  clean trash by path: %v, clean children: %v, err: %v", pathStr, cleanCount, err.Error())
			if !isFormatAsJSON {
				fmt.Println(msg)
			}
			log.LogError(msg)
		} else {
			msg := fmt.Sprintf("Succ to  clean trash by path: %v, clean children: %v", pathStr, cleanCount)
			if !isFormatAsJSON {
				fmt.Println(msg)
			}
			log.LogDebug(msg)
		}
	}()

	absPath := path.Clean(pathStr)
	if absPath == "/" {
		err = errors.New("not supported the root path")
		return
	}

	if strings.HasPrefix(absPath, "/") == false {
		err = fmt.Errorf("the path[%v] is invalid", pathStr)
		return
	}

	dir, name := path.Split(absPath)

	var (
		parentID  uint64
		isDeleted = false
	)
	parentID, isDeleted, _, err = lookupPath(dir)
	if err != nil {
		err = fmt.Errorf("failed to lookup pathStr: %v, err: %v", pathStr, err.Error())
		return
	}

	if !isDeleted {
		var ino uint64
		ino, _, err = gTrashEnv.metaWrapper.Lookup_ll(ctx, parentID, name)
		if err == syscall.ENOENT {
			return doCleanPath(parentID, name, 0)
		} else if err != nil {
			log.LogError(err.Error())
			return
		}
		var den proto.DeletedDentry
		den.Inode = ino
		err = doCleanOneDirectory(&den)
		if err != nil {
			log.LogError(err.Error())
			return
		}
		return
	}

	return doCleanPath(parentID, name, 0)
}

func doCleanPath(parentID uint64, name string, ts int64) (err error) {
	var (
		startTime, endTime int64
	)

	defer func() {
		if err != nil {
			log.LogDebugf("doClean, pid: %v, Name: %v, TS: %v, start: %v, end: %v, err: %v",
				parentID, name, ts, startTime, endTime, err.Error())
		} else {
			log.LogDebugf("doClean, pid: %v, Name: %v, TS: %v, start: %v, end: %v",
				parentID, name, ts, startTime, endTime)
		}
	}()

	if ts > 0 {
		startTime = ts
		endTime = ts
	} else if RegexpFileIsDeleted.MatchString(name) {
		name, startTime, err = parseDeletedName(name)
		if err != nil {
			return err
		}
		endTime = startTime
	} else {
		startTime = 0
		endTime = math.MaxInt64
	}

	var dentrys []*proto.DeletedDentry
	dentrys, err = gTrashEnv.metaWrapper.LookupDeleted_ll(ctx, parentID, name, startTime, endTime)
	if err != nil {
		log.LogErrorf("ino: %v, dir: %v, start: %v, end: %v, err: %v", parentID, name, startTime, endTime, err.Error())
		return
	}

	if len(dentrys) > 1 && !forceFlag {
		msg := fmt.Sprintf("[%v] has multiple deleted records with different timestamps", name)
		if !isFormatAsJSON {
			fmt.Println(msg)
		}
		log.LogError(msg)
		err = errors.New(msg)
		return
	}

	for _, d := range dentrys {
		if proto.IsDir(d.Type) == true {
			err = doCleanOneDirectory(d)
			if err != nil {
				log.LogErrorf(err.Error())
				return
			}
		}
	}

	for _, d := range dentrys {
		err = clean(d)
		if err != nil {
			log.LogErrorf(err.Error())
			return
		}
	}
	return
}

func doCleanOneDirectory(den *proto.DeletedDentry) (err error) {
	pid := den.Inode
	defer func() {
		if err != nil {
			log.LogDebugf("doCleanOneDirectory, isRecursive: %v, dentry: %v, err: %v", isRecursive, den, err.Error())
		} else {
			log.LogDebugf("doCleanOneDirectory, isRecursive: %v, dentry: %v", isRecursive, den)
		}
	}()

	var children []*proto.DeletedDentry
	children, err = gTrashEnv.metaWrapper.ReadDirDeleted(ctx, pid)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	if len(children) == 0 {
		return
	}

	for _, chd := range children {
		if proto.IsDir(chd.Type) == true {
			err = doCleanOneDirectory(chd)
			if err != nil {
				log.LogError(err.Error())
				return
			}
		}
	}

	inos := make([]uint64, 0, len(children))
	for _, chd := range children {
		inos = append(inos, chd.Inode)
	}
	_, err = gTrashEnv.metaWrapper.BatchCleanDeletedInode(ctx, inos)
	if err != nil {
		log.LogErrorf(err.Error())
		return
	}

	res := make(map[uint64]int, 0)
	res, err = gTrashEnv.metaWrapper.BatchCleanDeletedDentry(ctx, children)
	if err != nil {
		log.LogErrorf(err.Error())
		return
	}
	for _, chd := range children {
		st, ok := res[chd.Inode]
		if ok && st != meta.StatusOK && st != meta.StatusNoent {
			continue
		}
		cleanCount++
	}
	return
}

func clean(d *proto.DeletedDentry) (err error) {
	pid := d.ParentID
	defer func() {
		if err != nil {
			log.LogDebugf("clean, pid: %v, dentry: %v, err: %v", pid, d, err.Error())
		} else {
			log.LogDebugf("clean, pid: %v, dentry: %v", pid, d)
		}
	}()

	err = gTrashEnv.metaWrapper.CleanDeletedInode(ctx, d.Inode)
	if err != nil && err != syscall.ENOENT {
		if !isFormatAsJSON {
			fmt.Println(err.Error())
		}
		return
	}
	err = gTrashEnv.metaWrapper.CleanDeletedDentry(ctx, pid, d.Inode, d.Name, d.Timestamp)
	if err != nil && err != syscall.ENOENT {
		if !isFormatAsJSON {
			fmt.Println(err.Error())
		}
		return
	}
	err = nil
	cleanCount++
	return
}
