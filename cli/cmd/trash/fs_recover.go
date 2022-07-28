// Copyright 2020 The Chubao Authors.
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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
	"math"
	"path"
	"strings"
	"syscall"
)

var (
	count uint32 = 0
)

type RecoverResp struct {
	Code uint32
	Msg string
	Num uint32
}

type RecoverDentry struct {
	pid uint64
	dd  *proto.DeletedDentry
}

func newFSRecoverCmd(client *master.MasterClient) *cobra.Command {
	var vol string
	var c = &cobra.Command{
		Use:   "rc [path]",
		Short: "recover a deleted directory or file",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			err := newTrashEnv(client, vol)
			if err != nil {
				return
			}
			err = recoverPath(args[0])
			if err != nil {
				log.LogErrorf("failed to recover dir: %v, err: %v", args[0], err.Error())
				return
			}
		},
	}
	c.Flags().StringVarP(&vol, "vol", "v", "", "volume Name")
	c.MarkFlagRequired("vol")
	c.Flags().BoolVarP(&isRecursive, "recursive", "r", false, "Recursively recover a directory")
	c.Flags().BoolVarP(&forceFlag, "force", "f", false, "Force to recover a directory or file, "+
		"which has more deleted records, that have Name Name, just different timestamps")
	c.Flags().BoolVarP(&isFormatAsJSON, "json", "j", false, "output as json ")
	return c
}

func recoverPath(pathStr string) (err error) {
	defer func() {
		var rsp RecoverResp
		if err != nil {
			msg := fmt.Sprintf("Failed to  recover path: [%v] from trash, recovered children: %v, err: %v", pathStr, count, err.Error())
			if !isFormatAsJSON {
				fmt.Println(msg)
			}
			log.LogError(msg)
			rsp.Code = 1
			rsp.Msg = msg
			rsp.Num = count
		} else {
			msg := fmt.Sprintf("Succ to  recover path: [%v] from trash, recovered children: %v", pathStr, count)
			if !isFormatAsJSON {
				fmt.Println(msg)
			}
			log.LogDebug(msg)
			rsp.Code = 0
			rsp.Num = count
		}
		if isFormatAsJSON {
			printAsJson(&rsp)
		}
	}()

	err = gTrashEnv.init()
	if err != nil {
		return
	}

	absPath := path.Clean(pathStr)
	if absPath == "/" {
		err = errors.New("not supported the root path")
		if !isFormatAsJSON {
			fmt.Println(err.Error())
		}
		return
	}

	if strings.HasPrefix(absPath, "/") == false {
		err = fmt.Errorf("the path[%v] is invalid", pathStr)
		return
	}

	dir, name := path.Split(absPath)
	var (
		parentID uint64
	)
	parentID, err = recoverParentDir(dir)
	if err != nil {
		return
	}

	return doRecoverPath(parentID, name, 0)
}

func doRecoverPath(parentID uint64, name string, dtime int64) (err error) {
	var (
		startTime, endTime int64
	)

	log.LogDebugf("doRecover, isRecursive: %v, pid: %v, Name: %v, start: %v, end: %v",
		isRecursive, parentID, name, startTime, endTime)
	defer func() {
		if err != nil {
			log.LogDebugf("==> doRecover, pid: %v, Name: %v, start: %v, end:%v, err: %v",
				parentID, name, startTime, endTime, err.Error())
		} else {
			log.LogDebugf("==> doRecover, pid: %v, Name: %v, start: %v, end: %v",
				parentID, name, startTime, endTime)
		}
	}()

	if dtime > 0 {
		startTime = dtime
		endTime = dtime
	} else if RegexpFileIsDeleted.MatchString(name) {
		name, startTime, err = parseDeletedName(name)
		if err != nil {
			return
		}
		endTime = startTime
	} else {
		startTime = 0
		endTime = math.MaxInt64
	}

	var dens []*proto.DeletedDentry
	dens, err = gTrashEnv.metaWrapper.LookupDeleted_ll(ctx, parentID, name, startTime, endTime)
	if err != nil && err != syscall.ENOENT {
		err = fmt.Errorf("failed to lookup deleted dentry, pid: %v, Name: %v, starttime: %v, endtime: %v, err: %v",
			parentID, name, startTime, endTime, err.Error())
		return
	} else if err == nil {
		if len(dens) > 1 && !forceFlag {
			msg := fmt.Sprintf("[%v] has multiple deleted records with different timestamps", name)
			if !isFormatAsJSON {
				fmt.Println(msg)
			}
			log.LogError(msg)
			err = errors.New(msg)
			return
		}

		for _, d := range dens {
			err = recover(parentID, d)
			if err == syscall.ENOENT {
				log.LogWarnf("failed to recover pid: %v, dentry: %v, err: %v", parentID, d, err.Error())
				continue
			} else if err != nil {
				log.LogError(err.Error())
				return
			}

			if proto.IsDir(d.Type) == false {
				continue
			}

			err = doRecoverOneDirectory(d)
			if err != nil {
				log.LogError(err.Error())
				return
			}
		}
	} else {
		var (
			ino   uint64
			ftype uint32
		)
		ino, ftype, err = gTrashEnv.metaWrapper.Lookup_ll(ctx, parentID, name)
		if err != nil {
			err = fmt.Errorf("failed to lookup dentry, pid: %v, Name: %v, err: %v",
				parentID, name, err.Error())
			return
		}

		if proto.IsDir(ftype) == false {
			return
		}

		var den proto.DeletedDentry
		den.Inode = ino
		err = doRecoverOneDirectory(&den)
		if err != nil {
			log.LogError(err.Error())
			return
		}
	}

	return
}

func recoverParentDir(p string) (ino uint64, err error) {
	ino = proto.RootIno
	dirs := strings.Split(p, "/")
	var (
		child              uint64
		dentrys            []*proto.DeletedDentry
		startTime, endTime int64
	)
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}

		child, _, err = gTrashEnv.metaWrapper.Lookup_ll(ctx, ino, dir)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf(err.Error())
			return
		} else if err == syscall.ENOENT {
			startTime = 0
			endTime = math.MaxInt64
			if RegexpFileIsDeleted.MatchString(dir) {
				dir, startTime, err = parseDeletedName(dir)
				if err != nil {
					return
				}
				endTime = startTime
			}
			dentrys, err = gTrashEnv.metaWrapper.LookupDeleted_ll(ctx, ino, dir, startTime, endTime)
			if err != nil {
				log.LogErrorf("ino: %v, dir: %v, TS: %v, err: %v", ino, dir, math.MaxInt64, err.Error())
				return
			}
			if len(dentrys) > 1 {
				msg := fmt.Sprintf("This directory[%v] has multiple deleted records with time stamps", dir)
				if !isFormatAsJSON {
					fmt.Println(msg)
				}
				log.LogErrorf(msg)
				err = errors.New(msg)
				return
			}
			child = dentrys[0].Inode
			err = recover(ino, dentrys[0])
			if err != nil {
				log.LogError(err.Error())
				return
			}
		}
		ino = child
	}
	return
}

func doRecoverOneDirectory(den *proto.DeletedDentry) (err error) {
	pid := den.Inode
	defer func() {
		if err != nil {
			log.LogDebugf("doRecoverOneDirectory, isRecursive: %v, dentry: %v, err: %v", isRecursive, den, err.Error())
		} else {
			log.LogDebugf("doRecoverOneDirectory, isRecursive: %v, dentry: %v", isRecursive, den)
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

	inos := make([]uint64, 0, len(children))
	for _, chd := range children {
		inos = append(inos, chd.Inode)
	}

	res := make(map[uint64]int, 0)
	res, err = gTrashEnv.metaWrapper.BatchRecoverDeletedInode(ctx, inos)
	if err != nil {
		log.LogErrorf("doRecoverOneDirectory, failed to BatchRecoverDeletedInode, dentry: %v, err: %v", den, err.Error())
		return
	}

	dens := make([]*proto.DeletedDentry, 0, len(children))
	for _, chd := range children {
		st, ok := res[chd.Inode]
		if ok && st != meta.StatusOK && st != meta.StatusExist {
			continue
		}
		dens = append(dens, chd)
	}
	if len(dens) == 0 {
		return
	}

	res = make(map[uint64]int, 0)
	res, err = gTrashEnv.metaWrapper.BatchRecoverDeletedDentry(ctx, dens)
	if err != nil {
		log.LogErrorf("doRecoverOneDirectory, failed to BatchRecoverDeletedDentry, dentry: %v, err: %v", den, err.Error())
		return
	}
	for _, den := range dens {
		st, ok := res[den.Inode]
		if ok && st != meta.StatusOK && st != meta.StatusExist {
			log.LogDebugf("den: %v, status: %v", den, st)
			continue
		}
		count++
	}

	if !isRecursive {
		return
	}
	for _, chd := range dens {
		if !proto.IsDir(chd.Type) {
			continue
		}
		st, ok := res[chd.Inode]
		if ok && st != meta.StatusOK && st != meta.StatusExist {
			continue
		}
		err = doRecoverOneDirectory(chd)
		if err != nil {
			log.LogErrorf("doRecoverOneDirectory, failed to doRecoverOneDirectory, dentry: %v, err: %v", den, err.Error())
		}
	}

	return
}

func recover(pid uint64, d *proto.DeletedDentry) (err error) {
	defer func() {
		if err != nil {
			log.LogDebugf("recover, pid: %v, dentry: %v, err: %v", pid, d, err.Error())
		} else {
			log.LogDebugf("recover, pid: %v, dentry: %v", pid, d)
		}
	}()
	err = gTrashEnv.metaWrapper.RecoverDeletedInode(ctx, d.Inode)
	if err == syscall.ENOENT {
		log.LogWarnf("recover deleted INode for dentry: %v, pid: %v, err: %v", d, pid, err.Error())
		err = nil
		return
	} else if err != nil {
		err = fmt.Errorf("failed to recover deleted INode for dentry: %v, pid: %v, err: %v", d, pid, err.Error())
		return
	}
	err = gTrashEnv.metaWrapper.RecoverDeletedDentry(ctx, pid, d.Inode, d.Name, d.Timestamp)
	if err != nil && err != syscall.EEXIST {
		err = fmt.Errorf("failed to recover deleted dentry: %v, pid: %v, err: %v", d, pid, err.Error())
		return
	}
	err = nil
	count++
	return
}
