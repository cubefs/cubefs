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
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
	"math"
	"path"
	"strings"
	"syscall"
	"time"
)

const (
	listHeadFormat = "%-6s %-60s %10s %-2s %10s %-9s %-15s %-22s\n"
	listDataFormat = "%06d %-60s %10d %-4s %10d %-9s %-15s %-22s\n"
)

type ListResp struct {
	Code uint32
	Msg string
	Rows  []*listRow
	DelRows  []*listRow
}

func newFSListCmd(client *master.MasterClient) *cobra.Command {
	var vol string
	var c = &cobra.Command{
		Use:   "ls [path]",
		Short: "list a directory or file",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err error
				rows []*listRow
				delRows []*listRow
			)
			defer func() {
				if !isFormatAsJSON {
					return
				}
				var rsp ListResp
				if err != nil {
					rsp.Code = 1
					rsp.Msg = err.Error()
				} else {
					rsp.Code = 0
				}
				rsp.Rows = rows
				rsp.DelRows = delRows
				printAsJson(&rsp)
			}()
			err = newTrashEnv(client, vol)
			if err != nil {
				return
			}
			if isFormatAsJSON {
				err, rows, delRows = ListPath(args[0], true)
				if err != nil {
					return
				}
				return
			}
			err, _, _ = ListPath(args[0], false)
			if err != nil {
				if !isFormatAsJSON {
					fmt.Println(err)
				}
			}
		},
	}
	c.Flags().StringVarP(&vol, "vol", "v", "", "volume Name")
	c.MarkFlagRequired("vol")
	c.Flags().BoolVarP(&isFormatAsJSON, "json", "j", false, "output as json ")
	return c
}

func ListPath(pathStr string, isTest bool) (err error, rows, delRows []*listRow) {
	defer func() {
		printLine()
	}()

	absPath := path.Clean(pathStr)
	if absPath == "/" {
		printHead()
		return ListNormalPath(proto.RootIno, "", isTest)
	}

	if strings.HasPrefix(absPath, "/") == false {
		err = fmt.Errorf("the path[%v] is invalid", pathStr)
		if !isFormatAsJSON {
			fmt.Println(err.Error())
		}
		return
	}

	dir, name := path.Split(absPath)
	var (
		parentID  uint64
		isDeleted bool
	)
	parentID, isDeleted, _, err = lookupPath(dir)
	if err != nil {
		log.LogErrorf("failed to lookup pathStr: %v, err: %v\n", pathStr, err.Error())
		return
	}

	printHead()
	if isDeleted {
		return ListDeletedPath(parentID, name, isTest)
	}
	return ListNormalPath(parentID, name, isTest)
}

func ListNormalPath(parentID uint64, name string, isTest bool) (
	err error, rows, delRows []*listRow) {
	var (
		ino  uint64 = parentID
		mode uint32
	)
	defer func() {
		if err != nil {
			log.LogDebugf("ListNormalPath, pid: %v, Name: %v, err: %v", parentID, name, err.Error())
		} else {
			log.LogDebugf("ListNormalPath, pid: %v, Name: %v", parentID, name)
		}
	}()
	if len(name) > 0 {
		ino, mode, err = gTrashEnv.metaWrapper.Lookup_ll(ctx, parentID, name)
		if err != nil && err != syscall.ENOENT {
			log.LogErrorf("failed to get INode by pathStr: %v, err: %v\n", name, err.Error())
			return
		} else if err == syscall.ENOENT {
			return ListDeletedPath(parentID, name, isTest)
		}
		if isTest {
			rows = make([]*listRow, 0)
			delRows = make([]*listRow, 0)
		}
		if proto.IsDir(mode) == false {
			var inode *proto.InodeInfo
			inode, err = gTrashEnv.metaWrapper.InodeGet_ll(ctx, ino)
			if err != nil {
				log.LogErrorf("ListNormalPath, not found the INode of  %v, Name: %v, err: %v\n", ino, name, err.Error())
				return
			}
			if isTest {
				row := new(listRow)
				row.Seq = 1
				row.Name = name
				row.INode = inode.Inode
				row.FileType = "-"
				row.Size = inode.Size
				row.IsDel = "-"
				row.TS = "-"
				rows = append(rows, row)
			}
			if !isFormatAsJSON {
				fmt.Printf(listDataFormat, 1, name, inode.Inode, "-", inode.Size, "-", "-", "-")
			}
			return
		}
	}
	if isTest {
		rows = make([]*listRow, 0)
		delRows = make([]*listRow, 0)
	}

	var dentrys []proto.Dentry
	dentrys, err = gTrashEnv.metaWrapper.ReadDir_ll(ctx, ino)
	if err != nil {
		log.LogErrorf("failed to readdir: %v, env: %v, err: %v", ino, gTrashEnv, err.Error())
		return
	}

	inos := make([]uint64, len(dentrys))
	for _, dentry := range dentrys {
		inos = append(inos, dentry.Inode)
	}
	inodes := gTrashEnv.metaWrapper.BatchInodeGet(ctx, inos)
	if inodes == nil {
		err = fmt.Errorf("failed to batch get INode from %v", ino)
		return
	}

	inodesMap := make(map[uint64]*proto.InodeInfo, 0)
	for _, ino := range inodes {
		inodesMap[ino.Inode] = ino
	}

	seq := 0
	for _, dentry := range dentrys {
		ino, ok := inodesMap[dentry.Inode]
		if !ok {
			continue
		}
		seq++
		if isTest {
			row := new(listRow)
			row.Seq = seq
			row.Name = dentry.Name
			row.INode = ino.Inode
			row.FileType = getPathType(dentry.Type)
			row.Size = ino.Size
			row.IsDel = "-"
			row.TS = "-"
			rows = append(rows, row)
		}
		if !isFormatAsJSON {
			fmt.Printf(listDataFormat, seq, dentry.Name, dentry.Inode, getPathType(dentry.Type), ino.Size, "-", "-", "-")
		}
	}

	var ddentrys []*proto.DeletedDentry
	ddentrys, err = gTrashEnv.metaWrapper.ReadDirDeleted(ctx, ino)
	if len(ddentrys) == 0 {
		return
	}
	var delInfos map[uint64]*proto.DeletedInodeInfo
	delInfos, err = batchGetDeletedInode(ddentrys)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	printLine()
	for _, dentry := range ddentrys {
		ino, ok := delInfos[dentry.Inode]
		if !ok {
			msg := fmt.Sprintf("miss INode for dentry: %v", dentry)
			log.LogWarnf(msg)
			continue
		}

		seq++
		if isTest {
			row := new(listRow)
			row.Seq = seq
			row.Name = dentry.AppendTimestampToName()
			row.INode = ino.Inode
			row.FileType = getPathType(dentry.Type)
			row.Size = ino.Size
			row.IsDel = "true"
			row.TS = getTimeStr(dentry.Timestamp)
			delRows = append(delRows, row)
		}
		log.LogDebugf("%v, %v", dentry, ino)
		if !isFormatAsJSON {
			fmt.Printf(listDataFormat, seq, dentry.AppendTimestampToName(), ino.Inode,
				getPathType(dentry.Type), ino.Size, "true", dentry.From, getTimeStr(dentry.Timestamp))
		}

	}
	return
}

func ListDeletedPath(parentID uint64, name string, isTest bool) (
	err error, rows, delRows []*listRow) {
	var (
		startTime, endTime int64
	)

	if isTest {
		rows = make([]*listRow, 0)
		delRows = make([]*listRow, 0)
	}

	if RegexpFileIsDeleted.MatchString(name) {
		name, startTime, err = parseDeletedName(name)
		if err != nil {
			return
		}
		endTime = startTime
	} else {
		startTime = 0
		endTime = math.MaxInt64
	}

	var dentrys []*proto.DeletedDentry
	dentrys, err = gTrashEnv.metaWrapper.LookupDeleted_ll(ctx, parentID, name, startTime, endTime)
	if err != nil {
		log.LogErrorf("ino: %v, dir: %v, start:%v, end: %v, err: %v", parentID, name, startTime, endTime, err.Error())
		return
	}

	if len(dentrys) > 1 {
		msg := fmt.Sprintf("This directory[%v] has multiple deleted records with time stamps", name)
		if !isFormatAsJSON {
			fmt.Println(msg)
		}
		log.LogError(msg)
		err = errors.New(msg)
		return
	}

	if proto.IsDir(dentrys[0].Type) == false {
		var ino *proto.DeletedInodeInfo
		ino, err = gTrashEnv.metaWrapper.GetDeletedInode(ctx, dentrys[0].Inode)
		if err != nil {
			log.LogErrorf("failed to get INode by pathStr: %v, err: %v\n", name, err.Error())
			return
		}
		if isTest {
			row := new(listRow)
			row.Seq = 1
			row.Name = dentrys[0].AppendTimestampToName()
			row.INode = ino.Inode
			row.FileType = getPathType(dentrys[0].Type)
			row.Size = ino.Size
			row.IsDel = "true"
			row.TS = getTimeStr(dentrys[0].Timestamp)
			delRows = append(delRows, row)
		}
		log.LogDebugf("%v, %v", dentrys[0], ino)
		if !isFormatAsJSON {
			fmt.Printf(listDataFormat, 1, dentrys[0].AppendTimestampToName(),
				dentrys[0].Inode, "-", ino.Size, "true", dentrys[0].From, getTimeStr(dentrys[0].Timestamp))
		}
		return
	}

	seq := 0
	var ddentrys []*proto.DeletedDentry
	ddentrys, err = gTrashEnv.metaWrapper.ReadDirDeleted(ctx, dentrys[0].Inode)
	if err != nil {
		log.LogError(err.Error())
		return
	}

	if len(ddentrys) == 0 {
		return
	}
	var delInfos map[uint64]*proto.DeletedInodeInfo
	delInfos, err = batchGetDeletedInode(ddentrys)
	if err != nil {
		log.LogError(err.Error())
		return
	}
	for _, dentry := range ddentrys {
		ino, ok := delInfos[dentry.Inode]
		if !ok {
			err = fmt.Errorf("miss INode for dentry: %v", dentry)
			return
		}

		seq++
		if isTest {
			row := new(listRow)
			row.Seq = seq
			row.Name = dentry.AppendTimestampToName()
			row.INode = ino.Inode
			row.FileType = getPathType(dentry.Type)
			row.Size = ino.Size
			row.IsDel = "true"
			row.TS = getTimeStr(dentry.Timestamp)
			delRows = append(delRows, row)
		}
		log.LogDebugf("%v, %v", dentrys[0], ino)
		if !isFormatAsJSON {
			fmt.Printf(listDataFormat, seq, dentry.AppendTimestampToName(), ino.Inode,
				getPathType(dentry.Type), ino.Size, "true", dentry.From, getTimeStr(dentry.Timestamp))
		}
	}
	return
}

func lookupPath(p string) (ino uint64, isDeleted bool, timestamp int64, err error) {
	ino = proto.RootIno
	dirs := strings.Split(p, "/")
	isDeleted = false
	var (
		child              uint64
		dentrys            []*proto.DeletedDentry
		startTime, endTime int64
	)
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}

		if isDeleted {
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
				log.LogErrorf(err.Error())
				return
			}
			if len(dentrys) > 1 {
				msg := fmt.Sprintf("This directory[%v] has multiple deleted records with time stamps", dir)
				if !isFormatAsJSON {
					fmt.Println(msg)
				}
				log.LogErrorf(msg)
				return
			}
			child = dentrys[0].Inode
			timestamp = dentrys[0].Timestamp
		} else {
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
				isDeleted = true
				child = dentrys[0].Inode
				timestamp = dentrys[0].Timestamp
				log.LogDebugf("lookupPath, %v", dentrys[0])
			}
		}
		ino = child
	}
	return
}

func parseDeletedName(str string) (name string, ts int64, err error) {
	index := strings.LastIndex(str, "_")
	name = str[0:index]
	date := str[index+1:]
	loc, _ := time.LoadLocation("Local")
	var tm time.Time
	tm, err = time.ParseInLocation(dentryNameTimeFormat, date, loc)
	if err != nil {
		log.LogErrorf("parseDeletedName: str: %v, err: %v", str, err.Error())
		return
	}
	ts = tm.UnixNano() / 1000
	return
}

func getTimeStr(ts int64) string {
	return time.Unix(ts/1000/1000, ts%1000000*1000).Format(dentryNameTimeFormat)
}

func getPathType(t uint32) string {
	if proto.IsDir(t) {
		return "d"
	}
	return "-"
}

func batchGetDeletedInode(dentrys []*proto.DeletedDentry) (
	delInfos map[uint64]*proto.DeletedInodeInfo, err error) {
	inos := make([]uint64, len(dentrys))
	for _, dentry := range dentrys {
		inos = append(inos, dentry.Inode)
		log.LogDebugf("batchGetDeletedInode, ino:%v", dentry.Inode)
	}
	delInfos = gTrashEnv.metaWrapper.BatchGetDeletedInode(ctx, inos)
	// the hard link, has two dentrys and one INode
	/*
		if len(delInfos) != len(dentrys) {
			err = fmt.Errorf("miss some inodes from BatchGetDeletedInode, expect: [%v], real: %v",
				len(dentrys), len(delInfos))
			return
		}
	*/
	return
}

func printHead() {
	if isFormatAsJSON {
		return
	}
	fmt.Printf(listHeadFormat, "Seq", "Name", "Inode", "Type", "Size", "IsDeleted", "From", "DeleteTime")
	printLine()
}

func printLine() {
	if isFormatAsJSON {
		return
	}
	fmt.Println("------------------------------------------------" +
		"-------------------------------------------------------" +
		"----------------------------------------------")
}
