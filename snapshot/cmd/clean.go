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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ump"
)

// TODO: Support record full path for snapshot tool
const snapshotFullPath = "(Snapshot Cmd Unsupported)"

var gMetaWrapper *meta.MetaWrapper

var getSpan = proto.SpanFromContext

func newCtx() context.Context {
	_, ctx := proto.SpanContextPrefix("snapshot-")
	return ctx
}

func newCleanCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "clean",
		Short: "clean snapshot related inode or dentry according to some rules",
		Args:  cobra.MinimumNArgs(0),
	}

	c.AddCommand(
		newCleanSnapshotCmd(),
		newCleanInodeCmd(),
		newCleanDentryCmd(),
		newEvictInodeCmd(),
	)

	return c
}

func newCleanSnapshotCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "snapshot",
		Short: "clean snapshot",
		Run: func(cmd *cobra.Command, args []string) {
			if err := Clean("snapshot", args); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func newCleanInodeCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "inode",
		Short: "clean dirty inode",
		Run: func(cmd *cobra.Command, args []string) {
			if err := Clean("inode", args); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func newCleanDentryCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "dentry",
		Short: "clean dirty dentry",
		Run: func(cmd *cobra.Command, args []string) {
			if err := Clean("dentry", args); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func newEvictInodeCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "evict",
		Short: "clean dirty dentry",
		Run: func(cmd *cobra.Command, args []string) {
			if err := Clean("evict", args); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func Clean(opt string, args []string) error {
	if MasterAddr == "" || VolName == "" {
		return fmt.Errorf("Lack of parameters: master(%v) vol(%v)", MasterAddr, VolName)
	}

	ump.InitUmp("snapshot", "")

	log.SetOutputLevel(log.Ldebug)
	log.SetOutput(&lumberjack.Logger{
		Filename: path.Join("snapshotlog", "snapshot", "snapshot.log"),
		MaxSize:  1024, MaxAge: 7, MaxBackups: 7, LocalTime: true, Compress: true,
	})

	masters := strings.Split(MasterAddr, meta.HostsSeparator)
	metaConfig := &meta.MetaConfig{
		Volume:  VolName,
		Masters: masters,
	}

	var err error
	gMetaWrapper, err = meta.NewMetaWrapper(metaConfig)
	if err != nil {
		return fmt.Errorf("NewMetaWrapper failed: %v", err)
	}

	ctx := newCtx()
	switch opt {
	case "snapshot":
		err = cleanSnapshot(ctx)
		if err != nil {
			return fmt.Errorf("Clean snapshot failed: %v", err)
		}
	case "dentry":
		err = cleanDentries(ctx)
		if err != nil {
			return fmt.Errorf("Clean dentries failed: %v", err)
		}
	case "evict":
		err = evictInodes(ctx)
		if err != nil {
			return fmt.Errorf("Evict inodes failed: %v", err)
		}
	default:
	}

	return nil
}

func evictInodes(ctx context.Context) error {
	mps, err := getMetaPartitions(MasterAddr, VolName)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	for _, mp := range mps {
		cmdline := fmt.Sprintf("http://%s:%s/getAllInodes?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID)
		wg.Add(1)
		go evictOnTime(ctx, &wg, cmdline)
	}

	wg.Wait()
	return nil
}

func evictOnTime(ctx context.Context, wg *sync.WaitGroup, cmdline string) {
	defer wg.Done()

	span := getSpan(ctx)

	client := &http.Client{Timeout: 0}
	resp, err := client.Get(cmdline)
	if err != nil {
		span.Errorf("Get request failed: %v %v", cmdline, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		span.Errorf("Invalid status code: %v", resp.StatusCode)
		return
	}

	span.Infof("Dealing with meta partition: %v", cmdline)

	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		inode := &Inode{}
		err = dec.Decode(inode)
		if err != nil {
			span.Errorf("Decode inode failed: %v", err)
			return
		}
		doEvictInode(ctx, inode)
	}
	span.Infof("Done! Dealing with meta partition: %v", cmdline)
}

func doEvictInode(ctx context.Context, inode *Inode) error {
	if inode.NLink != 0 || time.Since(time.Unix(inode.ModifyTime, 0)) < 24*time.Hour || !proto.IsRegular(inode.Type) {
		return nil
	}
	err := gMetaWrapper.Evict(ctx, inode.Inode, snapshotFullPath)
	if err != nil {
		if err != syscall.ENOENT {
			return err
		}
	}
	getSpan(ctx).Infof("%v", inode)
	return nil
}

func doUnlinkInode(ctx context.Context, inode *Inode) error {
	/*
	 * Do clean inode with the following exceptions:
	 * 1. nlink == 0, might be a temorary inode
	 * 2. size == 0 && ctime is close to current time, might be in the process of file creation
	 */
	if inode.NLink == 0 ||
		(inode.Size == 0 &&
			time.Unix(inode.CreateTime, 0).Add(24*time.Hour).After(time.Now())) {
		return nil
	}

	_, err := gMetaWrapper.InodeUnlink_ll(ctx, inode.Inode, snapshotFullPath)
	if err != nil {
		if err != syscall.ENOENT {
			return err
		}
		err = nil
	}

	err = gMetaWrapper.Evict(ctx, inode.Inode, snapshotFullPath)
	if err != nil {
		if err != syscall.ENOENT {
			return err
		}
	}

	return nil
}

func cleanDentries(ctx context.Context) error {
	//filePath := fmt.Sprintf("_export_%s/%s", VolName, obsoleteDentryDumpFileName)
	// TODO: send request to meta node directly with pino, name and ino.
	//if ino, err := gMetaWrapper.Delete_ll_EX(1, child.Name, true, VerSeq); err != nil {
	//
	//}
	return nil
}

func readSnapshot(ctx context.Context) (err error) {
	span := getSpan(ctx).WithOperation("readSnapshot")
	span.Infof("vol %v verSeq %v", VolName, VerSeq)
	if VerSeq == 0 {
		VerSeq = math.MaxUint64
	}
	var (
		parents []proto.Dentry
		ino     *proto.InodeInfo
	)

	span.Debugf("ReadDirLimit_ll parent root verSeq %v", VerSeq)
	parents, err = gMetaWrapper.ReadDirLimitForSnapShotClean(ctx, 1, "", math.MaxUint64, VerSeq, false) // one more for nextMarker
	if err != nil && err != syscall.ENOENT {
		span.Errorf("parent root verSeq %v err %v", VerSeq, err)
		return err
	}
	for _, child := range parents {
		span.Debugf("parent root Delete_ll_EX child name %v verSeq %v ino %v success", child.Name, VerSeq, ino)
	}
	return
}

func cleanSnapshot(ctx context.Context) (err error) {
	span := getSpan(ctx).WithOperation("cleanSnapshot")
	span.Infof("vol %v verSeq %v", VolName, VerSeq)
	if VerSeq == 0 {
		VerSeq = math.MaxUint64
	}
	var (
		children []proto.Dentry
		parents  []proto.Dentry
		ino      *proto.InodeInfo
	)
	// return readSnapshot()

	span.Debugf("ReadDirLimit_ll parent root verSeq %v", VerSeq)
	parents, err = gMetaWrapper.ReadDirLimitForSnapShotClean(ctx, 1, "", math.MaxUint64, VerSeq, false) // one more for nextMarker
	if err != nil && err != syscall.ENOENT {
		span.Errorf("parent root verSeq %v err %v", VerSeq, err)
		return err
	}

	if err == syscall.ENOENT {
		span.Warnf("parent root verSeq %v found nothing", VerSeq)
		return nil
	}

	span.Debugf("parent root verSeq %v Delete_ll_EX children count %v", VerSeq, len(parents))
	for _, child := range parents {
		fullPath := "/" + child.Name
		if ino, err = gMetaWrapper.Delete_Ver_ll(ctx, 1, child.Name, proto.IsDir(child.Type), VerSeq, fullPath); err != nil {
			span.Errorf("parent root Delete_ll_EX child name %v verSeq %v err %v", child.Name, VerSeq, err)
		}
		span.Debugf("parent root Delete_ll_EX child name %v verSeq %v ino %v success", child.Name, VerSeq, ino)
	}

	for idx := 0; idx < len(parents); idx++ {
		parent := parents[idx]
		if proto.IsDir(parent.Type) {
			span.Debugf("try loop delete dir %v %v with verSeq %v", parent.Inode, parent.Name, VerSeq)
			children, err = gMetaWrapper.ReadDirLimitForSnapShotClean(ctx, parent.Inode, "", math.MaxUint64, VerSeq, false)
			if err != nil && err != syscall.ENOENT {
				span.Errorf("parent %v verSeq %v err %v", parent.Name, VerSeq, err)
				return err
			}
			if err == syscall.ENOENT {
				span.Warnf("parent %v verSeq %v found nothing", parent.Name, VerSeq)
				continue
			}
			for _, child := range children {
				if ino, err = gMetaWrapper.Delete_Ver_ll(ctx, parent.Inode, child.Name, proto.IsDir(child.Type), VerSeq, snapshotFullPath); err != nil || ino == nil {
					span.Errorf("parent %v Delete_ll_EX child name %v verSeq %v err %v", parent.Name, child.Name, VerSeq, err)
				} else {
					span.Debugf("parent %v Delete_ll_EX child name %v verSeq %v ino %v success", parent.Name, child.Name, VerSeq, ino)
				}
			}
			parents = append(parents, children...)
		} else {
			span.Debugf("file %v %v is not dir", parent.Inode, parent.Name)
		}
	}
	return nil
}
