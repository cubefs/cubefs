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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
)

const (
	BuffersTotalLimit = 1024 * 1024 // 1M
)

var gMetaWrapper *meta.MetaWrapper

func newCleanCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "clean",
		Short: "clean dirty inode or dentry according to some rules",
		Args:  cobra.MinimumNArgs(0),
	}

	c.AddCommand(
		newCleanInodeCmd(),
		newCleanDentryCmd(),
		newEvictInodeCmd(),
	)

	return c
}

func newCleanInodeCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "inode",
		Short: "clean dirty inode",
		Run: func(cmd *cobra.Command, args []string) {
			if err := Clean("inode"); err != nil {
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
			if err := Clean("dentry"); err != nil {
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
			if err := Clean("evict"); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func Clean(opt string) error {
	// defer log.LogFlush()

	if MasterAddr == "" || VolName == "" {
		return fmt.Errorf("Lack of parameters: master(%v) vol(%v)", MasterAddr, VolName)
	}

	// ump.InitUmp("fsck", "")

	// _, err := log.InitLog("fscklog", "fsck", log.InfoLevel, nil, log.DefaultLogLeftSpaceLimitRatio)
	// if err != nil {
	// 	return fmt.Errorf("Init log failed: %v", err)
	// }

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

	proto.InitBufferPool(BuffersTotalLimit)

	switch opt {
	case "inode":
		err = cleanInodes()
		if err != nil {
			return fmt.Errorf("Clean inodes failed: %v", err)
		}
	case "dentry":
		err = cleanDentries()
		if err != nil {
			return fmt.Errorf("Clean dentries failed: %v", err)
		}
	case "evict":
		err = evictInodes()
		if err != nil {
			return fmt.Errorf("Evict inodes failed: %v", err)
		}
	default:
	}

	return nil
}

func evictInodes() error {
	mps, err := getMetaPartitions(MasterAddr, VolName)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	for _, mp := range mps {
		cmdline := fmt.Sprintf("http://%s:%s/getAllInodes?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID)
		wg.Add(1)
		go evictOnTime(&wg, cmdline)
	}

	wg.Wait()
	return nil
}

func evictOnTime(wg *sync.WaitGroup, cmdline string) {
	defer wg.Done()

	client := &http.Client{Timeout: 0}
	resp, err := client.Get(cmdline)
	if err != nil {
		log.LogErrorf("Get request failed: %v %v", cmdline, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.LogErrorf("Invalid status code: %v", resp.StatusCode)
		return
	}

	log.LogWritef("Dealing with meta partition: %v", cmdline)

	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		inode := &Inode{}
		err = dec.Decode(inode)
		if err != nil {
			log.LogErrorf("Decode inode failed: %v", err)
			return
		}
		doEvictInode(inode)
	}
	log.LogWritef("Done! Dealing with meta partition: %v", cmdline)
}

func cleanInodes() error {
	filePath := fmt.Sprintf("_export_%s/%s", VolName, obsoleteInodeDumpFileName)

	fp, err := os.Open(filePath)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(fp)
	for dec.More() {
		inode := &Inode{}
		if err = dec.Decode(inode); err != nil {
			return err
		}
		doEvictInode(inode)
	}

	return nil
}

func doEvictInode(inode *Inode) error {
	if forceClean {
		_, err := gMetaWrapper.InodeUnlink_ll(inode.Inode, inode.Path)
		if err != nil {
			log.LogWarnf("doEvictInode: unlink inode err(%v)", err)
			return err
		}
		inode.NLink = 0
	}

	if inode.NLink != 0 || time.Since(time.Unix(inode.ModifyTime, 0)) < 24*time.Hour || !proto.IsRegular(inode.Type) {
		log.LogWarnf("doEvictInode: ino(%v) can't be deleted", inode.Inode)
		return nil
	}

	err := gMetaWrapper.Evict(inode.Inode, inode.Path)
	if err != nil {
		log.LogWarnf("doEvictInode: evict inode err(%v)", err)
		if err != syscall.ENOENT {
			return err
		}
	}
	log.LogWarnf("doEvictInode success: ino(%v)", inode)
	return nil
}

func cleanDentries() error {
	// filePath := fmt.Sprintf("_export_%s/%s", VolName, obsoleteDentryDumpFileName)
	// TODO: send request to meta node directly with pino, name and ino.
	return nil
}
