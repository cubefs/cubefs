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
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/spf13/cobra"
	"sync"
	"sync/atomic"
)

func newStatCmd(client *master.MasterClient) *cobra.Command {
	var vol string
	var c = &cobra.Command{
		Use:   "stat",
		Short: "stat the information of inode/dentry deleted",
		Run: func(cmd *cobra.Command, args []string) {
			err := newTrashEnv(client, vol)
			if err != nil {
				return
			}
			err = stat()
			if err != nil {
				return
			}
		},
	}
	c.Flags().StringVarP(&vol, "vol", "v", "", "volume name")
	c.MarkFlagRequired("vol")

	return c
}

func stat() error {
	mps, err := gTrashEnv.masterClient.ClientAPI().GetMetaPartitions(gTrashEnv.VolName)
	if err != nil {
		log.LogErrorf("failed to get meta partitions from %v , err: %v", gTrashEnv, err.Error())
		return err
	}

	res, err := statDeletedFileInfoFromMP(mps)
	if err != nil {
		log.LogErrorf("failed to get deleted file info %v, err: %v", gTrashEnv, err.Error())
		return err
	}

	fmt.Printf("%-4s %-15s %15s %15s %15s \n", "Seq", "Date", "DelInodeCount", "DelDentryCount", "DataSize(Bytes)")
	seq := 0
	for day := range res {
		fmt.Println("----------------------------------------------------------------------------------------------")
		seq++
		info, _ := res[day]
		fmt.Printf("%04d %-15s %15d %15d %15d \n", seq, day, info.InodeSum, info.DentrySum, info.Size)
	}
	fmt.Println("----------------------------------------------------------------------------------------------")
	return nil
}

func statDeletedFileInfoFromMP(views []*proto.MetaPartitionView) (resp map[string]*proto.DeletedFileInfo, err error) {
	var (
		wg         sync.WaitGroup
		concurrent int
		fails      int32
	)

	mptLen := len(views)
	mpChan := make(chan *proto.MetaPartitionView, mptLen)
	dataChan := make(chan *proto.StatDeletedFileInfoResponse, mptLen)
	for _, mp := range views {
		mpChan <- mp
	}

	if mptLen > 50 {
		concurrent = 50
	} else {
		concurrent = mptLen
	}

	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
			}()
			for {
				select {
				case mp := <-mpChan:
					if atomic.LoadInt32(&fails) > 0 {
						return
					}
					res, err := gTrashEnv.metaWrapper.StatDeletedFileInfo(ctx, mp.PartitionID)
					if err != nil {
						log.LogErrorf("failed to stat deleted file info from mp: %v", mp)
						atomic.AddInt32(&fails, 1)
						return
					}
					dataChan <- res
				default:
					return
				}
			}
		}()
	}
	wg.Wait()
	close(mpChan)

	if mptLen != len(dataChan) {
		err = fmt.Errorf("failed to get deleted file info, expected [%d], got [%d]", mptLen, len(dataChan))
		return
	}

	resp = make(map[string]*proto.DeletedFileInfo, 0)
	for {
		select {
		case item := <-dataChan:
			for day := range item.StatInfo {
				dayinfo, _ := item.StatInfo[day]
				info, ok := resp[day]
				if !ok {
					info = dayinfo
					resp[day] = info
				} else {
					info.Size += dayinfo.Size
					info.InodeSum += dayinfo.InodeSum
					info.DentrySum += dayinfo.DentrySum
				}
			}
		default:
			goto end
		}
	}
end:
	close(dataChan)
	return
}
