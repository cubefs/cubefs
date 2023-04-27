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
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
	"sync"
	"sync/atomic"
)

type StatResp struct {
	Code uint32
	Msg string
	Stats map[string]*proto.DeletedFileInfo
}

func printAsJson(v interface{}) {
	data, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(string(data))
}

func newStatCmd(client *master.MasterClient) *cobra.Command {
	var vol string
	var c = &cobra.Command{
		Use:   "stat",
		Short: "stat the information of INode/Dentry deleted",
		Run: func(cmd *cobra.Command, args []string) {
			err := newTrashEnv(client, vol)
			if err != nil {
				return
			}
			var resp StatResp
			defer func() {
				if isFormatAsJSON {
					printAsJson(&resp)
				}
			}()
			err, rsp := stat()
			if err != nil {
				resp.Code = 1
				resp.Msg =err.Error()
				return
			}
			resp.Code = 0
			resp.Stats = rsp
		},
	}
	c.Flags().StringVarP(&vol, "vol", "v", "", "volume Name")
	c.MarkFlagRequired("vol")
	c.Flags().BoolVarP(&isFormatAsJSON, "json", "j", false, "output as json ")

	return c
}

func stat() (error, map[string]*proto.DeletedFileInfo) {
	mps, err := gTrashEnv.masterClient.ClientAPI().GetMetaPartitions(gTrashEnv.VolName)
	if err != nil {
		log.LogErrorf("failed to get meta partitions from %v , err: %v", gTrashEnv, err.Error())
		return err, nil
	}

	res, err := statDeletedFileInfoFromMP(mps)
	if err != nil {
		log.LogErrorf("failed to get deleted file info %v, err: %v", gTrashEnv, err.Error())
		return err, nil
	}

	if !isFormatAsJSON {
		fmt.Printf("%-4s %-15s %15s %15s %15s \n", "Seq", "Date", "DelInodeCount", "DelDentryCount", "DataSize(Bytes)")
	}
	seq := 0
	for day := range res {
		printShortLine()
		seq++
		info, _ := res[day]
		if !isFormatAsJSON {
			fmt.Printf("%04d %-15s %15d %15d %15d \n", seq, day, info.InodeSum, info.DentrySum, info.Size)
		}
	}
	printShortLine()
	return nil, res
}

func printShortLine() {
	if isFormatAsJSON {
		return
	}
	fmt.Println("----------------------------------------------------------------------------------------------")
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
