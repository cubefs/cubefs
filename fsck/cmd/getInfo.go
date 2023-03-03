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
	"github.com/spf13/cobra"
	"io/ioutil"
	"net/http"
	"strings"
)

func newInfoCmd() *cobra.Command {
	var c = &cobra.Command{
		Use:   "get",
		Short: "get inode's info",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := getInodeInfo(); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func getInodeInfo() (err error) {
	mps, err := getMetaPartitions(MasterAddr, VolName)
	if err != nil {
		return err
	}

	for _, mp := range mps {
		if mp.Start <= InodeID && InodeID <= mp.End {
			res, er := getExtentsByInode(InodeID, mp)
			if er != nil {
				return er
			}
			dumpInodeInfo(InodeID, mp, res)
			break
		}
	}
	return
}

func getExtentsByInode(ino uint64, mp *proto.MetaPartitionView) (res *proto.GetExtentsResponse, err error) {
	cmdline := fmt.Sprintf("http://%s:%s/getExtentsByInode?pid=%d&ino=%d",
		strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID, ino)
	resp, err := http.Get(cmdline)
	if err != nil {
		return nil, fmt.Errorf("Get request failed: %v %v", cmdline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ReadAll failed: %v", err)
	}

	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(data, body); err != nil {
		return nil, fmt.Errorf("Unmarshal failed: %v", err)
	}

	if body.Code != http.StatusSeeOther {
		return nil, fmt.Errorf("getExtentsByInode failed: code[%v] not %v",
			body.Code, http.StatusSeeOther)
	}

	if strings.Compare(body.Msg, "Ok") != 0 {
		return nil, fmt.Errorf("getExtentsByInode failed: %v", body.Msg)
	}

	if err = json.Unmarshal(body.Data, &res); err != nil {
		return nil, fmt.Errorf("Unmarshal extents failed: %v", err)
	}
	return
}

func dumpInodeInfo(ino uint64, mp *proto.MetaPartitionView, res *proto.GetExtentsResponse) {
	fmt.Printf("Inode: %v\n", ino)
	fmt.Printf("Generation: %v\n", res.Generation)
	fmt.Printf("Size: %v\n", res.Size)
	partitionIds := make([]uint64, 0)
	if len(res.Extents) > 0 {
		fmt.Printf("Extents:\n")
		for i, et := range res.Extents {
			fmt.Printf(" %v. FileOffset:%v, PartitionId:%v, ExtentId:%v, ExtentOffset:%v, Size:%v, CRC:%v\n",
				i, et.FileOffset, et.PartitionId, et.ExtentId, et.ExtentOffset, et.Size, et.CRC)
			partitionIds = append(partitionIds, et.PartitionId)
		}
	}
	fmt.Printf("\nMetaPartition:\n")
	fmt.Printf(" Id:%v\n", mp.PartitionID)
	fmt.Printf(" Leader:%v\n", mp.LeaderAddr)
	fmt.Printf(" Hosts:%v\n", mp.Members)
	if len(partitionIds) > 0 {
		dps, err := getDataPartitions(MasterAddr, VolName)
		if err != nil {
			return
		}
		fmt.Printf("DataPartitions:\n")
		for i, id := range partitionIds {
			for _, dp := range dps {
				if dp.PartitionID == id {
					fmt.Printf(" %v. Id:%v\n", i, id)
					fmt.Printf("    Leader:%v\n", dp.LeaderAddr)
					fmt.Printf("    Hosts:%v\n", dp.Hosts)
					break
				}
			}
		}
	}
}

func getDataPartitions(addr, name string) ([]*proto.DataPartitionResponse, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s%s?name=%s", addr, proto.ClientDataPartitions, name))
	if err != nil {
		return nil, fmt.Errorf("Get data partitions failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Get data partitions read all body failed: %v", err)
	}

	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(data, body); err != nil {
		return nil, fmt.Errorf("Unmarshal data partitions body failed: %v", err)
	}

	var dpv *proto.DataPartitionsView
	if err = json.Unmarshal(body.Data, &dpv); err != nil {
		return nil, fmt.Errorf("Unmarshal data partitions view failed: %v", err)
	}

	return dpv.DataPartitions, nil
}
