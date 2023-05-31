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
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
)

const (
	MaxFollowPathTaskNum = 120
)

type Summary struct {
	files uint64
	dirs  uint64
	bytes uint64
}

func newInfoCmd() *cobra.Command {
	var c = &cobra.Command{
		Use:   "get",
		Short: "get info of specified inode",
		Args:  cobra.MinimumNArgs(0),
	}

	c.AddCommand(
		newGetLocationsCmd(),
		newGetPathCmd(),
		newGetSummaryCmd(),
	)

	return c
}

func newGetLocationsCmd() *cobra.Command {
	var c = &cobra.Command{
		Use:   "locations",
		Short: "get inode's locations",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := getInodeLocations(); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func newGetPathCmd() *cobra.Command {
	var c = &cobra.Command{
		Use:   "path",
		Short: "get inode's path",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := getInodePath(); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func newGetSummaryCmd() *cobra.Command {
	var c = &cobra.Command{
		Use:   "summary",
		Short: "get inode's summary",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if err := getInodeSummary(); err != nil {
				fmt.Println(err)
			}
		},
	}
	return c
}

func getInodeLocations() (err error) {
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
			dumpInodeLocations(InodeID, mp, res)
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

func dumpInodeLocations(ino uint64, mp *proto.MetaPartitionView, res *proto.GetExtentsResponse) {
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

func getInodePath() (err error) {
	imap := make(map[uint64]*Inode)
	if err = buildInodeMap(imap); err != nil {
		return
	}
	if InodeID != 0 {
		fmt.Printf("Inode: %v, Valid: %v\n", InodeID, imap[InodeID].Valid)
		fmt.Printf("Path: %v\n", imap[InodeID].Path)
	} else {
		// dump all inode path
		err = dumpAllInodePath(imap, fmt.Sprintf("%s_%s", VolName, pathDumpFileName))
		if err != nil {
			return err
		}
	}
	return
}

func buildInodeMap(imap map[uint64]*Inode) (err error) {
	mps, err := getMetaPartitions(MasterAddr, VolName)
	if err != nil {
		return err
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make([]error, 0)
	wg.Add(len(mps))
	for _, m := range mps {
		mp := m
		go func() {
			defer wg.Done()
			if e := getInodesFromMp(mp, imap, &mu); e != nil {
				mu.Lock()
				errs = append(errs, e)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		return errs[0]
	}

	wg.Add(len(mps))
	for _, m := range mps {
		mp := m
		go func() {
			defer wg.Done()
			if e := getDentriesFromMp(mp, imap, &mu); e != nil {
				mu.Lock()
				errs = append(errs, e)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		return errs[0]
	}

	root, ok := imap[1]
	if !ok {
		err = fmt.Errorf("No root inode")
		return
	}

	var taskNum int32 = 0
	wg.Add(1)
	atomic.AddInt32(&taskNum, 1)
	traversePath(imap, root, &wg, &taskNum, true)
	wg.Wait()

	root.Path = "/"

	return
}

func getInodesFromMp(mp *proto.MetaPartitionView, imap map[uint64]*Inode, mu *sync.Mutex) (err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%s/getInodeSnapshot?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID))
	if err != nil {
		return fmt.Errorf("Get request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	reader := bufio.NewReaderSize(resp.Body, 4*1024*1024)
	inoBuf := make([]byte, 4)
	for {
		inoBuf = inoBuf[:4]
		// first read length
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadInode] ReadHeader: %s", err.Error())
			return
		}
		length := binary.BigEndian.Uint32(inoBuf)

		// next read body
		if uint32(cap(inoBuf)) >= length {
			inoBuf = inoBuf[:length]
		} else {
			inoBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, inoBuf)
		if err != nil {
			err = errors.NewErrorf("[loadInode] ReadBody: %s", err.Error())
			return
		}
		inode := &Inode{Dens: make([]*Dentry, 0)}
		if err = decodeInode(inoBuf, inode); err != nil {
			err = errors.NewErrorf("[loadInode] Unmarshal: %s", err.Error())
			return
		}

		mu.Lock()
		imap[inode.Inode] = inode
		mu.Unlock()
	}
}

func getDentriesFromMp(mp *proto.MetaPartitionView, imap map[uint64]*Inode, mu *sync.Mutex) (err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%s/getDentrySnapshot?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID))
	if err != nil {
		return fmt.Errorf("Get request failed: %v %v", resp, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	reader := bufio.NewReaderSize(resp.Body, 4*1024*1024)
	dentryBuf := make([]byte, 4)
	for {
		dentryBuf = dentryBuf[:4]
		// First Read 4byte header length
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			err = errors.NewErrorf("[loadDentry] ReadHeader: %s", err.Error())
			return
		}

		length := binary.BigEndian.Uint32(dentryBuf)

		// next read body
		if uint32(cap(dentryBuf)) >= length {
			dentryBuf = dentryBuf[:length]
		} else {
			dentryBuf = make([]byte, length)
		}
		_, err = io.ReadFull(reader, dentryBuf)
		if err != nil {
			err = errors.NewErrorf("[loadDentry]: ReadBody: %s", err.Error())
			return
		}
		den := &Dentry{}
		if err = decodeDentry(dentryBuf, den); err != nil {
			err = errors.NewErrorf("[loadDentry] Unmarshal: %s", err.Error())
			return
		}

		inode, ok := imap[den.ParentId]
		if ok {
			inode.Dens = append(inode.Dens, den)
		}
	}
}

func traversePath(imap map[uint64]*Inode, inode *Inode, wg *sync.WaitGroup, taskNum *int32, newTask bool) {
	defer func() {
		if newTask {
			atomic.AddInt32(taskNum, -1)
			wg.Done()
		}
	}()
	inode.Valid = true
	// there is no down path for file inode
	if inode.Type == 0 || len(inode.Dens) == 0 {
		return
	}

	for _, den := range inode.Dens {
		childInode, ok := imap[den.Inode]
		if !ok {
			continue
		}
		den.Valid = true
		childInode.Path = inode.Path + "/" + den.Name
		if proto.IsRegular(childInode.Type) {
			inode.Bytes += childInode.Size
			inode.Files++
		} else if proto.IsDir(childInode.Type) {
			inode.Dirs++
		} else if proto.IsSymlink(childInode.Type) {
			inode.Files++
		}
		if atomic.LoadInt32(taskNum) < MaxFollowPathTaskNum {
			wg.Add(1)
			atomic.AddInt32(taskNum, 1)
			go traversePath(imap, childInode, wg, taskNum, true)
		} else {
			traversePath(imap, childInode, wg, taskNum, false)
		}
	}
}

func dumpAllInodePath(imap map[uint64]*Inode, name string) error {
	fp, err := os.Create(name)
	if err != nil {
		return err
	}
	defer fp.Close()

	for _, inode := range imap {
		context := fmt.Sprintf("Inode: %v, Valid: %v, Path: %v\n", inode.Inode, inode.Valid, inode.Path)
		if _, err = fp.WriteString(context); err != nil {
			return err
		}
	}
	return nil
}

func getInodeSummary() (err error) {
	imap := make(map[uint64]*Inode)
	if err = buildInodeMap(imap); err != nil {
		return
	}

	start := time.Now()
	inode := imap[InodeID]
	if proto.IsRegular(inode.Type) {
		fmt.Printf("Inode: %v, Valid: %v\n", InodeID, inode.Valid)
		fmt.Printf("Path(is file): %v, Bytes: %v\n", inode.Path, inode.Bytes)
	} else if proto.IsDir(inode.Type) {
		files, dirs, bytes := dirSummary(imap, inode)
		fmt.Printf("Summary cost: %v\n", time.Since(start))
		fmt.Printf("Inode: %v, Valid: %v\n", InodeID, inode.Valid)
		fmt.Printf("Path(is dir): %v, Files: %v, Dirs: %v, Bytes: %v\n", inode.Path, files, dirs, bytes)
	} else {
		return fmt.Errorf("inode type: %v not support", inode.Type)
	}
	return
}

func dirSummary(imap map[uint64]*Inode, inode *Inode) (files, dirs, bytes uint64) {
	if len(inode.Dens) == 0 {
		return
	}
	files = inode.Files
	dirs = inode.Dirs
	bytes = inode.Bytes
	for _, den := range inode.Dens {
		if proto.IsDir(den.Type) {
			f, d, b := dirSummary(imap, imap[den.Inode])
			files += f
			dirs += d
			bytes += b
		}
	}
	return
}

func decodeDentry(raw []byte, d *Dentry) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	keyBuff := bytes.NewBuffer(keyBytes)
	if err = binary.Read(keyBuff, binary.BigEndian, &d.ParentId); err != nil {
		return
	}
	d.Name = string(keyBuff.Bytes())
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	valBuff := bytes.NewBuffer(valBytes)
	if err = binary.Read(valBuff, binary.BigEndian, &d.Inode); err != nil {
		return
	}
	err = binary.Read(valBuff, binary.BigEndian, &d.Type)
	return
}

func decodeInode(raw []byte, ino *Inode) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}

	ino.Inode = binary.BigEndian.Uint64(keyBytes)

	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = unmarshalValue(valBytes, ino)
	return
}

func unmarshalValue(val []byte, i *Inode) (err error) {
	buff := bytes.NewBuffer(val)

	if err = binary.Read(buff, binary.BigEndian, &i.Type); err != nil {
		return
	}
	var tmp1 uint32
	var tmp2 uint64
	if err = binary.Read(buff, binary.BigEndian, &tmp1); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &tmp1); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.Size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &tmp2); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.CreateTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.AccessTime); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &i.ModifyTime); err != nil {
		return
	}
	// read symLink
	symSize := uint32(0)
	if err = binary.Read(buff, binary.BigEndian, &symSize); err != nil {
		return
	}
	if symSize > 0 {
		tmpLink := make([]byte, symSize)
		if _, err = io.ReadFull(buff, tmpLink); err != nil {
			return
		}
	}

	if err = binary.Read(buff, binary.BigEndian, &i.NLink); err != nil {
		return
	}
	return
}
