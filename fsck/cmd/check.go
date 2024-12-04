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
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/spf13/cobra"
)

const (
	InodeCheckOpt int = 1 << iota
	DentryCheckOpt
)

var mpCheckLog *os.File

type MpMap struct {
	Imap map[uint64]*metanode.Inode
	Dmap map[string]*metanode.Dentry
}

func newCheckCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "check",
		Short: "check and verify specified volume",
		Args:  cobra.MinimumNArgs(0),
	}

	c.AddCommand(
		newCheckInodeCmd(),
		newCheckDentryCmd(),
		newCheckBothCmd(),
		newCheckMpCmd(),
	)

	return c
}

func newCheckInodeCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "inode",
		Short: "check and verify inode",
		Run: func(cmd *cobra.Command, args []string) {
			if err := Check(InodeCheckOpt); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func newCheckDentryCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "dentry",
		Short: "check and verify dentry",
		Run: func(cmd *cobra.Command, args []string) {
			if err := Check(DentryCheckOpt); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func newCheckBothCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "both",
		Short: "check and verify both inode and dentry",
		Run: func(cmd *cobra.Command, args []string) {
			if err := Check(InodeCheckOpt | DentryCheckOpt); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func newCheckMpCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "mp",
		Short: "check inode and dentry consistency of mp",
		Run: func(cmd *cobra.Command, args []string) {
			if err := CheckMP(); err != nil {
				fmt.Println(err)
			}
		},
	}

	return c
}

func Check(chkopt int) (err error) {
	var remote bool

	if InodesFile == "" || DensFile == "" {
		remote = true
	}

	if VolName == "" || (remote && (MasterAddr == "")) {
		err = fmt.Errorf("Lack of mandatory args: master(%v) vol(%v)", MasterAddr, VolName)
		return
	}

	/*
	 * Record all the inodes and dentries retrieved from metanode
	 */
	var (
		ifile *os.File
		dfile *os.File
	)

	dirPath := fmt.Sprintf("_export_%s", VolName)
	if err = os.MkdirAll(dirPath, 0o666); err != nil {
		return
	}

	if remote {
		if ifile, err = os.Create(fmt.Sprintf("%s/%s", dirPath, inodeDumpFileName)); err != nil {
			return
		}
		defer ifile.Close()
		if dfile, err = os.Create(fmt.Sprintf("%s/%s", dirPath, dentryDumpFileName)); err != nil {
			return
		}
		defer dfile.Close()
		if err = importRawDataFromRemote(ifile, dfile, chkopt); err != nil {
			return
		}
		// go back to the beginning of the files
		ifile.Seek(0, 0)
		dfile.Seek(0, 0)
	} else {
		if ifile, err = os.Open(InodesFile); err != nil {
			return
		}
		defer ifile.Close()
		if dfile, err = os.Open(DensFile); err != nil {
			return
		}
		defer dfile.Close()
	}

	/*
	 * Perform analysis
	 */
	imap, dlist, err := analyze(ifile, dfile)
	if err != nil {
		return
	}

	if chkopt&InodeCheckOpt != 0 {
		if err = dumpObsoleteInode(imap, fmt.Sprintf("%s/%s", dirPath, obsoleteInodeDumpFileName)); err != nil {
			return
		}
	}
	if chkopt&DentryCheckOpt != 0 {
		if err = dumpObsoleteDentry(dlist, fmt.Sprintf("%s/%s", dirPath, obsoleteDentryDumpFileName)); err != nil {
			return
		}
	}
	return
}

func CheckMP() (err error) {
	var dirPath string

	if (MpId == 0 && VolName == "") || MasterAddr == "" {
		err = fmt.Errorf("Lack of mandatory args: master(%v) vol(%v)", MasterAddr, VolName)
		return
	}
	if VolName != "" {
		dirPath = fmt.Sprintf("_export_%s", VolName)
	} else {
		dirPath = fmt.Sprintf("_export_mp_%d", MpId)
	}
	if err = os.MkdirAll(dirPath, 0o666); err != nil {
		return
	}

	if mpCheckLog, err = os.Create(fmt.Sprintf("%s/%s", dirPath, "mpCheck.log")); err != nil {
		return
	}
	defer mpCheckLog.Close()

	if MpId != 0 {
		var mp *proto.MetaPartitionInfo
		mp, err = getMetaPartitionById(MasterAddr, MpId)
		if err != nil {
			return
		}
		startTime := time.Now()
		mpCheckLog.WriteString(fmt.Sprintf("StartTime: %v\n", startTime))
		err = importAndAnalyzePartitionData(MpId, mp.Hosts, dirPath)
		if err != nil {
			return
		}
		mpCheckLog.WriteString(fmt.Sprintf("EndTime: %v\n", time.Now()))
		mpCheckLog.WriteString(fmt.Sprintf("CostTime: %v\n", time.Since(startTime)))
		return
	}

	mps, err := getMetaPartitions(MasterAddr, VolName)
	if err != nil {
		return
	}

	startTime := time.Now()
	mpCheckLog.WriteString(fmt.Sprintf("StartTime: %v\n", startTime))
	for _, mp := range mps {
		err = importAndAnalyzePartitionData(mp.PartitionID, mp.Members, dirPath)
		if err != nil {
			return
		}
	}
	mpCheckLog.WriteString(fmt.Sprintf("EndTime: %v\n", time.Now()))
	mpCheckLog.WriteString(fmt.Sprintf("CostTime: %v\n", time.Since(startTime)))

	return
}

func importRawDataFromRemote(ifile, dfile *os.File, opt int) error {
	/*
	 * Get all the meta partitions info
	 */
	mps, err := getMetaPartitions(MasterAddr, VolName)
	if err != nil {
		return err
	}

	/*
	 * Note that if we are about to clean obsolete inodes,
	 * we should get all inodes before geting all dentries.
	 */
	if opt&InodeCheckOpt != 0 {
		for _, mp := range mps {
			cmdline := fmt.Sprintf("http://%s:%s/getAllInodes?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID)
			if err := exportToFile(ifile, cmdline); err != nil {
				return err
			}
		}

		for _, mp := range mps {
			cmdline := fmt.Sprintf("http://%s:%s/getAllDentry?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID)
			if err = exportToFile(dfile, cmdline); err != nil {
				return err
			}
		}
	} else if opt&DentryCheckOpt != 0 {
		for _, mp := range mps {
			cmdline := fmt.Sprintf("http://%s:%s/getAllDentry?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID)
			if err = exportToFile(dfile, cmdline); err != nil {
				return err
			}
		}

		for _, mp := range mps {
			cmdline := fmt.Sprintf("http://%s:%s/getAllInodes?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], MetaPort, mp.PartitionID)
			if err := exportToFile(ifile, cmdline); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("Invalid opt: %v", opt)
	}
	return nil
}

func importAndAnalyzePartitionData(mpId uint64, addrs []string, dirPath string) error {
	var (
		mpMap    = make(map[string]MpMap)
		applieds = make(map[string]uint64)
		wg       sync.WaitGroup
		mu       sync.Mutex
		err      error
	)

	if _, err = mpCheckLog.WriteString(fmt.Sprintf("analyze mp %v start\n", mpId)); err != nil {
		return err
	}

	// addrs := mp.Members
	for _, addr := range addrs {
		resp, err := http.Get(fmt.Sprintf("http://%s:%s/getRaftStatus?id=%d", strings.Split(addr, ":")[0], MetaPort, mpId))
		if err != nil {
			return fmt.Errorf("Get request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return fmt.Errorf("Invalid status code: %v", resp.StatusCode)
		}

		var raftStatus struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
			Data struct {
				Applied uint64 `json:"applied"`
			} `json:"data"`
		}
		if err = json.NewDecoder(resp.Body).Decode(&raftStatus); err != nil {
			return fmt.Errorf("Decode raft status failed: %v", err)
		}
		applieds[addr] = raftStatus.Data.Applied
	}

	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			imap := make(map[uint64]*metanode.Inode)
			dmap := make(map[string]*metanode.Dentry)
			if err = getInodes(mpId, imap, addr); err != nil {
				return
			}
			if err = getDentries(mpId, dmap, addr); err != nil {
				return
			}
			mu.Lock()
			mpMap[addr] = MpMap{Imap: imap, Dmap: dmap}
			mu.Unlock()
		}(addr)
	}
	wg.Wait()

	for i, addr1 := range addrs {
		for j := i + 1; j < len(addrs); j++ {
			addr2 := addrs[j]
			if !isCheckApplyId {
				analyzeInode(mpMap[addr1].Imap, mpMap[addr2].Imap, addr1, addr2)
				continue
			}
			if applieds[addr1] == applieds[addr2] {
				analyzeInode(mpMap[addr1].Imap, mpMap[addr2].Imap, addr1, addr2)
			} else {
				mpCheckLog.WriteString(fmt.Sprintf("mp %v in %v and %v have different applyId\n", mpId, addr1, addr2))
			}
		}
	}
	for i, addr1 := range addrs {
		for j := i + 1; j < len(addrs); j++ {
			addr2 := addrs[j]
			if !isCheckApplyId {
				analyzeDentry(mpMap[addr1].Dmap, mpMap[addr2].Dmap, addr1, addr2)
				continue
			}
			if applieds[addr1] == applieds[addr2] {
				analyzeDentry(mpMap[addr1].Dmap, mpMap[addr2].Dmap, addr1, addr2)
			}
		}
	}

	if _, err = mpCheckLog.WriteString(fmt.Sprintf("analyze mp %v end\n", mpId)); err != nil {
		return err
	}

	return nil
}

func getInodes(mpId uint64, imap map[uint64]*metanode.Inode, addr string) (err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%s/getInodeSnapshot?pid=%d", strings.Split(addr, ":")[0], MetaPort, mpId))
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
		inode := &metanode.Inode{}
		if err = inode.Unmarshal(inoBuf); err != nil {
			err = errors.NewErrorf("[loadInode] Unmarshal: %s", err.Error())
			return
		}
		imap[inode.Inode] = inode
	}
}

func getDentries(mpId uint64, dmap map[string]*metanode.Dentry, addr string) (err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%s/getDentrySnapshot?pid=%d", strings.Split(addr, ":")[0], MetaPort, mpId))
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
		den := &metanode.Dentry{}
		if err = den.Unmarshal(dentryBuf); err != nil {
			err = errors.NewErrorf("[loadDentry] Unmarshal: %s", err.Error())
			return
		}
		key := fmt.Sprintf("%d/%s", den.Inode, den.Name)
		dmap[key] = den
	}
}

func compareInodes(i1 *metanode.Inode, i2 *metanode.Inode) *bytes.Buffer {
	var buffer bytes.Buffer

	if i1.Inode != i2.Inode {
		buffer.WriteString(fmt.Sprintf("Inode: %v != %v ", i1.Inode, i2.Inode))
	}
	if i1.Type != i2.Type {
		buffer.WriteString(fmt.Sprintf("Type: %v != %v ", i1.Type, i2.Type))
	}
	if i1.Uid != i2.Uid {
		buffer.WriteString(fmt.Sprintf("Uid: %v != %v ", i1.Uid, i2.Uid))
	}
	if i1.Gid != i2.Gid {
		buffer.WriteString(fmt.Sprintf("Gid: %v != %v ", i1.Gid, i2.Gid))
	}
	if i1.Size != i2.Size {
		buffer.WriteString(fmt.Sprintf("Size: %v != %v ", i1.Size, i2.Size))
	}
	if i1.Generation != i2.Generation {
		buffer.WriteString(fmt.Sprintf("Generation: %v != %v ", i1.Generation, i2.Generation))
	}
	if i1.CreateTime != i2.CreateTime {
		buffer.WriteString(fmt.Sprintf("CreateTime: %v != %v ", i1.CreateTime, i2.CreateTime))
	}
	if i1.AccessTime != i2.AccessTime {
		buffer.WriteString(fmt.Sprintf("AccessTime: %v != %v ", i1.AccessTime, i2.AccessTime))
	}
	if i1.ModifyTime != i2.ModifyTime {
		buffer.WriteString(fmt.Sprintf("ModifyTime: %v != %v ", i1.ModifyTime, i2.ModifyTime))
	}
	if !bytes.Equal(i1.LinkTarget, i2.LinkTarget) {
		buffer.WriteString(fmt.Sprintf("LinkTarget: %v != %v ", i1.LinkTarget, i2.LinkTarget))
	}
	if i1.NLink != i2.NLink {
		buffer.WriteString(fmt.Sprintf("NLink: %v != %v ", i1.NLink, i2.NLink))
	}
	if i1.Flag != i2.Flag {
		buffer.WriteString(fmt.Sprintf("Flag: %v != %v ", i1.Flag, i2.Flag))
	}
	if i1.Reserved != i2.Reserved {
		buffer.WriteString(fmt.Sprintf("Reserved: %v != %v ", i1.Reserved, i2.Reserved))
	}

	if !i1.Extents.Equals(i2.Extents) {
		buffer.WriteString(fmt.Sprintf("Extents [%v] != [%v] ", i1.Extents, i2.Extents))
	}

	if i1.StorageClass != i2.StorageClass {
		buffer.WriteString(fmt.Sprintf("StorageClass: %v != %v ", i1.StorageClass, i2.StorageClass))
	} else {
		if i1.HybridCloudExtents.GetSortedEks() != nil && i2.HybridCloudExtents.GetSortedEks() == nil ||
			i1.HybridCloudExtents.GetSortedEks() == nil && i2.HybridCloudExtents.GetSortedEks() != nil {
			buffer.WriteString(fmt.Sprintf("HybridCloudExtents [%v] != [%v] ", i1.HybridCloudExtents.GetSortedEks(), i2.HybridCloudExtents.GetSortedEks()))
		} else if i1.HybridCloudExtents.GetSortedEks() != nil && i2.HybridCloudExtents.GetSortedEks() != nil {
			if proto.IsStorageClassReplica(i1.StorageClass) {
				ext1 := i1.HybridCloudExtents.GetSortedEks().(*metanode.SortedExtents)
				ext2 := i2.HybridCloudExtents.GetSortedEks().(*metanode.SortedExtents)
				if !ext1.Equals(ext2) {
					buffer.WriteString(fmt.Sprintf("HybridCloudExtents [%v] != [%v] ", ext1, ext2))
				}
			} else {
				ext1 := i1.HybridCloudExtents.GetSortedEks().(*metanode.SortedObjExtents)
				ext2 := i2.HybridCloudExtents.GetSortedEks().(*metanode.SortedObjExtents)
				if !ext1.Equals(ext2) {
					buffer.WriteString(fmt.Sprintf("HybridCloudExtents [%v] != [%v] ", ext1, ext2))
				}
			}
		}
	}

	if i1.HybridCloudExtentsMigration != nil && i2.HybridCloudExtentsMigration == nil ||
		i1.HybridCloudExtentsMigration == nil && i2.HybridCloudExtentsMigration != nil {
		buffer.WriteString(fmt.Sprintf("HybridCloudExtentsMigration [%v] != [%v] ", i1.HybridCloudExtentsMigration, i2.HybridCloudExtentsMigration))
	} else if i1.HybridCloudExtentsMigration != nil && i2.HybridCloudExtentsMigration != nil {
		if i1.HybridCloudExtentsMigration.GetStorageClass() != i2.HybridCloudExtentsMigration.GetStorageClass() ||
			i1.HybridCloudExtentsMigration.GetExpiredTime() != i2.HybridCloudExtentsMigration.GetExpiredTime() {
			buffer.WriteString(fmt.Sprintf("HybridCloudExtentsMigration [%v] != [%v] ", i1.HybridCloudExtentsMigration, i2.HybridCloudExtentsMigration))
		} else {
			if i1.HybridCloudExtentsMigration.GetSortedEks() != nil && i2.HybridCloudExtentsMigration.GetSortedEks() == nil ||
				i1.HybridCloudExtentsMigration.GetSortedEks() == nil && i2.HybridCloudExtentsMigration.GetSortedEks() != nil {
				buffer.WriteString(fmt.Sprintf("HybridCloudExtentsMigration [%v] != [%v] ", i1.HybridCloudExtentsMigration, i2.HybridCloudExtentsMigration))
			} else if i1.HybridCloudExtentsMigration.GetSortedEks() != nil && i2.HybridCloudExtentsMigration.GetSortedEks() != nil {
				if proto.IsStorageClassReplica(i1.HybridCloudExtentsMigration.GetStorageClass()) {
					ext1 := i1.HybridCloudExtentsMigration.GetSortedEks().(*metanode.SortedExtents)
					ext2 := i2.HybridCloudExtentsMigration.GetSortedEks().(*metanode.SortedExtents)
					if !ext1.Equals(ext2) {
						buffer.WriteString(fmt.Sprintf("HybridCloudExtentsMigration [%v] != [%v] ", i1.HybridCloudExtentsMigration, i2.HybridCloudExtentsMigration))
					}
				} else {
					ext1 := i1.HybridCloudExtentsMigration.GetSortedEks().(*metanode.SortedObjExtents)
					ext2 := i2.HybridCloudExtentsMigration.GetSortedEks().(*metanode.SortedObjExtents)
					if !ext1.Equals(ext2) {
						buffer.WriteString(fmt.Sprintf("HybridCloudExtentsMigration [%v] != [%v] ", i1.HybridCloudExtentsMigration, i2.HybridCloudExtentsMigration))
					}
				}
			}
		}
	}

	if i1.ClientID != i2.ClientID {
		buffer.WriteString(fmt.Sprintf("ClientID: %v != %v ", i1.ClientID, i2.ClientID))
	}

	if i1.LeaseExpireTime != i2.LeaseExpireTime {
		buffer.WriteString(fmt.Sprintf("LeaseExpireTime : %v != %v ", i1.LeaseExpireTime, i2.LeaseExpireTime))
	}

	return &buffer
}

// func compareInodes(v1, v2 *metanode.Inode) *bytes.Buffer {
// 	var buffer bytes.Buffer

// 	v1Val := reflect.ValueOf(v1).Elem()
// 	v2Val := reflect.ValueOf(v2).Elem()

// 	t := v1Val.Type()
// 	for i := 0; i < t.NumField(); i++ {
// 		field1 := v1Val.Field(i)
// 		field2 := v2Val.Field(i)
// 		if field1.CanInterface() && field2.CanInterface() {
// 			if !field1.Type().Comparable() {
// 				continue
// 			}
// 			if !reflect.DeepEqual(field1.Interface(), field2.Interface()) {
// 				fieldName := t.Field(i).Name
// 				buffer.WriteString(fmt.Sprintf("%s: %v != %v", fieldName, field1.Interface(), field2.Interface()))
// 			}
// 		}
// 	}
// 	return &buffer
// }

func compareDentries(v1, v2 *metanode.Dentry) *bytes.Buffer {
	var buffer bytes.Buffer

	v1Val := reflect.ValueOf(v1).Elem()
	v2Val := reflect.ValueOf(v2).Elem()

	t := v1Val.Type()
	for i := 0; i < t.NumField(); i++ {
		field1 := v1Val.Field(i)
		field2 := v2Val.Field(i)
		if field1.CanInterface() && field2.CanInterface() {
			if !field1.Type().Comparable() {
				continue
			}
			if !reflect.DeepEqual(field1.Interface(), field2.Interface()) {
				fieldName := t.Field(i).Name
				buffer.WriteString(fmt.Sprintf("%s: %v != %v", fieldName, field1.Interface(), field2.Interface()))
			}
		}
	}
	return &buffer
}

func analyzeInode(imap1, imap2 map[uint64]*metanode.Inode, addr1, addr2 string) error {
	for k, v1 := range imap1 {
		v2, ok2 := imap2[k]
		if !ok2 {
			if _, err := mpCheckLog.WriteString(fmt.Sprintf("Inode %v Exists in %v but not exist in %v \n", k, addr1, addr2)); err != nil {
				return err
			}
			continue
		}
		differences := compareInodes(v1, v2)
		if differences.Len() > 0 {
			if _, err := mpCheckLog.WriteString(fmt.Sprintf("Inode %v and %v Exists in both %v and %v but has different fields:  ", v1, v2, addr1, addr2)); err != nil {
				return err
			}
			if _, err := mpCheckLog.WriteString(differences.String()); err != nil {
				return err
			}
			if _, err := mpCheckLog.WriteString("\n"); err != nil {
				return err
			}
		}
	}

	for k := range imap2 {
		if _, ok1 := imap1[k]; !ok1 {
			if _, err := mpCheckLog.WriteString(fmt.Sprintf("Inode %v Exists in %v but not exist in %v \n", k, addr2, addr1)); err != nil {
				return err
			}
		}
	}
	return nil
}

func analyzeDentry(dmap1, dmap2 map[string]*metanode.Dentry, addr1, addr2 string) error {
	for k, v1 := range dmap1 {
		v2, ok2 := dmap2[k]
		if !ok2 {
			if _, err := mpCheckLog.WriteString(fmt.Sprintf("Dentry %v Exists in %v but not exist in %v \n", k, addr1, addr2)); err != nil {
				return err
			}
			continue
		}
		differences := compareDentries(v1, v2)
		if differences.Len() > 0 {
			if _, err := mpCheckLog.WriteString(fmt.Sprintf("Dentry %v Exists in both %v and %v but has different fields: ", k, addr1, addr2)); err != nil {
				return err
			}
			if _, err := mpCheckLog.WriteString(differences.String()); err != nil {
				return err
			}
			if _, err := mpCheckLog.WriteString("\n"); err != nil {
				return err
			}
		}
	}

	for k := range dmap2 {
		if _, ok1 := dmap1[k]; !ok1 {
			if _, err := mpCheckLog.WriteString(fmt.Sprintf("Dentry %v Exists in %v but not exist in %v \n", k, addr2, addr1)); err != nil {
				return err
			}
		}
	}
	return nil
}

func analyze(ifile, dfile *os.File) (imap map[uint64]*Inode, dlist []*Dentry, err error) {
	imap = make(map[uint64]*Inode)
	dlist = make([]*Dentry, 0)

	/*
	 * Walk through all the inodes to establish inode index
	 */
	dec := json.NewDecoder(ifile)
	for dec.More() {
		inode := &Inode{Dens: make([]*Dentry, 0)}
		if err = dec.Decode(inode); err != nil {
			fmt.Printf("Unmarshal inode failed: %v", err)
			return
		}
		imap[inode.Inode] = inode
	}

	/*
	 * Walk through all the dentries to establish inode relations.
	 */
	dec = json.NewDecoder(dfile)
	for dec.More() {
		body := &struct {
			Code int32     `json:"code"`
			Msg  string    `json:"msg"`
			Data []*Dentry `json:"data"`
		}{}

		if err = dec.Decode(body); err != nil {
			err = fmt.Errorf("Decode failed: %v", err)
			return
		}

		for _, den := range body.Data {
			inode, ok := imap[den.ParentId]
			if !ok {
				dlist = append(dlist, den)
			} else {
				inode.Dens = append(inode.Dens, den)
			}
		}
	}

	root, ok := imap[1]
	if !ok {
		err = fmt.Errorf("No root inode")
		return
	}

	/*
	 * Iterate all the path, and mark reachable inode and dentry.
	 */
	followPath(imap, root)
	return
}

func followPath(imap map[uint64]*Inode, inode *Inode) {
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
		followPath(imap, childInode)
	}
}

func dumpObsoleteInode(imap map[uint64]*Inode, name string) error {
	var (
		obsoleteTotalFileSize uint64
		totalFileSize         uint64
		safeCleanSize         uint64
	)

	fp, err := os.Create(name)
	if err != nil {
		return err
	}
	defer fp.Close()

	for _, inode := range imap {
		if !inode.Valid {
			if _, err = fp.WriteString(inode.String() + "\n"); err != nil {
				return err
			}
			obsoleteTotalFileSize += inode.Size
			if inode.NLink == 0 {
				safeCleanSize += inode.Size
			}
		}
		totalFileSize += inode.Size
	}

	fmt.Printf("Total File Size: %v\nObselete Total File Size: %v\nNLink Zero Total File Size: %v\n", totalFileSize, obsoleteTotalFileSize, safeCleanSize)
	return nil
}

func dumpObsoleteDentry(dlist []*Dentry, name string) error {
	/*
	 * Note: if we get all the inodes raw data first, then obsolete
	 * dentries are not trustable.
	 */
	fp, err := os.Create(name)
	if err != nil {
		return err
	}
	defer fp.Close()

	for _, den := range dlist {
		if _, err = fp.WriteString(den.String() + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func getMetaPartitions(addr, name string) ([]*proto.MetaPartitionView, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s%s?name=%s", addr, proto.ClientMetaPartitions, name))
	if err != nil {
		return nil, fmt.Errorf("Get meta partitions failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Get meta partitions read all body failed: %v", err)
	}

	var mps []*proto.MetaPartitionView
	if err = proto.UnmarshalHTTPReply(data, &mps); err != nil {
		return nil, fmt.Errorf("Unmarshal meta partitions view failed: %v", err)
	}
	return mps, nil
}

func getMetaPartitionById(addr string, id uint64) (*proto.MetaPartitionInfo, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s%s?id=%d", addr, proto.ClientMetaPartition, id))
	if err != nil {
		return nil, fmt.Errorf("Get meta partitions failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Get meta partitions read all body failed: %v", err)
	}

	var mp *proto.MetaPartitionInfo
	if err = proto.UnmarshalHTTPReply(data, &mp); err != nil {
		return nil, fmt.Errorf("Unmarshal meta partitions view failed: %v", err)
	}
	return mp, nil
}

func exportToFile(fp *os.File, cmdline string) error {
	resp, err := http.Get(cmdline)
	fmt.Printf("resp:%v", resp.Body)
	if err != nil {
		return fmt.Errorf("Get request failed: %v %v", cmdline, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	if _, err = io.Copy(fp, resp.Body); err != nil {
		return fmt.Errorf("io Copy failed: %v", err)
	}
	_, err = fp.WriteString("\n")
	return err
}
