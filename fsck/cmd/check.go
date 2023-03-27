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
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/chubaofs/chubaofs/proto"
)

const (
	InodeCheckOpt int = 1 << iota
	DentryCheckOpt
)

func newCheckCmd() *cobra.Command {
	var c = &cobra.Command{
		Use:   "check",
		Short: "check and verify specified volume",
		Args:  cobra.MinimumNArgs(0),
	}

	c.AddCommand(
		newCheckInodeCmd(),
		newCheckDentryCmd(),
		newCheckBothCmd(),
	)

	return c
}

func newCheckInodeCmd() *cobra.Command {
	var c = &cobra.Command{
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
	var c = &cobra.Command{
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
	var c = &cobra.Command{
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
	if err = os.MkdirAll(dirPath, 0666); err != nil {
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

func analyze(ifile, dfile *os.File) (imap map[uint64]*Inode, dlist []*Dentry, err error) {
	imap = make(map[uint64]*Inode)
	dlist = make([]*Dentry, 0)

	/*
	 * Walk through all the inodes to establish inode index
	 */
	dec := json.NewDecoder(ifile)
	for dec.More() {
		body := &struct {
			Code int32    `json:"code"`
			Msg  string   `json:"msg"`
			Data []*Inode `json:"data"`
		}{}
		if err = dec.Decode(body); err != nil {
			err = fmt.Errorf("Decode inode failed: %v", err)
			return
		}
		for _, inode := range body.Data {
			imap[inode.Inode] = inode
		}
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

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Get meta partitions read all body failed: %v", err)
	}

	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(data, body); err != nil {
		return nil, fmt.Errorf("Unmarshal meta partitions body failed: %v", err)
	}

	var mps []*proto.MetaPartitionView
	if err = json.Unmarshal(body.Data, &mps); err != nil {
		return nil, fmt.Errorf("Unmarshal meta partitions view failed: %v", err)
	}
	return mps, nil
}

func exportToFile(fp *os.File, cmdline string) error {
	resp, err := http.Get(cmdline)
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
