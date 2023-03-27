// Copyright 2018 The CFS Authors.
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

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cubefs/cubefs/metanode"
)

type OpKvData struct {
	Op        uint32 `json:"op"`
	K         string `json:"k"`
	V         []byte `json:"v"`
	From      string `json:"frm"` // The address of the client that initiated the operation.
	Timestamp int64  `json:"ts"`  // Timestamp of operation
}

var (
	errorLogfile string
	h            = flag.Bool("h", false, "this for help")
	raftLog      = flag.String("f", "", "config raft log path, [../cfs/data/raft/0000000000000000-0000000000000001.log]")
	raftDir      = flag.String("d", "", "config raft log dir, handle all logs in this dir")
)

type ByModTime []os.FileInfo

func (f ByModTime) Less(i, j int) bool {
	return f[i].ModTime().Before(f[j].ModTime())
}

func (f ByModTime) Len() int {
	return len(f)
}

func (f ByModTime) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func usage() {
	fmt.Println("Usage: ./readwal [-h] [-f file] [-d dir]")
	fmt.Println("Options:")
	flag.PrintDefaults()
}

func main() {
	flag.Parse()
	if *h {
		flag.Usage = usage
		flag.Usage()
	}

	var err error
	fmt.Println("Read raft wal record")
	errorLogfile, err = creatParseRaftErrorLog()
	if err != nil {
		fmt.Printf("creat a errorLogfile[%v] failed, err[%v]\n", errorLogfile, err)
		return
	}
	// 有d优先d，屏蔽f； 没d 处理 f
	if *raftDir != "" {
		_, err = os.Stat(*raftDir)
		if os.IsNotExist(err) {
			fmt.Printf("raftLogDir[%v] doesn't exist\n", *raftDir)
			return
		}
		var (
			files   []os.FileInfo
			logFile ByModTime
		)
		if files, err = ioutil.ReadDir(*raftDir); err != nil {
			fmt.Printf("Read raftLogDir[%v] err[%v]\n", *raftDir, err)
			return
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".log") {
				match, _ := regexp.MatchString("\\d{16}-\\d{16}.log", f.Name())
				if match {
					logFile = append(logFile, f)
				}
			}
		}
		if len(logFile) < 1 {
			fmt.Printf("raftLogDir[%v] doesn't have raftLog\n", *raftDir)
			return
		}
		sort.Sort(logFile)
		for _, file := range logFile {
			readWal(path.Join(*raftDir, file.Name()))
		}
		return
	}
	if *raftLog != "" {
		_, err = os.Stat(*raftLog)
		if os.IsNotExist(err) {
			fmt.Printf("raftLogFile[%v] doesn't exist\n", *raftLog)
			return
		}
		readWal(*raftLog)
	}
	return
}

func creatParseRaftErrorLog() (file string, err error) {
	dir := path.Join(".", "parseRaft")
	_, err = os.Stat(dir)
	if os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}
	file = path.Join(dir, "error.log")
	if _, err = os.Create(file); err != nil {
		return
	}
	return
}

func readWal(name string) {
	var (
		fileOffset uint64
		dataSize   uint64
		dataString string
	)

	data, _ := ioutil.ReadFile(name)
	fmt.Printf("parsing raftlog[%v]\n", name)
	fmt.Println("data len", len(data))
	var err error
	fileOffset = 0

	for {
		if fileOffset >= uint64(len(data)) {
			return
		}
		dataSize = 0
		dataTemp := data[fileOffset:]
		//first byte is record type
		recordType := dataTemp[0]
		//sencond 8 bytes is datasize
		dataSize = binary.BigEndian.Uint64(dataTemp[1:9])
		//third byte is op type
		opType := dataTemp[9]
		//and 8+8 types is term and index
		term := binary.BigEndian.Uint64(dataTemp[10:18])
		index := binary.BigEndian.Uint64(dataTemp[18:26])

		// dataSize >17, 1 opType + 8 term + 8 index, and data
		if dataSize > 17 {
			if opType == 0 {
				cmd := new(OpKvData)
				if err = json.Unmarshal(dataTemp[26:26+dataSize-17], cmd); err != nil {
					fmt.Println("unmarshal fail", cmd, err)
					return
				}
				dataString = fmt.Sprintf("opt:%v, k:%v, v:%v", cmd.Op, cmd.K, cmd.V)
				if err = parseKvdataOp(cmd); err != nil {
					fmt.Printf("readwal: parse raft Kvdata Op failed: err[%v]\n", err)
					continue
				}
			} else if opType == 1 {
				cType := dataTemp[26]
				pType := dataTemp[27]
				prt := binary.BigEndian.Uint16(dataTemp[28:30])
				pid := binary.BigEndian.Uint64(dataTemp[30:38])
				dataString = fmt.Sprintf("cngType:%v, peerType:%v, priority:%v, id:%v", cType, pType, prt, pid)
			} else {
				fmt.Printf("opType[%v] is not 0or1\n", opType)
			}
		} else if dataSize < 17 {
			fmt.Printf("dataSize[%v] < 17, the wrong content\n", dataSize)
			return
		}
		crcOffset := 9 + dataSize                                         // 最前面 1+8 加数据长度的偏移
		crc := binary.BigEndian.Uint32(dataTemp[crcOffset : crcOffset+4]) // ctx   context.Context 4 bytes
		fmt.Sprintf("recType[%v] dataSize[%v] opType[%v] term[%v] index[%v] data[%v] crc[%x]", recordType, dataSize, opType, term, index, dataString, crc)
		recordSize := 1 + 8 + dataSize + 4
		fileOffset = fileOffset + recordSize
	}

}

func parseKvdataOp(cmd *OpKvData) (err error) {
	switch cmd.Op {
	case opFSMCreateInode:
		ino := metanode.NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
			writeIntoLog(errorLogfile, err.Error())
			return
		}
		fmt.Println(fmt.Sprintf("create inode %v", ino))
		fmt.Println(fmt.Sprintf("ip[%v], time[%v]", cmd.From, timeStampToString(cmd.Timestamp)))
	case opFSMUnlinkInode:
		ino := metanode.NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
			writeIntoLog(errorLogfile, err.Error())
			return
		}
		fmt.Println(fmt.Sprintf("unlink inode %v", ino))
		fmt.Println(fmt.Sprintf("ip[%v], time[%v]", cmd.From, timeStampToString(cmd.Timestamp)))
	case opFSMCreateDentry:
		den := &metanode.Dentry{}
		if err = den.Unmarshal(cmd.V); err != nil {
			writeIntoLog(errorLogfile, err.Error())
			return
		}
		fmt.Println(fmt.Sprintf("create dentry {ParentId[%v]Name[%v]Inode[%v]Type[%v]}", den.ParentId, den.Name,
			den.Inode, den.Type))
		fmt.Println(fmt.Sprintf("ip[%v], time[%v]", cmd.From, timeStampToString(cmd.Timestamp)))
	case opFSMDeleteDentry:
		den := &metanode.Dentry{}
		if err = den.Unmarshal(cmd.V); err != nil {
			writeIntoLog(errorLogfile, err.Error())
			return
		}
		fmt.Println(fmt.Sprintf("delete dentry {ParentId[%v]Name[%v]Inode[%v]Type[%v]}", den.ParentId, den.Name,
			den.Inode, den.Type))
		fmt.Println(fmt.Sprintf("ip[%v], time[%v]", cmd.From, timeStampToString(cmd.Timestamp)))
	case opFSMExtentTruncate:
		ino := metanode.NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
			writeIntoLog(errorLogfile, err.Error())
			return
		}
		fmt.Println(fmt.Sprintf("extent truncate %v", ino))
		fmt.Println(fmt.Sprintf("ip[%v], time[%v]", cmd.From, timeStampToString(cmd.Timestamp)))
	case opFSMEvictInode:
		ino := metanode.NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
			writeIntoLog(errorLogfile, err.Error())
			return
		}
		fmt.Println(fmt.Sprintf("evict inode %v", ino))
		fmt.Println(fmt.Sprintf("ip[%v], time[%v]", cmd.From, timeStampToString(cmd.Timestamp)))
	default:
		return
	}
	return
}

func writeIntoLog(logName string, info string) (err error) {
	var (
		fd *os.File
	)
	if fd, err = os.Open(logName); err != nil {
		fmt.Printf("Open errorLogfile[%v] failed, err[%v]\n", logName, err)
		return
	}
	defer fd.Close()
	if _, err = fd.WriteString(info); err != nil {
		fmt.Printf("Write errorLogfile[%v] failed, err[%v]\n", logName, err)
		return
	}
	return
}

func timeStampToString(timeStamp int64) string {
	return time.Unix(timeStamp, 0).Format("2006-01-02 15:04:05")
}
