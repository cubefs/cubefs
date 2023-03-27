// Copyright 2018 The CubeFS Authors.
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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/metanode"
	"github.com/spf13/cobra"
)

type OpKvData struct {
	Op        uint32 `json:"op"`
	K         string `json:"k"`
	V         []byte `json:"v"`
	From      string `json:"frm"` // The address of the client that initiated the operation.
	Timestamp int64  `json:"ts"`  // Timestamp of operation
}
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

const (
	cmdParseRaftLogUse   = "parseraftlog"
	cmdParseRaftLogShort = "Customized parsing raft log"

	defaultOutputPath = "/tmp/cfs/parseRaft"
)

func newParseRaftLogCmd() *cobra.Command {
	var (
		optDir    string
		optFile   string
		optInode  uint64
		optName   string
		optOutput string

		outputPath string
		outputFile string
		data       []string
	)
	var cmd = &cobra.Command{
		Use:   cmdParseRaftLogUse,
		Short: cmdParseRaftLogShort,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				err      error
				logFiles ByModTime
			)
			if optOutput != "" {
				outputPath = optOutput
			} else {
				outputPath = defaultOutputPath
			}
			if outputFile, err = creatRaftOutFile(outputPath); err != nil {
				return
			}

			if optDir != "" {
				dir := optDir
				logFiles, err = handleDir(dir)
				if logFiles == nil || err != nil {
					return
				}
				for _, file := range logFiles {
					var d []string
					if d, err = readWal(path.Join(dir, file.Name()), optInode, optName); err != nil {
						return
					}
					data = append(data, d...)
				}
			} else if optFile != "" {
				file := optFile
				if err = handleFile(file); err != nil {
					return
				}
				if data, err = readWal(file, optInode, optName); err != nil {
					return
				}
			}
			if len(data) < 1 {
				fmt.Printf("No output matching given inode[%v]/name[%v]\n", optInode, optName)
				return
			} else {
				if err = writeRaftOutfile(outputFile, data); err != nil {
					return
				}
			}
		},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return nil, cobra.ShellCompDirectiveNoFileComp
		},
	}
	cmd.Flags().StringVarP(&optDir, "dir", "d", "", "Set directory where logs are stored")
	cmd.Flags().StringVarP(&optFile, "filename", "f", "", "Set name of raft log file")
	cmd.Flags().Uint64VarP(&optInode, "inode", "i", 0, "Export the log of the specified inode")
	cmd.Flags().StringVarP(&optName, "name", "n", "", "Export the log of the specified file/dir name")
	cmd.Flags().StringVarP(&optOutput, "output", "o", "", "Specify output file [PATH]"+
		"to store raft log, default:[/tmp/cfs/parseRaft], fixed filename[raftparse_output.log]")
	return cmd
}

func handleDir(dir string) (logFile ByModTime, err error) {
	_, err = os.Stat(dir)
	if os.IsNotExist(err) {
		fmt.Printf("raftLogDir[%v] doesn't exist\n", dir)
		return
	}
	var (
		files []os.FileInfo
	)
	if files, err = ioutil.ReadDir(dir); err != nil {
		fmt.Printf("Read raftLogDir[%v] err[%v]\n", dir, err)
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
		fmt.Printf("raftLogDir[%v] doesn't have raftLog\n", dir)
		return
	}
	sort.Sort(logFile)
	return logFile, nil
}
func handleFile(file string) (err error) {
	_, err = os.Stat(file)
	if os.IsNotExist(err) {
		fmt.Printf("raftLogFile[%v] doesn't exist\n", file)
		return
	}
	return
}
func creatRaftOutFile(Path string) (name string, err error) {
	_, err = os.Stat(Path)
	if os.IsNotExist(err) {
		os.MkdirAll(Path, 0755)
	}
	name = path.Join(Path, "raftparse_output.log")
	if _, err = os.Create(name); err != nil {
		fmt.Printf("creat parse raft log outfile[%v] failed, err[%v]", name, err)
		return
	}
	fmt.Printf("set parse raft log outfile[%v] successfully\n", name)
	return
}

func writeRaftOutfile(logfile string, info []string) (err error) {
	var (
		fd *os.File
	)
	if fd, err = os.OpenFile(logfile, os.O_APPEND|os.O_RDWR, os.ModeAppend); err != nil {
		fmt.Printf("Open parse raft log outfile[%v] failed, err[%v]\n", logfile, err)
		return
	}
	defer fd.Close()
	for _, i := range info {
		if _, err = fd.WriteString(i); err != nil {
			fmt.Printf("Write parse raft log outfile[%v] failed, err[%v]\n", logfile, err)
			return
		}
	}
	fmt.Printf("Write parse raft log outfile[%v] successfully\n", logfile)
	return
}

func readWal(file string, inode uint64, name string) (dd []string, err error) {
	var (
		fileOffset uint64
		dataSize   uint64
		dataString string
	)

	data, _ := ioutil.ReadFile(file)
	fmt.Printf("parsing raftlog[%v]\n", file)
	fmt.Println("data len", len(data))
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
				var d string
				if d, err = parseKvdataOp(cmd, inode, name); err != nil {
					fmt.Printf("readwal: parse raft Kvdata Op failed: err[%v]\n", err)
					continue
				} else {
					dd = append(dd, d)
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

func parseKvdataOp(cmd *OpKvData, inode uint64, name string) (data string, err error) {
	switch cmd.Op {
	// 每个 case 会返回一条string语句
	case opFSMCreateInode:
		ino := metanode.NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
			return
		}
		if inode != 0 {
			if ino.Inode == inode {
				data = fmt.Sprintf("create inode %v\n", ino) +
					fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
				return
			} else {
				return
			}
		} else if name != "" {
			return
		}
		data = fmt.Sprintf("create inode %v\n", ino) +
			fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
	case opFSMUnlinkInode:
		ino := metanode.NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
			return
		}
		if inode != 0 {
			if ino.Inode == inode {
				data = fmt.Sprintf("unlink inode %v\n", ino) +
					fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
				return
			} else {
				return
			}
		} else if name != "" {
			return
		}
		data = fmt.Sprintf("unlink inode %v\n", ino) +
			fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
	case opFSMCreateDentry:
		den := &metanode.Dentry{}
		if err = den.Unmarshal(cmd.V); err != nil {
			return
		}
		if inode != 0 || name != "" {
			if inode == den.Inode || name == den.Name {
				data = fmt.Sprintf("create dentry {ParentId[%v]Name[%v]Inode[%v]Type[%v]}\n", den.ParentId, den.Name, den.Inode, den.Type) +
					fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
				return
			} else {
				return
			}
		}
		data = fmt.Sprintf("create dentry {ParentId[%v]Name[%v]Inode[%v]Type[%v]}\n", den.ParentId, den.Name, den.Inode, den.Type) +
			fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
	case opFSMDeleteDentry:
		den := &metanode.Dentry{}
		if err = den.Unmarshal(cmd.V); err != nil {
			return
		}
		if inode == 0 && name == "" {
			data = fmt.Sprintf("delete dentry {ParentId[%v]Name[%v]Inode[%v]Type[%v]}\n", den.ParentId, den.Name,
				den.Inode, den.Type) + fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
		} else if name != "" {
			if name == den.Name {
				data = fmt.Sprintf("delete dentry {ParentId[%v]Name[%v]Inode[%v]Type[%v]}\n", den.ParentId, den.Name,
					den.Inode, den.Type) + fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
				return
			} else {
				return
			}
		} else {
			return
		}
	case opFSMExtentTruncate:
		ino := metanode.NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
			return
		}
		if inode != 0 {
			if ino.Inode == inode {
				data = fmt.Sprintf("extent inode %v\n", ino) +
					fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
				return
			} else {
				return
			}
		} else if name != "" {
			return
		}
		data = fmt.Sprintf("extent truncate %v\n", ino) +
			fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
	case opFSMEvictInode:
		ino := metanode.NewInode(0, 0)
		if err = ino.Unmarshal(context.Background(), cmd.V); err != nil {
			return
		}
		if inode != 0 {
			if ino.Inode == inode {
				data = fmt.Sprintf("evict inode %v\n", ino) +
					fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
				return
			} else {
				return
			}
		} else if name != "" {
			return
		}
		data = fmt.Sprintf("evict inode %v\n", ino) +
			fmt.Sprintf("ip[%v], time[%v]\n", cmd.From, timeStampToString(cmd.Timestamp))
	default:
		return
	}
	return
}

func timeStampToString(timeStamp int64) string {
	return time.Unix(timeStamp, 0).Format("2006-01-02 15:04:05")
}
