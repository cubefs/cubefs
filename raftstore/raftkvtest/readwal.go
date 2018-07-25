package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/tiglabs/baudstorage/util/config"
	"io/ioutil"
	"os"
)

type OpKvData struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

func main() {
	fmt.Println("Read raft wal record")
	var (
		confFile   = flag.String("c", "", "config file path")
		fileOffset uint64
		dataSize   uint64
		dataString string
	)

	flag.Parse()
	cfg := config.LoadConfigFile(*confFile)
	fileName := cfg.GetString("filename")

	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println("open wal failed ", err)
		return
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Println("read wal failed ", err)
		return
	}

	fmt.Println("data len", len(data))

	fileOffset = 0

	for {
		if fileOffset >= uint64(len(data)) {
			return
		}

		dataSize = 0
		dataTemp := data[fileOffset:]
		recordType := dataTemp[0]
		dataSize = binary.BigEndian.Uint64(dataTemp[1:9])
		opType := dataTemp[9]
		term := binary.BigEndian.Uint64(dataTemp[10:18])
		index := binary.BigEndian.Uint64(dataTemp[18:26])
		if dataSize > 17 {
			cmd := new(OpKvData)
			if err = json.Unmarshal(dataTemp[26:26+dataSize-17], cmd); err != nil {
				fmt.Println("unmarshal fail", cmd, err)
				return
			}
			dataString = fmt.Sprintf("opt:%v, k:%v, v:%v", cmd.Op, cmd.K, cmd.V)
		}
		crcOffset := 9 + dataSize
		crc := binary.BigEndian.Uint32(dataTemp[crcOffset : crcOffset+4])
		fmt.Println(fmt.Sprintf("recType[%v] dataSize[%v] opType[%v] term[%v] index[%v] data[%v] crc[%v]", recordType, dataSize, opType, term, index, dataString, crc))
		recordSize := 1 + 8 + dataSize + 4
		fileOffset = fileOffset + recordSize
	}

	f.Close()
	return
}
