// Copyright 2018 The ChuBao Authors.
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
	//"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/tiglabs/containerfs/sdk/data/stream"
	"github.com/tiglabs/containerfs/sdk/meta"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	"hash/crc32"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	CRCBYTELEN = 4
)

var (
	cfgFile = flag.String("c", "", "config file")
)

func init() {
	_, err := log.InitLog("log", "writer_test", log.DebugLevel)
	if err != nil {
		panic("Log module init failed")
	}
}

var uppercaseLetters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

func uppercaseSeq(n int, a uint64) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = uppercaseLetters[a%26]
	}
	return string(b)
}

func uint32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func openFileForWrite(inode uint64, action string) (f *os.File, err error) {
	return os.Create(fmt.Sprintf("inode_%v_%v.txt", inode, action))
}

func openFileFromMountFuse(dir, file string) (f *os.File, err error) {
	return os.Create(fmt.Sprintf("%v/%v", dir, file))
}

func initClient() (ec *stream.ExtentClient) {
	var err error

	go func() {
		fmt.Println(http.ListenAndServe(":6060", nil))
	}()

	mw, err := meta.NewMetaWrapper("intest", "10.196.31.173:80,10.196.31.141:80,10.196.30.200:80")
	if err != nil {
		fmt.Println(err)
		return
	}
	ec, err = stream.NewExtentClient("intest", "10.196.31.173:80,10.196.31.141:80,10.196.30.200:80",
		mw.AppendExtentKey, mw.GetExtents, 3*util.MB)
	if err != nil {
		fmt.Println(err)
		return
	}

	return
}

func prepare(inode uint64) (localWriteFp *os.File, localReadFp *os.File) {
	var err error
	localWriteFp, err = openFileForWrite(inode, "write")
	if err != nil {
		OccoursErr(fmt.Errorf("write localFile inode[%v] err[%v]\n", inode, err))
	}
	localReadFp, err = openFileForWrite(inode, "read")
	if err != nil {
		OccoursErr(fmt.Errorf("read localFile inode[%v] err[%v]\n", inode, err))
	}
	return
}

func OccoursErr(err error) {
	fmt.Println(err.Error())
}

func writeFlushAndRecord(inode uint64, seqNo uint64, //client *stream.ExtentClient,
	writeData []byte, localWriteFp, mountWriteFp *os.File) (write int, err error) {

	//write
	//write, err = client.Write(inode, writeData)
	//if err != nil || write != len(writeData) {
	//	OccoursErr(fmt.Errorf("write seqNO[%v] bytes[%v] len[%v] err[%v]\n", seqNo, write, len(writeData), err))
	//	return write, err
	//}

	//flush
	//err = client.Flush(inode)
	//if err != nil {
	//	OccoursErr(fmt.Errorf("flush inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err))
	//	return write, err
	//}

	write, err = mountWriteFp.Write(writeData)
	if err != nil {
		fmt.Println("mount fuse write err", err)
		return write, err
	}
	fmt.Println("mount fuse write size", write)

	_, err = localWriteFp.Write(writeData)
	if err != nil {
		OccoursErr(fmt.Errorf("write localFile write inode [%v] seqNO[%v] bytes[%v] err[%v]\n", inode, seqNo, write, err))
		return write, err
	}

	return write, nil
}

func checkReadDataCrc(rdata []byte, writeSize int, seqNo uint64) (err error) {
	tempData := rdata[:writeSize]
	crc := uint32ToBytes(crc32.ChecksumIEEE(tempData))
	crcData := rdata[writeSize:]

	fmt.Printf("read crc seqNo[%v], Crc[%v], record write Crc[%v]\n", seqNo, crc, crcData)

	for i := 0; i < CRCBYTELEN; i++ {
		if crc[i] != crcData[i] {
			err = fmt.Errorf("wrong seqNo[%v] i[%v] readcrc[%v] writecrc[%v]\n", seqNo, i, crc[i], crcData[i])
			return err
		}
	}
	return
}

func main() {
	wg := &sync.WaitGroup{}

	runtime.GOMAXPROCS(runtime.NumCPU())
	var (
		seqNo     uint64
		readBytes int64
	)

	flag.Parse()
	cfg := config.LoadConfigFile(*cfgFile)

	fmt.Printf("cfg file [%v]\n", cfg)
	ino := cfg.GetString("inode")
	loop := cfg.GetString("loop")
	size := cfg.GetString("size")
	dir := cfg.GetString("dir")
	file := cfg.GetString("file")
	fmt.Printf("enter verity inod[%v], loop[%v] size[%v]\n", ino, loop, size)

	inode, _ := strconv.ParseUint(ino, 10, 64)
	loopCnt, _ := strconv.ParseUint(loop, 10, 64)
	iSize, _ := strconv.Atoi(size)
	writeSize := iSize * util.MB

	//client := initClient()

	writeBytes := 0
	readBytes = 0
	totalSize := (writeSize + CRCBYTELEN) * int(loopCnt)

	localWriteFp, localReadFp := prepare(inode)

	mountWriteFp, _ := openFileFromMountFuse(dir, file)

	fmt.Printf("enter write verity 2 inod[%v], loop[%v] size[%v]\n", inode, loopCnt, writeSize)

	//client.Open(inode)
	for seqNo = 0; seqNo < loopCnt; seqNo++ {

		writeStr := uppercaseSeq(writeSize+CRCBYTELEN, seqNo)
		writeData := ([]byte)(writeStr)

		//add checksum
		tempData := writeData[:writeSize]
		crc := uint32ToBytes(crc32.ChecksumIEEE(tempData))
		fmt.Printf("write crc seqNo[%v], Crc[%v]\n", seqNo, crc)
		for i := 0; i < CRCBYTELEN; i++ {
			writeData[writeSize+i] = crc[i]
		}

		wg.Add(1)
		go func(seqNo uint64) {
			defer wg.Done()
			//			write, err := writeFlushAndRecord(inode, seqNo, client, writeData, localWriteFp, mountWriteFp)
			write, err := writeFlushAndRecord(inode, seqNo, writeData, localWriteFp, mountWriteFp)
			if err != nil {
				OccoursErr(fmt.Errorf("write inode[%v] seqNO[%v]  err[%v]\n", inode, seqNo, err))
				return
			}
			writeBytes += write
		}(seqNo)
	}

	wg.Wait()

	fmt.Printf("write size [%v]\n", writeBytes)

	//read check
	for seqNo = 0; seqNo < loopCnt; seqNo++ {

		//rdata := make([]byte, writeSize+CRCBYTELEN)
		readData := make([]byte, writeSize+CRCBYTELEN)
		//read, err := client.Read(inode, rdata, readBytes, int(writeSize)+CRCBYTELEN)
		//if err != nil || read != int(writeSize)+CRCBYTELEN {
		//	OccoursErr(fmt.Errorf("read bytes[%v] err[%v]\n", read, err))
		//	return
		//}

		readFromFuse, err := mountWriteFp.ReadAt(readData, readBytes)
		if err != nil || readFromFuse != int(writeSize)+CRCBYTELEN {
			OccoursErr(fmt.Errorf("read from fuse bytes[%v] err[%v]\n", readFromFuse, err))
			return
		}

		_, err = localReadFp.Write(readData)
		if err != nil {
			OccoursErr(fmt.Errorf("write localFile read inode[%v] err[%v]\n", inode, err))
			return
		}

		//check crc
		//err = checkReadDataCrc(rdata, writeSize, seqNo)
		//if err != nil{
		//	OccoursErr(err)
		//	panic("read crc error")
		//	return
		//}

		err = checkReadDataCrc(readData, writeSize, seqNo)
		if err != nil {
			OccoursErr(err)
			panic("read from fuse error")
			return
		}

		readBytes += int64(readFromFuse)
	}

	if readBytes != int64(totalSize) {
		OccoursErr(fmt.Errorf("read err size [%v]", readBytes))
		panic("verity failed")
	}

	time.Sleep(time.Second)

	//finish
	//client.Close(inode)

	localReadFp.Close()
	localWriteFp.Close()
	mountWriteFp.Close()
}
