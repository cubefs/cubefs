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

package stream

import (
	//"bytes"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util"
	"github.com/chubaoio/cbfs/util/log"
	"hash/crc32"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	CLIENTREADSIZE  = 4 * util.KB
	CLIENTWRITESIZE = 4 * util.KB
	CLIENTWRITENUM  = 1000 * 1000
	CRCBYTELEN      = 4
	TOTALSIZE       = (CLIENTWRITESIZE + CRCBYTELEN) * CLIENTWRITENUM
)

func init() {
	_, err := log.InitLog("log", "writer_test", log.DebugLevel)
	if err != nil {
		panic("Log module init failed")
	}
}

var aalock sync.Mutex
var allKeys map[uint64]*proto.StreamKey

func saveExtentKey(inode uint64, k proto.ExtentKey) (err error) {
	aalock.Lock()
	defer aalock.Unlock()
	sk := allKeys[inode]
	sk.Put(k)
	sk.Inode = inode
	return
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

var uppercaseLetters = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func uppercaseSeq(n int, a int) string {
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

func updateKey(inode uint64) (extents []proto.ExtentKey, err error) {
	aalock.Lock()
	defer aalock.Unlock()
	extents = allKeys[inode].Extents
	return
}

func openFileForWrite(inode uint64, action string) (f *os.File, err error) {
	return os.Create(fmt.Sprintf("inode_%v_%v.txt", inode, action))
}

func initClient(t *testing.T) (client *ExtentClient) {
	go func() {
		fmt.Println(http.ListenAndServe(":6060", nil))
	}()
	var err error
	client, err = NewExtentClient("intest", "10.196.31.173:80", saveExtentKey, updateKey)
	if err != nil {
		OccoursErr(fmt.Errorf("init client err(%v)", err.Error()), t)
	}
	if client == nil {
		OccoursErr(fmt.Errorf("init client err(%v)", err.Error()), t)
	}
	return
}

func initInode(inode uint64) (sk *proto.StreamKey) {
	sk = new(proto.StreamKey)
	sk.Inode = inode
	allKeys[inode] = sk
	return
}

func prepare(inode uint64, t *testing.T) (localWriteFp *os.File, localReadFp *os.File) {
	var err error
	localWriteFp, err = openFileForWrite(inode, "write")
	if err != nil {
		OccoursErr(fmt.Errorf("write localFile inode(%v) err(%v)\n", inode, err), t)
	}
	localReadFp, err = openFileForWrite(inode, "read")
	if err != nil {
		OccoursErr(fmt.Errorf("read localFile inode(%v) err(%v)\n", inode, err), t)
	}
	return
}

func OccoursErr(err error, t *testing.T) {
	fmt.Println(err.Error())
	t.FailNow()
}

func TestExtentClient_RandWrite(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	allKeys = make(map[uint64]*proto.StreamKey)
	client := initClient(t)
	var (
		inode uint64
	)
	inode = 2000
	initInode(inode)
	datasize := 10 * 1024 * 1024
	client.Open(inode)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			index := 0
			for {
				if index > 100 {
					break
				}
				index++
				writeStr := randSeq(datasize)
				data := ([]byte)(writeStr)
				crc := crc32.ChecksumIEEE(data)
				crcdata := make([]byte, 4)
				binary.BigEndian.PutUint32(crcdata, crc)
				data = append(data, crcdata...)
				write, err := client.Write(inode, data)
				if err != nil {
					OccoursErr(fmt.Errorf("write error can write (%v) err(%v)", write, err), t)
				}
			}
			fmt.Printf("FININSH")
		}()
	}
	client.Flush(inode)
	wg.Wait()
	offset := 0
	for {
		data := make([]byte, datasize+4)
		can, err := client.Read(inode, data, offset, len(data))
		if err != nil {
			OccoursErr(fmt.Errorf("read error can read (%v) err(%v)", can, err), t)
		}
		if can != len(data) {
			OccoursErr(fmt.Errorf("read error can read (%v) err(%v)", can, err), t)
		}

		data1 := data[:datasize]
		data2 := data[datasize-4:]
		actualCrc := crc32.ChecksumIEEE(data1)
		expectCrc := binary.BigEndian.Uint32(data2)
		if actualCrc != expectCrc {
			OccoursErr(fmt.Errorf("crc not match"), t)
		}
	}

}

func TestExtentClient_Write(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	allKeys = make(map[uint64]*proto.StreamKey)
	client := initClient(t)
	var (
		inode uint64
		read  int
	)
	inode = 2000
	sk := initInode(inode)
	writebytes := 0
	writeStr := randSeq(util.BlockSize*5 + 1)
	data := ([]byte)(writeStr)
	localWriteFp, localReadFp := prepare(inode, t)

	client.Open(inode)
	client.Open(inode)
	client.Open(inode)
	for seqNo := 0; seqNo < 100000000000; seqNo++ {
		rand.Seed(time.Now().UnixNano())
		ndata := data[:util.BlockSize*2]
		if len(ndata) == 0 {
			continue
		}
		//write
		write, err := client.Write(inode, ndata)
		if err != nil || write != len(ndata) {
			OccoursErr(fmt.Errorf("write inode (%v) seqNO(%v) bytes(%v) err(%v)\n", inode, seqNo, write, err), t)
		}
		fmt.Printf("hahah ,write ok (%v)\n", seqNo)

		//flush
		err = client.Flush(inode)
		if err != nil {
			OccoursErr(fmt.Errorf("flush inode (%v) seqNO(%v) bytes(%v) err(%v)\n", inode, seqNo, write, err), t)
		}
		fmt.Printf("hahah ,flush ok (%v)\n", seqNo)

		//read
		rdata := make([]byte, len(ndata))
		read, err = client.Read(inode, rdata, writebytes, len(ndata))
		if err != nil || read != len(ndata) {
			fmt.Printf("stream filesize(%v) offset(%v) size(%v) skstream(%v)\n",
				sk.Size(), writebytes, len(ndata), sk.ToString())
			OccoursErr(fmt.Errorf("read inode (%v) seqNO(%v) bytes(%v) err(%v)\n", inode, seqNo, read, err), t)
		}
		if !bytes.Equal(rdata, ndata) {
			fp, _ := os.OpenFile("org.data", os.O_CREATE|os.O_RDWR, 0666)
			fp.WriteString(string(ndata))
			fp.Close()
			fp, _ = os.OpenFile("rdata.data", os.O_CREATE|os.O_RDWR, 0666)
			fp.WriteString(string(rdata))
			fp.Close()
			fmt.Printf("stream filesize(%v) offset(%v) size(%v) skstream(%v)\n",
				sk.Size(), writebytes, len(ndata), sk.ToString())
			OccoursErr(fmt.Errorf("acatual read is differ to writestr"), t)
		}
		writebytes += write
	}

	//test case: read size more than write size
	readData := make([]byte, CLIENTREADSIZE)
	readOffset := (writebytes - CLIENTWRITESIZE + 1024)
	read, err := client.Read(inode, readData, readOffset, CLIENTREADSIZE)
	if err != nil || read != (CLIENTREADSIZE-1024) {
		OccoursErr(fmt.Errorf("read inode (%v) bytes(%v) err(%v)\n", inode, read, err), t)
	}

	//finish
	client.Close(inode)
	client.Close(inode)
	client.Close(inode)

	localWriteFp.Close()
	localReadFp.Close()

	for {
		time.Sleep(time.Second)
		if sk.Size() == uint64(writebytes) {
			break
		}
	}
}

func writeFlushReadTest(t *testing.T, inode uint64, seqNo int, client *ExtentClient,
	writeData []byte, localWriteFp *os.File) (write int, err error) {

	//write
	write, err = client.Write(inode, writeData)
	if err != nil || write != len(writeData) {
		OccoursErr(fmt.Errorf("write seqNO(%v) bytes(%v) len(%v) err(%v)\n", seqNo, write, len(writeData), err), t)
	}
	//fmt.Printf("write ok seqNo(%v), Size(%v), Crc(%v)\n", seqNo, write, writeData[CLIENTWRITESIZE])

	//flush
	err = client.Flush(inode)
	if err != nil {
		OccoursErr(fmt.Errorf("flush inode (%v) seqNO(%v) bytes(%v) err(%v)\n", inode, seqNo, write, err), t)
	}

	_, err = localWriteFp.Write(writeData)
	if err != nil {
		OccoursErr(fmt.Errorf("write localFile write inode (%v) seqNO(%v) bytes(%v) err(%v)\n", inode, seqNo, write, err), t)
	}

	return write, nil
}

func TestExtentClient_MultiRoutineWrite(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	allKeys = make(map[uint64]*proto.StreamKey)
	client := initClient(t)
	var (
		inode uint64
	)
	inode = 3
	sk := initInode(inode)
	writeBytes := 0
	readBytes := 0
	localWriteFp, localReadFp := prepare(inode, t)

	client.Open(inode)
	client.Open(inode)
	client.Open(inode)
	for seqNo := 0; seqNo < CLIENTWRITENUM; seqNo++ {
		writeStr := uppercaseSeq(CLIENTWRITESIZE+CRCBYTELEN, seqNo)
		writeData := ([]byte)(writeStr)

		//add checksum
		tempData := writeData[:CLIENTWRITESIZE]
		crc := uint32ToBytes(crc32.ChecksumIEEE(tempData))
		fmt.Printf("write crc seqNo(%v), Crc(%v)\n", seqNo, crc)
		for i := 0; i < CRCBYTELEN; i++ {
			writeData[CLIENTWRITESIZE+i] = crc[i]
		}

		go func(seqNo int) {
			write, err := writeFlushReadTest(t, inode, seqNo, client, writeData, localWriteFp)
			if err != nil {
				OccoursErr(fmt.Errorf("write inode(%v) seqNO(%v)  err(%v)\n", inode, seqNo, err), t)
			}
			writeBytes += write
		}(seqNo)
	}

	for {
		time.Sleep(time.Second)
		if writeBytes == TOTALSIZE {
			break
		}
	}
	fmt.Printf("write size (%v)\n", writeBytes)

	//read check
	for seqNo := 0; seqNo < CLIENTWRITENUM; seqNo++ {
		rand.Seed(time.Now().UnixNano())

		rdata := make([]byte, CLIENTWRITESIZE+CRCBYTELEN)
		read, err := client.Read(inode, rdata, readBytes, CLIENTWRITESIZE+CRCBYTELEN)
		if err != nil || read != CLIENTWRITESIZE+CRCBYTELEN {
			OccoursErr(fmt.Errorf("read bytes(%v) err(%v)\n", read, err), t)
		}

		_, err = localReadFp.Write(rdata)
		if err != nil {
			OccoursErr(fmt.Errorf("write localFile read inode(%v) err(%v)\n", inode, err), t)
		}

		//check crc
		tempData := rdata[:CLIENTWRITESIZE]
		crc := uint32ToBytes(crc32.ChecksumIEEE(tempData))
		crcData := rdata[CLIENTWRITESIZE:]

		//		fmt.Printf("readCrc(%v) writeCrc(%v)\n", crc, crcData)
		for i := 0; i < CRCBYTELEN; i++ {
			if crc[i] != crcData[i] {
				OccoursErr(fmt.Errorf("wrong(%v) readcrc(%v) writecrc(%v)\n", i, crc[i], crcData[i]), t)
			}
		}

		readBytes += read
	}

	if readBytes != TOTALSIZE {
		OccoursErr(fmt.Errorf("read err size (%v)", readBytes), t)
	}

	time.Sleep(time.Second)

	//finish
	client.Close(inode)
	client.Close(inode)
	client.Close(inode)

	localReadFp.Close()
	localWriteFp.Close()

	fmt.Printf("fileSize %d \n", sk.Size())
}
