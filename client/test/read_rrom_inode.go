package main

import (
	"flag"
	"fmt"
	"github.com/chubaoio/cbfs/sdk/data/stream"
	"github.com/chubaoio/cbfs/sdk/meta"
	"github.com/chubaoio/cbfs/util"
	"github.com/chubaoio/cbfs/util/log"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	line     = flag.Int("l", 0, "read file line")
	useInode = flag.Bool("inode", true, "use inode ")

	sumBytes int64
)

func read(intinode string, wg *sync.WaitGroup, mw *meta.MetaWrapper, ec *stream.ExtentClient) {
	defer wg.Done()
	inode, err := strconv.Atoi(intinode)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if err := mw.Open_ll(uint64(inode)); err != nil {
		fmt.Println(fmt.Sprintf("inode %v open err %v", inode, err.Error()))
		return
	}
	var offset int
	stream, err := ec.OpenForRead(uint64(inode))
	if err != nil {
		fmt.Println(fmt.Sprintf("inode %v open err %v", inode, err.Error()))
		return
	}
	for {
		data := make([]byte, util.BlockSize)
		_, err := ec.Read(stream, uint64(inode), data, offset, util.BlockSize)
		if err != nil {
			fmt.Println(fmt.Sprintf("inode [%v] offset [%v]error is[%v] ", intinode, offset, err.Error()))
			return
		}
		offset += util.BlockSize
		atomic.AddInt64(&sumBytes, util.BlockSize)
	}
}

func readFromFile(filename string, wg *sync.WaitGroup, mw *meta.MetaWrapper, ec *stream.ExtentClient) {
	defer wg.Done()
	var inode uint64
	fp, err := os.Open(filename)
	if err != nil {
		fmt.Println(fmt.Sprintf("inode %v open err %v", inode, err.Error()))
		return
	}
	var offset int64
	for {
		data := make([]byte, util.BlockSize)
		_, err := fp.ReadAt(data, offset)
		if err != nil {
			fmt.Println(fmt.Sprintf("inode [%v] offset [%v]error is[%v] ", filename, offset, err.Error()))
			return
		}
		offset += util.BlockSize
		atomic.AddInt64(&sumBytes, util.BlockSize)
	}
	fp.Close()
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	_, err := log.NewLog("log", "writer_test", log.ErrorLevel)
	if err != nil {
		panic("Log module init failed")
	}
	go func() {
		fmt.Println(http.ListenAndServe(":6060", nil))
	}()
	mw, err := meta.NewMetaWrapper("intest", "10.196.31.173:80,10.196.31.141:80,10.196.30.200:80")
	if err != nil {
		fmt.Println(err)
		return
	}
	ec, err := stream.NewExtentClient("intest", "10.196.31.173:80,10.196.31.141:80,10.196.30.200:80",
		mw.AppendExtentKey, mw.GetExtents, 3*util.MB)
	if err != nil {
		fmt.Println(err)
		return
	}
	var filename string
	if *useInode {
		filename = "inode.txt"
	} else {
		filename = "inode.filename"
	}
	f, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Println(err)
		return
	}
	f.Close()

	mesg := string(data)
	arr1 := strings.Split(mesg, "\n")
	end := (*line+1)*8 - 1
	start := (*line) * 8
	arr := arr1[start:end]
	var wg sync.WaitGroup
	for i := 0; i < len(arr); i++ {
		wg.Add(1)
		fmt.Println(fmt.Sprintf("read inode[%v]\n", arr[i]))
		if *useInode {
			go read(arr[i], &wg, mw, ec)
		} else {
			go readFromFile(arr[i], &wg, mw, ec)
		}

	}
	for {
		atomic.StoreInt64(&sumBytes, 0)
		time.Sleep(time.Second * 5)
		fmt.Printf("[%v]MB/s\n", atomic.LoadInt64(&sumBytes)/(1024*1024*5))
	}
}
