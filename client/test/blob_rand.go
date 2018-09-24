package main

import (
	"math/rand"
	"time"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/sdk/data/blob"
	"fmt"
	"runtime"
	"os"
)

var (
	gBlobClient     *blob.BlobClient
	keyChan         = make(chan string, 10000000)
	deleteChan      = make(chan string, 1000000)
	readChan        = make(chan string, 1000000)
	deleteSuccessFp *os.File
	deleteFailFp    *os.File
	readFailFp      *os.File
	writeFailFp     *os.File
	writeSuccessFp  *os.File
	volname         = "blob"
	masters         = "10.196.31.173:8001,10.196.31.141:8001,10.196.30.200:8001"
)

func main() {
	var err error
	gBlobClient, err = blob.NewBlobClient(volname, masters)
	if err != nil {
		fmt.Println(err)
		return
	}
	prefix := "logs/blob_"
	deleteSuccessFp, err = os.OpenFile(prefix+"delsuccess.log", os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}
	deleteFailFp, err = os.OpenFile(prefix+"delfail.log", os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}

	readFailFp, err = os.OpenFile(prefix+"readfail.log", os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}
	writeFailFp, err = os.OpenFile(prefix+"writefail.log", os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}
	writeSuccessFp, err = os.OpenFile(prefix+"writesuccess.log", os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	go dealWriteThread()
	for i := 0; i < 100; i++ {
		go writeThread()
	}
	for i := 0; i < 20; i++ {
		go deleteThread()
	}

	for i := 0; i < 10; i++ {
		go readThead()
	}
	for {
		time.Sleep(time.Second)
	}
}

func deleteThread() {
	for {
		select {
		case key := <-deleteChan:
			err := gBlobClient.Delete(key)
			if err != nil {
				deleteSuccessFp.Write([]byte((key + err.Error() + "\n")))
			} else {
				deleteFailFp.Write([]byte((key + "\n")))
			}
		}
	}
}

func readThead() {
	for {
		select {
		case key := <-readChan:
			_, err := gBlobClient.Read(key)
			if err != nil {
				m := fmt.Sprintf("read key:(%v) failed (%v)", key, err.Error())
				readFailFp.Write([]byte(m))
			}
		}
	}

}

func writeThread() {
	maxLen := util.MB * 5
	for {
		rand.Seed(time.Now().UnixNano())
		size := rand.Intn(maxLen)
		data := util.RandStringBytesMaskImprSrc(size)
		key, err := gBlobClient.Write(data)
		if err == nil {
			keyChan <- key
		} else {
			fmt.Printf("write key err:%v", err.Error())
		}
	}
}

func dealWriteThread() {
	var index uint64
	for {
		select {
		case key := <-keyChan:
			index++
			writeSuccessFp.Write([]byte((key + "\n")))
			if index%3 == 0 {
				deleteChan <- key
			} else if index%2 == 0 {
				readChan <- key
			}
		}
	}
}
