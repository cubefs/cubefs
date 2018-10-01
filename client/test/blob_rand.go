package main

import (
	"fmt"
	"github.com/tiglabs/containerfs/sdk/data/blob"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"math/rand"
	"os"
	"runtime"
	"time"
)

var (
	gBlobClient       *blob.BlobClient
	keyChan           = make(chan string, 10000000)
	deleteChan        = make(chan string, 1000000)
	readChan          = make(chan string, 1000000)
	writeChan         = make(chan string, 10000000)
	readFailChan      = make(chan string, 10000000)
	deleteFailChan    = make(chan string, 10000000)
	deleteSuccessChan = make(chan string, 10000000)
	writeSuccessChan  = make(chan string, 10000000)

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
	log.InitLog("clogs", "client", log.DebugLevel)
	deleteSuccessFp, err = os.OpenFile(prefix+"delsuccess.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	deleteFailFp, err = os.OpenFile(prefix+"delfail.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}

	readFailFp, err = os.OpenFile(prefix+"readfail.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	writeFailFp, err = os.OpenFile(prefix+"writefail.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	writeSuccessFp, err = os.OpenFile(prefix+"writesuccess.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	go dispatchThread()
	for i := 0; i < 50; i++ {
		go writeThread()
	}
	for i := 0; i < 50; i++ {
		go deleteThread()
	}

	for i := 0; i < 10; i++ {
		go readThead()
	}
	go writeFileThread()
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
				m := fmt.Sprintf("%v delete error(%v)", key, err.Error())
				deleteFailChan <- m
			} else {
				deleteSuccessChan <- key
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
				readFailChan <- m
			}
		}
	}

}

func geratorRandbytes(size int) (data []byte) {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	data = make([]byte, size)
	for i := range data {
		data[i] = byte(letters[rand.Intn(len(letters))])
	}
	return
}

func writeThread() {
	maxLen := util.MB * 5
	rand.Seed(time.Now().UnixNano())
	data := geratorRandbytes(maxLen)
	for {
		rand.Seed(time.Now().UnixNano())
		size := rand.Intn(maxLen)
		key, err := gBlobClient.Write(data[:size])
		if err == nil {
			keyChan <- key
		} else {
			fmt.Printf("write key err:%v", err.Error())
		}
	}
}

func dispatchThread() {
	var index uint64
	for {
		select {
		case key := <-keyChan:
			index++
			writeSuccessChan <- key
			if index%3 == 0 {
				deleteChan <- key
			} else if index%2 == 0 {
				readChan <- key
			}
		}
	}
}

func writeFileThread() {
	for {
		select {
		case key := <-writeSuccessChan:
			m := fmt.Sprintf("%v\n", key)
			if _, err := writeSuccessFp.WriteString(m); err != nil {
				fmt.Println(err)
				panic(err)
				return
			}
		case key := <-deleteSuccessChan:
			m := fmt.Sprintf("%v\n", key)
			if _, err := deleteSuccessFp.WriteString(m); err != nil {
				fmt.Println(err)
				panic(err)
				return
			}
		case key := <-deleteFailChan:
			m := fmt.Sprintf("%v\n", key)
			if _, err := deleteFailFp.WriteString(m); err != nil {
				fmt.Println(err)
				panic(err)
				return
			}
		case key := <-readFailChan:
			m := fmt.Sprintf("%v\n", key)
			if _, err := readFailFp.WriteString(m); err != nil {
				fmt.Println(err)
				panic(err)
				return
			}
		}
	}
}
