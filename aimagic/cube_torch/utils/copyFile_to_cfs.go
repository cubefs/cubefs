package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func copyFIle(src,dst string)(err error){
	var data []byte
	data,err=ioutil.ReadFile(src)
	if err!=nil {
		return
	}

	fi,err:=os.Stat(dst)
	if err==nil {
		if fi.Size()==int64(len(data)){
			return nil
		}
	}
	err=ioutil.WriteFile(dst,data,0777)

	return
}

var (
	input=flag.String("input","0_1_10000000.txt","input file list")
	threads=flag.Int("threads",9000,"default copy threadNum")
	seqNum= flag.Int("seq",1,"default seq number")
	fininshCnt uint64
)

func asyncCopy(fileCh chan string,wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	for {
		select {
			case src:=<-fileCh:
				dst:=strings.Replace(src,"/local/","/cfs/",-1)
				dst=strings.Replace(dst,"vlp/000",fmt.Sprintf("vlp/%03d",*seqNum),-1)
				basedir:=path.Dir(dst)
				_,err:=os.Stat(basedir)
				if err!=nil {
					os.MkdirAll(basedir,0777)
				}
				err=copyFIle(src,dst)
				if err!=nil {
					fmt.Println(fmt.Sprintf("copy File %v, dst %v err %v",src,dst,err))
				}
				atomic.AddUint64(&fininshCnt,1)
		}
	}

}

func main() {
	flag.Parse()
	data,err:=ioutil.ReadFile(*input)
	if err!=nil {
		fmt.Println(fmt.Sprintf("read inputFIle(%v) err(%v)",*input,err))
		return
	}

	fileCh:=make(chan string,20480000)
	var wg sync.WaitGroup
	for i:=0;i<*threads;i++{
		wg.Add(1)
		go asyncCopy(fileCh,&wg)
	}

	arr:=bytes.Split(data,[]byte("\n"))

	for _,a:=range arr {
		text:=string(a)
		text=strings.Trim(text,"\n")
		text=strings.Trim(text," ")
		if strings.Contains(text,"vlp/001"){
			continue
		}
		fileCh<-text
	}
	for {
		time.Sleep(time.Second)
		fmt.Println(fmt.Sprintf("has copy %v files ",atomic.LoadUint64(&fininshCnt)))
	}

}



