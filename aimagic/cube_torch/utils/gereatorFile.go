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
	if err!=nil {
		return
	}
	_,err=fpWrite.WriteString(fmt.Sprintf("%v\n",dst))

	return
}

var (
	input=flag.String("input","/mnt/local/chubaofs_tech_data-test/sangqingyuan1/vlp/0_2.txt","input file list")
	out=flag.String("out","/mnt/local/chubaofs_tech_data-test/sangqingyuan1/vlp/0_2_new.txt","input file list")
	threads=flag.Int("threads",1000,"default copy threadNum")
	dstDir=flag.String("dst","/mnt/cfs/chubaofs_tech_data-test/sangqingyuan1/vlp/titles_back","default dst dir")
	fininshCnt uint64
	fpWrite *os.File
	copyInfoCh=make(chan string,204800000)
	fileCh=make(chan string,204800000)
)

func asyncCopy(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	for {
		select {
			case copyInfo:=<-copyInfoCh:
				arr:=strings.Split(copyInfo,"|")
				src:=arr[0]
				dst:=arr[1]
				err:=copyFIle(src,dst)
				if err!=nil {
					fmt.Println(fmt.Sprintf("copy File %v, dst %v err %v",src,dst,err))
				}
				atomic.AddUint64(&fininshCnt,1)
		}
	}

}


func asyncgenerator(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	for {
		select {
		case text:=<-fileCh:
			ext:=path.Ext(text)
			fileName:=path.Base(text)
			var dst string
			if ext==".jpg"{
				for i:=0;i<120;i++{
					prefix:=strings.Split(fileName,"_")[0]
					dst=path.Join(*dstDir,fmt.Sprintf("%v_%03d%v",prefix,i,ext))
					copyInfo:=fmt.Sprintf("%v|%v",text,dst)
					copyInfoCh<-copyInfo
				}
			}else {
				for i:=0;i<120;i++{
					prefix:=strings.Split(fileName,".")[0]
					dst=path.Join(*dstDir,fmt.Sprintf("%v_%03d%v",prefix,i,ext))
					copyInfo:=fmt.Sprintf("%v|%v",text,dst)
					copyInfoCh<-copyInfo
				}
			}
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

	fpWrite,err=os.OpenFile(*out,os.O_CREATE|os.O_TRUNC|os.O_RDWR,0777)
	if err!=nil {
		fmt.Println(fmt.Sprintf("read inputFIle(%v) err(%v)",*input,err))
		return
	}

	var wg sync.WaitGroup
	for i:=0;i<*threads;i++{
		wg.Add(1)
		go asyncCopy(&wg)
	}


	for i:=0;i<30;i++{
		wg.Add(1)
		go asyncgenerator(&wg)
	}

	arr:=bytes.Split(data,[]byte("\n"))

	for _,a:=range arr {
		text:=string(a)
		text=strings.Trim(text,"\n")
		text=strings.Trim(text," ")
		fileCh<-text

	}
	for {
		time.Sleep(time.Second)
		fpWrite.Sync()
		fmt.Println(fmt.Sprintf("has copy %v files ",atomic.LoadUint64(&fininshCnt)))
	}

}



