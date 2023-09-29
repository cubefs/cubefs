package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func copyFIle(src,dst string)(err error){
	fsrc,errsrc:=os.Stat(src)
	if errsrc!=nil {
		return errsrc
	}

	fdst,errdst:=os.Stat(dst)
	if errdst==nil{
		if fsrc.Size()==fdst.Size(){
			return nil
		}

	}

	var data []byte
	data,err=ioutil.ReadFile(src)
	if err!=nil {
		return
	}

	err=ioutil.WriteFile(dst,data,0777)

	return
}

var (
	input=flag.String("input","0_1_10000000.txt","input file list")
	threads=flag.Int("threads",3000,"default copy threadNum")
	skipNum=flag.Int("skip",0,"default skip number")
	oldsubPath=flag.String("oldsubpath","/cfs/","default old sub path on input file")
	newsubPath=flag.String("newsubpath","/local/","default new sub path on input file")
	port=flag.Int("port",8081,"default http port")
	fininshCnt uint64
	fileCh=make(chan string,204800000)
)

func asyncCopy(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	for {
		select {
		case src:=<-fileCh:
			dst:=strings.Replace(src,*oldsubPath,*newsubPath,-1)
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
	go func() {
		http.ListenAndServe(fmt.Sprintf(":%v",*port), nil)
	}()

	var wg sync.WaitGroup
	for i:=0;i<*threads;i++{
		wg.Add(1)
		go asyncCopy(&wg)
	}
    atomic.AddUint64(&fininshCnt,uint64(*skipNum))

	file, err := os.Open(*input)
	if err != nil {
		fmt.Println(fmt.Sprintf("read inputFIle(%v) err(%v)",*input,err))
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	i:=0
	for scanner.Scan() {
		i++
		if i< *skipNum{
			continue
		}
		line := scanner.Text()
		line=strings.Trim(line,"\n")
		line=strings.Trim(line," ")
		fileCh<-line
	}

	for {
		time.Sleep(time.Second)
		fmt.Println(fmt.Sprintf("has copy %v files ",atomic.LoadUint64(&fininshCnt)))
	}

}



