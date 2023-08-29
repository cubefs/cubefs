package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"net/http"
	_ "net/http/pprof"
	"time"
)


var (
	input              =flag.String("input","/mnt/cfs/chubaofs_tech_data-test/sangqingyuan1/vlp/0_1_new.txt","input file list")
	cnt                =flag.Uint64("cnt",10000000,"default get cnt")
	checkCh            =make(chan string,204800000)
	writeCh            =make(chan string,204800000)
	signleCh           =make(chan struct{})
	gereatorIndexCount uint64
)


func checkError() {
	for {
		select {
			case text:=<-checkCh:
				text=strings.Trim(text,"\n")
				text=strings.Trim(text," ")
				parent:=path.Dir(text)
				name:=path.Base(text)
				prefix:=strings.Split(name,".")[0]
				par_parent:=path.Dir(parent)
				bertpath:=path.Join(par_parent,"titles_back",fmt.Sprintf("%v.bertids",prefix))
				_,err1:=os.Stat(text)
				_,err2:=os.Stat(bertpath)
				if err1!=nil {
					continue
				}
				if err2!=nil {
					continue
				}
				info:=fmt.Sprintf("%v|%v",text,bertpath)
				writeCh<-info
			case <-signleCh:
				return
		}
	}
}



func writeFile(f1,f2 *os.File,wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		f1.Sync()
		f2.Sync()
	}()
	for {
		select {
		case text:=<-writeCh:
			arr:=strings.Split(text,"|")
			f1.WriteString(fmt.Sprintf("%v\n",arr[0]))
			f2.WriteString(fmt.Sprintf("%v\n",arr[1]))

			atomic.AddUint64(&gereatorIndexCount,1)
			if atomic.LoadUint64(&gereatorIndexCount)> *cnt {
				close(signleCh)
				return
			}

		case <-signleCh:
			return
		}
	}
}





func main() {
	flag.Parse()
	index1:=fmt.Sprintf("/mnt/cfs/chubaofs_tech_data-test/sangqingyuan1/vlp/0_1_%v.txt",*cnt)
	index2:=fmt.Sprintf("/mnt/cfs/chubaofs_tech_data-test/sangqingyuan1/vlp/0_2_%v.txt",*cnt)

	go func() {
		http.ListenAndServe("0.0.0.0:8081", nil)
	}()

	f1,err:=os.OpenFile(index1,os.O_CREATE|os.O_TRUNC|os.O_RDWR,0777)
	if err!=nil {
		fmt.Println(fmt.Sprintf("open FIle(%v) err(%v)",index1,err))
		return
	}
	defer f1.Close()

	f2,err:=os.OpenFile(index2,os.O_CREATE|os.O_TRUNC|os.O_RDWR,0777)
	if err!=nil {
		fmt.Println(fmt.Sprintf("open FIle(%v) err(%v)",index2,err))
		return
	}
	defer f2.Close()

	fi, err := os.Open(*input)
	if err != nil {
		fmt.Println(fmt.Sprintf("read inputFIle(%v) err(%v)",*input,err))
		return
	}
	defer fi.Close()

	for i:=0;i<30;i++{
		go checkError()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go writeFile(f1,f2,&wg)
	br := bufio.NewReader(fi)
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		text:=string(a)
		checkCh<-text
		if atomic.LoadUint64(&gereatorIndexCount)> *cnt {
			break
		}
	}
	wg.Wait()

	for {
		time.Sleep(time.Second)
		fmt.Println(fmt.Sprintf("has gereator %v index ",atomic.LoadUint64(&gereatorIndexCount)))
	}
}



