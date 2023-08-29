package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
)


func runInLinux(cmd string) (string, error) {
	fmt.Println("Running Linux cmd:" + cmd)
	result, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(result)), err
}

var (
	port=flag.Int("port",10090,"default 10090 port")
)

func main() {
	flag.Parse()
	cmd := `ps -ef | grep python | grep -v grep | awk '{print $2}'`
	text,err:=runInLinux(cmd)
	if err!=nil {
		fmt.Println(fmt.Sprintf("cmd (%v), failed(%v) ,out (%v)",cmd,err,text))
		return
	}
	arr:=strings.Split(text,"\n")
	newArr:=make([]int,0)
	for _,a:=range arr{
		pid,err1:=strconv.Atoi(string(a))
		if err1!=nil {
			continue
		}
		newArr=append(newArr,pid)
	}

	if len(newArr)==0{
		fmt.Println(fmt.Sprintf("cannot find any process (%v)",text))
		return
	}

	url:=fmt.Sprintf("http://127.0.0.1:%v/post/processID",*port)

	data,_:=json.Marshal(newArr)
	r, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		fmt.Println(fmt.Sprintf("post (%v) data(%v) failed(%v) ",url,string(data),err))
		return
	}
	r.Header.Add("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(r)
	if err != nil {
		fmt.Println(fmt.Sprintf("post (%v) data(%v) failed(%v) ",url,string(data),err))
		return
	}
	defer res.Body.Close()
	body,_:=ioutil.ReadAll(res.Body)
	if res.StatusCode!=http.StatusOK {
		fmt.Println(fmt.Sprintf("post (%v) data(%v) failed(%v) ",url,string(data),string(body)))
		return
	}
	fmt.Println(fmt.Sprintf("post (%v) data(%v) success ",url,string(data)))

	return

}
