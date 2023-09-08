package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
)



func batchDownloadHandler(w http.ResponseWriter, r *http.Request) {

	// 解析请求 body
	var train_paths [][]string
	err := json.NewDecoder(r.Body).Decode(&train_paths)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	binary.Write(w, binary.BigEndian, uint64(1))
	binary.Write(w, binary.BigEndian, uint64(len(train_paths[0])))
	for _, file_path := range train_paths[0] {
		binary.Write(w, binary.BigEndian, uint64(len(file_path)))
		w.Write([]byte(file_path))
		path_data,path_err:=ioutil.ReadFile(file_path)
		if path_err!=nil{
			fmt.Println(fmt.Sprintf("file_path:%v readerror:%v", file_path,path_err))
			continue
		}
		binary.Write(w, binary.BigEndian, uint64(len(path_data)))
		w.Write(path_data)

	}

}

var (
	prof=flag.Int("prof",8001,"default prof port")
)

func main() {

	flag.Parse()
	http.HandleFunc("/batchdownload/path", batchDownloadHandler)

	fmt.Println(fmt.Sprintf("Starting server on port %v",*prof))
	err:=http.ListenAndServe(fmt.Sprintf(":%v",*prof), nil)
	if err!=nil{
		fmt.Println(fmt.Sprintf("error listen %v",err))
	}

}