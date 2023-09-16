package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
)

type DownloadInfo struct {
	fileName string
	data     []byte
	result   chan error
}

func NewDownloadInfo(fileName string) (di *DownloadInfo) {
	di = new(DownloadInfo)
	di.fileName = fileName
	di.result = make(chan error, 1)
	return di
}

var (
	asyncReadCh = make(chan *DownloadInfo, 10000)
)

func asyncReadFile() {
	for {
		select {
		case di := <-asyncReadCh:
			var err error
			di.data, err = ioutil.ReadFile(di.fileName)
			di.result <- err
		}
	}
}

func batchDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var train_paths [][]string
	err := json.NewDecoder(r.Body).Decode(&train_paths)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Transfer-Encoding", "chunked")
	binary.Write(w, binary.BigEndian, uint64(1))
	binary.Write(w, binary.BigEndian, uint64(len(train_paths[0])))
	readDownloadInfos := make([]*DownloadInfo, 0)
	for _, file_path := range train_paths[0] {
		di := NewDownloadInfo(file_path)
		readDownloadInfos = append(readDownloadInfos, di)
		asyncReadCh <- di
	}

	flusher, _ := w.(http.Flusher)
	for _, di := range readDownloadInfos {
		pathErr := <-di.result
		if pathErr != nil {
			fmt.Println(fmt.Sprintf("file_path:%v readerror:%v", di.fileName, pathErr))
			continue
		}
		err = binary.Write(w, binary.BigEndian, uint64(len(di.fileName)))
		if err != nil {
			fmt.Println(fmt.Sprintf("write error:%v", err))
			break
		}
		_, err = w.Write([]byte(di.fileName))
		if err != nil {
			fmt.Println(fmt.Sprintf("write error:%v", err))
			break
		}
		pathData := di.data
		err = binary.Write(w, binary.BigEndian, uint64(len(pathData)))
		if err != nil {
			fmt.Println(fmt.Sprintf("write error:%v", err))
			break
		}
		_, err = w.Write(pathData)
		if err != nil {
			fmt.Println(fmt.Sprintf("write error:%v", err))
			break
		}
		flusher.Flush()
	}

}

var (
	prof = flag.Int("prof", 8001, "default prof port")
)

func main() {

	flag.Parse()
	for i := 0; i < 10; i++ {
		go asyncReadFile()
	}
	http.HandleFunc("/batchdownload/path", batchDownloadHandler)

	fmt.Println(fmt.Sprintf("Starting server on port %v", *prof))
	err := http.ListenAndServe(fmt.Sprintf(":%v", *prof), nil)
	if err != nil {
		fmt.Println(fmt.Sprintf("error listen %v", err))
	}

}
