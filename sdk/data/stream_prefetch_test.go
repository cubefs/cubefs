package data

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func TestPrefetch_Load_Prefetch(t *testing.T)  {
	logDir := "/tmp/logs/cfs"
	log.InitLog(logDir, "test", log.DebugLevel, nil)

	datasetCnt := 5
	indexPath := "/cfs/mnt/TestPrefetch/index"
	dirIndexIno, err := createIndexFile(indexPath, datasetCnt)
	assert.Equal(t, nil, err, "createIndexFile err")

	var ec *ExtentClient
	_, ec, err = creatHelper(t)
	assert.Equal(t, nil, err, "init err")
	pManager := NewPrefetchManager(ec, ltptestVolume, "/cfs/mnt", proto.RootIno, 5)
	defer func() {
		pManager.Close()
	}()

	err = pManager.AddIndexFilepath(strconv.FormatInt(int64(datasetCnt), 10), indexPath, 10)
	assert.Equal(t, nil, err, "add index err")
	// assert duplicate index
	err = pManager.AddIndexFilepath(strconv.FormatInt(int64(datasetCnt), 10), indexPath, 1)
	assert.Equal(t, nil, err, "reset index err")
	time.Sleep(1 * time.Second)

	count := 0
	// assert prefetch info
	pManager.indexFileInfoMap.Range(func(key, value interface{}) bool {
		count++
		assert.Equal(t, indexPath, key, "index file path")
		indexInfo := value.(*IndexInfo)
		assert.Equal(t, strconv.FormatInt(int64(datasetCnt), 10), indexInfo.datasetCnt, "index dataset cnt")
		assert.Equal(t, indexPath, indexInfo.path, "index dataset cnt")
		assert.Equal(t, int64(1), indexInfo.validMinute, "index file valid minute")
		assert.Equal(t, datasetCnt, len(indexInfo.fileInfoMap), "index info count")
		return true
	})
	assert.Equal(t, 1, count, "index info count")
	pManager.dcacheMap.Range(func(key, value interface{}) bool {
		assert.Equal(t, dirIndexIno, key, "parent inode ID of dcache")
		return true
	})
	dCache := pManager.GetDentryCache(dirIndexIno)
	assert.NotEqual(t, nil, dCache, "get dentry cache")
	assert.Equal(t, datasetCnt, dCache.Count(), "the count of dentry cache")

	// assert prefetch
	batchArr := [][]uint64{{0, 1}, {3}}
	pManager.PrefetchInodeInfo(strconv.FormatInt(int64(datasetCnt), 10), batchArr)
	for _, indexArr := range batchArr {
		for _, index := range indexArr {
			err = pManager.PrefetchIndex(strconv.FormatInt(int64(datasetCnt), 10), index)
			assert.Equal(t, nil, err, "prefetch index err")
		}
	}

	// wait for the index file to expire
	time.Sleep(2 * time.Minute)
	// assert no prefetch info
	count = 0
	pManager.indexFileInfoMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "index info count")
	count = 0
	pManager.dcacheMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "dcache count")
}

func TestPrefetch_AppPid(t *testing.T)  {
	pManager := &PrefetchManager{}
	pManager.PutAppPid(1000)
	pManager.PutAppPid(2000)
	assert.Equal(t, true, pManager.ContainsAppPid(1000), "app pid")
	assert.Equal(t, true, pManager.ContainsAppPid(2000), "app pid")
	assert.Equal(t, false, pManager.ContainsAppPid(3000), "app pid")
	pManager.DeleteAppPid(1000)
	assert.Equal(t, false, pManager.ContainsAppPid(1000), "app pid")
	assert.Equal(t, true, pManager.ContainsAppPid(2000), "app pid")
}

func createIndexFile(indexPath string, datasetCnt int) (dirIno uint64, err error) {
	var (
		indexFh	*os.File
		dirInfo	os.FileInfo
		fh		*os.File
	)
	indexDir := path.Dir(indexPath)
	if dirInfo, err = os.Stat(indexDir); os.IsExist(err) {
		return dirInfo.Sys().(*syscall.Stat_t).Ino, nil
	}
	if err = os.MkdirAll(indexDir, 0777); err != nil {
		return
	}
	if dirInfo, err = os.Stat(indexDir); err != nil {
		return
	}
	dirIno = dirInfo.Sys().(*syscall.Stat_t).Ino
	if indexFh, err = os.Create(indexPath); err != nil {
		return
	}
	indexWriter := bufio.NewWriter(indexFh)
	defer func() {
		indexWriter.Flush()
		indexFh.Close()
	}()

	for i := 0; i < datasetCnt; i++ {
		filePath := path.Join(indexDir, "file" + strconv.FormatInt(int64(i), 10))
		if fh, err = os.Create(filePath); err != nil {
			return
		}
		b := RandStringRunes(10240)
		if _, err = fh.Write(b); err != nil {
			return
		}
		fh.Close()
		if _, err = indexWriter.WriteString(filePath + "\n"); err != nil {
			return
		}
	}
	return
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
func RandStringRunes(n int) []byte {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return []byte(string(b))
}

func TestPrefetchReadByPath(t *testing.T)  {
	profPort, err := getProfPort()
	assert.Equal(t, nil, err, "get pprof port failed")
	assert.NotEqual(t, uint64(0), profPort, "pprof port")

	batchArr := [][]string{{"/cfs/mnt/TestPrefetch/file1","/cfs/mnt/TestPrefetch/file2","cfs"},{"/cfs/mnt/TestPrefetch/file3","/cfs/mnt/TestPrefetch/file4","/cfs/mnt/TestPrefetch/file0"}}
	var reqBody []byte
	reqBody, err = json.Marshal(batchArr)
	assert.Equal(t, nil, err, "marshal err")

	_, err = post(fmt.Sprintf("http://127.0.0.1:%v/prefetch/read/path", profPort), reqBody)
	assert.Equal(t, nil, err, "post err")
}

func TestBatchDownload(t *testing.T)  {
	profPort, err := getProfPort()
	assert.Equal(t, nil, err, "get pprof port failed")
	assert.NotEqual(t, uint64(0), profPort, "pprof port")

	indexPath := "/cfs/mnt/TestPrefetch/index"
	datasetCnt := 5
	_, err = createIndexFile(indexPath, datasetCnt)
	assert.Equal(t, nil, err, "createIndexFile err")
	_, err = http.Get(fmt.Sprintf("http://127.0.0.1:%v/prefetch/pathAdd?path=%v&ttl=5&dataset_cnt=%v", profPort, indexPath, datasetCnt))
	assert.Equal(t, nil, err, "add index path err")
	time.Sleep(1 * time.Second)

	batchArr := [][]uint64{{0,1,2},{3,4,5}}
	var reqBody, res []byte
	reqBody, err = json.Marshal(batchArr)
	assert.Equal(t, nil, err, "marshal err")
	res, err = post(fmt.Sprintf("http://127.0.0.1:%v/batchdownload?dataset_cnt=%v", profPort, datasetCnt), reqBody)
	assert.Equal(t, nil, err, "post err")
	assert.Equal(t, uint64(0), binary.BigEndian.Uint64(res[:8]), "version")
	pathCount := binary.BigEndian.Uint64(res[8:16])
	assert.Equal(t, uint64(5), pathCount, "count of path")
	start := uint64(16)
	for i := uint64(0); i < pathCount; i++ {
		if start >= uint64(len(res)) {
			break
		}
		pathSize := binary.BigEndian.Uint64(res[start:start+8])
		start += 8
		fmt.Println("path: ", string(res[start:start+pathSize]))
		start += pathSize
		contentSize := binary.BigEndian.Uint64(res[start:start+8])
		assert.Equal(t, uint64(10240), contentSize, "content size")
		start += 8
		start += contentSize
	}
}

func getProfPort() (profPort uint64, err error) {
	filePath := fmt.Sprintf("%v.%v", Cube_Torch_ConfigFile, ltptestVolume)
	var fh *os.File
	fh, err = os.Open(filePath)
	if err != nil {
		return
	}
	defer fh.Close()

	b := make([]byte, 128*1024)
	readSize, _ := fh.Read(b)
	info := &CubeInfo{}
	err = json.Unmarshal(b[:readSize], info)
	return info.Prof, err
}

func TestBatchDownloadPath(t *testing.T)  {
	profPort, err := getProfPort()
	assert.Equal(t, nil, err, "get pprof port failed")
	assert.NotEqual(t, uint64(0), profPort, "pprof port")

	batchArr := [][]string{{"/cfs/mnt/TestPrefetch/file1","/cfs/mnt/TestPrefetch/file2","cfs"},{"/cfs/mnt/TestPrefetch/file3","/cfs/mnt/TestPrefetch/file4","/cfs/mnt/TestPrefetch/file0"}}
	var reqBody, res []byte
	reqBody, err = json.Marshal(batchArr)
	assert.Equal(t, nil, err, "marshal err")

	res, err = post(fmt.Sprintf("http://127.0.0.1:%v/batchdownload/path", profPort), reqBody)
	assert.Equal(t, nil, err, "post err")
	assert.Equal(t, uint64(0), binary.BigEndian.Uint64(res[:8]), "version")
	pathCount := binary.BigEndian.Uint64(res[8:16])
	assert.Equal(t, uint64(6), pathCount, "count of path")
	start := uint64(16)
	for i := uint64(0); i < pathCount; i++ {
		if start >= uint64(len(res)) {
			break
		}
		pathSize := binary.BigEndian.Uint64(res[start:start+8])
		start += 8
		filepath := string(res[start:start+pathSize])
		fmt.Println("path: ", filepath)
		start += pathSize
		contentSize := binary.BigEndian.Uint64(res[start:start+8])
		assert.Equal(t, uint64(10240), contentSize, "content size")
		start += 8
		start += contentSize
	}
}

func post(reqURL string, data []byte) (res []byte, err error) {
	var req *http.Request
	reader := bytes.NewReader(data)
	if req, err = http.NewRequest(http.MethodPost, reqURL, reader); err != nil {
		return
	}

	var resp *http.Response
	client := http.DefaultClient
	client.Timeout = 5 * time.Second
	if resp, err = client.Do(req); err != nil {
		return
	}

	res, err = ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	return
}