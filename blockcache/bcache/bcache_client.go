package bcache

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/stat"
	"golang.org/x/net/context"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultTimeOut = 5 * time.Second
	SlowRequest    = 2 * time.Second
)

type BcacheClient struct {
	httpc http.Client
}

var once sync.Once
var bClient *BcacheClient

func NewBcacheClient() *BcacheClient {
	once.Do(func() {
		httpc := http.Client{
			Transport: &http.Transport{
				DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
					return net.DialTimeout("unix", UnixSocketPath, DefaultTimeOut)
				},
				MaxConnsPerHost:     20,
				IdleConnTimeout:     DefaultTimeOut,
				MaxIdleConnsPerHost: 20,
			},
			Timeout: DefaultTimeOut,
		}
		bClient = &BcacheClient{httpc: httpc}
	})
	return bClient
}

func (bc *BcacheClient) Get(key string, buf []byte, offset uint64, size uint32) (int, error) {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("bcache-get", err, bgTime, 1)
	}()

	url := fmt.Sprintf("http://unix/load?cachekey=%v&offset=%v&len=%v", key, offset, size)
	log.LogDebugf("TRACE BCache client Get() Enter. key(%v) offset(%v) size(%v) request(%v)", key, offset, size, url)
	start := time.Now()
	log.LogDebugf("TRACE BCache client Get(). bc(%v) bc.httpc(%v)", bc, bc.httpc)
	response, err := bc.httpc.Get(url)
	defer func() {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
	}()
	if err != nil {
		log.LogErrorf("TRACE BCache client Get() FAIL err(%v)", err)
		return 0, err
	}
	if response.StatusCode != http.StatusOK {
		log.LogErrorf("TRACE BCache client Get() FAIL. response(%v)", response)
		return 0, errors.NewErrorf("Bad response(%v)", response)
	}

	result, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.LogErrorf("TRACE BCache client ReadAll FAIL. err(%v)", err)
		return 0, err
	}
	n := copy(buf, result)
	delay := time.Since(start)
	if delay > SlowRequest {
		log.LogWarnf("slow request:GET cache key:%v,  consume:%.3f s", key, delay.Seconds())
	}
	if n != int(size) {
		log.LogErrorf("BCache client GET() error,exception size(%v),but readSize(%v)", size, n)
		return 0, errors.NewErrorf("BCache client GET() error,exception size(%v),but readSize(%v)", size, n)
	}
	log.LogDebugf("q. key(%v) offset(%v) size(%v) consume(%v)ns",
		key, offset, size, delay.Nanoseconds())
	return n, nil
}

func (bc *BcacheClient) Put(key string, buf []byte) error {
	var err error
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("bcache-put", err, bgTime, 1)
	}()

	url := fmt.Sprintf("http://unix/cache?cachekey=%v", key)
	md5 := md5.New()
	md5.Write(buf)
	checkSum := hex.EncodeToString(md5.Sum(nil))
	url = fmt.Sprintf("%v&md5sum=%v", url, checkSum)
	log.LogDebugf("TRACE BCache client Put() Enter. key(%v) lenbuf(%v) request(%v)", key, len(buf), url)
	body := bytes.NewBuffer(buf)
	req, _ := http.NewRequest(http.MethodPut, url, body)
	//req.Header.Set("Content-Type", "application/octet-stream")
	start := time.Now()
	response, err := bc.httpc.Do(req)
	if err != nil {
		log.LogErrorf("TRACE BCache client Put() FAIL err(%v)", err)
		return err
	}
	if response.StatusCode != http.StatusOK {
		log.LogErrorf("TRACE BCache client Put() FAIL. response(%v)", response)
		return errors.NewErrorf("Bad response(%v)", response)
	}
	delay := time.Since(start)
	log.LogDebugf("TRACE BCache client Put() Exit. key(%v)  response(%v) consume(%v)ns", key, response, delay.Nanoseconds())
	return nil
}

func (bc *BcacheClient) Evict(key string) {
	url := fmt.Sprintf("http://unix/evict?cachekey=%v", key)
	req, _ := http.NewRequest(http.MethodDelete, url, nil)
	_, err := bc.httpc.Do(req)
	if err != nil {
		log.LogErrorf("evict cache key:%v fail, err= %v", key, err)
	}
}
