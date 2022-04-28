// Copyright 2022 The ChubaoFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package bcache

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"

	_ "net/http/pprof"
)

const (
	UnixSocketPath         = "/var/lib/kubelet/bcache.socket"
	CacheKey               = "cachekey"
	CheckSum               = "md5sum"
	OffSet                 = "offset"
	Len                    = "len"
	Volume                 = "volume"
	ContextKeyStatusCode   = "status_code"
	ContextKeyErrorMessage = "error_message"
	ContextKeyRequestID    = "ctx_request_id"

	//config
	CacheDir     = "cacheDir"
	CacheSize    = "cacheSize"
	CacheLimit   = "cacheLimit"
	CacheFree    = "cacheFree"
	BlockSize    = "blockSize"
	MaxBlockSize = 128 << 20
)

var (
	keyRegexp = regexp.MustCompile("^.+?_(\\d)+_[0-9a-zA-Z]+?")
)

var (
	BadDigest       = &ErrorCode{ErrorCode: "BadDigest", ErrorMessage: "The Content-MD5 you specified did not match what we received.", StatusCode: http.StatusBadRequest}
	KeyTooLongError = &ErrorCode{ErrorCode: "KeyTooLongError", ErrorMessage: "", StatusCode: http.StatusBadRequest}
	InvalidKey      = &ErrorCode{ErrorCode: "InvalidKey", ErrorMessage: "Cache key is Illegal", StatusCode: http.StatusBadRequest}
	EntityTooLarge  = &ErrorCode{ErrorCode: "EntityTooLarge", ErrorMessage: "Your proposed upload exceeds the maximum allowed object size.", StatusCode: http.StatusBadRequest}
	InvalidArgument = &ErrorCode{ErrorCode: "InvalidArgument", ErrorMessage: "Invalid Argument,cachekey|md5|offsetlen is required.", StatusCode: http.StatusBadRequest}
	InvalidNumber   = &ErrorCode{ErrorCode: "InvalidNumber", ErrorMessage: "Invalid Argument type,need number .", StatusCode: http.StatusBadRequest}
	NoSuchCacheKey  = &ErrorCode{ErrorCode: "NoSuchCacheKey", ErrorMessage: "The specified cache key does not exist.", StatusCode: http.StatusNotFound}
	ReadStreamError = &ErrorCode{ErrorCode: "ReadStreamError", ErrorMessage: "Read body stream unknown exception.", StatusCode: http.StatusBadRequest}
	InternalError   = &ErrorCode{ErrorCode: "InternalError", ErrorMessage: "Server interval error.", StatusCode: http.StatusInternalServerError}
)

type ErrorCode struct {
	ErrorCode    string
	ErrorMessage string
	StatusCode   int
}

type bcacheConfig struct {
	CacheDir  string
	BlockSize uint32
	Mode      uint32
	CacheSize int64
	FreeRatio float32
	Limit     int
}

type bcacheStore struct {
	bcache       BcacheManager
	conf         *bcacheConfig
	unixListener net.Listener
	control      common.Control
	bufPool      sync.Pool
}

func NewServer() *bcacheStore {
	return &bcacheStore{}
}
func (s *bcacheStore) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return s.control.Start(s, cfg, doStart)
}
func (s *bcacheStore) Shutdown() {
	s.control.Shutdown(s, doShutdown)
}

func (s *bcacheStore) Sync() {
	s.control.Sync()
}

func doStart(server common.Server, cfg *config.Config) (err error) {
	s, ok := server.(*bcacheStore)
	if !ok {
		return errors.New("Invalid node Type!")
	}
	// parse the config file
	var bconf *bcacheConfig
	bconf, err = s.parserConf(cfg)
	if err != nil {
		err = errors.NewErrorf("block config parser error.")
		panic(err)
	}
	// start bcache manage
	bm := newBcacheManager(bconf)
	if bm == nil {
		err = errors.NewErrorf("block cache manager init fail.")
		panic(err)
	}
	s.bcache = bm
	s.conf = bconf
	s.bufPool = sync.Pool{New: func() interface{} {
		buf := make([]byte, MaxBlockSize)
		return &buf
	}}
	// start unix domain socket
	s.startUnixHttpServer()
	return
}
func doShutdown(server common.Server) {
	s, ok := server.(*bcacheStore)
	if !ok {
		return
	}
	//stop unix domain socket
	if s.unixListener != nil {
		s.unixListener.Close()
	}
}

func (s *bcacheStore) registerHandler(router *mux.Router) {
	router.NewRoute().Methods(http.MethodPut).Path("/cache").HandlerFunc(s.cacheBlock)
	router.NewRoute().Methods(http.MethodGet).Path("/load").HandlerFunc(s.loadBlock)
	router.NewRoute().Methods(http.MethodDelete).Path("/evict").HandlerFunc(s.evictBlock)
	router.NewRoute().Methods(http.MethodHead).Path("/stop").HandlerFunc(s.stopService)
}

func (s *bcacheStore) startUnixHttpServer() {
	os.Mkdir(filepath.Dir(UnixSocketPath), FilePerm)
	if _, err := os.Stat(UnixSocketPath); err == nil {
		existErr := fmt.Sprintf("Another process is running or %s already exist.", UnixSocketPath)
		panic(errors.New(existErr))
	}
	lis, err := net.Listen("unix", UnixSocketPath)
	if err != nil {
		panic(err)
	}
	router := mux.NewRouter().SkipClean(true)
	s.unixListener = lis
	s.registerHandler(router)
	go func() {
		for {
			time.Sleep(time.Minute * 2)
			runtime.GC()
		}
	}()
	go http.Serve(lis, router)
}

func (s *bcacheStore) cacheBlock(w http.ResponseWriter, r *http.Request) {
	var err error
	var errorCode *ErrorCode

	defer func() {
		if errorCode != nil {
			errorCode.ServeResponse(w, r)
		}
		if r != nil && r.Body != nil {
			r.Body.Close()
		}
	}()

	key := r.URL.Query().Get(CacheKey)
	md5Sum := r.URL.Query().Get(CheckSum)
	if key == "" || md5Sum == "" {
		log.LogErrorf("InvalidArgument,cacheKey or md5 is nil.")
		errorCode = InvalidArgument
		return
	}

	if !keyRegexp.MatchString(key) {
		log.LogErrorf("InvalidKey,cacheKey is invalid.  key(%v)", key)
		errorCode = InvalidKey
		return
	}

	var (
		readN int
		hash  = md5.New()
	)
	buf := s.bufPool.Get().(*[]byte)
	defer func() {
		s.bufPool.Put(buf)
	}()

	defer r.Body.Close()
	content := make([]byte, r.ContentLength)
	n, err := io.ReadFull(r.Body, content)
	if err != nil && err != io.EOF && int64(n) != r.ContentLength {
		log.LogErrorf("ReadStreamError. key(%v)  err(%v) ContentLength(%v) readN(%v)", key, err, r.ContentLength, n)
		errorCode = ReadStreamError
		return
	}

	readN = copy(*buf, content)
	if uint32(readN) > MaxBlockSize {
		log.LogErrorf("EntityTooLarge. key(%v)  readN(%v) blockSize(%v)", key, readN, s.conf.BlockSize)
		errorCode = EntityTooLarge
		return
	}
	if readN > 0 {
		hash.Write((*buf)[:readN])
		md5 := hex.EncodeToString(hash.Sum(nil))
		if md5 == md5Sum {
			s.bcache.cache(key, content, false)
		} else {
			errorCode = BadDigest
			log.LogErrorf("BadDigest. key(%v)  request_md5(%v) md5(%v)", key, md5Sum, md5)
			return
		}

	}
	return
}

func (s *bcacheStore) loadBlock(w http.ResponseWriter, r *http.Request) {
	var errorCode *ErrorCode
	defer func() {
		if errorCode != nil {
			errorCode.ServeResponse(w, r)
		}
	}()
	parameter := r.URL.Query()
	key := parameter.Get(CacheKey)
	offsetStr := parameter.Get(OffSet)
	lenStr := parameter.Get(Len)

	if !keyRegexp.MatchString(key) {
		errorCode = InvalidKey
		return
	}

	if key == "" || offsetStr == "" || lenStr == "" {
		errorCode = InvalidArgument
		return
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		errorCode = InvalidNumber
		return
	}
	length, err := strconv.ParseUint(lenStr, 10, 64)
	if err != nil {
		errorCode = InvalidNumber
		return
	}
	in, err := s.bcache.read(key, offset, uint32(length))
	if err != nil {
		if os.IsNotExist(err) {
			errorCode = NoSuchCacheKey
		} else {
			errorCode = InternalError
		}
		return
	}
	w.Header()["Content-Type"] = []string{"application/octet-stream"}
	defer in.Close()
	io.Copy(w, in)
	return
}

func (s *bcacheStore) stopService(w http.ResponseWriter, r *http.Request) {
	s.Shutdown()
}

func (s *bcacheStore) parserConf(cfg *config.Config) (*bcacheConfig, error) {
	bconf := &bcacheConfig{}
	cacheDir := cfg.GetString(CacheDir)
	cacheLimit := cfg.GetString(CacheLimit)
	cacheFree := cfg.GetString(CacheFree)
	blockSize := cfg.GetString(BlockSize)
	bconf.CacheDir = cacheDir
	if cacheDir == "" {
		return nil, errors.NewErrorf("cacheDir is required.")
	}
	if v, err := strconv.Atoi(blockSize); err == nil {
		bconf.BlockSize = uint32(v)

	}
	if v, err := strconv.Atoi(cacheLimit); err == nil {
		bconf.Limit = v

	}
	if v, err := strconv.ParseFloat(cacheFree, 32); err == nil {
		bconf.FreeRatio = float32(v)
	}
	return bconf, nil

}

func (s *bcacheStore) evictBlock(w http.ResponseWriter, r *http.Request) {
	var errorCode *ErrorCode
	defer func() {
		if errorCode != nil {
			errorCode.ServeResponse(w, r)
		}
	}()
	parameter := r.URL.Query()
	key := parameter.Get(CacheKey)
	if !keyRegexp.MatchString(key) {
		errorCode = InvalidKey
		return
	}

	if key == "" {
		errorCode = InvalidArgument
		return
	}
	s.bcache.erase(key)
}

func (code ErrorCode) ServeResponse(w http.ResponseWriter, r *http.Request) error {
	var err error
	var marshaled []byte

	mux.Vars(r)[ContextKeyStatusCode] = strconv.Itoa(code.StatusCode)
	mux.Vars(r)[ContextKeyErrorMessage] = code.ErrorMessage
	requestId := mux.Vars(r)[ContextKeyRequestID]

	var xmlError = struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Resource  string   `xml:"Resource"`
		RequestId string   `xml:"RequestId"`
	}{
		Code:      code.ErrorCode,
		Message:   code.ErrorMessage,
		Resource:  r.URL.String(),
		RequestId: requestId,
	}

	if marshaled, err = xml.Marshal(&xmlError); err != nil {
		return err
	}
	w.Header()["Content-Type"] = []string{"application/xml"}
	w.WriteHeader(code.StatusCode)
	if _, err = w.Write(marshaled); err != nil {
		return err
	}
	return nil

}
