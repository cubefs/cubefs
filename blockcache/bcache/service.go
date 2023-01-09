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
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"io"
	"net"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
)

const (
	UnixSocketPath = "/var/lib/kubelet/device-plugins/bcache.socket"

	//config
	CacheDir     = "cacheDir"
	CacheLimit   = "cacheLimit"
	CacheFree    = "cacheFree"
	BlockSize    = "blockSize"
	MaxBlockSize = 128 << 30
)

var (
	keyRegexp = regexp.MustCompile("^.+?_(\\d)+_[0-9a-zA-Z]+?")
)

type bcacheConfig struct {
	CacheDir  string
	BlockSize uint32
	Mode      uint32
	CacheSize int64
	FreeRatio float32
	Limit     int
}

type bcacheStore struct {
	bcache  BcacheManager
	conf    *bcacheConfig
	control common.Control
	bufPool sync.Pool
	stopC   chan struct{}
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

	// start unix domain socket
	s.startServer()
	return
}

func doShutdown(server common.Server) {
	s, ok := server.(*bcacheStore)
	if !ok {
		return
	}
	//stop unix domain socket
	s.stopServer()
	//close connpool
}

func (s *bcacheStore) startServer() (err error) {
	//create socket dir
	os.MkdirAll(filepath.Dir(UnixSocketPath), FilePerm)

	if _, err := os.Stat(UnixSocketPath); err == nil {
		existErr := fmt.Sprintf("Another process is running or %s already exist,force delete it.", UnixSocketPath)
		log.LogErrorf(existErr)
		os.Remove(UnixSocketPath)
	}

	s.stopC = make(chan struct{})
	ln, err := net.Listen("unix", UnixSocketPath)
	if err != nil {
		panic(err)
	}
	go func(stopC chan struct{}) {
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			select {
			case <-stopC:
				return
			default:
			}
			if err != nil {
				continue
			}
			go s.serveConn(conn, stopC)
		}
	}(s.stopC)

	log.LogInfof("start blockcache server.")
	return
}

func (s *bcacheStore) stopServer() {
	if s.stopC != nil {
		defer func() {
			if r := recover(); r != nil {
				log.LogErrorf("action[StopBcacheServer],err:%v", r)
			}
		}()
		close(s.stopC)
	}
}

func (s *bcacheStore) serveConn(conn net.Conn, stopC chan struct{}) {
	defer conn.Close()
	for {
		select {
		case <-stopC:
			return
		default:
		}
		p := &BlockCachePacket{}
		if err := p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
			if err != io.EOF {
				log.LogDebugf("serve BcacheServer: %v", err.Error())
			}
			return
		}
		if err := s.handlePacket(conn, p); err != nil {
			log.LogDebugf("serve handlePacket fail: %v", err)
		}
	}
}

func (s *bcacheStore) handlePacket(conn net.Conn, p *BlockCachePacket) (err error) {
	switch p.Opcode {
	case OpBlockCachePut:
		err = s.opBlockCachePut(conn, p)
	case OpBlockCacheGet:
		err = s.opBlockCacheGet(conn, p)
	case OpBlockCacheDel:
		err = s.opBlockCacheEvict(conn, p)
	default:
		err = fmt.Errorf("unknown Opcode: %d", p.Opcode)
	}
	return
}

func (s *bcacheStore) opBlockCachePut(conn net.Conn, p *BlockCachePacket) (err error) {
	req := &PutCacheRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		s.response(conn, p)
		err = errors.NewErrorf("req[%v],err[%v]", req, err.Error())
		return
	}
	s.bcache.cache(req.CacheKey, req.Data, false)
	p.PacketOkReplay()
	s.response(conn, p)
	return
}

func (s *bcacheStore) opBlockCacheGet(conn net.Conn, p *BlockCachePacket) (err error) {
	req := &GetCacheRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		s.response(conn, p)
		err = errors.NewErrorf("req[%v],err[%v]", req, string(p.Data))
		return
	}

	cachePath, err := s.bcache.queryCachePath(req.CacheKey, req.Offset, req.Size)
	if err != nil {
		if err == os.ErrNotExist {
			p.PacketErrorWithBody(proto.OpNotExistErr, ([]byte)(err.Error()))
		} else {
			p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		}
		s.response(conn, p)
		err = errors.NewErrorf("req[%v],err[%v]", req, string(p.Data))
		return
	}

	resp := &GetCachePathResponse{CachePath: cachePath}
	reply, err := json.Marshal(resp)
	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		s.response(conn, p)
		err = errors.NewErrorf("req[%v],err[%v]", req, string(p.Data))
		return
	}
	p.PacketOkWithBody(reply)
	s.response(conn, p)
	return

}

func (s *bcacheStore) opBlockCacheEvict(conn net.Conn, p *BlockCachePacket) (err error) {
	req := &DelCacheRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PacketErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		s.response(conn, p)
		err = errors.NewErrorf("req[%v],err[%v]", req, err.Error())
		return
	}
	s.bcache.erase(req.CacheKey)
	p.PacketOkReplay()
	s.response(conn, p)
	return
}

func (s *bcacheStore) response(conn net.Conn, p *BlockCachePacket) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	err = p.WriteToConn(conn)
	if err != nil {
		log.LogDebugf("response to client[%s], "+
			"request[%s]",
			err.Error(), p.GetOpMsg())
	}
	return

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
