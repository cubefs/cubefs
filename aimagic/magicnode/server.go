// Copyright 2018 The CubeFS Authors.
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

package main

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tiglabs/raft/util"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	numWorkers = 8000
	numChans   = 1000
)

const (
	ModuleName = "aimagic"
)

type MagicNode struct {
	port            string
	control         common.Control
	accessKey       string
	secertKey       string
	endPoint        string
	toDownloadChans []chan *DownloadJob
	cacheSize       uint64
	toCleanCh       chan *DownloadJob
	maxCacheSize    uint64
	downloadMap     *sync.Map
	client          *S3Client
}

func NewServer() *MagicNode {
	return &MagicNode{}
}

func (s *MagicNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return s.control.Start(s, cfg, doStart)
}

// Shutdown shuts down the current data node.
func (s *MagicNode) Shutdown() {
	s.control.Shutdown(s, doShutdown)
}

// Sync keeps data node in sync.
func (s *MagicNode) Sync() {
	s.control.Sync()
}

// Workflow of starting up a data node.
func doStart(server common.Server, cfg *config.Config) (err error) {
	s, ok := server.(*MagicNode)
	if !ok {
		return errors.New("Invalid Node Type!")
	}
	if err = s.parseConfig(cfg); err != nil {
		return
	}
	log.LogErrorf("doStart parseConfig finish")
	exporter.Init("cube_torch", ModuleName, "", cfg)
	s.initMagicNode()

	return
}

func doShutdown(server common.Server) {
	return
}

const (
	ConfigAccessKey    = "access_key"
	ConfigSecertKey    = "secert_key"
	ConfigEndPoint     = "endpoint"
	ConfigMaxCacheSize = "max_cache_size"
	MinCacheSize       = 10 * util.GB
	MaxCacheSize       = util.GB * 1024
)

func (s *MagicNode) parseConfig(cfg *config.Config) (err error) {

	s.accessKey = cfg.GetString(ConfigAccessKey)
	s.secertKey = cfg.GetString(ConfigSecertKey)
	s.endPoint = cfg.GetString(ConfigEndPoint)
	s.maxCacheSize = uint64(cfg.GetInt64(ConfigMaxCacheSize))
	if s.maxCacheSize < MinCacheSize || s.maxCacheSize > MaxCacheSize {
		return fmt.Errorf("max_cache_size(%v) must bettwen (%v)~(%v) ", s.maxCacheSize, MinCacheSize, MaxCacheSize)
	}

	creds := credentials.NewStaticCredentials(s.accessKey, s.secertKey, "")
	s3cfg := aws.NewConfig().
		WithCredentials(creds).
		WithEndpoint(s.endPoint).
		WithRegion("spark")

	sess, err := session.NewSession(s3cfg)
	if err != nil {
		return fmt.Errorf("Failed to create session(%v) ", err)
	}

	s3Client := s3.New(sess)
	_, err = s3Client.ListBuckets(nil)
	if err != nil {
		return fmt.Errorf("Failed to list buckets:(%v) ", err)
	}
	return
}

func (s *MagicNode) donnotNeedClean() bool {
	return float64(atomic.LoadUint64(&s.cacheSize)) < float64(s.maxCacheSize)*0.9
}

func (s *MagicNode) isCleanFininsh() bool {
	return float64(atomic.LoadUint64(&s.cacheSize)) < float64(s.maxCacheSize)*0.5
}

func (s *MagicNode) doClean() {
	for {
		if s.isCleanFininsh() {
			return
		}
		job:=s.getJobFromCleanCh()
		if job==nil{
			return
		}
		os.RemoveAll(job.LocalPath)
		log.LogDebugf("job(%v) cleanup  success,  downTime(%v) ", job,job.DownTime)
		atomic.AddUint64(&s.cacheSize, -uint64(job.Size))
		PutDownLoadJobToPool(job)
	}
}

func (s *MagicNode) backGroundCleanup() {
	for {
		if s.donnotNeedClean() {
			time.Sleep(time.Second)
			continue
		}
		s.doClean()
	}
}

func (s *MagicNode)printCacheSize() {
	for {
		log.LogInfof("CACHE SIZE {%v} cleanCh(%v)",atomic.LoadUint64(&s.cacheSize),len(s.toCleanCh))
		time.Sleep(time.Second*1)
	}
}

func (s *MagicNode) initMagicNode() {
	s.toCleanCh = make(chan *DownloadJob, 10240000)
	s.toDownloadChans = make([]chan *DownloadJob, numChans)
	for i := range s.toDownloadChans {
		s.toDownloadChans[i] = make(chan *DownloadJob, 102400)
	}
	s.client = NewS3Client(s.accessKey, s.secertKey, s.endPoint)
	for i := 0; i < numWorkers; i++ {
		go s.DownloadWorker(i, s.toDownloadChans[i%numChans])
	}
	s.registerHandler()
	for i := 0; i < 100; i++ {
		go s.backGroundCleanup()
	}
	go s.printCacheSize()
}

func (s *MagicNode) fetchDownloadForCubeS3(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	err := s.HandleDownloadRequestForCubeS3(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Fprint(w, "OK")

}


func (s *MagicNode) fetchDownloadForCubeFuse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	err := s.HandleDownloadRequestForCubeFuse(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Fprint(w, "OK")

}


func (s *MagicNode) registerHandler() {
	http.HandleFunc("/fetchDownload/cubes3", s.fetchDownloadForCubeS3)
	http.HandleFunc("/fetchDownload/cubefuse", s.fetchDownloadForCubeFuse)


}
