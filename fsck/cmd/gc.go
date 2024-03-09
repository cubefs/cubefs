// Copyright 2020 The CubeFS Authors.
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

package cmd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
	"io"
	"io/ioutil"
	slog "log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
)

const (
	MaxBloomFilterSize uint64 = 50 * 1024 * 1024 * 1024
	// MaxBloomFilterSize uint64 = 50 * 1024 * 1024
	MpDir      string = "mp"
	DpDir      string = "dp"
	BadDir     string = "bad"
	BackSuffix string = ".succ"
)

var (
	gBloomFilter   *util.BloomFilter
	smuxPoolCfg    = util.DefaultSmuxConnPoolConfig()
	smuxPool       *util.SmuxConnectPool
	streamConnPool *util.ConnectPool
)

type ExtentForGc struct {
	Id     string // dpId + '_' + extentId
	Offset uint64
	Size   uint32
}

type BadNornalExtent struct {
	ExtentId string
	Size     uint32
}

type VerifyInfo struct {
	ClusterName string
	VolName     string
}

var hostLimit = make(map[string]chan struct{})
var cntLimit = 1
var hostLk = sync.RWMutex{}

func setHostCntLimit(cnt int) {
	clearChan()
	cntLimit = cnt
}

func getToken(host string) {
	hostLk.Lock()
	defer hostLk.Unlock()

	ch, ok := hostLimit[host]
	if !ok {
		ch = make(chan struct{}, cntLimit)
		hostLimit[host] = ch
	}

	ch <- struct{}{}
}

func releaseToken(host string) {
	hostLk.RLock()
	defer hostLk.RUnlock()

	ch, ok := hostLimit[host]

	if !ok {
		slog.Fatalf("not find chan for host %s, can't release", host)
	}

	select {
	case <-ch:
		return
	default:
		slog.Fatalf("there is not token in chan, host %s", host)
	}

}

func clearChan() {
	for k := range hostLimit {
		delete(hostLimit, k)
	}
}

func newGCCommand() *cobra.Command {
	var c = &cobra.Command{
		Use:   "gc",
		Short: "gc specified volume",
		Args:  cobra.MinimumNArgs(0),
	}
	proto.InitBufferPool(32768)
	smuxPool = util.NewSmuxConnectPool(smuxPoolCfg)
	streamConnPool = util.NewConnectPool()
	c.AddCommand(
		newGetMpExtentsCmd(),
		newGetDpExtentsCmd(),
		newCalcBadExtentsCmd(),
		newCheckBadExtentsCmd(),
		newCleanBadExtentsCmd(),
		newRollbackBadExtentsCmd(),
	)
	return c
}

func setCleanStatus() {
	switch CleanFlag {
	case "true":
		CleanS = true
		return
	case "false":
		CleanS = false
	default:
		slog.Fatalf("clean arg is not legal, clean %s", CleanFlag)
	}
}

func backOldDir(dir, volname, dirType string) {
	normalPath := filepath.Join(dir, volname, dirType, normalDir)
	_, err := os.Stat(normalPath)
	if err == nil {
		backDir := fmt.Sprintf("%s_%d", normalPath, time.Now().Unix())
		err = os.Rename(normalPath, backDir)
		if err != nil {
			slog.Fatalf("reanme dir failed, old %s, new %s, err %s", normalPath, backDir, err.Error())
		}
		slog.Printf("backOldDir success, old %s, back dir %s", normalPath, backDir)
	} else if !os.IsNotExist(err) {
		slog.Fatalf("stat path %s failed, err %s", normalPath, err.Error())
	}
}

func newGetMpExtentsCmd() *cobra.Command {
	var mpId string
	// var fromMpId string
	var concurrency uint64

	var cmd = &cobra.Command{
		Use:   "getMpExtents [VOLNAME] [DIR]",
		Short: "get extents of mp",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			volname := args[0]
			dir := args[1]

			backOldDir(dir, volname, MpDir)

			if mpId != "" {
				getExtentsByMpId(dir, volname, mpId)
				slog.Printf("Get extents by mpId %s success\n", mpId)
				return
			}

			start := time.Now()
			getExtentsFromMp(dir, volname, int(concurrency))
			writeNormalVerifyInfo(dir, volname, MpDir)
			slog.Printf("get all extents for vol %s success after write verify info, cost %d ms",
				volname, time.Since(start).Milliseconds())
		},
	}

	cmd.Flags().StringVar(&mpId, "mp", "", "get extents in mp id")
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 0, "clean bad dp concurrency")
	return cmd
}

func getMpInfoById(mpId string) (mpInfo *proto.MetaPartitionInfo, err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s%s?id=%s", MasterAddr, proto.ClientMetaPartition, mpId))
	if err != nil {
		log.LogErrorf("Get meta partition by mpId %s failed, err: %v", mpId, err)

		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.LogErrorf("Invalid status code: %v", resp.StatusCode)
		err = fmt.Errorf("Invalid status code: %v", resp.StatusCode)
		return nil, err
	}

	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.LogErrorf("Read body failed, err: %v", err)
		return nil, err
	}

	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err := json.Unmarshal(respData, body); err != nil {
		log.LogErrorf("Unmarshal response body err:%v", err)
		return nil, fmt.Errorf("unmarshal response body err:%v", err)

	}

	mpInfo = &proto.MetaPartitionInfo{}
	err = json.Unmarshal(body.Data, mpInfo)
	if err != nil {
		log.LogErrorf("Unmarshal meta partition info failed, err: %v", err)
		return nil, err
	}

	if mpInfo.Status != 1 && mpInfo.Status != 2 {
		log.LogErrorf("mp status is unavailable, mpInfo %v", mpInfo)
		return nil, fmt.Errorf("mp status is unavailable, mp %s", mpId)
	}

	return mpInfo, nil
}

func initCnt(cnt int) int {
	if cnt == 0 {
		return 1
	}

	// if cnt > max {
	// 	slog.Printf("concurrency %d is too big, use max %d")
	// 	return max
	// }

	return cnt
}

type taskPool struct {
	queue chan func()
	stop  chan struct{}
}

func newTaskPool(cnt int) *taskPool {
	p := &taskPool{
		queue: make(chan func(), cnt*10),
		stop:  make(chan struct{}),
	}

	for idx := 0; idx < cnt; idx++ {
		go func() {
			for {
				select {
				case <-p.stop:
					return
				case fn := <-p.queue:
					fn()
				}
			}
		}()
	}
	return p
}

func (p *taskPool) addTask(f func()) {
	p.queue <- f
}

func (p *taskPool) close() {
	close(p.stop)
}

func getExtentsFromMp(dir string, volname string, cnt int) {
	// allow 3 thread to get snapshot from one host
	setHostCntLimit(3)
	start := time.Now()
	mpvs, err := getMetaPartitions(MasterAddr, volname)
	if err != nil {
		slog.Fatalf("Get meta partitions failed, err: %v", err)
	}

	// fromMp, err := strconv.ParseUint(fromMpId, 10, 64)
	// if err != nil {
	// 	slog.Fatalf("Parse fromMpId failed, err: %v", err)
	// }

	cnt = initCnt(cnt)
	if cnt > len(mpvs) {
		cnt = len(mpvs)
	}

	wg := sync.WaitGroup{}
	pool := newTaskPool(cnt)
	defer pool.close()

	for _, mpv := range mpvs {
		wg.Add(1)
		mpId := mpv.PartitionID

		pool.addTask(func() {
			defer wg.Done()
			getExtentsByMpId(dir, volname, strconv.FormatUint(mpId, 10))
		})
	}
	wg.Wait()

	slog.Printf("Get extents from vol %v success, cost %d ms\n", volname, time.Since(start).Milliseconds())
	return
}

func getExtentsByMpId(dir string, volname string, mpId string) {
	start := time.Now()
	defer func() {
		log.LogWarnf("getExtentsByMpId: get extents by mp %s, vol %s, cost %d ms", mpId, volname, time.Since(start).Milliseconds())
	}()

	mpInfo, err := getMpInfoById(mpId)
	if err != nil {
		slog.Fatalf("Get meta partition info failed, err: %v", err)
	}

	_, NormalFilePath, err := InitLocalDir(dir, volname, mpId, MpDir)
	if err != nil {
		slog.Fatalf("Init local dir failed, mpId %s, err: %v", mpId, err)
	}

	normalFile, err := os.OpenFile(NormalFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Fatalf("Open file failed, mp %s, err: %v", mpId, err)
	}
	defer func() {
		err = normalFile.Sync()
		if err != nil {
			slog.Fatalf("sync file failed, mp %s, err %s", mpId, err.Error())
		}
		err = normalFile.Close()
		if err != nil {
			slog.Fatalf("sync file failed, mp %s, err %s", mpId, err.Error())
		}
	}()

	normalData := make([]byte, 0)
	// tinyBuf := bytes.NewBuffer(make([]byte, 0, 1024*1024*256))

	var leaderAddr string
	for _, mr := range mpInfo.Replicas {
		if mr.IsLeader {
			leaderAddr = mr.Addr
			break
		}
	}

	if leaderAddr == "" {
		slog.Fatalf("Get leader address failed mpId %v", mpId)
	}

	loadExtByHost := func(addr string) {
		getToken(addr)
		defer releaseToken(addr)

		startIn := time.Now()
		normalBuf := bytes.NewBuffer(make([]byte, 0, 1024*1024*32))
		resp, err := http.Get(fmt.Sprintf("http://%s:%s/getInodeSnapshot?pid=%s", strings.Split(addr, ":")[0], MetaPort, mpId))
		if err != nil {
			slog.Fatalf("Get inode snapshot failed, mp %s, addr %s, err: %v", mpId, addr, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			slog.Fatalf("Invalid status code: %v, mpId %s, addr %s", resp.StatusCode, mpId, addr)
		}

		reader := bufio.NewReaderSize(resp.Body, 16*1024*1024)
		inoBuf := make([]byte, 4)
		for {
			inoBuf = inoBuf[:4]
			// first read length
			_, err := io.ReadFull(reader, inoBuf)
			if err != nil {
				if err == io.EOF {
					if len(normalData) < normalBuf.Len() {
						normalData = normalBuf.Bytes()
					}
					log.LogWarnf("read snapshot from mp success, mp %s, host %s, cost %d ms",
						mpId, addr, time.Since(startIn).Milliseconds())
					err = nil
					break
				}
				slog.Fatalf("loadInode failed, read header error, mp %s, host %s, err %s", mpId, addr, err.Error())
			}

			length := binary.BigEndian.Uint32(inoBuf)
			// then read inode
			if uint32(cap(inoBuf)) >= length {
				inoBuf = inoBuf[:length]
			} else {
				inoBuf = make([]byte, length)
			}

			_, err = io.ReadFull(reader, inoBuf)
			if err != nil {
				slog.Fatalf("loadInode failed, read body error, mp %s, host %s, err %s", mpId, addr, err.Error())
			}
			ino := &metanode.Inode{
				Inode:      0,
				Type:       0,
				Generation: 1,
				CreateTime: 0,
				AccessTime: 0,
				ModifyTime: 0,
				NLink:      1,
				Extents:    metanode.NewSortedExtents(),
				ObjExtents: metanode.NewSortedObjExtents(),
			}
			if err = ino.Unmarshal(inoBuf); err != nil {
				slog.Fatalf("loadInode failed, unmarshal error, mp %s, host %s, err %s", mpId, addr, err.Error())
			}

			if log.EnableDebug() {
				eks := ino.Extents.CopyExtents()
				log.LogDebugf("inode:%v, extent:%v", ino.Inode, eks)
			}

			walkFunc := func(ek proto.ExtentKey) bool {
				if storage.IsTinyExtent(ek.ExtentId) {
					return true
				}

				var eksForGc ExtentForGc
				eksForGc.Id = strconv.FormatUint(ek.PartitionId, 10) + "_" + strconv.FormatUint(ek.ExtentId, 10)
				eksForGc.Offset = ek.ExtentOffset
				eksForGc.Size = ek.Size
				data, err := json.Marshal(eksForGc)
				if err != nil {
					slog.Fatalf("Marshal extents failed,  mp %s, host %s, err: %v", mpId, addr, err)
				}
				log.LogDebugf("normal extent:%v, data:%v", ek.ExtentId, len(data))
				normalBuf.Write(append(data, '\n'))
				return true
			}

			ino.Extents.Range(walkFunc)
		}
	}

	for _, addr := range mpInfo.Hosts {
		loadExtByHost(addr)
	}

	_, err = normalFile.Write(normalData)
	if err != nil {
		slog.Fatalf("write normal extent file failed, mp %s, err %s", mpId, err.Error())
	}
}

func InitLocalDir(dir string, volname string, partitionId string, dirType string) (tinyFilePath string, normalFilePath string, err error) {
	// tinyPath := filepath.Join(dir, volname, dirType, tinyDir)
	// if _, err = os.Stat(tinyPath); os.IsNotExist(err) {
	// 	if err = os.MkdirAll(tinyPath, 0755); err != nil {
	// 		log.LogErrorf("create dir failed: %v", err)
	// 		return
	// 	}
	// }

	normalPath := filepath.Join(dir, volname, dirType, normalDir)
	_, err = os.Stat(normalPath)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(normalPath, 0755); err != nil {
			slog.Fatalf("create dir failed, path %s, err : %v", normalFilePath, err)
			return
		}
	}

	// tinyFilePath = filepath.Join(tinyPath, partitionId)
	// if _, err = os.Stat(tinyFilePath); os.IsNotExist(err) {
	// 	file, err := os.Create(tinyFilePath)
	// 	if err != nil {
	// 		log.LogErrorf("create tiny file failed:%v", err)
	// 		return "", "", err
	// 	}
	// 	defer file.Close()
	// } else {
	// 	err = os.WriteFile(tinyFilePath, []byte{}, os.ModePerm)
	// 	if err != nil {
	// 		log.LogErrorf("clear tiny file failed:%v", err)
	// 		return "", "", err
	// 	}
	// 	log.LogInfof("tiny file contents have been cleared successfully: %v", tinyFilePath)
	// }

	normalFilePath = filepath.Join(normalPath, partitionId)
	if _, err = os.Stat(normalFilePath); os.IsNotExist(err) {
		file, err := os.Create(normalFilePath)
		if err != nil {
			log.LogErrorf("create normal file failed:%v", err)
			return "", "", err
		}
		defer file.Close()
	} else {
		err = os.WriteFile(normalFilePath, []byte{}, os.ModePerm)
		if err != nil {
			log.LogErrorf("clear normal file failed:%v", err)
			return "", "", err
		}
		log.LogInfof("normal file contents have been cleared successfully: %v", normalFilePath)
	}

	return tinyFilePath, normalFilePath, nil
}

func newGetDpExtentsCmd() *cobra.Command {
	var (
		dpId     string
		fromDpId string
	)
	cmd := &cobra.Command{
		Use:   "getDpExtents [VOLNAME] [DIR] [BEFORE_TIME]",
		Short: "get extents of dp",
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			volname := args[0]
			dir := args[1]
			beforeTime := args[2]
			layout := "2006-01-02 15:04:05"

			location, err := time.LoadLocation("Asia/Shanghai")
			if err != nil {
				slog.Fatalf("load location failed: %v", err)
				return
			}
			t, err := time.ParseInLocation(layout, beforeTime, location)
			if err != nil {
				slog.Fatalf("parse time failed: %v", err)
				return
			}
			beforeTime = strconv.FormatInt(t.Unix(), 10)
			log.LogInfof("beforeTime: %v", beforeTime)
			currentTimeUnix := time.Now().Unix()
			beforeTimeUnix := t.Unix()
			threeHours := int64(3600 * 3)

			if currentTimeUnix-beforeTimeUnix < threeHours {
				slog.Fatalf("beforeTime should be at least 3 hours earlier than current time %v %v", currentTimeUnix, beforeTimeUnix)
				return
			}

			backOldDir(dir, volname, DpDir)

			start := time.Now()

			if dpId != "" {
				err = getExtentsByDpId(dir, volname, dpId, beforeTime)
				if err != nil {
					slog.Fatalf("get extent by dpId failed, vol %s, dp %s, err %s", volname, dpId, err.Error())
				}
				writeNormalVerifyInfo(dir, volname, DpDir)
				slog.Printf("get extent by dpId success, vol %s, dp %s, cost %d ms",
					volname, dpId, time.Since(start).Milliseconds())
				return
			}

			err = getExtentsFromDpId(dir, volname, fromDpId, beforeTime)
			if err != nil {
				slog.Fatalf("get extents from dp failed, vol %s, from-dp %s, err %s",
					volname, fromDpId, err.Error())
			}

			writeNormalVerifyInfo(dir, volname, DpDir)
			slog.Printf("get extent from dpId success, vol %s, cost %d ms",
				volname, time.Since(start).Milliseconds())
			return
		},
	}

	cmd.Flags().StringVar(&dpId, "dp", "", "get extents in dp id")
	cmd.Flags().StringVar(&fromDpId, "from-dp", "", "get extents from dp id")
	return cmd
}

func getDpInfoById(dpId string) (dpInfo *proto.DataPartitionInfo, err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s%s?id=%s", MasterAddr, proto.AdminGetDataPartition, dpId))
	if err != nil {
		log.LogErrorf("Get data partition failed, err: %v", err)
		return
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.LogErrorf("Invalid status code: %v", resp.StatusCode)
		return
	}

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		log.LogErrorf("Read response body failed, err: %v", err)
		return
	}

	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err := json.Unmarshal(respData, body); err != nil {
		log.LogErrorf("Unmarshal response body err:%v", err)
		return nil, fmt.Errorf("unmarshal response body err:%v", err)
	}

	dpInfo = &proto.DataPartitionInfo{}
	err = json.Unmarshal(body.Data, dpInfo)
	if err != nil {
		log.LogErrorf("Unmarshal response body err:%v", err)
		return nil, err
	}

	return dpInfo, nil
}

func getExtentsByDpId(dir string, volname string, dpId string, beforeTime string) (err error) {
	start := time.Now()
	totalCnt := 0
	totalSize := 0

	defer func() {
		log.LogWarnf("getExtentsByDpId: get extents by dp success, vol %s, dp %s, time %s, cost %d ms, totalCnt %d, totalSize %d, err %v",
			volname, dpId, beforeTime, time.Since(start).Milliseconds(), totalCnt, totalSize, err)
	}()

	dpInfo, err := getDpInfoById(dpId)
	if err != nil {
		log.LogErrorf("Get dp info failed, dp %s, err: %v", dpId, err)
		return err
	}

	if dpInfo.VolName != volname {
		slog.Fatalf("datapartition %d is not in vol %s, got vol %s", dpInfo.PartitionID, volname, dpInfo.VolName)
	}

	var leaderAddr string
	for _, dr := range dpInfo.Replicas {
		if dr.IsLeader {
			leaderAddr = dr.Addr
			break
		}
	}

	if leaderAddr == "" {
		return fmt.Errorf("Get leader addr failed, dpId %s, err: %v", dpId, err)
	}

	resp, err := http.Get(fmt.Sprintf("http://%s:%s/getAllExtent?id=%s&beforeTime=%s",
		strings.Split(leaderAddr, ":")[0], DataPort, dpId, beforeTime))
	if err != nil {
		log.LogErrorf("Get all extents failed, dpId %s, err: %v", dpId, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Invalid status, dp id %s, code: %v", dpId, resp.StatusCode)
	}

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Read response body failed, err: %v", err)
	}

	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}

	if err = json.Unmarshal(respData, body); err != nil {
		log.LogErrorf("Unmarshal response body dp %s, err:%v", dpId, err.Error())
		return
	}

	extents := make([]*storage.ExtentInfo, 0)
	err = json.Unmarshal(body.Data, &extents)
	if err != nil {
		log.LogErrorf("Unmarshal response body dp %s, err:%v", dpId, err)
		return
	}

	_, normalFilePath, err := InitLocalDir(dir, volname, dpId, DpDir)
	if err != nil {
		log.LogErrorf("Init local dir failed, dpId %s, err: %v", dpId, err)
		return
	}

	normalFile, err := os.OpenFile(normalFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.LogErrorf("Open normalFile failed, err: %v", err)
		return
	}
	defer normalFile.Close()

	for _, extent := range extents {
		if storage.IsTinyExtent(extent.FileID) {
			continue
		}

		var eksForGc ExtentForGc
		eksForGc.Id = dpId + "_" + strconv.FormatUint(extent.FileID, 10)
		eksForGc.Offset = 0
		eksForGc.Size = uint32(extent.Size)
		totalCnt++
		totalSize += int(extent.Size)
		data, err := json.Marshal(eksForGc)
		if err != nil {
			log.LogErrorf("Marshal extents failed, err: %v", err)
			return err
		}
		_, err = normalFile.Write(append(data, '\n'))
		if err != nil {
			log.LogErrorf("Write normal extent to file failed, err: %v", err)
			return err
		}
	}

	beforeTimeFilePath := filepath.Join(dir, volname, "dp", normalDir, beforeTimeFile)
	if _, err = os.Stat(beforeTimeFilePath); os.IsNotExist(err) {
		_, err = os.Create(beforeTimeFilePath)
		if err != nil {
			log.LogErrorf("Create before time file failed, err: %v", err)
			return
		}
	}
	err = os.WriteFile(beforeTimeFilePath, []byte(beforeTime), 0644)
	if err != nil {
		log.LogErrorf("Write before time file failed, err: %v", err)
		return
	}

	return nil
}

func getExtentsFromDpId(dir string, volname string, fromDpId string, beforeTime string) (err error) {
	dps, err := getDataPartitions(MasterAddr, volname)
	if err != nil {
		return fmt.Errorf("Get data partitions failed, err: %v", err.Error())
	}

	// fromDp, err := strconv.ParseUint(fromDpId, 10, 64)
	// if err != nil {
	// 	log.LogErrorf("Parse from dp id failed, err: %v", err)
	// 	return err
	// }

	start := time.Now()
	pool := newTaskPool(10)
	defer pool.close()
	wg := sync.WaitGroup{}
	for _, dp := range dps {
		wg.Add(1)
		dpId := dp.PartitionID
		pool.addTask(func() {
			defer wg.Done()
			err = getExtentsByDpId(dir, volname, strconv.FormatUint(dpId, 10), beforeTime)
			if err != nil {
				log.LogErrorf("get extent by dpId failed, vol %s, dp %d, time %s, err %s",
					volname, dp.PartitionID, beforeTime, err.Error())
			}
		})
	}
	wg.Wait()
	log.LogWarnf("get all dp extents success, vol %s, cost %d ms", volname, time.Since(start).Milliseconds())
	return nil
}

func getClusterInfo() (clusterView *proto.ClusterView, err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s%s", MasterAddr, proto.AdminGetCluster))
	if err != nil {
		log.LogErrorf("Get cluster info failed, err: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.LogErrorf("Invalid status code: %v", resp.StatusCode)
		return
	}

	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.LogErrorf("Read response body failed, err: %v", err)
		return
	}

	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err := json.Unmarshal(respData, body); err != nil {
		log.LogErrorf("Unmarshal response body err:%v", err)
		return nil, fmt.Errorf("unmarshal response body err:%v", err)
	}

	clusterView = &proto.ClusterView{}
	err = json.Unmarshal(body.Data, clusterView)
	if err != nil {
		log.LogErrorf("Unmarshal response body err:%v", err)
		return nil, err
	}

	return clusterView, nil
}

func writeNormalVerifyInfo(dir, volname, dirType string) {
	cluster, err := getClusterInfo()
	if err != nil {
		slog.Fatalf("Get cluster info failed, err: %v", err)
		return
	}
	var verifyInfo VerifyInfo
	verifyInfo.ClusterName = cluster.Name
	verifyInfo.VolName = volname
	data, err := json.Marshal(verifyInfo)
	if err != nil {
		slog.Fatalf("Marshal verify info failed, err: %v", err)
		return
	}

	filePath := filepath.Join(dir, volname, dirType, normalDir, verifyInfoFile)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		_, err = os.Create(filePath)
		if err != nil {
			slog.Fatalf("Create verify info file failed, err: %v", err)
			return
		}
	}
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		slog.Fatalf("Write verify info file failed, err: %v", err)
		return
	}
	log.LogWarnf("writeNormalVerifyInfo: vol %s, write normal verifyInfo success, type %s", volname, dirType)
}

func checkNormalVerfyInfo(dir, volname, dirType string) (err error) {
	var verifyInfo VerifyInfo
	filePath := filepath.Join(dir, volname, dirType, normalDir, verifyInfoFile)
	if _, err = os.Stat(filePath); os.IsNotExist(err) {
		err = fmt.Errorf("verify info file not exist")
		return
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.LogErrorf("Read verify info file failed, err: %v", err)
		return
	}
	err = json.Unmarshal(data, &verifyInfo)
	if err != nil {
		log.LogErrorf("Unmarshal verify info failed, err: %v", err)
		return
	}

	cluster, err := getClusterInfo()
	if err != nil {
		log.LogErrorf("Get cluster info failed, err: %v", err)
		return
	}

	if verifyInfo.ClusterName != cluster.Name {
		err = fmt.Errorf("cluster name not match %v %v", verifyInfo.ClusterName, cluster.Name)
		return
	}

	if verifyInfo.VolName != volname {
		err = fmt.Errorf("volname not match %v %v", verifyInfo.VolName, volname)
		return
	}

	return nil
}

func newCalcBadExtentsCmd() *cobra.Command {
	var concurrency uint64
	cmd := &cobra.Command{
		Use:   "calcBadExtents [VOLNAME] [EXTENT TYPE] [DIR]",
		Short: "calc bad extents",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			volname := args[0]
			extType := args[1]
			dir := args[2]

			backOldDir(dir, volname, BadDir)

			start := time.Now()
			if extType == "normal" {
				err := checkNormalVerfyInfo(dir, volname, MpDir)
				if err != nil {
					slog.Fatalf("Check normal verify info failed, err: %v", err)
					return
				}
				err = checkNormalVerfyInfo(dir, volname, DpDir)
				if err != nil {
					slog.Fatalf("Check normal verify info failed, err: %v", err)
					return
				}
				err = calcBadNormalExtents(volname, dir, int(concurrency))
				if err != nil {
					slog.Fatalf("Calc bad normal extents failed, err: %v", err)
					return
				}
				writeNormalVerifyInfo(dir, volname, BadDir)
				slog.Printf("calc bad extents success, vol %s, cost %d ms", volname, time.Since(start).Milliseconds())
			} else if extType == "tiny" {
				slog.Fatalf("tiny is not supported now")
				return
			} else {
				slog.Fatalf("Invalid extent type: %s", extType)
				return
			}

		},
	}
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 0, "clean bad dp concurrency")
	return cmd
}

func calcFNVHash(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func calcCRC64Hash(data []byte) uint64 {
	return crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
}

func calcBadNormalExtents(volname, dir string, concurrency int) (err error) {
	gBloomFilter = util.NewBloomFilter(MaxBloomFilterSize, calcFNVHash, calcCRC64Hash)
	normalMpDir := filepath.Join(dir, volname, MpDir, normalDir)

	err = addMpExtentToBF(normalMpDir)
	if err != nil {
		slog.Fatalf("Add normal extents to bloom filter failed, dir %s, err: %v", normalMpDir, err.Error())
	}

	normalDpDir := filepath.Join(dir, volname, DpDir, normalDir)
	destDir := filepath.Join(dir, volname, BadDir, normalDir)
	_, err = os.Stat(destDir)
	if err == nil {
		// back up old dir
		backPath := fmt.Sprintf("%s_%d", destDir, time.Now().Unix())
		err = os.Rename(destDir, backPath)
		if err != nil {
			slog.Fatalf("rename dir failed, destDir %s, backDir %s, err %s", destDir, backPath, err.Error())
		}
	}

	err = os.MkdirAll(destDir, 0755)
	if err != nil {
		slog.Fatalf("Mkdir dest dir failed, err: %v", err)
		return
	}

	err = calcDpBadNormalExtentByBF(volname, normalDpDir, destDir, concurrency)
	return err
}

func addMpExtentToBF(mpDir string) (err error) {
	totalStart := time.Now()
	defer func() {
		log.LogWarnf("addMpExtentToBF: add all mp to BF success, dir %s, cost %d ms", mpDir, time.Since(totalStart).Milliseconds())
	}()

	fileInfos, err := os.ReadDir(mpDir)
	if err != nil {
		slog.Fatalf("Read mp dir failed, dir %s, err: %v", mpDir, err)
		return
	}

	walkMp := func(fileName string) {
		stat := time.Now()
		filePath := filepath.Join(mpDir, fileName)
		file, err := os.Open(filePath)
		if err != nil {
			slog.Fatalf("Open file failed, path %s, err: %v", filePath, err.Error())
		}
		defer file.Close()

		cnt := 0
		defer func() {
			log.LogWarnf("addMpExtentToBF： add mp %s success, cnt %d, cost %d ms",
				filePath, cnt, time.Since(stat).Milliseconds())
		}()

		reader := bufio.NewReaderSize(file, 16*1024*1024)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				slog.Fatalf("Read line failed, path %s, err: %v", filePath, err)
			}

			var eksForGc ExtentForGc
			err = json.Unmarshal(line, &eksForGc)
			if err != nil {
				slog.Fatalf("Unmarshal extent failed, err: %v", err)
			}

			gBloomFilter.Add([]byte(eksForGc.Id)) // add extent id to bloom filter
			cnt++
		}
	}

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}

		fileName := fileInfo.Name()
		if fileName == verifyInfoFile {
			continue
		}

		walkMp(fileName)
	}
	return
}

func calcDpBadNormalExtentByBF(vol, dpDir, badDir string, concurrency int) (err error) {
	fileInfos, err := os.ReadDir(dpDir)
	if err != nil {
		slog.Fatalf("Read dp dir failed, err: %v", err)
		return
	}

	badSize := uint64(0)
	badCount := uint64(0)
	start := time.Now()
	defer func() {
		msg := fmt.Sprintf("finally vol %s, get total bad extent count %d, size %d, cost %d ms",
			vol, badCount, badSize, time.Since(start).Milliseconds())
		slog.Println(msg)
		log.LogWarn(msg)
	}()

	if concurrency == 0 {
		concurrency = 1
	}
	pool := newTaskPool(concurrency)
	defer pool.close()
	wg := sync.WaitGroup{}

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}

		fileName := fileInfo.Name()
		if fileName == beforeTimeFile || fileName == verifyInfoFile {
			continue
		}

		walk := func() {
			defer wg.Done()
			filePath := filepath.Join(dpDir, fileName)
			file, err := os.Open(filePath)
			if err != nil {
				slog.Fatalf("Open file failed, path %s, err: %v", filePath, err.Error())
			}
			defer file.Close()

			size := 512 * 1024
			reader := bufio.NewReaderSize(file, size)
			for {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						err = nil
						break
					}
					slog.Fatalf("Read line failed, path %s, err: %v", filePath, err)
				}

				var eksForGc ExtentForGc
				err = json.Unmarshal(line, &eksForGc)
				if err != nil {
					slog.Fatalf("Unmarshal extent failed, path %s, err: %v", filePath, err)
				}

				if !gBloomFilter.Contains([]byte(eksForGc.Id)) {
					atomic.AddUint64(&badCount, 1)
					atomic.AddUint64(&badSize, uint64(eksForGc.Size))

					var badExtent BadNornalExtent
					parts := strings.Split(eksForGc.Id, "_")
					dpId := parts[0]
					badExtent.ExtentId = parts[1]
					badExtent.Size = eksForGc.Size

					err = writeBadNormalExtentoLocal(dpId, badDir, badExtent)
					if err != nil {
						slog.Fatalf("Write bad extent failed, err: %v", err)
					}
				}
			}
		}
		wg.Add(1)
		pool.addTask(walk)
	}

	wg.Wait()
	return
}

// write bad extent to local
func writeBadNormalExtentoLocal(dpId, badDir string, badExtent BadNornalExtent) (err error) {
	log.LogInfof("Write dir %s, dpId: %s bad extent: %v", badDir, dpId, badExtent)
	filePath := filepath.Join(badDir, dpId)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		slog.Fatalf("Open file failed, path %s, err: %v", filePath, err)
		return err
	}

	defer file.Close()
	data, err := json.Marshal(&badExtent)
	if err != nil {
		slog.Fatalf("Marshal bad extent failed, path %s, err: %v", filePath, err)
		return err
	}

	_, err = file.Write(append(data, '\n'))
	if err != nil {
		slog.Fatalf("Write bad extent failed, path %s, err: %v", filePath, err.Error())
		return err
	}
	return
}

func newCheckBadExtentsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "checkBadExtents [VOLNAME] [EXTENT TYPE] [DIR] [DATA PARTITION]",
		Short: "check bad extents",
		Args:  cobra.ExactArgs(4),
		Run: func(cmd *cobra.Command, args []string) {
			volname := args[0]
			extType := args[1]
			dir := args[2]
			dpId := args[3]

			if extType == "normal" {
				checkBadNormalExtents(volname, dir, dpId)
			} else if extType == "tiny" {
				slog.Fatalf("tiny is not supported now")
				return
			} else {
				slog.Fatalf("Invalid extent type: %s", extType)
				return
			}
		},
	}
	return cmd
}

func checkBadNormalExtents(volname, dir, checkDpId string) {
	normalMpDir := filepath.Join(dir, volname, MpDir, normalDir)
	badExtentPath := filepath.Join(dir, volname, BadDir, normalDir, checkDpId)

	start := time.Now()
	fileInfos, err := os.ReadDir(normalMpDir)
	if err != nil {
		log.LogErrorf("Read mp dir failed, err: %v", err)
		return
	}

	readBadExtent := func() []BadNornalExtent {
		file, err := os.Open(badExtentPath)
		if err != nil {
			slog.Fatalf("Open file failed, path %s, err: %v", badExtentPath, err)
		}
		defer file.Close()

		exts := make([]BadNornalExtent, 0, 10240)

		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				slog.Fatalf("Read line failed, path %s, err: %v", badExtentPath, err)
			}
			var badExt BadNornalExtent
			err = json.Unmarshal(line, &badExt)
			if err != nil {
				slog.Fatalf("Unmarshal extent failed, path %s, err: %v", badExtentPath, err)
			}

			exts = append(exts, badExt)
		}
		return exts
	}

	exts := readBadExtent()
	badExtMap := make(map[string]struct{}, len(exts))
	for _, e := range exts {
		badExtMap[e.ExtentId] = struct{}{}
	}

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}

		fileName := fileInfo.Name()
		if fileName == verifyInfoFile {
			continue
		}

		filePath := filepath.Join(normalMpDir, fileName)
		file, err := os.Open(filePath)
		if err != nil {
			slog.Fatalf("Open file failed, err: %v", err)
			return
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				slog.Fatalf("Read line failed, path %s, err: %v", filePath, err)
			}
			var eksForGc ExtentForGc
			err = json.Unmarshal(line, &eksForGc)
			if err != nil {
				slog.Fatalf("Unmarshal extent failed, path %s, err: %v", filePath, err)
			}

			parts := strings.Split(eksForGc.Id, "_")
			dpId := parts[0]
			extentId := parts[1]
			if dpId != checkDpId {
				continue
			}

			_, ok := badExtMap[extentId]
			if ok {
				slog.Fatalf("some thing may be wrong, find extent in mp list, ext %s, path %s, dp %s",
					extentId, filePath, dpId)
			}
		}
	}
	slog.Printf("finally check success, no wrong bad extents vol %s, dp %s, cost %d ms", volname, checkDpId, time.Since(start).Milliseconds())
	return
}

func batchLockBadNormalExtent(dpIdStr string, exts []*BadNornalExtent, IsCreate bool, beforeTime string) (err error) {
	dpInfo, err := getDpInfoById(dpIdStr)
	if err != nil {
		slog.Fatalf("Get dp %v info failed, err: %v", dpIdStr, err)
		return
	}

	eks := make([]*proto.ExtentKey, 0, len(exts))
	for _, ext := range exts {
		extentId, err := strconv.ParseUint(ext.ExtentId, 10, 64)
		if err != nil {
			log.LogErrorf("batchLockBadNormalExtent：Parse extent id failed, err: %v", err)
			return err
		}
		ek := &proto.ExtentKey{
			FileOffset:   0,
			PartitionId:  dpInfo.PartitionID,
			ExtentId:     extentId,
			ExtentOffset: 0,
			Size:         ext.Size,
			CRC:          0,
		}
		eks = append(eks, ek)
	}

	flag := proto.GcMarkFlag
	if CleanS {
		flag = proto.GcDeleteFlag
	}
	gcLockEks := &proto.GcLockExtents{
		IsCreate:   IsCreate,
		BeforeTime: beforeTime,
		Eks:        eks,
		Flag:       flag,
	}

	addr := dpInfo.Hosts[0]
	getToken(addr)
	defer releaseToken(addr)

	conn, err := streamConnPool.GetConnect(addr)
	defer func() {
		streamConnPool.PutConnect(conn, true)
		log.LogInfof("batchLockBadNormalExtent PutConnect (%v)", dpInfo.Hosts[0])
	}()

	if err != nil {
		log.LogErrorf("batchLockBadNormalExtent：Parse Get connect failed, err: %v", err)
		return err
	}

	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBatchLockNormalExtent
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = dpInfo.PartitionID
	p.Data, err = json.Marshal(gcLockEks)
	if err != nil {
		slog.Fatalf("Marshal extent keys failed, err: %v", err)
		return
	}

	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = uint8(len(dpInfo.Hosts) - 1)
	p.Arg = []byte(strings.Join(dpInfo.Hosts[1:], proto.AddrSplit) + proto.AddrSplit)
	p.ArgLen = uint32(len(p.Arg))

	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("WriteToConn dp %v failed, err: %v", dpIdStr, err)
		return
	}

	if err = p.ReadFromConn(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		log.LogErrorf("ReadFromConn dp %v failed, err: %v", dpIdStr, err)
		return
	}

	if p.ResultCode != proto.OpOk {
		log.LogErrorf("batchLockBadNormalExtent dp %v failed, ResultCode: %v", dpIdStr, p.String())
		err = fmt.Errorf("batchLockBadNormalExtent dp %v failed, ResultCode: %v", dpIdStr, p.String())
		return
	}

	log.LogInfof("batchLockBadNormalExtent success dpId: %s", dpIdStr)
	return
}

func batchUnlockBadNormalExtent(dpIdStr string, exts []*BadNornalExtent) (err error) {
	dpInfo, err := getDpInfoById(dpIdStr)
	if err != nil {
		log.LogErrorf("Get dp info failed, dp %s, err: %v", dpIdStr, err)
		return
	}

	eks := make([]*proto.ExtentKey, 0, len(exts))
	for _, ext := range exts {
		extentId, err := strconv.ParseUint(ext.ExtentId, 10, 64)
		if err != nil {
			log.LogErrorf("Parse extent id failed, err: %v", err)
			return err
		}
		ek := &proto.ExtentKey{
			FileOffset:   0,
			PartitionId:  dpInfo.PartitionID,
			ExtentId:     extentId,
			ExtentOffset: 0,
			Size:         ext.Size,
			CRC:          0,
		}
		eks = append(eks, ek)
	}

	addr := dpInfo.Hosts[0]
	getToken(addr)
	defer releaseToken(addr)

	conn, err := streamConnPool.GetConnect(addr)
	defer func() {
		streamConnPool.PutConnect(conn, true)
		log.LogInfof("batchUnlockBadNormalExtent PutConnect (%v)", dpInfo.Hosts[0])
	}()

	if err != nil {
		log.LogErrorf("Get connect failed, addr %s, err: %v", addr, err)
		return
	}

	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBatchUnlockNormalExtent
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = dpInfo.PartitionID
	p.Data, err = json.Marshal(eks)
	if err != nil {
		log.LogErrorf("Marshal extent keys failed, addr %s, err: %v", addr, err)
		return
	}
	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = uint8(len(dpInfo.Hosts) - 1)
	p.Arg = []byte(strings.Join(dpInfo.Hosts[1:], proto.AddrSplit) + proto.AddrSplit)
	p.ArgLen = uint32(len(p.Arg))

	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("WriteToConn failed, addr %s, err: %v", addr, err)
		return
	}
	if err = p.ReadFromConn(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		log.LogErrorf("ReadFromConn failed, addr %s, err: %v", addr, err)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = fmt.Errorf("batchUnlockBadNormalExtent failed, addr %s, ResultCode: %v", addr, p.String())
		log.LogError(err.Error())
		return
	}

	log.LogInfof("batchUnlockBadNormalExtent success dpId: %s", dpIdStr)
	return
}

func newCleanBadExtentsCmd() *cobra.Command {
	var (
		fromDpId    string
		dpId        string
		extent      string
		concurrency uint64
	)

	cmd := &cobra.Command{
		Use:   "cleanBadExtents [VOLNAME] [EXTENT TYPE] [DIR] [BACKUP DIR]",
		Short: "clean bad extents",
		Args:  cobra.ExactArgs(4),
		Run: func(cmd *cobra.Command, args []string) {
			volname := args[0]
			extType := args[1]
			dir := args[2]
			backupDir := args[3]

			setCleanStatus()
			start := time.Now()
			if extType == "normal" {
				err := checkNormalVerfyInfo(dir, volname, BadDir)
				if err != nil {
					slog.Fatalf("Check normal verfy info failed, err: %v", err)
					return
				}

				// back up old dir
				backDir := filepath.Join(backupDir, volname, normalDir)
				_, err = os.Stat(backDir)
				if err == nil {
					backPath := fmt.Sprintf("%s_%d", backDir, time.Now().Unix())
					err = os.Rename(backDir, backPath)
					if err != nil {
						slog.Fatalf("reanme dir failed, old %s, new %s, err %s", backDir, backPath, err.Error())
					}
					slog.Printf("ackOldDir success, old %s, back dir %s", backDir, backPath)
				}

				if fromDpId != "" {
					fromDpIdNum, err := strconv.ParseUint(fromDpId, 10, 64)
					if err != nil {
						slog.Fatalf("Parse from dp id failed, err: %v", err)
						return
					}

					cleanBadNormalExtentFromDp(volname, dir, backupDir, fromDpIdNum, concurrency)
					return
				}

				if dpId != "" {
					if extent != "" {
						err = cleanBadNormalExtent(volname, dir, backupDir, dpId, extent)
						if err != nil {
							slog.Fatalf("clean bad normal extent failed, name %s, dp %s, ext %s, err %s",
								volname, dpId, extent, err.Error())
						}
						return
					}

					err = cleanBadNormalExtentOfDp(volname, dir, backupDir, dpId)
					if err != nil {
						slog.Fatalf("clean dp normal extent failed, vol %s, dp %s, err %s", volname, dpId, err.Error())
					}
					slog.Printf("cleanBadNormalExtentOfDp success, vol %s, dp %s, cost %d ms",
						volname, dpId, time.Since(start).Milliseconds())
					return
				}
				slog.Fatalf("from-dp and dpId can't be both empty")
			} else if extType == "tiny" {
				slog.Fatalln("tiny is not supported now")
				return
			} else {
				slog.Fatalf("Invalid extent type: %s", extType)
				return
			}
		},
	}

	cmd.Flags().StringVar(&fromDpId, "from-dp", "", "clean bad extent from dp id")
	cmd.Flags().StringVar(&dpId, "dp", "", "clean bad extent in dp id")
	cmd.Flags().StringVar(&extent, "e", "", "clean one bad extent")
	cmd.Flags().Uint64Var(&concurrency, "concurrency", 0, "clean bad dp concurrency")
	return cmd
}

func cleanBadNormalExtent(volname, dir, backupDir, dpIdStr, extentStr string) (err error) {
	log.LogWarnf("Clean bad normal volname: %s, dir: %s, backupDir: %s, dpId: %s, extent: %s", volname, dir, backupDir, dpIdStr, extentStr)
	badNormalDpFile := filepath.Join(dir, volname, BadDir, normalDir, dpIdStr)
	file, err := os.Open(badNormalDpFile)
	if err != nil {
		log.LogErrorf("Open bad normal dp file failed, err: %v", err)
		return
	}
	defer file.Close()

	start := time.Now()
	defer func() {
		slog.Printf("cleanBadNormalExtent: after clean bad ext, name %s, dp %s, ext %s, cost %d ms, err %v",
			volname, dpIdStr, extentStr, time.Since(start).Milliseconds(), err)
	}()

	beforeTimeFilePath := filepath.Join(dir, volname, "dp", normalDir, beforeTimeFile)
	data, err := os.ReadFile(beforeTimeFilePath)
	if err != nil {
		slog.Fatalf("Read before time file failed, path %s, err: %v", badNormalDpFile, err)
	}

	reader := bufio.NewReaderSize(file, 512*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
				slog.Fatalf("extent %s is not in target dp %s bad file", extentStr, dpIdStr)
				break
			}
			slog.Fatalf("Read line failed, path %s, err: %v", badNormalDpFile, err)
			return err
		}

		line = line[:len(line)-1]
		var badExtent BadNornalExtent
		err = json.Unmarshal(line, &badExtent)
		if err != nil {
			slog.Fatalf("Unmarshal bad extent failed, path %s, err: %v", badNormalDpFile, err)
			return err
		}

		if badExtent.ExtentId != extentStr {
			continue
		}

		var exts = []*BadNornalExtent{&badExtent}

		err = batchLockBadNormalExtent(dpIdStr, exts, false, string(data))
		if err != nil {
			slog.Fatalf("Batch lock bad normal extent failed, err: %v", err)
		}

		if CleanS {
			err = copyBadNormalExtentToBackup(volname, backupDir, dpIdStr, badExtent)
			if err != nil {
				log.LogErrorf("Copy bad extent failed, err: %v", err)
				return err
			}

			slog.Printf("gc clean switch is open, start clean data, dp %s, ext %s", dpIdStr, extentStr)
			err = batchDeleteBadExtent(dpIdStr, exts)
			if err != nil {
				log.LogErrorf("Delete bad extent failed, err: %v", err)
				return err
			}

			err = batchUnlockBadNormalExtent(dpIdStr, exts)
			if err != nil {
				log.LogErrorf("Batch unlock bad normal extent failed, err: %v", err)
				return err
			}
		}

		log.LogInfof("cleanBadNormalExtent extent: %s", extentStr)
		return nil
	}
	log.LogErrorf("Can not find bad extent: %s", extentStr)
	return
}

func copyBadNormalExtentToBackup(volname, backupDir, dpIdStr string, badExtent BadNornalExtent) (err error) {
	log.LogInfof("copyBadNormalExtentToBackup: clean bad normal extent Internal backupDir: %s, dpIdStr: %s, badExtent:%v",
		backupDir, dpIdStr, badExtent.ExtentId)

	extentId, err := strconv.ParseUint(badExtent.ExtentId, 10, 64)
	if err != nil {
		log.LogErrorf("Parse extent id failed, err: %v", err)
		return
	}

	var readBytes int
	var data []byte
	data, readBytes, err = readBadExtentFromDp(dpIdStr, extentId, badExtent.Size)
	if err != nil {
		log.LogErrorf("Read bad extent from dp failed, err: %v", err)
		return err
	}

	if len(data) != int(badExtent.Size) {
		return fmt.Errorf("dp(%s)_ext(%s) data is empty, size(%d), expect(%d)", dpIdStr, badExtent.ExtentId, len(data), badExtent.Size)
	}

	err = writeBadNormalExtentToBackup(backupDir, volname, dpIdStr, badExtent, data, readBytes)
	if err != nil {
		log.LogErrorf("Write bad extent to dest dir failed, err: %v", err)
		return
	}

	return
}

func renameSucceExts(backUpDir, vol, dp string, exts []*BadNornalExtent) {
	dpDir := filepath.Join(backUpDir, vol, normalDir, dp)
	start := time.Now()
	for _, e := range exts {
		oldDir := path.Join(dpDir, e.ExtentId)
		newDir := path.Join(dpDir, fmt.Sprintf("%s%s", e.ExtentId, BackSuffix))
		err := os.Rename(oldDir, newDir)
		if err != nil {
			log.LogErrorf("rename dir failed, old (%s), new (%s)", oldDir, newDir)
		}
	}
	log.LogInfof("finish rename all succ exts, path (%s), count(%d), cost(%d)", dpDir, len(exts), time.Since(start).Milliseconds())
}

func batchDeleteBadExtent(dpIdStr string, exts []*BadNornalExtent) (err error) {
	dpInfo, err := getDpInfoById(dpIdStr)
	if err != nil {
		log.LogErrorf("Get dp info failed, err: %v", err)
		return
	}

	eks := make([]*proto.ExtentKey, 0, len(exts))
	for _, ext := range exts {
		extentId, err := strconv.ParseUint(ext.ExtentId, 10, 64)
		if err != nil {
			log.LogErrorf("Parse extent id failed, err: %v", err)
			return err
		}
		ek := &proto.ExtentKey{
			FileOffset:   0,
			PartitionId:  dpInfo.PartitionID,
			ExtentId:     extentId,
			ExtentOffset: 0,
			Size:         ext.Size,
			CRC:          0,
		}
		eks = append(eks, ek)
	}

	addr := util.ShiftAddrPort(dpInfo.Hosts[0], util.DefaultSmuxPortShift)
	conn, err := smuxPool.GetConnect(addr)

	getToken(addr)
	defer releaseToken(addr)

	defer func() {
		smuxPool.PutConnect(conn, true)
		log.LogInfof("batchDeleteBadExtent PutConnect (%v)", addr)
	}()

	if err != nil {
		log.LogInfof("Get connect failed, addr %s, err: %v", addr, err)
		return
	}

	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpGcBatchDeleteExtent
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = dpInfo.PartitionID
	p.Data, _ = json.Marshal(eks)
	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = uint8(len(dpInfo.Hosts) - 1)
	p.Arg = []byte(strings.Join(dpInfo.Hosts[1:], proto.AddrSplit) + proto.AddrSplit)
	p.ArgLen = uint32(len(p.Arg))
	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("WriteToConn failed, addr %s, err: %v", addr, err)
		return
	}

	if err = p.ReadFromConn(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		log.LogErrorf("ReadFromConn failed, addr %s, err: %v", addr, err)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = fmt.Errorf("BatchDeleteExtent failed, addr %s, ResultCode: %v", addr, p.String())
		log.LogError(err.Error())
		return
	}
	return
}

func readBadExtentFromDp(dpIdStr string, extentId uint64, size uint32) (data []byte, readBytes int, err error) {
	start := time.Now()
	defer func() {
		log.LogInfof("read data from datanode success, dpId %s, ext %d, cost %d, size(%d)",
			dpIdStr, extentId, time.Since(start).Milliseconds(), len(data))
	}()
	dpId, err := strconv.ParseUint(dpIdStr, 10, 64)
	if err != nil {
		log.LogErrorf("Parse dp id failed, err: %v", err)
		return
	}

	dpInfo, err := getDpInfoById(dpIdStr)
	if err != nil {
		log.LogErrorf("Get dp info failed, err: %v", err)
		return
	}
	var leaderAddr string
	for _, replica := range dpInfo.Replicas {
		if replica.IsLeader {
			leaderAddr = replica.Addr
			break
		}
	}

	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBackupRead
	p.ExtentID = extentId
	p.PartitionID = dpId
	p.ExtentOffset = 0
	p.ExtentType = proto.NormalExtentType
	p.Size = size
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = 0

	getToken(leaderAddr)
	defer releaseToken(leaderAddr)

	conn, err := streamConnPool.GetConnect(leaderAddr)
	if err != nil {
		log.LogErrorf("GetConnect failed, addr %s, %v", leaderAddr, err.Error())
		return
	}

	err = p.WriteToConn(conn)
	if err != nil {
		log.LogErrorf("WriteToConn failed, addr %s, %v", leaderAddr, err)
		return
	}
	data = make([]byte, size)
	for readBytes < int(size) {
		replyPacket := stream.NewReply(p.ReqID, dpId, extentId)
		bufSize := util.Min(util.ReadBlockSize, int(size)-readBytes)
		//replyPacket.Data = data[readBytes : readBytes+bufSize]
		replyPacket.Size = uint32(bufSize)
		err = replyPacket.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			log.LogErrorf("ReadFromConn failed, addr %s, %v", leaderAddr, err)
			return
		}

		if replyPacket.ResultCode == proto.OpNotExistErr {
			log.LogInfof("extent may be already been deleted, dp %d, extId %d, pkt %s", dpId, extentId, replyPacket.String())
			return nil, 0, nil
		}

		if replyPacket.ResultCode != proto.OpOk {
			err = fmt.Errorf("ReadFromConn failed ResultCode, addr %s, %v", leaderAddr, replyPacket.String())
			log.LogError(err.Error())
			return
		}
		copy(data[readBytes:readBytes+bufSize], replyPacket.Data)
		readBytes += bufSize
	}

	return
}

func writeBadNormalExtentToBackup(backupDir, volname, dpIdStr string, badExtent BadNornalExtent, data []byte, readBytes int) (err error) {

	dpDir := filepath.Join(backupDir, volname, normalDir, dpIdStr)
	start := time.Now()
	defer func() {
		log.LogInfof("writeBadNormalExtentToBackup: after Write bad extent to dest dir: %s, extId %s, cost %d ms",
			dpDir, badExtent.ExtentId, time.Since(start).Milliseconds())
	}()
	_, err = os.Stat(dpDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dpDir, 0755)
		if err != nil {
			slog.Fatalf("Mkdir failed, dir %s, err: %v", dpDir, err.Error())
		}
	} else if err != nil {
		slog.Fatalf("mkdir failed, dir %s, err %s", dpDir, err.Error())
	}

	extentFile := filepath.Join(dpDir, badExtent.ExtentId)
	file, err := os.Create(extentFile)
	if err != nil {
		slog.Fatalf("Create extent file failed, filePath %s, err: %v", extentFile, err)
	}
	defer file.Close()

	_, err = file.Write(data[:readBytes])
	if err != nil {
		log.LogErrorf("Write extent file failed, err: %v", err)
		return
	}

	return
}

func cleanBadNormalExtentOfDp(volname, dir, backupDir, dpIdStr string) (err error) {
	log.LogInfof("cleanBadNormalExtentOfDp: Clean bad normal extents in dpId: %s", dpIdStr)
	badNormalDpFile := filepath.Join(dir, volname, BadDir, normalDir, dpIdStr)
	file, err := os.Open(badNormalDpFile)
	if err != nil {
		log.LogErrorf("cleanBadNormalExtentOfDp: Open bad normal dp file failed, path %s, err: %v", badNormalDpFile, err)
		return
	}
	defer file.Close()

	start := time.Now()
	reader := bufio.NewReader(file)
	exts := make([]*BadNornalExtent, 0)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.LogErrorf("cleanBadNormalExtentOfDp: Read line failed, err: %v", err)
			return err
		}
		line = line[:len(line)-1]
		var badExtent BadNornalExtent
		err = json.Unmarshal(line, &badExtent)
		if err != nil {
			log.LogErrorf("cleanBadNormalExtentOfDp: Unmarshal bad extent failed, err: %v", err)
			return err
		}

		exts = append(exts, &badExtent)
	}
	log.LogInfof("cleanBadNormalExtentOfDp: read bad extent success, dp %s, cost %d ms", dpIdStr, time.Since(start).Milliseconds())

	beforeTimeFilePath := filepath.Join(dir, volname, "dp", normalDir, beforeTimeFile)
	data, err := os.ReadFile(beforeTimeFilePath)
	if err != nil {
		log.LogErrorf("cleanBadNormalExtentOfDp: Read before time file failed, err: %v", err)
		return err
	}

	err = batchLockBadNormalExtent(dpIdStr, exts, false, string(data))
	if err != nil {
		log.LogErrorf("Batch lock dp %v bad normal extent failed, err: %v", dpIdStr, err)
		return err
	}

	log.LogInfof("cleanBadNormalExtentOfDp: batchLockBadNormalExtent success, dp %s, cost %d ms", dpIdStr, time.Since(start).Milliseconds())

	if CleanS {
		succExts := make([]*BadNornalExtent, 0, len(exts))
		for _, badExtent := range exts {
			err = copyBadNormalExtentToBackup(volname, backupDir, dpIdStr, *badExtent)
			if err != nil {
				log.LogErrorf("Clean bad normal extent failed, dp %v, extent %v, err: %v", dpIdStr, badExtent.ExtentId, err)
				err = nil
				continue
			}
			succExts = append(succExts, badExtent)
		}

		log.LogInfof("cleanBadNormalExtentOfDp: copyBadNormalExtentToBackup success, dp %s, cost %d ms, befor %d, after %d",
			dpIdStr, len(exts), len(succExts), time.Since(start).Milliseconds())

		err = batchDeleteBadExtent(dpIdStr, succExts)
		if err != nil {
			log.LogErrorf("Batch delete bad extent failed, dp %v, err: %v", dpIdStr, err)
			return err
		}
		log.LogInfof("cleanBadNormalExtentOfDp: batchDeleteBadExtent success, dp %s, cost %d ms", dpIdStr, time.Since(start).Milliseconds())

		renameSucceExts(backupDir, volname, dpIdStr, succExts)

		err = batchUnlockBadNormalExtent(dpIdStr, exts)
		if err != nil {
			log.LogErrorf("Batch unlock bad normal extent failed, err: %v", err)
			return err
		}
		log.LogInfof("cleanBadNormalExtentOfDp: batchUnlockBadNormalExtent success, dp %s, cost %d ms", dpIdStr, time.Since(start).Milliseconds())
	}
	return
}

func cleanBadNormalExtentFromDp(volname, dir, backupDir string, fromDpId uint64, concurrency uint64) {
	badNormalDir := filepath.Join(dir, volname, BadDir, normalDir)

	start := time.Now()
	defer func() {
		slog.Printf("cleanBadNormalExtentFromDp finish, from-dp %d, vol %s, cost %d ms",
			fromDpId, volname, time.Since(start).Milliseconds())
	}()

	fileInfos, err := os.ReadDir(badNormalDir)
	if err != nil {
		slog.Fatalf("Read bad normal dir failed, path %s, err: %v", badNormalDir, err)
		return
	}

	cnt := int(concurrency)
	if concurrency <= 0 {
		cnt = 1
	}

	setHostCntLimit(3)
	var wg sync.WaitGroup
	pool := newTaskPool(cnt)
	for _, fileInfo := range fileInfos {
		dpIdStr := fileInfo.Name()
		if dpIdStr == verifyInfoFile {
			continue
		}

		dpId, err := strconv.ParseUint(dpIdStr, 10, 64)
		if err != nil {
			slog.Fatalf("Parse dp id failed, dpId %s err: %v", dpIdStr, err)
		}

		if dpId < fromDpId {
			continue
		}

		wg.Add(1)
		pool.addTask(func() {
			defer wg.Done()
			err1 := cleanBadNormalExtentOfDp(volname, dir, backupDir, dpIdStr)
			if err1 != nil {
				err = fmt.Errorf("cleanBadNormalExtentFromDp: clean bad normal ext failed, dp %s, err %s", dpIdStr, err1.Error())
				log.LogError(err.Error())
				slog.Println(err.Error())
				return
			}
		})
	}
	wg.Wait()
}

func newRollbackBadExtentsCmd() *cobra.Command {
	var (
		dpId   string
		extent string
	)

	cmd := &cobra.Command{
		Use:   "rollbackBadExtents [VOLNAME] [EXTENT TYPE] [BACKUP DIR]",
		Short: "rollback bad extents",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			volname := args[0]
			extType := args[1]
			backupDir := args[2]
			start := time.Now()
			CleanS = true

			if extType == "normal" {
				if dpId != "" {
					if extent != "" {
						err := rollbackBadNormalExtent(volname, backupDir, dpId, extent)
						if err != nil {
							slog.Fatalf("rollbackBadNormalExtent failed, vol %s, dp %s, ext %s, err %s",
								volname, dpId, extent, err.Error())
						}
						slog.Printf("rollbackBadNormalExtent success, vol %s, dp %s, ext %s, cost %d ms",
							volname, dpId, extent, time.Since(start).Milliseconds())
						return
					} else {
						rollbackBadNormalExtentInDpId(volname, backupDir, dpId)
						return
					}
				}
				slog.Fatalf("dp Id can't be empty")
			} else if extType == "tiny" {
				log.LogErrorf("tiny is not supported now")
				return
			} else {
				log.LogErrorf("Invalid extent type: %s", extType)
				return
			}
		},
	}

	cmd.Flags().StringVar(&dpId, "dp", "", "rollback bad extent in dp id")
	cmd.Flags().StringVar(&extent, "e", "", "rollback one bad extent")
	return cmd
}

func rollbackBadNormalExtentInDpId(volname, backupDir, dpId string) {
	rollbackDpDir := filepath.Join(backupDir, volname, normalDir, dpId)
	log.LogInfof("Rollback bad normal extents in dp: %s", rollbackDpDir)
	fileInfos, err := os.ReadDir(rollbackDpDir)
	if err != nil {
		slog.Fatalf("Read rollback dp %v dir failed, err: %v", dpId, err)
		return
	}
	start := time.Now()
	defer func() {
		slog.Printf("rollback dp finish, vol %s, dp %s, cost %d ms", volname, dpId, time.Since(start).Milliseconds())
	}()

	for _, fileInfo := range fileInfos {
		extent := fileInfo.Name()
		if !strings.HasSuffix(extent, BackSuffix) {
			continue
		}

		err := rollbackBadNormalExtent(volname, backupDir, dpId, extent)
		if err != nil {
			err = fmt.Errorf("Rollback bad normal dp %v extent %v failed, err: %v", dpId, extent, err.Error())
			log.LogError(err)
			slog.Println(err.Error())
			continue
		}
	}
}

func rollbackBadNormalExtent(volname, backupDir, dpId, extent string) (err error) {
	var badExtent BadNornalExtent
	var data []byte

	rollbackFilePath := filepath.Join(backupDir, volname, normalDir, dpId, extent)
	log.LogInfof("Rollback bad normal extent: %s", rollbackFilePath)

	data, err = os.ReadFile(rollbackFilePath)
	if err != nil {
		log.LogErrorf("Read bad extent failed, dp %v, extent %v, err: %v", dpId, extent, err)
		return
	}

	if strings.HasSuffix(extent, BackSuffix) {
		extent = strings.TrimSuffix(extent, BackSuffix)
	}

	badExtent.Size = uint32(len(data))
	badExtent.ExtentId = extent
	err = batchLockBadNormalExtent(dpId, []*BadNornalExtent{&badExtent}, true, "")
	if err != nil {
		log.LogErrorf("Batch lock bad normal extent failed, dp %v, extent %v, err: %v", dpId, extent, err)
		return
	}

	err = writeBadNormalExtentToDp(data, volname, dpId, extent)
	if err != nil {
		log.LogErrorf("Write bad extent to dp failed, dp %v, extent %v, err: %v", dpId, extent, err)
		return
	}

	err = batchUnlockBadNormalExtent(dpId, []*BadNornalExtent{&badExtent})
	if err != nil {
		log.LogErrorf("Batch unlock bad normal extent failed, dp %v, extent %v, err: %v", dpId, extent, err)
		return
	}

	return
}

func writeBadNormalExtentToDp(data []byte, volname, dpId, extent string) (err error) {
	dpInfo, err := getDpInfoById(dpId)
	if err != nil {
		log.LogErrorf("Get dp info failed, dp %v, err: %v", dpId, err)
		return
	}

	var leaderAddr string = dpInfo.Hosts[0]
	conn, err := streamConnPool.GetConnect(leaderAddr)

	start := time.Now()
	defer func() {
		streamConnPool.PutConnect(conn, true)
		log.LogInfof("writeBadNormalExtentToDp finish write. PutConnect (%v), dp (%s), ext (%s), cost(%d)",
			leaderAddr, dpId, extent, time.Since(start).Milliseconds())
	}()

	if err != nil {
		log.LogErrorf("Get connect failed, err: %v", err)
		return
	}

	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBackupWrite
	length := len(data)
	//length := util.BlockSize
	p.Data = make([]byte, length)
	copy(p.Data, data)
	p.Size = uint32(length)
	p.ReqID = proto.GenerateRequestID()
	p.PartitionID, err = strconv.ParseUint(dpId, 10, 64)
	if err != nil {
		log.LogErrorf("Parse dp %s id failed, err: %v", dpId, err)
		return
	}
	p.ExtentType = proto.NormalExtentType
	p.ExtentID, err = strconv.ParseUint(extent, 10, 64)
	if err != nil {
		log.LogErrorf("Parse extent %s id failed, err: %v", extent, err)
		return
	}
	p.ExtentOffset = 0
	p.Arg = []byte(strings.Join(dpInfo.Hosts[1:], proto.AddrSplit) + proto.AddrSplit)
	p.ArgLen = uint32(len(p.Arg))
	p.RemainingFollowers = uint8(len(dpInfo.Hosts) - 1)
	if len(dpInfo.Hosts) == 1 {
		p.RemainingFollowers = 127
	}
	p.StartT = time.Now().UnixNano()
	p.CRC = crc32.ChecksumIEEE(p.Data[:p.Size])
	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("WriteToConn failed, dp %v, extent %v, err: %v", dpId, extent, err)
		return
	}
	if err = p.ReadFromConn(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		log.LogErrorf("ReadFromConn failed, dp %v, extent %v, err: %v", dpId, extent, err)
		return
	}
	if p.ResultCode != proto.OpOk {
		log.LogErrorf("writeBadNormalExtentToDp failed, addr, host %s, dp %v, extent %v, ResultCode: %v",
			leaderAddr, dpId, extent, p.String())
		return
	}
	return
}
