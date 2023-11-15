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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/spf13/cobra"
)

const (
	//MaxBloomFilterSize uint64 = 25 * 1024 * 1024 * 1024
	MaxBloomFilterSize uint64 = 25 * 1024 * 1024
	MpDir              string = "mp"
	DpDir              string = "dp"
	BadDir             string = "bad"
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

func newGetMpExtentsCmd() *cobra.Command {
	var mpId string
	var fromMpId string
	var cmd = &cobra.Command{
		Use:   "getMpExtents [VOLNAME] [DIR]",
		Short: "get extents of mp",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			volname := args[0]
			dir := args[1]

			if fromMpId != "" {
				getExtentsFromMp(dir, volname, fromMpId)
				return
			}

			if mpId != "" {
				err := getExtentsByMpId(dir, volname, mpId)
				if err != nil {
					log.LogErrorf("Get extents by mpId %s failed, err: %v", mpId, err)
					fmt.Printf("Get extents by mpId %s failed, err: %v\n", mpId, err)
				}
				fmt.Printf("Get extents by mpId %s success\n", mpId)
				return
			}

			log.LogErrorf("mpId and fromMpId is empty")
		},
	}

	cmd.Flags().StringVar(&mpId, "mp", "", "get extents in mp id")
	cmd.Flags().StringVar(&fromMpId, "from-mp", "", "get extents from mp id")
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

	return mpInfo, nil
}

func getExtentsFromMp(dir string, volname string, fromMpId string) {
	mpvs, err := getMetaPartitions(MasterAddr, volname)
	if err != nil {
		log.LogErrorf("Get meta partitions failed, err: %v", err)
		return
	}
	fromMp, err := strconv.ParseUint(fromMpId, 10, 64)
	if err != nil {
		log.LogErrorf("Parse fromMpId failed, err: %v", err)
		return
	}
	for _, mpv := range mpvs {
		if mpv.PartitionID >= fromMp {
			err := getExtentsByMpId(dir, volname, strconv.FormatUint(mpv.PartitionID, 10))
			if err != nil {
				log.LogErrorf("Get extents by mpId %v failed, err: %v", mpv.PartitionID, err)
				fmt.Printf("Get extents by mpId %v failed, err: %v\n", mpv.PartitionID, err)
				return
			}
		}
	}
	fmt.Printf("Get extents from mpId %v success\n", fromMpId)
}

func getExtentsByMpId(dir string, volname string, mpId string) (err error) {
	mpInfo, err := getMpInfoById(mpId)
	if err != nil {
		log.LogErrorf("Get meta partition info failed, err: %v", err)
		return err
	}
	var leaderAddr string
	for _, mr := range mpInfo.Replicas {
		if mr.IsLeader {
			leaderAddr = mr.Addr
			break
		}
	}

	if leaderAddr == "" {
		log.LogErrorf("Get leader address failed mpId %v", mpId)
		err = fmt.Errorf("Get leader address failed mpId %v", mpId)
		return err
	}

	resp, err := http.Get(fmt.Sprintf("http://%s:%s/getInodeSnapshot?pid=%s", strings.Split(leaderAddr, ":")[0], MetaPort, mpId))
	if err != nil {
		log.LogErrorf("Get inode snapshot failed, err: %v", err)
		err = fmt.Errorf("Get inode snapshot failed, err: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.LogErrorf("Invalid status code: %v", resp.StatusCode)
		err = fmt.Errorf("Invalid status code: %v", resp.StatusCode)
		return err
	}

	reader := bufio.NewReaderSize(resp.Body, 4*1024*1024)
	inoBuf := make([]byte, 4)

	tinyFilePath, NormalFilePath, err := InitLocalDir(dir, volname, mpId, MpDir)
	if err != nil {
		log.LogErrorf("Init local dir failed, err: %v", err)
		return err
	}

	tinyFile, err := os.OpenFile(tinyFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.LogErrorf("Open file failed, err: %v", err)
		return err
	}
	defer tinyFile.Close()
	normalFile, err := os.OpenFile(NormalFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.LogErrorf("Open file failed, err: %v", err)
		return err
	}
	defer normalFile.Close()
	for {
		inoBuf = inoBuf[:4]
		// first read length
		_, err := io.ReadFull(reader, inoBuf)
		if err != nil {
			if err == io.EOF {
				err = nil
				return err
			}
			err = errors.NewErrorf("[loadInode] ReadHeader: %s", err.Error())
			return err
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
			err = errors.NewErrorf("[loadInode] ReadBody: %s", err.Error())
			return err
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
			err = errors.NewErrorf("[loadInode] Unmarshal: %s", err.Error())
			return err
		}
		eks := ino.Extents.CopyExtents()
		log.LogInfof("inode:%v, extent:%v", ino.Inode, eks)
		for _, ek := range eks {
			var eksForGc ExtentForGc
			eksForGc.Id = strconv.FormatUint(ek.PartitionId, 10) + "_" + strconv.FormatUint(ek.ExtentId, 10)
			eksForGc.Offset = ek.ExtentOffset
			eksForGc.Size = ek.Size
			data, err := json.Marshal(eksForGc)
			if err != nil {
				log.LogErrorf("Marshal extents failed, err: %v", err)
				return err
			}
			if !storage.IsTinyExtent(ek.ExtentId) {
				log.LogInfof("normal extent:%v, data:%v", ek.ExtentId, len(data))
				_, err = normalFile.Write(append(data, '\n'))
				if err != nil {
					log.LogErrorf("Write normal extent to file failed, err: %v", err)
					return err
				}
			} else {
				_, err = tinyFile.Write(append(data, '\n'))
				if err != nil {
					log.LogErrorf("Write tiny extent to file failed, err: %v", err)
					return err
				}
			}
		}
	}
}

func InitLocalDir(dir string, volname string, partitionId string, dirType string) (tinyFilePath string, normalFilePath string, err error) {
	tinyPath := filepath.Join(dir, volname, dirType, tinyDir)
	if _, err = os.Stat(tinyPath); os.IsNotExist(err) {
		if err = os.MkdirAll(tinyPath, 0755); err != nil {
			log.LogErrorf("create dir failed: %v", err)
			return
		}
	}

	normalPath := filepath.Join(dir, volname, dirType, normalDir)
	if _, err = os.Stat(normalPath); os.IsNotExist(err) {
		if err = os.MkdirAll(normalPath, 0755); err != nil {
			log.LogErrorf("create dir failed: %v", err)
			return
		}
	}

	tinyFilePath = filepath.Join(tinyPath, partitionId)
	if _, err = os.Stat(tinyFilePath); os.IsNotExist(err) {
		file, err := os.Create(tinyFilePath)
		if err != nil {
			log.LogErrorf("create tiny file failed:%v", err)
			return "", "", err
		}
		defer file.Close()
	} else {
		err = ioutil.WriteFile(tinyFilePath, []byte{}, os.ModePerm)
		if err != nil {
			log.LogErrorf("clear tiny file failed:%v", err)
			return "", "", err
		}
		log.LogInfof("tiny file contents have been cleared successfully: %v", tinyFilePath)
	}

	normalFilePath = filepath.Join(normalPath, partitionId)
	if _, err = os.Stat(normalFilePath); os.IsNotExist(err) {
		file, err := os.Create(normalFilePath)
		if err != nil {
			log.LogErrorf("create normal file failed:%v", err)
			return "", "", err
		}
		defer file.Close()
	} else {
		err = ioutil.WriteFile(normalFilePath, []byte{}, os.ModePerm)
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

			t, err := time.Parse(layout, beforeTime)
			if err != nil {
				log.LogErrorf("parse time failed: %v", err)
				return
			}
			beforeTime = strconv.FormatInt(t.Unix(), 10)
			log.LogInfof("beforeTime: %v", beforeTime)

			if fromDpId != "" {
				getExtentsFromDpId(dir, volname, fromDpId, beforeTime)
				return
			}

			if dpId != "" {
				getExtentsByDpId(dir, volname, dpId, beforeTime)
				return
			}

			log.LogErrorf("dpId and fromDpId is empty")
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

	dpInfo = &proto.DataPartitionInfo{}
	err = json.Unmarshal(body.Data, dpInfo)
	if err != nil {
		log.LogErrorf("Unmarshal response body err:%v", err)
		return nil, err
	}

	return dpInfo, nil
}

func getExtentsByDpId(dir string, volname string, dpId string, time string) {
	dpInfo, err := getDpInfoById(dpId)
	if err != nil {
		log.LogErrorf("Get dp info failed, err: %v", err)
		return
	}
	var leaderAddr string
	for _, dr := range dpInfo.Replicas {
		if dr.IsLeader {
			leaderAddr = dr.Addr
			break
		}
	}

	if leaderAddr == "" {
		log.LogErrorf("Get leader addr failed, err: %v", err)
		return
	}

	resp, err := http.Get(fmt.Sprintf("http://%s:%s/getAllExtent?id=%s&beforeTime=%s",
		strings.Split(leaderAddr, ":")[0], DataPort, dpId, time))
	if err != nil {
		log.LogErrorf("Get all extents failed, err: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.LogErrorf("Invalid status id %v, code: %v", dpId, resp.StatusCode)
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
		return
	}

	extents := make([]*storage.ExtentInfo, 0)
	err = json.Unmarshal(body.Data, &extents)
	if err != nil {
		log.LogErrorf("Unmarshal response body err:%v", err)
		return
	}

	tinyFilePath, normalFilePath, err := InitLocalDir(dir, volname, dpId, "dp")
	if err != nil {
		log.LogErrorf("Init local dir failed, err: %v", err)
		return
	}
	tinyFile, err := os.OpenFile(tinyFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.LogErrorf("Open tinyFile failed, err: %v", err)
		return
	}
	defer tinyFile.Close()
	normalFile, err := os.OpenFile(normalFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.LogErrorf("Open normalFile failed, err: %v", err)
		return
	}
	defer normalFile.Close()

	for _, extent := range extents {
		var eksForGc ExtentForGc
		eksForGc.Id = dpId + "_" + strconv.FormatUint(extent.FileID, 10)
		eksForGc.Offset = 0
		eksForGc.Size = uint32(extent.Size)
		data, err := json.Marshal(eksForGc)
		if err != nil {
			log.LogErrorf("Marshal extents failed, err: %v", err)
			return
		}
		if !storage.IsTinyExtent(extent.FileID) {
			_, err = normalFile.Write(append(data, '\n'))
			if err != nil {
				log.LogErrorf("Write normal extent to file failed, err: %v", err)
				return
			}
		} else {
			_, err = tinyFile.Write(append(data, '\n'))
			if err != nil {
				log.LogErrorf("Write tiny extent to file failed, err: %v", err)
				return
			}
		}
	}
}

func getExtentsFromDpId(dir string, volname string, fromDpId string, time string) {
	dps, err := getDataPartitions(MasterAddr, volname)
	if err != nil {
		log.LogErrorf("Get data partitions failed, err: %v", err)
		return
	}

	fromDp, err := strconv.ParseUint(fromDpId, 10, 64)
	if err != nil {
		log.LogErrorf("Parse from dp id failed, err: %v", err)
		return
	}
	for _, dp := range dps {
		if dp.PartitionID >= fromDp {
			getExtentsByDpId(dir, volname, strconv.FormatUint(dp.PartitionID, 10), time)
		}
	}
}

func newCalcBadExtentsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "calcBadExtents [VOLNAME] [EXTENT TYPE] [DIR]",
		Short: "calc bad extents",
		Args:  cobra.ExactArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			volname := args[0]
			extType := args[1]
			dir := args[2]

			if extType == "normal" {
				calcBadNormalExtents(volname, dir)
			} else if extType == "tiny" {
				log.LogErrorf("tiny is not supported now")
				return
			} else {
				log.LogErrorf("Invalid extent type: %s", extType)
				return
			}

		},
	}
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

func calcBadNormalExtents(volname, dir string) (err error) {
	gBloomFilter = util.NewBloomFilter(MaxBloomFilterSize, calcFNVHash, calcCRC64Hash)
	normalMpDir := filepath.Join(dir, volname, MpDir, normalDir)

	err = addMpExtentToBF(normalMpDir)
	if err != nil {
		log.LogErrorf("Add normal extents to bloom filter failed, err: %v", err)
		return
	}

	normalDpDir := filepath.Join(dir, volname, DpDir, normalDir)
	destDir := filepath.Join(dir, volname, BadDir, normalDir)
	if _, err = os.Stat(destDir); os.IsNotExist(err) {
		err = os.MkdirAll(destDir, 0755)
		if err != nil {
			log.LogErrorf("Mkdir dest dir failed, err: %v", err)
			return
		}
	}
	err = calcDpBadNormalExtentByBF(normalDpDir, destDir)
	return err
}

func addMpExtentToBF(mpDir string) (err error) {
	fileInfos, err := ioutil.ReadDir(mpDir)
	if err != nil {
		log.LogErrorf("Read mp dir failed, err: %v", err)
		return
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() {
			filePath := filepath.Join(mpDir, fileInfo.Name())
			file, err := os.Open(filePath)
			if err != nil {
				log.LogErrorf("Open file failed, err: %v", err)
				return err
			}
			defer file.Close()

			reader := bufio.NewReader(file)
			for {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						break
					}
					log.LogErrorf("Read line failed, err: %v", err)
					return err
				}
				var eksForGc ExtentForGc
				err = json.Unmarshal(line, &eksForGc)
				if err != nil {
					log.LogErrorf("Unmarshal extent failed, err: %v", err)
					return err
				}

				gBloomFilter.Add([]byte(eksForGc.Id)) // add extent id to bloom filter
			}
		}
	}
	return
}

func calcDpBadNormalExtentByBF(dpDir, badDir string) (err error) {
	fileInfos, err := ioutil.ReadDir(dpDir)
	if err != nil {
		log.LogErrorf("Read dp dir failed, err: %v", err)
		return
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() {
			filePath := filepath.Join(dpDir, fileInfo.Name())
			file, err := os.Open(filePath)
			if err != nil {
				log.LogErrorf("Open file failed, err: %v", err)
				return err
			}
			defer file.Close()

			reader := bufio.NewReader(file)
			for {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						break
					}
					log.LogErrorf("Read line failed, err: %v", err)
					return err
				}
				var eksForGc ExtentForGc
				err = json.Unmarshal(line, &eksForGc)
				if err != nil {
					log.LogErrorf("Unmarshal extent failed, err: %v", err)
					return err
				}
				if !gBloomFilter.Contains([]byte(eksForGc.Id)) {
					var badExtent BadNornalExtent
					parts := strings.Split(eksForGc.Id, "_")
					dpId := parts[0]
					badExtent.ExtentId = parts[1]
					badExtent.Size = eksForGc.Size

					err = writeBadNormalExtentoLocal(dpId, badDir, badExtent)
					if err != nil {
						log.LogErrorf("Write bad extent failed, err: %v", err)
						return err
					}
				}
			}
		}
	}
	return
}

// write bad extent to local
func writeBadNormalExtentoLocal(dpId, badDir string, badExtent BadNornalExtent) (err error) {
	log.LogInfof("Write dpId: %s bad extent: %v", dpId, badExtent)
	filePath := filepath.Join(badDir, dpId)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.LogErrorf("Open file failed, err: %v", err)
		return err
	}
	defer file.Close()
	data, err := json.Marshal(&badExtent)
	if err != nil {
		log.LogErrorf("Marshal bad extent failed, err: %v", err)
		return err
	}
	_, err = file.Write(append(data, '\n'))
	if err != nil {
		log.LogErrorf("Write bad extent failed, err: %v", err)
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
				log.LogErrorf("tiny is not supported now")
				return
			} else {
				log.LogErrorf("Invalid extent type: %s", extType)
				return
			}
		},
	}
	return cmd
}

func checkBadNormalExtents(volname, dir, checkDpId string) {
	normalMpDir := filepath.Join(dir, volname, MpDir, normalDir)
	badExtentPath := filepath.Join(dir, volname, BadDir, normalDir, checkDpId)

	fileInfos, err := ioutil.ReadDir(normalMpDir)
	if err != nil {
		log.LogErrorf("Read mp dir failed, err: %v", err)
		return
	}

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() {
			filePath := filepath.Join(normalMpDir, fileInfo.Name())
			file, err := os.Open(filePath)
			if err != nil {
				log.LogErrorf("Open file failed, err: %v", err)
				return
			}
			defer file.Close()

			reader := bufio.NewReader(file)
			for {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						break
					}
					log.LogErrorf("Read line failed, err: %v", err)
					return
				}
				var eksForGc ExtentForGc
				err = json.Unmarshal(line, &eksForGc)
				if err != nil {
					log.LogErrorf("Unmarshal extent failed, err: %v", err)
					return
				}
				parts := strings.Split(eksForGc.Id, "_")
				dpId := parts[0]
				extentId := parts[1]
				if dpId != checkDpId {
					continue
				}
				if checkMpExtent(extentId, badExtentPath) {
					log.LogErrorf("Find bad extent in mp: %s", eksForGc.Id)
					return
				}
			}
		}
	}
	log.LogInfo("check success")
	return
}

func checkMpExtent(extentId, badExtentPath string) (isFind bool) {
	log.LogInfof("check extent: %s", extentId)
	file, err := os.Open(badExtentPath)
	if err != nil {
		log.LogErrorf("Open file failed, err: %v", err)
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.LogErrorf("Read line failed, err: %v", err)
			return
		}
		line = line[:len(line)-1]
		var badExtent BadNornalExtent
		err = json.Unmarshal(line, &badExtent)
		if err != nil {
			log.LogErrorf("Unmarshal bad extent failed, err: %v", err)
			return
		}
		if badExtent.ExtentId == extentId {
			return true
		}
	}
	return false
}

func batchLockBadNormalExtent(dpIdStr string, exts []*BadNornalExtent, IsCreate bool) (err error) {
	dpInfo, err := getDpInfoById(dpIdStr)
	if err != nil {
		log.LogErrorf("Get dp %v info failed, err: %v", dpIdStr, err)
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

	gcLockEks := &proto.GcLockExtents{
		IsCreate: IsCreate,
		Eks:      eks,
	}
	conn, err := streamConnPool.GetConnect(dpInfo.Hosts[0])
	defer func() {
		streamConnPool.PutConnect(conn, true)
		log.LogInfof("batchLockBadNormalExtent PutConnect (%v)", dpInfo.Hosts[0])
	}()

	if err != nil {
		log.LogErrorf("Get connect failed, err: %v", err)
		return
	}

	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBatchLockNormalExtent
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = dpInfo.PartitionID
	p.Data, err = json.Marshal(gcLockEks)
	if err != nil {
		log.LogErrorf("Marshal extent keys failed, err: %v", err)
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
		log.LogErrorf("batchLockBadNormalExtent dp %v failed, ResultCode: %v", dpIdStr, p.ResultCode)
		err = fmt.Errorf("batchLockBadNormalExtent dp %v failed, ResultCode: %v", dpIdStr, p.ResultCode)
		return
	}

	log.LogInfof("batchLockBadNormalExtent success dpId: %s", dpIdStr)
	return
}

func batchUnlockBadNormalExtent(dpIdStr string, exts []*BadNornalExtent) (err error) {
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

	conn, err := streamConnPool.GetConnect(dpInfo.Hosts[0])
	defer func() {
		streamConnPool.PutConnect(conn, true)
		log.LogInfof("batchUnlockBadNormalExtent PutConnect (%v)", dpInfo.Hosts[0])
	}()

	if err != nil {
		log.LogErrorf("Get connect failed, err: %v", err)
		return
	}

	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBatchUnlockNormalExtent
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = dpInfo.PartitionID
	p.Data, err = json.Marshal(eks)
	if err != nil {
		log.LogErrorf("Marshal extent keys failed, err: %v", err)
		return
	}
	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = uint8(len(dpInfo.Hosts) - 1)
	p.Arg = []byte(strings.Join(dpInfo.Hosts[1:], proto.AddrSplit) + proto.AddrSplit)
	p.ArgLen = uint32(len(p.Arg))

	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("WriteToConn failed, err: %v", err)
		return
	}
	if err = p.ReadFromConn(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		log.LogErrorf("ReadFromConn failed, err: %v", err)
		return
	}

	if p.ResultCode != proto.OpOk {
		log.LogErrorf("batchUnlockBadNormalExtent failed, ResultCode: %v", p.ResultCode)
		err = fmt.Errorf("batchUnlockBadNormalExtent failed, ResultCode: %v", p.ResultCode)
		return
	}

	log.LogInfof("batchUnlockBadNormalExtent success dpId: %s", dpIdStr)
	return
}

func newCleanBadExtentsCmd() *cobra.Command {
	var (
		fromDpId string
		dpId     string
		extent   string
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

			if extType == "normal" {
				if fromDpId != "" {
					fromDpIdNum, err := strconv.ParseUint(fromDpId, 10, 64)
					if err != nil {
						log.LogErrorf("Parse from dp id failed, err: %v", err)
						return
					}
					cleanBadNormalExtentFromDp(volname, dir, backupDir, fromDpIdNum)
					return
				}
				if dpId != "" {
					if extent != "" {
						cleanBadNormalExtent(volname, dir, backupDir, dpId, extent)
					} else {
						cleanBadNormalExtentOfDp(volname, dir, backupDir, dpId)
					}
					return
				}
			} else if extType == "tiny" {
				log.LogErrorf("tiny is not supported now")
				return
			} else {
				log.LogErrorf("Invalid extent type: %s", extType)
				return
			}
		},
	}

	cmd.Flags().StringVar(&fromDpId, "from-dp", "", "clean bad extent from dp id")
	cmd.Flags().StringVar(&dpId, "dp", "", "clean bad extent in dp id")
	cmd.Flags().StringVar(&extent, "e", "", "clean one bad extent")
	return cmd
}

func cleanBadNormalExtent(volname, dir, backupDir, dpIdStr, extentStr string) (err error) {
	log.LogInfof("Clean bad normal volname: %s, dir: %s, backupDir: %s, dpId: %s, extent: %s", volname, dir, backupDir, dpIdStr, extentStr)
	badNornalDpFile := filepath.Join(dir, volname, BadDir, normalDir, dpIdStr)
	file, err := os.Open(badNornalDpFile)
	if err != nil {
		log.LogErrorf("Open bad normal dp file failed, err: %v", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.LogErrorf("Read line failed, err: %v", err)
			return err
		}
		line = line[:len(line)-1]
		var badExtent BadNornalExtent
		err = json.Unmarshal(line, &badExtent)
		if err != nil {
			log.LogErrorf("Unmarshal bad extent failed, err: %v", err)
			return err
		}
		if badExtent.ExtentId == extentStr {
			var exts = []*BadNornalExtent{&badExtent}
			err = batchLockBadNormalExtent(dpIdStr, exts, false)
			if err != nil {
				log.LogErrorf("Batch lock bad normal extent failed, err: %v", err)
				return err
			}

			err = copyBadNormalExtentToBackup(volname, backupDir, dpIdStr, badExtent)
			if err != nil {
				log.LogErrorf("Copy bad extent failed, err: %v", err)
				return err
			}

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
			log.LogInfof("cleanBadNormalExtent extent: %s", extentStr)
			return nil
		}
	}
	log.LogErrorf("Can not find bad extent: %s", extentStr)
	return
}

func copyBadNormalExtentToBackup(volname, backupDir, dpIdStr string, badExtent BadNornalExtent) (err error) {
	log.LogInfof("Clean bad normal extent Internal backupDir: %s, dpIdStr: %s, badExtent:%v",
		backupDir, dpIdStr, badExtent.ExtentId)

	extentId, err := strconv.ParseUint(badExtent.ExtentId, 10, 64)
	if err != nil {
		log.LogErrorf("Parse extent id failed, err: %v", err)
		return
	}

	data, readBytes, err := readBadExtentFromDp(dpIdStr, extentId, badExtent.Size)
	if err != nil {
		log.LogErrorf("Read bad extent from dp failed, err: %v", err)
		return
	}

	err = writeBadNormalExtentToBackup(backupDir, volname, dpIdStr, badExtent, data, readBytes)
	if err != nil {
		log.LogErrorf("Write bad extent to dest dir failed, err: %v", err)
		return
	}

	return
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

	defer func() {
		smuxPool.PutConnect(conn, true)
		log.LogInfof("batchDeleteBadExtent PutConnect (%v)", addr)
	}()

	if err != nil {
		log.LogInfof("Get connect failed, err: %v", err)
		return
	}

	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBatchDeleteExtent
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = dpInfo.PartitionID
	p.Data, _ = json.Marshal(eks)
	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = uint8(len(dpInfo.Hosts) - 1)
	p.Arg = []byte(strings.Join(dpInfo.Hosts[1:], proto.AddrSplit) + proto.AddrSplit)
	p.ArgLen = uint32(len(p.Arg))
	if err = p.WriteToConn(conn); err != nil {
		log.LogErrorf("WriteToConn failed, err: %v", err)
		return
	}
	if err = p.ReadFromConn(conn, proto.BatchDeleteExtentReadDeadLineTime); err != nil {
		log.LogErrorf("ReadFromConn failed, err: %v", err)
		return
	}

	if p.ResultCode != proto.OpOk {
		log.LogErrorf("BatchDeleteExtent failed, ResultCode: %v", p.ResultCode)
		err = fmt.Errorf("BatchDeleteExtent failed, ResultCode: %v", p.ResultCode)
		return
	}
	return
}

func readBadExtentFromDp(dpIdStr string, extentId uint64, size uint32) (data []byte, readBytes int, err error) {
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

	conn, err := streamConnPool.GetConnect(leaderAddr)
	if err != nil {
		log.LogErrorf("GetConnect failed %v", err)
		return
	}
	err = p.WriteToConn(conn)
	if err != nil {
		log.LogErrorf("WriteToConn failed %v", err)
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
			log.LogErrorf("ReadFromConn failed %v", err)
			return
		}

		if replyPacket.ResultCode != proto.OpOk {
			log.LogErrorf("ReadFromConn failed ResultCode %v", replyPacket.ResultCode)
			err = fmt.Errorf("ReadFromConn failed ResultCode %v", replyPacket.ResultCode)
			return
		}
		copy(data[readBytes:readBytes+bufSize], replyPacket.Data)
		readBytes += bufSize
	}

	return
}

func writeBadNormalExtentToBackup(backupDir, volname, dpIdStr string, badExtent BadNornalExtent, data []byte, readBytes int) (err error) {
	dpDir := filepath.Join(backupDir, volname, normalDir, dpIdStr)
	log.LogInfof("Write bad extent to dest dir: %s", dpDir)
	if _, err = os.Stat(dpDir); os.IsNotExist(err) {
		err = os.MkdirAll(dpDir, 0755)
		if err != nil {
			log.LogErrorf("Mkdir failed, err: %v", err)
			return
		}
	}

	extentFile := filepath.Join(dpDir, badExtent.ExtentId)
	file, err := os.Create(extentFile)
	if err != nil {
		log.LogErrorf("Create extent file failed, err: %v", err)
		return
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
	log.LogInfof("Clean bad normal extents in dpId: %s", dpIdStr)
	badNormalDpFile := filepath.Join(dir, volname, BadDir, normalDir, dpIdStr)
	file, err := os.Open(badNormalDpFile)
	if err != nil {
		log.LogErrorf("Open bad normal dp file failed, err: %v", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	exts := make([]*BadNornalExtent, 0)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.LogErrorf("Read line failed, err: %v", err)
			return err
		}
		line = line[:len(line)-1]
		var badExtent BadNornalExtent
		err = json.Unmarshal(line, &badExtent)
		if err != nil {
			log.LogErrorf("Unmarshal bad extent failed, err: %v", err)
			return err
		}

		exts = append(exts, &badExtent)
	}

	err = batchLockBadNormalExtent(dpIdStr, exts, false)
	if err != nil {
		log.LogErrorf("Batch lock dp %v bad normal extent failed, err: %v", dpIdStr, err)
		return err
	}

	for _, badExtent := range exts {
		err = copyBadNormalExtentToBackup(volname, backupDir, dpIdStr, *badExtent)
		if err != nil {
			log.LogErrorf("Clean bad normal extent failed, dp %v, extent %v, err: %v", dpIdStr, badExtent.ExtentId, err)
			return err
		}
	}
	err = batchDeleteBadExtent(dpIdStr, exts)
	if err != nil {
		log.LogErrorf("Batch delete bad extent failed, dp %v, err: %v", dpIdStr, err)
		return err
	}

	err = batchUnlockBadNormalExtent(dpIdStr, exts)
	if err != nil {
		log.LogErrorf("Batch unlock bad normal extent failed, err: %v", err)
		return err
	}
	return
}

func cleanBadNormalExtentFromDp(volname, dir, backupDir string, fromDpId uint64) {
	badNormalDir := filepath.Join(dir, volname, BadDir, normalDir)

	fileInfos, err := ioutil.ReadDir(badNormalDir)
	if err != nil {
		log.LogErrorf("Read bad normal dir failed, err: %v", err)
		return
	}

	for _, fileInfo := range fileInfos {
		dpIdStr := fileInfo.Name()
		dpId, err := strconv.ParseUint(dpIdStr, 10, 64)
		if err != nil {
			log.LogErrorf("Parse dp id failed, err: %v", err)
			continue
		}
		if dpId < fromDpId {
			continue
		}
		err = cleanBadNormalExtentOfDp(volname, dir, backupDir, dpIdStr)
		if err != nil {
			log.LogErrorf("Clean bad normal extents in dp id failed, err: %v", err)
			continue
		}
	}
	return
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

			if extType == "normal" {
				if dpId != "" {
					if extent != "" {
						rollbackBadNormalExtent(volname, backupDir, dpId, extent)
					} else {
						rollbackBadNormalExtentInDpId(volname, backupDir, dpId)
					}
				}

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
	fileInfos, err := ioutil.ReadDir(rollbackDpDir)
	if err != nil {
		log.LogErrorf("Read rollback dp %v dir failed, err: %v", dpId, err)
		return
	}

	for _, fileInfo := range fileInfos {
		extent := fileInfo.Name()
		err := rollbackBadNormalExtent(volname, backupDir, dpId, extent)
		if err != nil {
			log.LogErrorf("Rollback bad normal dp %v extent %v failed, err: %v", dpId, extent, err)
			continue
		}
	}
}

func rollbackBadNormalExtent(volname, backupDir, dpId, extent string) (err error) {
	rollbackFilePath := filepath.Join(backupDir, volname, normalDir, dpId, extent)
	log.LogInfof("Rollback bad normal extent: %s", rollbackFilePath)
	data, err := ioutil.ReadFile(rollbackFilePath)
	if err != nil {
		log.LogErrorf("Read bad extent failed, dp %v, extent %v, err: %v", dpId, extent, err)
		return
	}
	var badExtent BadNornalExtent
	badExtent.ExtentId = extent
	badExtent.Size = uint32(len(data))
	err = batchLockBadNormalExtent(dpId, []*BadNornalExtent{&badExtent}, true)
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

	defer func() {
		streamConnPool.PutConnect(conn, true)
		log.LogInfof("writeBadNormalExtentToDp PutConnect (%v)", leaderAddr)
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
		log.LogErrorf("writeBadNormalExtentToDp failed, dp %v, extent %v, ResultCode: %v", dpId, extent, p.ResultCode)
		return
	}
	return
}
