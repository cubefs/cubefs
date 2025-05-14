// Copyright 2022 The CubeFS Authors.
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

package blobstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/codemode"
	ebsproto "github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stat"
	"github.com/google/uuid"
)

const (
	MaxRetryTimes      = 3
	RetrySleepInterval = 100 * time.Millisecond
)

type BlobStoreClient struct {
	client access.API
}

func NewEbsClient(cfg access.Config) (*BlobStoreClient, error) {
	cli, err := access.New(cfg)
	return &BlobStoreClient{
		client: cli,
	}, err
}

func (ebs *BlobStoreClient) Read(ctx context.Context, volName string, buf []byte, offset uint64, size uint64, oek proto.ObjExtentKey) (readN int, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ebs-read", err, bgTime, 1)
	}()

	requestId := uuid.New().String()
	log.LogDebugf("TRACE Ebs Read Enter requestId(%v), oek(%v)", requestId, oek)
	ctx = access.WithRequestID(ctx, requestId)
	start := time.Now()

	metric := exporter.NewTPCnt(createOPMetric(buf, "ebsread"))
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: volName})
	}()
	blobs := oek.Blobs
	sliceInfos := make([]ebsproto.Slice, 0)
	for _, b := range blobs {
		sliceInfo := ebsproto.Slice{
			MinSliceID: ebsproto.BlobID(b.MinBid),
			Vid:        ebsproto.Vid(b.Vid),
			Count:      uint32(b.Count),
		}
		sliceInfos = append(sliceInfos, sliceInfo)
	}
	loc := ebsproto.Location{
		ClusterID: ebsproto.ClusterID(oek.Cid),
		Size_:     oek.Size,
		Crc:       oek.Crc,
		CodeMode:  codemode.CodeMode(oek.CodeMode),
		SliceSize: oek.BlobSize,
		Slices:    sliceInfos,
	}
	// func get has retry
	log.LogDebugf("TRACE Ebs Read,oek(%v) loc(%v)", oek, loc)
	var body io.ReadCloser
	defer func() {
		if body != nil {
			body.Close()
		}
	}()
	for i := 0; i < MaxRetryTimes; i++ {
		body, err = ebs.client.Get(ctx, &access.GetArgs{Location: loc, Offset: offset, ReadSize: size})
		if err == nil {
			break
		}
		log.LogWarnf("TRACE Ebs Read,oek(%v), err(%v), requestId(%v),retryTimes(%v)", oek, err, requestId, i)
		time.Sleep(RetrySleepInterval)
	}
	if err != nil {
		log.LogErrorf("TRACE Ebs Read,oek(%v), err(%v), requestId(%v)", oek, err, requestId)
		return 0, err
	}

	readN, err = io.ReadFull(body, buf)
	if err != nil {
		log.LogErrorf("TRACE Ebs Read,oek(%v), err(%v), requestId(%v)", oek, err, requestId)
		return 0, err
	}
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Ebs Read Exit,oek(%v) readN(%v),bufLen(%v),consume(%v)ns", oek, readN, len(buf), elapsed.Nanoseconds())
	return readN, nil
}

func (ebs *BlobStoreClient) Write(ctx context.Context, volName string, data []byte, size uint32) (location ebsproto.Location, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ebs-write", err, bgTime, 1)
	}()

	requestId := uuid.New().String()
	log.LogDebugf("TRACE Ebs Write Enter,requestId(%v)  len(%v)", requestId, size)
	start := time.Now()
	ctx = access.WithRequestID(ctx, requestId)
	metric := exporter.NewTPCnt(createOPMetric(data, "ebswrite"))
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: volName})
	}()

	for i := 0; i < MaxRetryTimes; i++ {
		location, _, err = ebs.client.Put(ctx, &access.PutArgs{
			Size: int64(size),
			Body: bytes.NewReader(data),
		})
		if err == nil {
			break
		}
		log.LogWarnf("TRACE Ebs write, err(%v), requestId(%v),retryTimes(%v)", err, requestId, i)
		time.Sleep(RetrySleepInterval)
	}
	if err != nil {
		log.LogErrorf("TRACE Ebs write,err(%v),requestId(%v)", err.Error(), requestId)
		return location, err
	}
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Ebs Write Exit,requestId(%v)  len(%v) consume(%v)ns", requestId, len(data), elapsed.Nanoseconds())
	return location, nil
}

func (ebs *BlobStoreClient) Delete(oeks []proto.ObjExtentKey) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ebs-delete", err, bgTime, 1)
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()

	locs := make([]ebsproto.Location, 0)

	for _, oek := range oeks {
		sliceInfos := make([]ebsproto.Slice, 0)
		for _, b := range oek.Blobs {
			sliceInfo := ebsproto.Slice{
				MinSliceID: ebsproto.BlobID(b.MinBid),
				Vid:        ebsproto.Vid(b.Vid),
				Count:      uint32(b.Count),
			}
			sliceInfos = append(sliceInfos, sliceInfo)
		}

		loc := ebsproto.Location{
			ClusterID: ebsproto.ClusterID(oek.Cid),
			Size_:     oek.Size,
			Crc:       oek.Crc,
			CodeMode:  codemode.CodeMode(oek.CodeMode),
			SliceSize: oek.BlobSize,
			Slices:    sliceInfos,
		}
		locs = append(locs, loc)
	}

	requestId := uuid.New().String()
	log.LogDebugf("start Ebs delete Enter,requestId(%v)  len(%v)", requestId, len(oeks))
	start := time.Now()
	ctx = access.WithRequestID(ctx, requestId)
	metric := exporter.NewTPCnt("ebsdel")
	defer func() {
		metric.SetWithLabels(err, map[string]string{})
	}()

	elapsed := time.Since(start)
	_, err = ebs.client.Delete(ctx, &access.DeleteArgs{Locations: locs})
	if err != nil {
		log.LogErrorf("[EbsDelete] Ebs delete error, id(%v), consume(%v)ns, err(%v)", requestId, elapsed.Nanoseconds(), err.Error())
		return err
	}

	log.LogDebugf("Ebs delete Exit,requestId(%v)  len(%v) consume(%v)ns", requestId, len(oeks), elapsed.Nanoseconds())

	return err
}

func createOPMetric(buf []byte, tag string) string {
	if len(buf) >= 0 && len(buf) < 4*util.KB {
		return tag + "0K_4K"
	} else if len(buf) >= 4*util.KB && len(buf) < 128*util.KB {
		return tag + "4K_128K"
	} else if len(buf) >= 128*util.KB && len(buf) < 1*util.MB {
		return tag + "128K_1M"
	} else if len(buf) >= 1*util.MB && len(buf) < 4*util.MB {
		return tag + "1M_4M"
	}
	return tag + "4M_8M"
}

func createOPMetricBySize(size uint64, tag string) string {
	if size < 4*util.KB {
		return tag + "0K_4K"
	} else if size >= 4*util.KB && size < 128*util.KB {
		return tag + "4K_128K"
	} else if size >= 128*util.KB && size < 1*util.MB {
		return tag + "128K_1M"
	} else if size >= 1*util.MB && size < 4*util.MB {
		return tag + "1M_4M"
	} else if size >= 4*util.MB && size < 16*util.MB {
		return tag + "4M_16M"
	} else if size >= 16*util.MB && size < 64*util.MB {
		return tag + "16M_64M"
	} else if size >= 64*util.MB && size < 256*util.MB {
		return tag + "64M_256M"
	} else if size >= 256*util.MB && size < 1024*util.MB {
		return tag + "256M_1G"
	}
	return tag + "1G_"
}

func (ebs *BlobStoreClient) Put(ctx context.Context, volName string, f io.Reader, size uint64) (oek []proto.ObjExtentKey, md5 [][]byte, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ebs-write", err, bgTime, 1)
	}()

	requestId := uuid.New().String()
	log.LogDebugf("TRACE Ebs Put Enter, requestId(%v)  len(%v)", requestId, size)
	start := time.Now()
	ctx = access.WithRequestID(ctx, requestId)
	metric := exporter.NewTPCnt(createOPMetricBySize(size, "ebswrite"))
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: volName})
	}()

	var from uint64
	var part uint64 = util.ExtentSize
	rest := size
	for rest > 0 {
		var putSize uint64
		if rest > part {
			putSize = part
		} else {
			putSize = rest
		}
		rest -= putSize
		var location ebsproto.Location
		var hash access.HashSumMap
		location, hash, err = ebs.client.Put(ctx, &access.PutArgs{
			Size:   int64(putSize),
			Hashes: access.HashAlgMD5,
			Body:   f,
		})
		if err != nil {
			log.LogErrorf("TRACE Ebs Put, err(%v),requestId(%v)", err.Error(), requestId)
			return
		}

		var _md5 []byte
		_md5, err = hex.DecodeString(hash.GetSumVal(access.HashAlgMD5).(string))
		if err != nil {
			log.LogErrorf("decode md5 %v, err %v", hash.GetSumVal(access.HashAlgMD5).(string), err)
			return
		}
		oek = append(oek, locationToObjExtentKey(location, from))
		md5 = append(md5, _md5)
		from += putSize
		log.LogDebugf("TRACE Ebs Put, requestId(%v) loc(%v) putSize(%v)", requestId, location, putSize)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Ebs Put Exit, requestId(%v) oek(%v) md5(%v) size(%v) consume(%v)ns", requestId, oek, md5, size, elapsed.Nanoseconds())
	return
}

func locationToObjExtentKey(location ebsproto.Location, from uint64) (oek proto.ObjExtentKey) {
	blobs := make([]proto.Blob, 0)
	for _, info := range location.Slices {
		blob := proto.Blob{
			MinBid: uint64(info.MinSliceID),
			Count:  uint64(info.Count),
			Vid:    uint64(info.Vid),
		}
		blobs = append(blobs, blob)
	}
	oek = proto.ObjExtentKey{
		Cid:        uint64(location.ClusterID),
		CodeMode:   uint8(location.CodeMode),
		Size:       location.Size_,
		BlobSize:   location.SliceSize,
		Blobs:      blobs,
		BlobsLen:   uint32(len(blobs)),
		FileOffset: from,
		Crc:        location.Crc,
	}
	return
}

func (ebs *BlobStoreClient) Get(ctx context.Context, volName string, offset uint64, size uint64, oek proto.ObjExtentKey) (body io.ReadCloser, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ebs-read", err, bgTime, 1)
	}()

	requestId := uuid.New().String()
	log.LogDebugf("TRACE Ebs Read Enter requestId(%v), oek(%v)", requestId, oek)
	ctx = access.WithRequestID(ctx, requestId)
	start := time.Now()

	metric := exporter.NewTPCnt(createOPMetricBySize(size, "ebsread"))
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: volName})
	}()
	blobs := oek.Blobs
	sliceInfos := make([]ebsproto.Slice, 0)
	for _, b := range blobs {
		sliceInfo := ebsproto.Slice{
			MinSliceID: ebsproto.BlobID(b.MinBid),
			Vid:        ebsproto.Vid(b.Vid),
			Count:      uint32(b.Count),
		}
		sliceInfos = append(sliceInfos, sliceInfo)
	}
	loc := ebsproto.Location{
		ClusterID: ebsproto.ClusterID(oek.Cid),
		Size_:     oek.Size,
		Crc:       oek.Crc,
		CodeMode:  codemode.CodeMode(oek.CodeMode),
		SliceSize: oek.BlobSize,
		Slices:    sliceInfos,
	}
	// func get has retry
	log.LogDebugf("TRACE Ebs Read, oek(%v) loc(%v)", oek, loc)
	defer func() {
		if body != nil {
			body.Close()
		}
	}()
	for i := 0; i < MaxRetryTimes; i++ {
		body, err = ebs.client.Get(ctx, &access.GetArgs{Location: loc, Offset: offset, ReadSize: size})
		if err == nil {
			break
		}
		log.LogWarnf("TRACE Ebs Read, oek(%v), err(%v), requestId(%v),retryTimes(%v)", oek, err, requestId, i)
		time.Sleep(RetrySleepInterval)
	}
	if err != nil {
		log.LogErrorf("TRACE Ebs Read, oek(%v), err(%v), requestId(%v)", oek, err, requestId)
		return
	}
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Ebs Read Exit, oek(%v) size(%v), consume(%v)ns", oek, size, elapsed.Nanoseconds())
	return
}
