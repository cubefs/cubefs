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
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"time"

	stream "github.com/cubefs/cubefs/blobstore/access"
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
	sdk    stream.SDK
}

func NewEbsClient(cfg access.Config) (*BlobStoreClient, error) {
	cli, err := access.New(cfg)
	return &BlobStoreClient{
		client: cli,
	}, err
}

// ParseStreamConfig parse stream config from base64.
func ParseStreamConfig(baseString string) (cfg stream.StreamConfig, err error) {
	b, err := base64.StdEncoding.DecodeString(baseString)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &cfg)
	return
}

// NewEbsSDK return BlobStoreClient with sdk.
//
// Notice: required config items:
// stream.StreamConfig.IDC = "required"
// stream.StreamConfig.ClusterConfig.ConsulAgentAddr = "required"
// stream.StreamConfig.ClusterConfig.Region = "required"
// stream.StreamConfig.ClusterConfig.RegionMagic = "required"
//
// Notice: Redirect logger of "github.com/cubefs/cubefs/blobstore/util/log"
func NewEbsSDK(cfg stream.StreamConfig) (*BlobStoreClient, error) {
	sdk, err := stream.NewSDK(cfg)
	return &BlobStoreClient{sdk: sdk}, err
}

func (ebs *BlobStoreClient) get(ctx context.Context, buf []byte, loc access.Location, offset uint64, size uint64) (readN int, err error) {
	if ebs.sdk != nil {
		w := &limitedWriter{buffer: buf}
		if err = ebs.sdk.Get(ctx, w, &loc, size, offset); err != nil {
			return
		}
		if w.Len() != len(buf) {
			return w.Len(), io.ErrShortBuffer
		}
		return w.Len(), nil
	} else if ebs.client != nil {
		var body io.ReadCloser
		defer func() {
			if body != nil {
				body.Close()
			}
		}()
		body, err = ebs.client.Get(ctx, &access.GetArgs{Location: loc, Offset: offset, ReadSize: size})
		if err != nil {
			return 0, err
		}
		readN, err = io.ReadFull(body, buf)
		return
	}
	err = errors.New("not implement interface get")
	return
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
	sliceInfos := make([]access.SliceInfo, 0)
	for _, b := range blobs {
		sliceInfo := access.SliceInfo{
			MinBid: ebsproto.BlobID(b.MinBid),
			Vid:    ebsproto.Vid(b.Vid),
			Count:  uint32(b.Count),
		}
		sliceInfos = append(sliceInfos, sliceInfo)
	}
	loc := access.Location{
		ClusterID: ebsproto.ClusterID(oek.Cid),
		Size:      oek.Size,
		Crc:       oek.Crc,
		CodeMode:  codemode.CodeMode(oek.CodeMode),
		BlobSize:  oek.BlobSize,
		Blobs:     sliceInfos,
	}
	//func get has retry
	log.LogDebugf("TRACE Ebs Read,oek(%v) loc(%v)", oek, loc)
	for i := 0; i < MaxRetryTimes; i++ {
		if readN, err = ebs.get(ctx, buf, loc, offset, size); err == nil {
			break
		}
		log.LogWarnf("TRACE Ebs Read,oek(%v), err(%v), requestId(%v),retryTimes(%v)", oek, err, requestId, i)
		time.Sleep(RetrySleepInterval)
	}
	if err != nil {
		log.LogErrorf("TRACE Ebs Read,oek(%v), err(%v), requestId(%v)", oek, err, requestId)
		return 0, err
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Ebs Read Exit,oek(%v) readN(%v),bufLen(%v),consume(%v)ns", oek, readN, len(buf), elapsed.Nanoseconds())
	return readN, nil
}

func (ebs *BlobStoreClient) put(ctx context.Context, body io.Reader, size int64) (location access.Location, err error) {
	if ebs.sdk != nil {
		var loc *access.Location
		if loc, err = ebs.sdk.Put(ctx, body, size, nil); err == nil {
			location = *loc
		}
		return
	} else if ebs.client != nil {
		location, _, err = ebs.client.Put(ctx, &access.PutArgs{Size: int64(size), Body: body})
		return
	}
	err = errors.New("not implement interface put")
	return
}

func (ebs *BlobStoreClient) Write(ctx context.Context, volName string, data []byte, size uint32) (location access.Location, err error) {
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
		location, err = ebs.put(ctx, bytes.NewReader(data), int64(size))
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

func (ebs *BlobStoreClient) del(ctx context.Context, locs []access.Location) (err error) {
	if ebs.sdk != nil {
		for _, loc := range locs {
			if err = ebs.sdk.Delete(ctx, &loc); err != nil {
				return
			}
		}
		return nil
	} else if ebs.client != nil {
		_, err = ebs.client.Delete(ctx, &access.DeleteArgs{Locations: locs})
		return
	}
	return errors.New("not implement interface delete")
}

func (ebs *BlobStoreClient) Delete(oeks []proto.ObjExtentKey) (err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ebs-delete", err, bgTime, 1)
	}()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*3)
	defer cancel()

	locs := make([]access.Location, 0)

	for _, oek := range oeks {
		sliceInfos := make([]access.SliceInfo, 0)
		for _, b := range oek.Blobs {
			sliceInfo := access.SliceInfo{
				MinBid: ebsproto.BlobID(b.MinBid),
				Vid:    ebsproto.Vid(b.Vid),
				Count:  uint32(b.Count),
			}
			sliceInfos = append(sliceInfos, sliceInfo)
		}

		loc := access.Location{
			ClusterID: ebsproto.ClusterID(oek.Cid),
			Size:      oek.Size,
			Crc:       oek.Crc,
			CodeMode:  codemode.CodeMode(oek.CodeMode),
			BlobSize:  oek.BlobSize,
			Blobs:     sliceInfos,
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
	if err = ebs.del(ctx, locs); err != nil {
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

type limitedWriter struct {
	buffer []byte
	offset int
}

func (w *limitedWriter) Write(p []byte) (n int, err error) {
	if w.offset+len(p) > len(w.buffer) {
		return 0, errors.New("write over buffer size")
	}
	n = copy(w.buffer[w.offset:], p)
	w.offset += n
	return
}

func (w *limitedWriter) Len() int {
	return w.offset
}
