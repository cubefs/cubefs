package blobstore

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/stat"

	"github.com/chubaofs/blobstore/api/access"
	"github.com/chubaofs/blobstore/common/codemode"
	ebsproto "github.com/chubaofs/blobstore/common/proto"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
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
	var (
		body io.ReadCloser
	)
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
		return 0, nil
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

func (ebs *BlobStoreClient) Write(ctx context.Context, volName string, data []byte) (location access.Location, err error) {
	bgTime := stat.BeginStat()
	defer func() {
		stat.EndStat("ebs-write", err, bgTime, 1)
	}()

	requestId := uuid.New().String()
	log.LogDebugf("TRACE Ebs Write Enter,requestId(%v)  len(%v)", requestId, len(data))
	start := time.Now()
	size := int64(len(data))
	hashAlg := access.HashAlgMD5
	ctx = access.WithRequestID(ctx, requestId)
	metric := exporter.NewTPCnt(createOPMetric(data, "ebswrite"))
	defer func() {
		metric.SetWithLabels(err, map[string]string{exporter.Vol: volName})
	}()

	for i := 0; i < MaxRetryTimes; i++ {
		location, _, err = ebs.client.Put(ctx, &access.PutArgs{
			Size:   size,
			Hashes: hashAlg,
			Body:   bytes.NewReader(data),
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
