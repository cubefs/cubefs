package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/taskpool"
)

/*
1. 请求大小
2. 并发深度
3. 上传、下载（比例）
*/
/*
 * READ:
 *   Generate a dump list
 * 	./ebsfio --diskid=10 --op=read --size=1024 --concurrent_count=16
 *
 *   Use dumplist to read data（random）
 * 	./ebsfio --diskid=10 --op=read --size=1024 --concurrent_count=16 --auto_create_chunk=false --task_count=8192  --order=true   --bid_list=./dump_bid_list.txt
 *
 *   Use dumplist to read data（order）
 * 	./ebsfio --diskid=10 --op=read --size=1024 --concurrent_count=16 --auto_create_chunk=false --task_count=8192  --order=false --bid_list=./dump_bid_list.txt
 *
 * WRITE:
 * 	./ebsfio --diskid=10 --op=write --size=1024 --concurrent_count=16
 * DELETE:
 * 	./ebsfio --diskid=10 --vuid=3411 --op=del
 */

const (
	ReadRequest uint8 = iota
	WriteRequest
	MixRequest
	DeleteRequest
)

var opCodeMap = [...]string{
	"read",
	"write",
	"mix",
	"delete",
}

var (
	gBaseVuid uint64 = 1024
	gBaseBid  uint64 = 1024
	gDiskID   proto.DiskID
)

var (
	blobnodeClient = bnapi.New(&bnapi.Config{})
	existBidList   []*userbid
)

type RequestStat struct {
	TotalWriteCnt   uint64 `json:"total_write_cnt"`
	TotalReadCnt    uint64 `json:"total_read_cnt"`
	TotalReadBytes  uint64 `json:"total_read_bytes"`
	TotalWriteBytes uint64 `json:"total_write_bytes"`
	TotalWriteDelay uint64 `json:"total_write_delay"`
	TotalReadDelay  uint64 `json:"total_read_delay"`
}

type userbid struct {
	Disk proto.DiskID `json:"diskID"`
	Vuid uint64       `json:"vuid"`
	Bid  proto.BlobID `json:"bid"`
}

type task struct {
	opcode      uint8
	buff        []byte
	taskCnt     int
	concurrency int

	bidListFile string
	order       bool

	blobnodeUrl     string
	vuid            uint64
	autoCreateChunk bool
}

func (t *task) String() string {
	return fmt.Sprintf("op:%s size:%d taskCnt:%d concurrency:%d",
		opCodeMap[t.opcode], len(t.buff), t.taskCnt, t.concurrency)
}

var (
	stat     RequestStat
	interval int64 // ns
)

func prepareBufferData(size int64) (data []byte, err error) {
	log.Infof("----------- prepare data ------------")
	f, err := os.OpenFile("/dev/urandom", os.O_RDONLY, 0)
	if err != nil {
		log.Fatalf("failed read random, err:%v", err)
	}

	buf := make([]byte, size)
	body := bytes.NewBuffer(buf)

	_, err = io.CopyN(body, f, size)
	if err != nil {
		log.Fatalf("failed io.copy, err:%v", err)
	}

	return buf, nil
}

func genBid() proto.BlobID {
	bid := atomic.AddUint64(&gBaseBid, 1)
	return proto.BlobID(bid)
}

func writeShard(url string, vuid proto.Vuid, bid proto.BlobID, body io.Reader, bodySize int64) (err error) {
	putShardArg := &bnapi.PutShardArgs{
		DiskID: gDiskID,
		Vuid:   vuid,
		Bid:    bid,
		Size:   bodySize,
		Body:   body,
	}

	_, err = blobnodeClient.PutShard(context.TODO(), url, putShardArg)
	if err != nil {
		log.Errorf("Failed put shard, err:%v\n", err)
		return
	}

	return nil
}

func listShards(url string, vuid proto.Vuid) (sis []*bnapi.ShardInfo, err error) {
	ctx := context.TODO()

	startBid := proto.InValidBlobID
	for {
		args := &bnapi.ListShardsArgs{
			DiskID:   gDiskID,
			Vuid:     vuid,
			StartBid: startBid,
		}

		infos, next, err := blobnodeClient.ListShards(ctx, url, args)
		if err != nil {
			log.Errorf("Failed to ListShards, vuid: %v, StartBid:%v, err:%+v\n", vuid, startBid, err)
			return nil, err
		}
		for _, info := range infos {
			ti := *info
			sis = append(sis, &ti)
		}
		startBid = next
		if startBid == proto.InValidBlobID {
			break
		}
	}

	return sis, nil
}

func deleteShard(url string, vuid proto.Vuid, bid proto.BlobID) (err error) {
	ctx := context.TODO()
	delShardArg := &bnapi.DeleteShardArgs{
		DiskID: gDiskID,
		Vuid:   vuid,
		Bid:    bid,
	}

	err = blobnodeClient.MarkDeleteShard(ctx, url, delShardArg)
	if err != nil {
		log.Errorf("Failed mark delete shard(%v), err:%v\n", delShardArg, err)
		return
	}

	err = blobnodeClient.DeleteShard(ctx, url, delShardArg)
	if err != nil {
		log.Errorf("Failed delete shard(%v), err:%v\n", delShardArg, err)
		return
	}

	return nil
}

func resetStat() {
	stat = RequestStat{}
	interval = 0
}

func taskEnd(t time.Time) {
	interval = time.Since(t).Nanoseconds()
}

func showResult() {
	var (
		readqps         float64
		writeqps        float64
		readlatency     uint64
		writelatency    uint64
		readthroughput  float64
		writethroughput float64
	)

	writeqps = (float64(stat.TotalWriteCnt) / float64(interval)) * 1000 * 1000 * 1000
	readqps = (float64(stat.TotalReadCnt) / float64(interval)) * 1000 * 1000 * 1000

	if stat.TotalWriteCnt > 0 {
		writelatency = stat.TotalWriteDelay / 1000 / 1000 / stat.TotalWriteCnt
	}
	if stat.TotalReadCnt > 0 {
		readlatency = stat.TotalReadDelay / 1000 / 1000 / stat.TotalReadCnt
	}

	readthroughput = float64(stat.TotalReadBytes * 1000 * 1000 * 1000 / (uint64(interval)) / 1024 / 1024)
	writethroughput = float64(stat.TotalWriteBytes * 1000 * 1000 * 1000 / (uint64(interval)) / 1024 / 1024)

	fmt.Printf("\n\n")
	fmt.Printf("=== result(%v) interval:%.2f ===\n\n", time.Now(), float64(interval)/1000/1000/1000)
	fmt.Printf("r count:\t\t%v\n", stat.TotalReadCnt)
	fmt.Printf("r get qps:\t\t%.2f\n", readqps)
	fmt.Printf("r latency:\t\t%v ms\n", readlatency)
	fmt.Printf("r through:\t\t%.2f MB/s\n", readthroughput)
	fmt.Printf("\n")
	fmt.Printf("w count:\t\t%v\n", stat.TotalWriteCnt)
	fmt.Printf("w put qps:\t\t%.2f\n", writeqps)
	fmt.Printf("w latency:\t\t%v ms\n", writelatency)
	fmt.Printf("w through:\t\t%.2f MB/s\n", writethroughput)
	fmt.Printf("=== result ===\n\n")

	resetStat()
}

func createChunk(blobnodeUrl string) {
	log.Infof("create new chunk from:%v", blobnodeUrl)

	if gDiskID == proto.InvalidDiskID {
		log.Fatalf("invalid diskid")
	}

	var failed uint64

retry:
	if failed <= 10 {
		atomic.AddUint64(&gBaseVuid, 1)
	} else {
		atomic.AddUint64(&gBaseVuid, 100)
	}

	args := &bnapi.CreateChunkArgs{
		DiskID: gDiskID,
		Vuid:   proto.Vuid(gBaseVuid),
	}

	err := blobnodeClient.CreateChunk(context.TODO(), blobnodeUrl, args)
	if err != nil {
		if rpc.DetectStatusCode(err) == errors.CodeAlreadyExist ||
			rpc.DetectStatusCode(err) == errors.CodeOutOfLimit {
			log.Infof("create args:%v, err:%v", args, err)
			failed++
			goto retry
		}
		log.Fatalf("failed create chunk, err:%v", err)
	}
}

func recordStats(opcode uint8, size int64, latency int64) {
	if opcode == ReadRequest {
		atomic.AddUint64(&stat.TotalReadBytes, uint64(size))
		atomic.AddUint64(&stat.TotalReadDelay, uint64(latency))
		atomic.AddUint64(&stat.TotalReadCnt, 1)
	} else if opcode == WriteRequest {
		atomic.AddUint64(&stat.TotalWriteBytes, uint64(size))
		atomic.AddUint64(&stat.TotalWriteDelay, uint64(latency))
		atomic.AddUint64(&stat.TotalWriteCnt, 1)
	}
}

func RoundOneRead(t *task) {
	pool := taskpool.New(t.concurrency, t.concurrency)
	defer pool.Close()

	wg := sync.WaitGroup{}
	for i := 0; i < t.taskCnt; i++ {
		wg.Add(1)

		iter := i

		pool.Run(func() {
			defer wg.Done()

			idx := iter % len(existBidList)
			user := existBidList[idx]

			getShardArg := &bnapi.GetShardArgs{
				DiskID: gDiskID,
				Vuid:   proto.Vuid(user.Vuid),
				Bid:    proto.BlobID(user.Bid),
			}

			begin := time.Now().UnixNano()

			rc, _, err := blobnodeClient.GetShard(context.TODO(), t.blobnodeUrl, getShardArg)
			if err != nil {
				log.Errorf("Failed read shard, err:%v\n", err)
				return
			}

			n, err := io.Copy(ioutil.Discard, rc)
			if err != nil {
				log.Errorf("Failed get shard: %v", err)
				return
			}

			latency := time.Now().UnixNano() - begin
			recordStats(ReadRequest, n, latency)
		})
	}

	wg.Wait()
}

func RoundOneWrite(t *task) (writeList []*userbid) {
	pool := taskpool.New(t.concurrency, t.concurrency)
	defer pool.Close()

	one := sync.Once{}
	stop := false

	wg := sync.WaitGroup{}
	wg1 := sync.WaitGroup{}
	done := make(chan struct{})
	wlist := make(chan userbid)

	wg1.Add(1)
	go func() {
		defer wg1.Done()
		for {
			select {
			case <-done:
				log.Infof("get done .....")
				return
			case bid := <-wlist:
				writeList = append(writeList, &bid)
			}
		}
	}()

	for i := 0; i < t.taskCnt; i++ {
		wg.Add(1)
		pool.Run(func() {
			defer wg.Done()

			if stop {
				return
			}

			bid := genBid()
			bodySize := int64(len(t.buff))

			body := io.LimitReader(bytes.NewReader(t.buff), bodySize)

			begin := time.Now().UnixNano()
			err := writeShard(t.blobnodeUrl, proto.Vuid(t.vuid), bid, body, bodySize)
			if err != nil && rpc.DetectStatusCode(err) == errors.CodeChunkNoSpace {
				one.Do(func() {
					createChunk(t.blobnodeUrl)
					stop = true
				})
			}
			latency := time.Now().UnixNano() - begin
			recordStats(WriteRequest, bodySize, latency)

			if err == nil {
				wbid := userbid{Disk: gDiskID, Vuid: t.vuid, Bid: bid}
				wlist <- wbid
			}
		})
	}

	wg.Wait()
	close(done)

	wg1.Wait()

	log.Infof("return")

	return
}

func RoundOneMix(t *task) {
	pool := taskpool.New(t.concurrency, t.concurrency)
	defer pool.Close()

	one := sync.Once{}
	stop := false

	wg := sync.WaitGroup{}
	for i := 0; i < t.taskCnt; i++ {
		wg.Add(1)
		pool.Run(func() {
			defer wg.Done()

			if stop {
				return
			}

			bid := genBid()
			bodySize := int64(len(t.buff))

			body := io.LimitReader(bytes.NewReader(t.buff), bodySize)

			begin := time.Now().UnixNano()
			err := writeShard(t.blobnodeUrl, proto.Vuid(t.vuid), bid, body, bodySize)
			if err != nil && rpc.DetectStatusCode(err) == errors.CodeChunkNoSpace {
				one.Do(func() {
					createChunk(t.blobnodeUrl)
					stop = true
				})
			}
			latency := time.Now().UnixNano() - begin

			recordStats(WriteRequest, bodySize, latency)
		})
	}
	wg.Wait()
}

func RoundOneDelete(t *task) {
	pool := taskpool.New(t.concurrency, t.concurrency)
	defer pool.Close()

	shardInfos, err := listShards(t.blobnodeUrl, proto.Vuid(t.vuid))
	if err != nil {
		log.Fatalf("failed list shards, disk:%v vuid:%v\n", gDiskID, t.vuid)
		return
	}

	if len(shardInfos) == 0 {
		log.Warnf("disk:%v vuid:%v empty.\n", gDiskID, t.vuid)
		return
	}

	var pos int32
	pos, endIdx := 0, int32(len(shardInfos))

	wg := sync.WaitGroup{}
	for i := 0; i < t.taskCnt; i++ {
		if pos >= endIdx {
			log.Infof("pos:%v endIdx:%v\n", pos, endIdx)
			return
		}

		shard := shardInfos[pos]
		atomic.AddInt32(&pos, 1)

		wg.Add(1)
		pool.Run(func() {
			defer wg.Done()

			begin := time.Now().UnixNano()

			err := deleteShard(t.blobnodeUrl, proto.Vuid(t.vuid), shard.Bid)
			if err != nil {
				log.Errorf("failed delete disk:%v vuid:%v bid:%v\n", gDiskID, t.vuid, shard.Bid)
				return
			}

			latency := time.Now().UnixNano() - begin
			recordStats(WriteRequest, 0, latency)
		})
	}
	wg.Wait()
}

func RoundOne(t *task) {
	log.Infof("---- start [%s] ----\n", t)
	defer showResult()
	defer taskEnd(time.Now())

	switch t.opcode {
	case ReadRequest:
		RoundOneRead(t)
	case WriteRequest:
		RoundOneWrite(t)
	case MixRequest:
		RoundOneMix(t)
	case DeleteRequest:
		RoundOneDelete(t)
	default:
		panic("not happend")
	}

	log.Infof("---- end [%s] ----\n", t)
}

func savewriteList(path string, wlist []*userbid) {
	fi, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("failed open path:%s, err:%v", path, err)
	}
	defer fi.Close()

	w := bufio.NewWriter(fi)

	for _, l := range wlist {
		data, err := json.Marshal(*l)
		if err != nil {
			log.Fatalf("failed marshal, l:%v, err:%v", l, err)
		}
		fmt.Fprintln(w, string(data))
	}

	w.Flush()
}

func prepareReadData(t *task) {
	var writeList []*userbid
	innerT := *t

	log.Infof("-------- prepare data for read ---------")
	defer log.Infof("-------- prepare data for read done ---------")

	if t.bidListFile != "" {
		log.Infof("read exist path:%s", t.bidListFile)
		writeList = readBidlist(t.bidListFile)
		goto out
	}

	log.Infof("will write some new data")

	innerT.concurrency = 1

	writeList = RoundOneWrite(t)
	if len(writeList) == 0 {
		log.Fatalf("failed prepare read data")
	}

	log.Infof("will save writeList:%v", len(writeList))
	// save to file
	savewriteList("./dump_bid_list.txt", writeList)

out:
	if !t.order && len(writeList) > 0 {
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(writeList), func(i, j int) {
			writeList[i], writeList[j] = writeList[j], writeList[i]
		})
	}

	existBidList = writeList
	log.Infof("bid list: %d", len(existBidList))
}

func readBidlist(path string) []*userbid {
	fi, err := os.Open(path)
	if err != nil {
		log.Errorf("open path<%s> failed, err:%v", path, err)
		return nil
	}
	defer fi.Close()

	bl := make([]*userbid, 0)
	br := bufio.NewReader(fi)
	for {
		var u userbid
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		err = json.Unmarshal(line, &u)
		if err != nil {
			log.Fatalf("unmarshal line:%s, err:%v", line, err)
		}
		bl = append(bl, &u)
	}
	return bl
}

func main() {
	var (
		diskID          uint64
		vuid            uint64
		operation       string
		bidListFile     string
		order           bool
		reqSize         int64
		blobnodeUrl     string
		concurrentCount int
		taskCount       int
		autoCreateChunk bool
	)

	flag.Uint64Var(&diskID, "diskid", 0, "disk id")
	flag.Uint64Var(&vuid, "vuid", 0, "vuid")
	flag.Int64Var(&reqSize, "size", 4096, "request size")
	flag.IntVar(&concurrentCount, "concurrent_count", 32, "concurrent count")
	flag.IntVar(&taskCount, "task_count", 1024, "total task count")
	flag.BoolVar(&order, "order", true, "order or random? The default is random request")
	flag.BoolVar(&autoCreateChunk, "auto_create_chunk", true, "auto create chunk")
	flag.StringVar(&operation, "op", "", "operation, read、write、read_write")
	flag.StringVar(&bidListFile, "bid_list", "", "")
	flag.StringVar(&blobnodeUrl, "url", "http://127.0.0.1:8899", "blobnode url")
	flag.Parse()

	if diskID == 0 {
		log.Fatalf("invalid diskid: %v", diskID)
	}
	gDiskID = proto.DiskID(diskID)

	var opcode uint8
	switch operation {
	case "read":
		opcode = ReadRequest
	case "write":
		opcode = WriteRequest
	case "mix":
		opcode = MixRequest
	case "del":
		opcode = DeleteRequest
	default:
		log.Fatal("invalid operations")
	}

	if (vuid == 0 && !autoCreateChunk && opcode != ReadRequest) || reqSize == 0 {
		log.Fatal("invalid params")
	}
	gBaseVuid = vuid

	buffData, err := prepareBufferData(reqSize)
	if err != nil {
		log.Fatalf("failed prepare, err:%v", err)
	}

	if opcode != DeleteRequest {
		if autoCreateChunk {
			createChunk(blobnodeUrl)
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Duration(1) * time.Second)
		defer ticker.Stop()

		for {
			task := &task{
				opcode:          opcode,
				buff:            buffData,
				taskCnt:         taskCount,
				concurrency:     concurrentCount,
				blobnodeUrl:     blobnodeUrl,
				vuid:            gBaseVuid,
				autoCreateChunk: autoCreateChunk,
				bidListFile:     bidListFile,
				order:           order,
			}

			if opcode == ReadRequest {
				prepareReadData(task)
			}

			<-ticker.C
			RoundOne(task)
		}
	}()

	// wait for signal
	wg.Wait()

	log.Infof("---------- done -----------")
}
