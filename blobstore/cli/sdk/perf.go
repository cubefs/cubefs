// Copyright 2024 The CubeFS Authors.
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

package sdk

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/desertbit/grumble"
	"golang.org/x/time/rate"

	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
)

func addCmdPerfTest(cmd *grumble.Command) {
	command := &grumble.Command{
		Name:     "perf",
		Help:     "perf test(put/get/del many blob)",
		LongHelp: "blobstore access sdk tools",
	}
	cmd.AddCommand(command)

	command.AddCommand(&grumble.Command{
		Name: "put",
		Help: "put many blob, for perf test or long-term stability test",
		Run:  perfPut,
		Flags: func(f *grumble.Flags) {
			perfFlags(f)
			f.String("s", "source_file", "4k.log", "source data: body file path")
			f.String("p", "location_path", "location.json", "save location file path")
			f.String("d", "data", "", "src_data: raw data body")
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "get",
		Help: "get many blob, for perf test ",
		Run:  perfGet,
		Flags: func(f *grumble.Flags) {
			perfFlags(f)
			f.String("s", "save_file", "read.json", "save read result data file path")
		},
	})

	command.AddCommand(&grumble.Command{
		Name: "del",
		Help: "del many blob, for perf test ",
		Run:  perfDel,
		Flags: func(f *grumble.Flags) {
			perfFlags(f)
			f.String("s", "save_file", "del.json", "save del blob result file path")
		},
	})
}

type ClientRet struct {
	BlobName string
	NameByte []byte
	Size     uint64
	Err      error
	Data     string        `json:"Data,omitempty"`
	rc       io.ReadCloser `json:"-"`
}

func perfPut(c *grumble.Context) (err error) {
	// 1. parse args, well need fill two field
	// sdk perf put --args='{"CodeMode":11,"NeedSeal":true}' -b=20100 -m=100 -c=1 --limit=2 -d="testData_"
	args, err := common.UnmarshalAny[acapi.PutBlobArgs]([]byte(c.Flags.String("args")))
	if err != nil {
		return fmt.Errorf("invalid (%s) %+v\n", c.Flags.String("args"), err)
	}
	fmt.Printf("put blob args json=%s\n", common.RawString(args))

	// 2. get original source data, parameter, client
	max, lmt, con, begin, round, prefix, locPath, data, err := getPerfPutArg(c)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(locPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s : %+v", locPath, err)
	}
	defer file.Close()
	fmt.Printf("put many blob, maxCount:%d, limitQps:%d, concurrence:%d, beginIdx:%d, round:%d\n", max, lmt, con, begin, round)

	client, err := getOrNewClient(c)
	if err != nil {
		return fmt.Errorf("get or new client fail: %+v", err)
	}

	succCnt, failCnt := int64(0), int64(0)
	// 3. put many blob
	workFunc := func(i, j int, ctx context.Context, finishCh chan<- ClientRet) {
		args1 := args
		args1.BlobName = []byte(prefix + strconv.Itoa(i+j+begin))
		go func(args1 acapi.PutBlobArgs) {
			args1.Size, args1.Body = getPutBody(data, args1.BlobName)
			_, err1 := client.PutBlob(ctx, &args1)
			finishCh <- ClientRet{
				BlobName: string(args1.BlobName),
				NameByte: args1.BlobName,
				Size:     args1.Size,
				Err:      err1,
			}
		}(args1)
	}
	doneFunc := func(finishCh <-chan ClientRet) {
		ret := <-finishCh
		defer common.NewEncoder(file).Encode(ret)
		if ret.Err != nil {
			atomic.AddInt64(&failCnt, 1)
			fmt.Printf("----fail to put, result: %+v\n", ret)
			return
		}
		atomic.AddInt64(&succCnt, 1)
		// fmt.Printf("----success to put: %+v\n", ret)
	}
	closeCh := make(chan bool)
	go showProgress(&succCnt, &failCnt, closeCh)

	for i := 0; i < round; i++ {
		doManyBlob(max, lmt, con, begin, workFunc, doneFunc)
		if i+1 < round {
			fmt.Printf("----do round:%d done, will sleep a while\n", i)
			time.Sleep(time.Minute)
		}
	}

	fmt.Println("----perf end----")
	closeCh <- true
	os.Exit(0)
	return nil
}

func perfGet(c *grumble.Context) (err error) {
	// 1. parse args, well need fill one field
	// sdk perf get --args='{"ClusterID":10000,"ReadSize":19}' -b=19900 -m=2 -c=1 --limit=2 -r=1
	args, err := common.UnmarshalAny[acapi.GetBlobArgs]([]byte(c.Flags.String("args")))
	if err != nil {
		return fmt.Errorf("invalid (%s) %+v\n", c.Flags.String("args"), err)
	}
	fmt.Printf("get blob args json=%s\n", common.RawString(args))

	// 2. get perf parameter, client
	max, lmt, con, begin, round, prefix, path := getPerfGetArg(c)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s : %+v", path, err)
	}
	defer file.Close()
	fmt.Printf("get many blob, maxCount:%d, limitQps:%d, concurrence:%d, beginIdx:%d, round:%d\n", max, lmt, con, begin, round)

	client, err := getOrNewClient(c)
	if err != nil {
		return fmt.Errorf("get or new client fail: %+v", err)
	}

	succCnt, failCnt := int64(0), int64(0)
	// 3. do perf get, many blob
	workFunc := func(i, j int, ctx context.Context, finishCh chan<- ClientRet) {
		args1 := args
		args1.BlobName = []byte(prefix + strconv.Itoa(i+j+begin))
		go func(args1 acapi.GetBlobArgs) {
			rc, err1 := client.GetBlob(ctx, &args1)
			finishCh <- ClientRet{
				BlobName: string(args1.BlobName),
				NameByte: args1.BlobName,
				Size:     args1.ReadSize,
				Err:      err1,
				rc:       rc,
			}
		}(args1)
	}
	doneFunc := func(finishCh <-chan ClientRet) {
		ret := <-finishCh
		defer common.NewEncoder(file).Encode(ret)

		if ret.Err != nil {
			atomic.AddInt64(&failCnt, 1)
			fmt.Printf("----fail to get, result: %+v\n", ret)
			return
		}
		defer ret.rc.Close()

		data, err1 := io.ReadAll(ret.rc)
		if err1 != nil {
			atomic.AddInt64(&failCnt, 1)
			ret.Err = err1
			fmt.Printf("----fail to get read, result: %+v, err: %+v\n", ret, err1)
			return
		}
		ret.Data = string(data)
		atomic.AddInt64(&succCnt, 1)
		// fmt.Printf("----success to get blob:%s, read:%s\n", ret.BlobName, ret.Data)
	}
	closeCh := make(chan bool)
	go showProgress(&succCnt, &failCnt, closeCh)

	for i := 0; i < round; i++ {
		doManyBlob(max, lmt, con, begin, workFunc, doneFunc)
		if i+1 < round {
			fmt.Printf("----do round:%d done, will sleep a while\n", i)
			time.Sleep(time.Minute)
		}
	}
	fmt.Println("----perf end----")
	closeCh <- true
	os.Exit(0)
	return nil
}

func perfDel(c *grumble.Context) (err error) {
	// 1. parse args, well need fill one field
	// sdk perf del --args='{"ClusterID":10000}' -b=20100 -m=2 -c=1 -l=2 -r=1
	args, err := common.UnmarshalAny[acapi.DelBlobArgs]([]byte(c.Flags.String("args")))
	if err != nil {
		return fmt.Errorf("invalid (%s) %+v\n", c.Flags.String("args"), err)
	}
	fmt.Printf("del blob args json=%s\n", common.RawString(args))

	// 2. del perf parameter, client
	max, lmt, con, begin, round, prefix, path := getPerfDelArg(c)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open file %s : %+v", path, err)
	}
	defer file.Close()
	fmt.Printf("del many blob, maxCount:%d, limitQps:%d, concurrence:%d, beginIdx:%d, round:%d\n", max, lmt, con, begin, round)

	client, err := getOrNewClient(c)
	if err != nil {
		return fmt.Errorf("get or new client fail: %+v", err)
	}

	succCnt, failCnt := int64(0), int64(0)
	// 3. do perf del, many blob
	workFunc := func(i, j int, ctx context.Context, finishCh chan<- ClientRet) {
		args1 := args
		args1.BlobName = []byte(prefix + strconv.Itoa(i+j+begin))
		go func(args1 acapi.DelBlobArgs) {
			err1 := client.DeleteBlob(ctx, &args1)
			finishCh <- ClientRet{
				BlobName: string(args1.BlobName),
				NameByte: args1.BlobName,
				Err:      err1,
			}
		}(args1)
	}
	doneFunc := func(finishCh <-chan ClientRet) {
		ret := <-finishCh
		defer common.NewEncoder(file).Encode(ret)
		if ret.Err != nil {
			atomic.AddInt64(&failCnt, 1)
			fmt.Printf("----fail to del, result: %+v\n", ret)
			return
		}
		atomic.AddInt64(&succCnt, 1)
		// fmt.Printf("----success to del, result: %+v\n", ret)
	}
	closeCh := make(chan bool)
	go showProgress(&succCnt, &failCnt, closeCh)

	for i := 0; i < round; i++ {
		doManyBlob(max, lmt, con, begin, workFunc, doneFunc)
		if i+1 < round {
			fmt.Printf("----do round:%d done, will sleep a while\n", i)
			time.Sleep(time.Minute)
		}
	}
	fmt.Println("----perf end----")
	closeCh <- true
	os.Exit(0)
	return nil
}

func perfFlags(f *grumble.Flags) {
	f.String("a", "args", "", "raw request args string by [json]")
	f.String("f", "config", "sdk.conf", "config path(json type file)")
	f.String("", "prefix", "", "blob name prefix, e.g. blob_")
	f.Int("m", "max_count", 10000, "max number of call put blob")
	f.Int("l", "limit", 60, "qps limiter, put count at per minute")
	f.Int("c", "concurrence", 10, "put blob concurrence")
	f.Int("b", "begin_index", 0, "blob name, begin index")
	f.Int("r", "round_count", 1, "total read round count")
	f.Int("", "prefix_size", 256, "blob name prefix size")
}

func getCommonPerfArg(c *grumble.Context) (int, int, int, int, int, string) {
	max := c.Flags.Int("max_count")
	lmt := c.Flags.Int("limit")
	con := c.Flags.Int("concurrence")
	begin := c.Flags.Int("begin_index")
	round := c.Flags.Int("round_count")

	prefix := c.Flags.String("prefix")
	if prefix == "" {
		data := []byte("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
		size := c.Flags.Int("prefix_size")
		preData := make([]byte, size+1)
		for i := 0; i < size; i++ {
			preData[i] = data[i%len(data)]
		}
		preData[size] = '_'
		prefix = string(preData)
	}

	return max, lmt, con, begin, round, prefix
}

func getPerfPutArg(c *grumble.Context) (int, int, int, int, int, string, string, []byte, error) {
	max, lmt, con, begin, round, prefix := getCommonPerfArg(c)
	filePath := c.Flags.String("source_file")
	raw := c.Flags.String("data")
	locPath := c.Flags.String("location_path")

	data, err := []byte(nil), error(nil)
	if len(raw) > 0 {
		data = []byte(raw)
	} else {
		data, err = ioutil.ReadFile(filePath)
		if err != nil {
			fmt.Println("----Error reading file: ", err)
			return 0, 0, 0, 0, 0, "", "", nil, err
		}
	}

	return max, lmt, con, begin, round, prefix, locPath, data, nil
}

func getPerfGetArg(c *grumble.Context) (int, int, int, int, int, string, string) {
	max, lmt, con, begin, round, prefix := getCommonPerfArg(c)
	filePath := c.Flags.String("save_file")
	return max, lmt, con, begin, round, prefix, filePath
}

func getPerfDelArg(c *grumble.Context) (int, int, int, int, int, string, string) {
	max, lmt, con, begin, round, prefix := getCommonPerfArg(c)
	filePath := c.Flags.String("save_file")
	return max, lmt, con, begin, round, prefix, filePath
}

func doManyBlob(max, lmt, con, begin int, workFunc func(int, int, context.Context, chan<- ClientRet), doneF func(finishCh <-chan ClientRet)) (err error) {
	limiter := rate.NewLimiter(rate.Limit(lmt), lmt)
	finishCh := make(chan ClientRet, con)

	for i := 0; i < max; i += con {
		ctx := common.CmdContext()
		err = limiter.Wait(ctx)
		if err != nil {
			fmt.Println("----exceed limiter, err: ", err)
			return err
		}

		for j := 0; j < con && i+j < max; j++ {
			workFunc(i, j, ctx, finishCh)
		}

		// if i%lmt == 0 {
		//	fmt.Println("----do blob index from:", i+begin)
		// }

		for j := 0; j < con && i+j < max; j++ {
			doneF(finishCh)
		}
	}

	return nil
}

// todo: use random put body
// func getRandomPutBody(originalData []byte, blobName []byte) (uint64, *bytes.Reader) {
//	const bufferSize = 1024 // 1KB
//	buffer := make([]byte, len(originalData))
//	copy(buffer, originalData)
//	// rand.Seed(time.Now().UnixNano())
//	for i := 0; i < len(buffer); i += bufferSize {
//		offset := i + rand.Intn(bufferSize)
//		if offset >= len(buffer) {
//			offset = len(buffer) - 1
//		}
//
//		buffer[offset] = buffer[offset] + byte(1)
//	}
//	return uint64(len(buffer)), bytes.NewReader(buffer)
// }

func getPutBody(originalData []byte, blobName []byte) (uint64, *bytes.Reader) {
	buffer := append(originalData, blobName...)
	return uint64(len(buffer)), bytes.NewReader(buffer)
}

func showProgress(succCnt *int64, failCnt *int64, closeCh <-chan bool) {
	tk := time.NewTicker(1 * time.Minute)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			sCnt := atomic.LoadInt64(succCnt)
			fCnt := atomic.LoadInt64(failCnt)
			now := time.Now().Format("2006-01-02 15:04:05")
			fmt.Printf("progress: totalCount: %d, success: %d, fail: %d, time: %s\n", sCnt+fCnt, sCnt, fCnt, now)
		case <-closeCh:
			return
		}
	}
}
