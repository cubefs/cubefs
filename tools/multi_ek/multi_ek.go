package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	diskv1 "github.com/shirou/gopsutil/disk"
	"io"
	"math/rand"
	"os"
	"path"
	"syscall"
	"time"
)

var (
	partitionNotFoundError = errors.NewErrorf("partition not found")
	illegalPathError       = errors.NewErrorf("illegal disk path")
)

var mountP = flag.String("path", "", "mount point")
var executeMin = flag.Int("minute", 10, "execute time/minutes")
var testMountPath string
var testLocalPath string

const (
	rootPath = "/"
	baseStr  = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func randBytes(length int) []byte {
	bytes := []byte(baseStr)
	result := make([]byte, 0)
	rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return result
}

func init() {
	flag.Parse()
	if *mountP == "" {
		panic("path must be set")
	}
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(*mountP, &fs)
	if fs.Type != diskv1.FUSE_SUPER_MAGIC {
		panic(fmt.Sprintf("fs type(%v) is not fuse(%v)", fs.Type, diskv1.FUSE_SUPER_MAGIC))
	}
	testMountPath = path.Join(*mountP, "multi_ek_test")
	testLocalPath = "/tmp/cfs_multi_ek_test"
	err = os.RemoveAll(testMountPath)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	err = os.RemoveAll(testLocalPath)
	os.Mkdir(testLocalPath, 0666)
	os.Mkdir(testMountPath, 0666)
}

func main() {
	fileNum := 20
	hashMap := make(map[string]string, 0)
	fileCh := make(chan string, 4)
	for i := 0; i < fileNum; i++ {
		file := generateRandomFile(128 * 1024 * 1024)
		name := fmt.Sprintf("%v/%v_%v", testMountPath, "test", i)
		localName := fmt.Sprintf("%v/%v_%v", testLocalPath, "test", i)
		fd, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		defer fd.Close()
		localFd, err := os.OpenFile(localName, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		defer localFd.Close()
		hash := md5.New()
		rand.New(rand.NewSource(time.Now().UnixNano()))
		//skip writing will generate multi extent keys for a single inode
		rand.Shuffle(len(file), func(i, j int) { file[i], file[j] = file[j], file[i] })
		for _, data := range file {
			n, e := fd.WriteAt(data.Data, data.Offset)
			if e != nil {
				panic(e)
			}
			if n != len(data.Data) {
				panic(fmt.Sprintf("illegal write, n:%v, bytes:%v", n, len(data.Data)))
			}
			n, e = localFd.WriteAt(data.Data, data.Offset)
			if e != nil {
				panic(e)
			}
			if n != len(data.Data) {
				panic(fmt.Sprintf("illegal write, n:%v, bytes:%v", n, len(data.Data)))
			}
		}
		io.Copy(hash, localFd)
		hashMap[name] = hex.EncodeToString(hash.Sum(nil))
		md5Fd, _ := os.Create(fmt.Sprintf("%v.md5", localName))
		defer md5Fd.Close()
		md5Fd.Write([]byte(hashMap[name]))
	}

	var read = func(name string) {
		fd, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		defer fd.Close()
		hash := md5.New()
		_, _ = io.Copy(hash, fd)
		if actual := hex.EncodeToString(hash.Sum(nil)); actual != hashMap[name] {
			panic(fmt.Sprintf("file: %v, invalid hash code, expect: %v, actrual: %v", name, hashMap[name], actual))
		} else {
			fmt.Printf("file check success:%v hash:%v\n", name, hashMap[name])
		}
	}

	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case file := <-fileCh:
					if file == "" {
						return
					}
					read(file)
				}
			}
		}()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*time.Duration(*executeMin))
	defer cancel()
	var count int
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("all check success")
			close(fileCh)
			return
		default:
			for i := 0; i < fileNum; i++ {
				name := fmt.Sprintf("%v/%v_%v", testMountPath, "test", i)
				fileCh <- name
			}
			count++
			fmt.Printf("check times: %v\n", count)
		}
		time.Sleep(time.Second)
	}

}

type packet struct {
	Data   []byte
	Offset int64
}

func generateRandomFile(fileSize int64) (p []packet) {
	p = make([]packet, 0)
	bufSlice := []int{4 * 1024, 16 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024}
	var offset int64
	//init test data
	for {
		if offset > fileSize {
			break
		}
		rand.New(rand.NewSource(time.Now().UnixNano()))
		index := rand.Intn(len(bufSlice))
		p = append(p, packet{
			Data:   randBytes(bufSlice[index]),
			Offset: offset,
		})
		offset += int64(bufSlice[index])
	}
	return
}
