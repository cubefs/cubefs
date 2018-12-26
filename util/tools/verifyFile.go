package tools

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	fileCnt    = flag.Int("count", 10000000, "create file count")
	para       = flag.Int("para", 300, "parallel processes")
	rootPath   = flag.String("root", "/mnt/intest", "rootPath")
	fileType   = flag.Bool("isdir", false, "create is dir")
	prefix     = flag.String("prefix", "1", "default prefix")
	random     = flag.Bool("random", false, "random write")
	currentCnt uint64
)

type VerifyInfo struct {
	Name          string
	Offset        int64
	Size          int64
	WriteCrc      uint32
	ReadCrc       uint32
	WriteContents string
	ReadContents  string
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	var wg sync.WaitGroup
	start := time.Now().Unix()
	for i := 0; i < *para; i++ {
		wg.Add(1)
		go create(&wg)
	}
	wg.Wait()
	end := time.Now().Unix()
	fmt.Println(fmt.Sprintf("create file or dir %v cost %v s", *fileCnt, end-start))

}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytesMaskImpr(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

func write(name string) (verifyInfo []*VerifyInfo, err error) {
	fp, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	allData := make([]byte, 0)
	verifyInfo = make([]*VerifyInfo, 0)
	var offset int64
	for i := 0; i < 10000; i++ {
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(1024)
		if n <= 1 {
			n = 2
		}
		v := new(VerifyInfo)
		v.Name = name
		v.Offset = offset
		v.Size = int64(n)
		data := []byte(RandStringBytesMaskImpr(n))
		v.WriteContents = string(data)
		v.WriteCrc = crc32.ChecksumIEEE(data)
		if writeCount, err := fp.WriteAt(data, offset); err != nil || writeCount != len(data) {
			return nil, fmt.Errorf("write: err(%v) len(%v) writeCount(%v)", err, len(data), writeCount)
		}
		verifyInfo = append(verifyInfo, v)
		allData = append(allData, data...)
		offset += int64(n)
	}

	crc := crc32.ChecksumIEEE(allData)
	crcBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBuf, crc)
	if _, err := fp.WriteAt(crcBuf, offset); err != nil {
		return nil, err
	}

	err = syscall.Fsync(int(fp.Fd()))
	if err != nil {
		return nil, err
	}
	return verifyInfo, fp.Close()
}

func readVerify(verifyInfo []*VerifyInfo) {
	fp, err := os.Open(verifyInfo[0].Name)
	if err != nil {
		return
	}
	defer fp.Close()

	for _, v := range verifyInfo {
		data := make([]byte, v.Size)
		_, err := fp.ReadAt(data, v.Offset)
		if err != nil {
			mesg, _ := json.Marshal(v)
			a := string(mesg) + err.Error()
			panic(string(a))
		}
		actualCrc := crc32.ChecksumIEEE(data)
		if actualCrc != v.WriteCrc {
			v.ReadContents = string(data)
			v.ReadCrc = uint32(actualCrc)
			mesg, _ := json.Marshal(v)
			fmt.Println(string(mesg))
			panic(string(mesg))
		}
	}

}

func read(name string) (err error) {
	fp, err := os.Open(name)
	if err != nil {
		return err
	}
	defer fp.Close()
	stat, err := fp.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	data := make([]byte, size)
	var n int
	if n, err = fp.Read(data); err != nil {
		return err
	}

	writeCrc := binary.BigEndian.Uint32(data[size-4:])
	actualCrc := crc32.ChecksumIEEE(data[:size-4])
	if writeCrc != actualCrc {
		err = fmt.Errorf("name(%v) crc not match actualCrc(%v) writeCrc(%v) size(%v) readn(%v)", name, actualCrc, writeCrc, size, n)
		return err
	}

	return nil
}

func verifyWriteAndRead(filename string) {
	verifyInfo, err := write(filename)
	if err != nil {
		err = fmt.Errorf("filename %v write %v error", filename, err)
		fmt.Println(err.Error())
	}

	err = read(filename)
	if err != nil {
		err = fmt.Errorf("filename %v read %v error", filename, err)
		fmt.Println(err.Error())
		readVerify(verifyInfo)
	}
}

func create(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		currentFileIndex := atomic.AddUint64(&currentCnt, 1)
		if currentFileIndex > uint64(*fileCnt) {
			break
		}
		filename := fmt.Sprintf("%v/%v_%v", *rootPath, *prefix, currentFileIndex)
		var (
			err error
		)
		if *fileType {
			err = os.MkdirAll(filename, 0755)
		} else {
			verifyWriteAndRead(filename)
			if *random {
				verifyWriteAndRead(filename)
			}
		}

		if err != nil {
			fmt.Println(fmt.Sprintf("create file or dir %v error %v ", filename, err))
			panic(err)
		}
	}
}
