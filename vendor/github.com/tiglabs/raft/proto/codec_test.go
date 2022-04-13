package proto

import (
	"math/rand"
	"os"
	"testing"
	"time"
)


const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)
var src = rand.NewSource(time.Now().UnixNano())
func RandStringBytesMaskImprSrc(n int) []byte {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return b
}
func BenchmarkEntry_Encode(b *testing.B) {
	b.ResetTimer()
	fp,err:=os.OpenFile("/data1/1.txt",os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_TRUNC,0777)
	if err!=nil {
		b.Fatalf("cannot open fp")
	}
	data:=RandStringBytesMaskImprSrc(16*1024)
	for i := 0; i < b.N; i++ {
		entry := &Entry{
			Type:      2,
			Term:      3,
			Index:     1,
			Data:      data,
			ctx:       nil,
			RefCnt:    0,
			OrgRefCnt: 0,
		}
		err:=entry.Encode(fp)
		if err!=nil {
			b.Fatalf("write to /data1/1.txt faileed %v",err)
		}
	}
	b.ReportAllocs()
	fp.Close()
}