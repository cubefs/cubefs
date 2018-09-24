package util

import (
	"math/rand"
	"testing"
	"time"
)

// Implementations
func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}
func RandStringBytesMask(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; {
		if idx := int(rand.Int63() & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i++
		}
	}
	return string(b)
}
func RandStringBytesMaskImpr(n int) (b []byte) {
	b = make([]byte, n)
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
	return
}

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

// Benchmark functions
const n = 16

func BenchmarkRunes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RandStringRunes(n)
	}
}
func BenchmarkBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RandStringBytes(n)
	}
}
func BenchmarkBytesRmndr(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RandStringBytesRmndr(n)
	}
}
func BenchmarkBytesMask(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RandStringBytesMask(n)
	}
}
func BenchmarkBytesMaskImpr(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RandStringBytesMaskImpr(n)
	}
}
func BenchmarkBytesMaskImprSrc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		RandStringBytesMaskImprSrc(n)
	}
}
