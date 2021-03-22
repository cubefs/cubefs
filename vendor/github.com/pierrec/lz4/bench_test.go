package lz4_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"

	"github.com/pierrec/lz4"
)

func BenchmarkCompress(b *testing.B) {
	buf := make([]byte, len(pg1661))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = lz4.CompressBlock(pg1661, buf, nil)
	}
}

func BenchmarkCompressRandom(b *testing.B) {
	buf := make([]byte, len(randomLZ4))

	b.ReportAllocs()
	b.SetBytes(int64(len(random)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = lz4.CompressBlock(random, buf, nil)
	}
}

func BenchmarkCompressHC(b *testing.B) {
	buf := make([]byte, len(pg1661))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = lz4.CompressBlockHC(pg1661, buf, 16)
	}
}

func BenchmarkUncompress(b *testing.B) {
	buf := make([]byte, len(pg1661))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = lz4.UncompressBlock(pg1661LZ4, buf)
	}
}

func mustLoadFile(f string) []byte {
	b, err := ioutil.ReadFile(f)
	if err != nil {
		panic(err)
	}
	return b
}

var (
	pg1661    = mustLoadFile("testdata/pg1661.txt")
	digits    = mustLoadFile("testdata/e.txt")
	twain     = mustLoadFile("testdata/Mark.Twain-Tom.Sawyer.txt")
	random    = mustLoadFile("testdata/random.data")
	pg1661LZ4 = mustLoadFile("testdata/pg1661.txt.lz4")
	digitsLZ4 = mustLoadFile("testdata/e.txt.lz4")
	twainLZ4  = mustLoadFile("testdata/Mark.Twain-Tom.Sawyer.txt.lz4")
	randomLZ4 = mustLoadFile("testdata/random.data.lz4")
)

func benchmarkUncompress(b *testing.B, compressed []byte) {
	r := bytes.NewReader(compressed)
	zr := lz4.NewReader(r)

	// Determine the uncompressed size of testfile.
	uncompressedSize, err := io.Copy(ioutil.Discard, zr)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(uncompressedSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.Reset(compressed)
		zr.Reset(r)
		_, _ = io.Copy(ioutil.Discard, zr)
	}
}

func BenchmarkUncompressPg1661(b *testing.B) { benchmarkUncompress(b, pg1661LZ4) }
func BenchmarkUncompressDigits(b *testing.B) { benchmarkUncompress(b, digitsLZ4) }
func BenchmarkUncompressTwain(b *testing.B)  { benchmarkUncompress(b, twainLZ4) }
func BenchmarkUncompressRand(b *testing.B)   { benchmarkUncompress(b, randomLZ4) }

func benchmarkSkipBytes(b *testing.B, compressed []byte) {
	r := bytes.NewReader(compressed)
	zr := lz4.NewReader(r)

	// Determine the uncompressed size of testfile.
	uncompressedSize, err := io.Copy(ioutil.Discard, zr)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(uncompressedSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.Reset(compressed)
		zr.Reset(r)
		zr.Seek(uncompressedSize, io.SeekCurrent)
		_, _ = io.Copy(ioutil.Discard, zr)
	}
}

func BenchmarkSkipBytesPg1661(b *testing.B) { benchmarkSkipBytes(b, pg1661LZ4) }
func BenchmarkSkipBytesDigits(b *testing.B) { benchmarkSkipBytes(b, digitsLZ4) }
func BenchmarkSkipBytesTwain(b *testing.B)  { benchmarkSkipBytes(b, twainLZ4) }
func BenchmarkSkipBytesRand(b *testing.B)   { benchmarkSkipBytes(b, randomLZ4) }

func benchmarkCompress(b *testing.B, uncompressed []byte) {
	w := bytes.NewBuffer(nil)
	zw := lz4.NewWriter(w)
	r := bytes.NewReader(uncompressed)

	// Determine the compressed size of testfile.
	compressedSize, err := io.Copy(zw, r)
	if err != nil {
		b.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(compressedSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r.Reset(uncompressed)
		zw.Reset(w)
		_, _ = io.Copy(zw, r)
	}
}

func BenchmarkCompressPg1661(b *testing.B) { benchmarkCompress(b, pg1661) }
func BenchmarkCompressDigits(b *testing.B) { benchmarkCompress(b, digits) }
func BenchmarkCompressTwain(b *testing.B)  { benchmarkCompress(b, twain) }
func BenchmarkCompressRand(b *testing.B)   { benchmarkCompress(b, random) }

// Benchmark to check reallocations upon Reset().
// See issue https://github.com/pierrec/lz4/issues/52.
func BenchmarkWriterReset(b *testing.B) {
	b.ReportAllocs()

	zw := lz4.NewWriter(nil)
	src := mustLoadFile("testdata/gettysburg.txt")
	var buf bytes.Buffer

	for n := 0; n < b.N; n++ {
		buf.Reset()
		zw.Reset(&buf)

		_, _ = zw.Write(src)
		_ = zw.Close()
	}
}
