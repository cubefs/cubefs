package lz4_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/pierrec/lz4"
)

func TestWriter(t *testing.T) {
	goldenFiles := []string{
		"testdata/e.txt",
		"testdata/gettysburg.txt",
		"testdata/Mark.Twain-Tom.Sawyer.txt",
		"testdata/Mark.Twain-Tom.Sawyer_long.txt",
		"testdata/pg1661.txt",
		"testdata/pi.txt",
		"testdata/random.data",
		"testdata/repeat.txt",
	}

	for _, fname := range goldenFiles {
		for _, size := range []int{0, 4} {
			for _, header := range []lz4.Header{
				{}, // Default header.
				{BlockChecksum: true},
				{NoChecksum: true},
				{BlockMaxSize: 64 << 10}, // 64Kb
				{CompressionLevel: 10},
				{Size: 123},
			} {
				label := fmt.Sprintf("%s/%s", fname, header)
				t.Run(label, func(t *testing.T) {
					fname := fname
					header := header
					t.Parallel()

					raw, err := ioutil.ReadFile(fname)
					if err != nil {
						t.Fatal(err)
					}
					r := bytes.NewReader(raw)

					// Compress.
					var zout bytes.Buffer
					zw := lz4.NewWriter(&zout)
					zw.Header = header
					zw.WithConcurrency(size)
					_, err = io.Copy(zw, r)
					if err != nil {
						t.Fatal(err)
					}
					err = zw.Close()
					if err != nil {
						t.Fatal(err)
					}

					// Uncompress.
					var out bytes.Buffer
					zr := lz4.NewReader(&zout)
					n, err := io.Copy(&out, zr)
					if err != nil {
						t.Fatal(err)
					}

					// The uncompressed data must be the same as the initial input.
					if got, want := int(n), len(raw); got != want {
						t.Errorf("invalid sizes: got %d; want %d", got, want)
					}

					if got, want := out.Bytes(), raw; !reflect.DeepEqual(got, want) {
						t.Fatal("uncompressed data does not match original")
					}
				})
			}
		}
	}
}

func TestIssue41(t *testing.T) {
	r, w := io.Pipe()
	zw := lz4.NewWriter(w)
	zr := lz4.NewReader(r)

	data := "x"
	go func() {
		_, _ = fmt.Fprint(zw, data)
		_ = zw.Flush()
		_ = zw.Close()
		_ = w.Close()
	}()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(zr)
	if got, want := buf.String(), data; got != want {
		t.Fatal("uncompressed data does not match original")
	}
}

func TestIssue43(t *testing.T) {
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		f, err := os.Open("testdata/issue43.data")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		zw := lz4.NewWriter(w)
		defer zw.Close()

		_, err = io.Copy(zw, f)
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err := io.Copy(ioutil.Discard, lz4.NewReader(r))
	if err != nil {
		t.Fatal(err)
	}
}

func TestIssue51(t *testing.T) {
	data, err := ioutil.ReadFile("testdata/issue51.data")
	if err != nil {
		t.Fatal(err)
	}

	zbuf := make([]byte, 8192)
	ht := make([]int, htSize)

	n, err := lz4.CompressBlock(data, zbuf, ht)
	if err != nil {
		t.Fatal(err)
	}
	zbuf = zbuf[:n]

	buf := make([]byte, 8192)
	n, err = lz4.UncompressBlock(zbuf, buf)
	if err != nil {
		t.Fatal(err)
	}
	buf = buf[:n]
	if !bytes.Equal(data, buf) {
		t.Fatal("processed data does not match input")
	}
}

func TestIssue71(t *testing.T) {
	for _, tc := range []string{
		"abc",               // < mfLimit
		"abcdefghijklmnopq", // > mfLimit
	} {
		t.Run(tc, func(t *testing.T) {
			src := []byte(tc)
			bound := lz4.CompressBlockBound(len(tc))

			// Small buffer.
			zSmall := make([]byte, bound-1)
			n, err := lz4.CompressBlock(src, zSmall, nil)
			if err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatal("should be incompressible")
			}

			// Large enough buffer.
			zLarge := make([]byte, bound)
			n, err = lz4.CompressBlock(src, zLarge, nil)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				t.Fatal("should be compressible")
			}
		})
	}
}
