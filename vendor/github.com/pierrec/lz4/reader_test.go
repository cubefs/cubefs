package lz4_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/pierrec/lz4"
)

func TestReader(t *testing.T) {
	goldenFiles := []string{
		"testdata/e.txt.lz4",
		"testdata/gettysburg.txt.lz4",
		"testdata/Mark.Twain-Tom.Sawyer.txt.lz4",
		"testdata/Mark.Twain-Tom.Sawyer_long.txt.lz4",
		"testdata/pg1661.txt.lz4",
		"testdata/pi.txt.lz4",
		"testdata/random.data.lz4",
		"testdata/repeat.txt.lz4",
		"testdata/pg_control.tar.lz4",
	}

	for _, fname := range goldenFiles {
		t.Run(fname, func(t *testing.T) {
			fname := fname
			t.Parallel()

			f, err := os.Open(fname)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()

			rawfile := strings.TrimSuffix(fname, ".lz4")
			raw, err := ioutil.ReadFile(rawfile)
			if err != nil {
				t.Fatal(err)
			}

			var out bytes.Buffer
			zr := lz4.NewReader(f)
			n, err := io.Copy(&out, zr)
			if err != nil {
				t.Fatal(err)
			}

			if got, want := int(n), len(raw); got != want {
				t.Errorf("invalid sizes: got %d; want %d", got, want)
			}

			if got, want := out.Bytes(), raw; !reflect.DeepEqual(got, want) {
				t.Fatal("uncompressed data does not match original")
			}

			if len(raw) < 20 {
				return
			}

			f2, err := os.Open(fname)
			if err != nil {
				t.Fatal(err)
			}
			defer f2.Close()

			out.Reset()
			zr = lz4.NewReader(f2)
			_, err = io.CopyN(&out, zr, 10)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(out.Bytes(), raw[:10]) {
				t.Fatal("partial read does not match original")
			}

			pos, err := zr.Seek(-1, io.SeekCurrent)
			if err == nil {
				t.Fatal("expected error from invalid seek")
			}
			if pos != 10 {
				t.Fatalf("unexpected position %d", pos)
			}
			pos, err = zr.Seek(1, io.SeekStart)
			if err == nil {
				t.Fatal("expected error from invalid seek")
			}
			if pos != 10 {
				t.Fatalf("unexpected position %d", pos)
			}
			pos, err = zr.Seek(-1, io.SeekEnd)
			if err == nil {
				t.Fatal("expected error from invalid seek")
			}
			if pos != 10 {
				t.Fatalf("unexpected position %d", pos)
			}

			pos, err = zr.Seek(int64(len(raw)-20), io.SeekCurrent)
			if err != nil {
				t.Fatal(err)
			}
			if pos != int64(len(raw)-10) {
				t.Fatalf("unexpected position %d", pos)
			}

			out.Reset()
			_, err = io.CopyN(&out, zr, 10)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(out.Bytes(), raw[len(raw)-10:]) {
				t.Fatal("after seek, partial read does not match original")
			}
		})
	}
}
