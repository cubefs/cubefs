// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package bufalloc

import (
	"bytes"
	"io"
	"math/rand"
	"runtime"
	"testing"
)

const n = 10000      // make this bigger for a larger (and slower) test
var data string      // test data for write tests
var testBytes []byte // test data; same as data but as a slice.

func init() {
	testBytes = make([]byte, n)
	for i := 0; i < n; i++ {
		testBytes[i] = 'a' + byte(i%26)
	}
	data = string(testBytes)
}

// Verify that contents of buf match the string s.
func check(t *testing.T, testname string, buf Buffer, s string) {
	bytes := buf.Bytes()
	str := buf.String()
	if buf.Len() != len(bytes) {
		t.Errorf("%s: buf.Len() == %d, len(buf.Bytes()) == %d", testname, buf.Len(), len(bytes))
	}

	if buf.Len() != len(str) {
		t.Errorf("%s: buf.Len() == %d, len(buf.String()) == %d", testname, buf.Len(), len(str))
	}

	if buf.Len() != len(s) {
		t.Errorf("%s: buf.Len() == %d, len(s) == %d", testname, buf.Len(), len(s))
	}

	if string(bytes) != s {
		t.Errorf("%s: string(buf.Bytes()) == %q, s == %q", testname, string(bytes), s)
	}
}

// Fill buf through n writes of byte slice fub.
// The initial contents of buf corresponds to the string s;
// the result is the final contents of buf returned as a string.
func fillBytes(t *testing.T, testname string, buf Buffer, s string, n int, fub []byte) string {
	check(t, testname+" (fill 1)", buf, s)
	for ; n > 0; n-- {
		m, err := buf.Write(fub)
		if m != len(fub) {
			t.Errorf(testname+" (fill 2): m == %d, expected %d", m, len(fub))
		}
		if err != nil {
			t.Errorf(testname+" (fill 3): err should always be nil, found err == %s", err)
		}
		s += string(fub)
		check(t, testname+" (fill 4)", buf, s)
	}
	return s
}

func TestNewBuffer(t *testing.T) {
	buf := buffPool.getBuffer(len(testBytes))
	buf.Write(testBytes)
	check(t, "NewBuffer", buf, data)
	buffPool.putBuffer(buf)
}

// Empty buf through repeated reads into fub.
// The initial contents of buf corresponds to the string s.
func empty(t *testing.T, testname string, buf Buffer, s string, fub []byte) {
	check(t, testname+" (empty 1)", buf, s)

	for {
		n, err := buf.Read(fub)
		if n == 0 {
			break
		}
		if err != nil {
			t.Errorf(testname+" (empty 2): err should always be nil, found err == %s", err)
		}
		s = s[n:]
		check(t, testname+" (empty 3)", buf, s)
	}

	check(t, testname+" (empty 4)", buf, "")
}

func TestBasicOperations(t *testing.T) {
	buf := buffPool.getBuffer(0)

	for i := 0; i < 5; i++ {
		check(t, "TestBasicOperations (1)", buf, "")

		buf.Reset()
		check(t, "TestBasicOperations (2)", buf, "")

		buf.Truncate(0)
		check(t, "TestBasicOperations (3)", buf, "")

		n, err := buf.Write([]byte(data[0:1]))
		if n != 1 {
			t.Errorf("wrote 1 byte, but n == %d", n)
		}
		if err != nil {
			t.Errorf("err should always be nil, but err == %s", err)
		}
		check(t, "TestBasicOperations (4)", buf, "a")

		buf.WriteByte(data[1])
		check(t, "TestBasicOperations (5)", buf, "ab")

		n, err = buf.Write([]byte(data[2:26]))
		if n != 24 {
			t.Errorf("wrote 25 bytes, but n == %d", n)
		}
		check(t, "TestBasicOperations (6)", buf, string(data[0:26]))

		buf.Truncate(26)
		check(t, "TestBasicOperations (7)", buf, string(data[0:26]))

		buf.Truncate(20)
		check(t, "TestBasicOperations (8)", buf, string(data[0:20]))

		empty(t, "TestBasicOperations (9)", buf, string(data[0:20]), make([]byte, 5))
		empty(t, "TestBasicOperations (10)", buf, "", make([]byte, 100))

		buf.WriteByte(data[1])
		c, err := buf.ReadByte()
		if err != nil {
			t.Error("ReadByte unexpected eof")
		}
		if c != data[1] {
			t.Errorf("ReadByte wrong value c=%v", c)
		}
		c, err = buf.ReadByte()
		if err == nil {
			t.Error("ReadByte unexpected not eof")
		}
	}
	buffPool.putBuffer(buf)
}

func TestLargeByteWrites(t *testing.T) {
	buf := buffPool.getBuffer(0)
	limit := 30
	if testing.Short() {
		limit = 9
	}
	for i := 3; i < limit; i += 3 {
		s := fillBytes(t, "TestLargeWrites (1)", buf, "", 5, testBytes)
		empty(t, "TestLargeByteWrites (2)", buf, s, make([]byte, len(data)/i))
	}
	check(t, "TestLargeByteWrites (3)", buf, "")
	buffPool.putBuffer(buf)
}

func TestLargeByteReads(t *testing.T) {
	buf := buffPool.getBuffer(0)
	for i := 3; i < 30; i += 3 {
		s := fillBytes(t, "TestLargeReads (1)", buf, "", 5, testBytes[0:len(testBytes)/i])
		empty(t, "TestLargeReads (2)", buf, s, make([]byte, len(data)))
	}
	check(t, "TestLargeByteReads (3)", buf, "")
	buffPool.putBuffer(buf)
}

func TestMixedReadsAndWrites(t *testing.T) {
	buf := buffPool.getBuffer(0)
	s := ""
	for i := 0; i < 50; i++ {
		wlen := rand.Intn(len(data))
		s = fillBytes(t, "TestMixedReadsAndWrites (1)", buf, s, 1, testBytes[0:wlen])
		rlen := rand.Intn(len(data))
		fub := make([]byte, rlen)
		n, _ := buf.Read(fub)
		s = s[n:]
	}
	empty(t, "TestMixedReadsAndWrites (2)", buf, s, make([]byte, buf.Len()))
	buffPool.putBuffer(buf)
}

func TestReadFrom(t *testing.T) {
	buf := buffPool.getBuffer(0)
	for i := 3; i < 30; i += 3 {
		s := fillBytes(t, "TestReadFrom (1)", buf, "", 5, testBytes[0:len(testBytes)/i])
		b := buffPool.getBuffer(0)
		b.ReadFrom(buf)
		empty(t, "TestReadFrom (2)", b, s, make([]byte, len(data)))
		buffPool.putBuffer(b)
	}
	buffPool.putBuffer(buf)
}

func TestWriteTo(t *testing.T) {
	buf := buffPool.getBuffer(0)
	for i := 3; i < 30; i += 3 {
		s := fillBytes(t, "TestWriteTo (1)", buf, "", 5, testBytes[0:len(testBytes)/i])
		b := buffPool.getBuffer(0)
		buf.WriteTo(b)
		empty(t, "TestWriteTo (2)", b, s, make([]byte, len(data)))
		buffPool.putBuffer(b)
	}
	buffPool.putBuffer(buf)
}

func TestNext(t *testing.T) {
	b := []byte{0, 1, 2, 3, 4}
	tmp := make([]byte, 5)
	for i := 0; i <= 5; i++ {
		for j := i; j <= 5; j++ {
			for k := 0; k <= 6; k++ {
				// 0 <= i <= j <= 5; 0 <= k <= 6
				// Check that if we start with a buffer
				// of length j at offset i and ask for
				// Next(k), we get the right bytes.
				buf := buffPool.getBuffer(0)
				buf.Write(b[0:j])
				n, _ := buf.Read(tmp[0:i])
				if n != i {
					t.Fatalf("Read %d returned %d", i, n)
				}
				bb := buf.Next(k)
				want := k
				if want > j-i {
					want = j - i
				}
				if len(bb) != want {
					t.Fatalf("in %d,%d: len(Next(%d)) == %d", i, j, k, len(bb))
				}
				for l, v := range bb {
					if v != byte(l+i) {
						t.Fatalf("in %d,%d: Next(%d)[%d] = %d, want %d", i, j, k, l, v, l+i)
					}
				}
				buffPool.putBuffer(buf)
			}
		}
	}
}

var readBytesTests = []struct {
	buffer   string
	delim    byte
	expected []string
	err      error
}{
	{"", 0, []string{""}, io.EOF},
	{"a\x00", 0, []string{"a\x00"}, nil},
	{"abbbaaaba", 'b', []string{"ab", "b", "b", "aaab"}, nil},
	{"hello\x01world", 1, []string{"hello\x01"}, nil},
	{"foo\nbar", 0, []string{"foo\nbar"}, io.EOF},
	{"alpha\nbeta\ngamma\n", '\n', []string{"alpha\n", "beta\n", "gamma\n"}, nil},
	{"alpha\nbeta\ngamma", '\n', []string{"alpha\n", "beta\n", "gamma"}, io.EOF},
}

func TestReadBytes(t *testing.T) {
	for _, test := range readBytesTests {
		buf := buffPool.getBuffer(0)
		buf.Write([]byte(test.buffer))
		var err error
		for _, expected := range test.expected {
			var bytes []byte
			bytes, err = buf.ReadBytes(test.delim)
			if string(bytes) != expected {
				t.Errorf("expected %q, got %q", expected, bytes)
			}
			if err != nil {
				break
			}
		}
		if err != test.err {
			t.Errorf("expected error %v, got %v", test.err, err)
		}
		buffPool.putBuffer(buf)
	}
}

func TestGrow(t *testing.T) {
	x := []byte{'x'}
	y := []byte{'y'}
	tmp := make([]byte, 72)
	for _, startLen := range []int{0, 100, 1000, 10000, 100000} {
		xBytes := bytes.Repeat(x, startLen)
		for _, growLen := range []int{0, 100, 1000, 10000, 100000} {
			buf := buffPool.getBuffer(0)
			buf.Write(xBytes)
			// If we read, this affects buf.off, which is good to test.
			readBytes, _ := buf.Read(tmp)
			buf.Grow(growLen)
			yBytes := bytes.Repeat(y, growLen)
			// Check no allocation occurs in write, as long as we're single-threaded.
			var m1, m2 runtime.MemStats
			runtime.ReadMemStats(&m1)
			buf.Write(yBytes)
			runtime.ReadMemStats(&m2)
			if runtime.GOMAXPROCS(-1) == 1 && m1.Mallocs != m2.Mallocs {
				t.Errorf("allocation occurred during write")
			}
			// Check that buffer has correct data.
			if !bytes.Equal(buf.Bytes()[0:startLen-readBytes], xBytes[readBytes:]) {
				t.Errorf("bad initial data at %d %d", startLen, growLen)
			}
			if !bytes.Equal(buf.Bytes()[startLen-readBytes:startLen-readBytes+growLen], yBytes) {
				t.Errorf("bad written data at %d %d", startLen, growLen)
			}
			buffPool.putBuffer(buf)
		}
	}
}

func TestReadEmptyAtEOF(t *testing.T) {
	b := buffPool.getBuffer(0)
	slice := make([]byte, 0)
	n, err := b.Read(slice)
	if err != nil {
		t.Errorf("read error: %v", err)
	}
	if n != 0 {
		t.Errorf("wrong count; got %d want 0", n)
	}
	buffPool.putBuffer(b)
}

func TestBufferGrowth(t *testing.T) {
	b := buffPool.getBuffer(0)
	buf := make([]byte, 1024)
	b.Write(buf[0:1])
	var cap0 int
	for i := 0; i < 5<<10; i++ {
		b.Write(buf)
		b.Read(buf)
		if i == 0 {
			cap0 = b.Cap()
		}
	}
	cap1 := b.Cap()
	// (*Buffer).grow allows for 2x capacity slop before sliding,
	// so set our error threshold at 3x.
	if cap1 > cap0*3 {
		t.Errorf("buffer cap = %d; too big (grew from %d)", cap1, cap0)
	}
	buffPool.putBuffer(b)
}

func BenchmarkBufferNotEmptyWriteRead(b *testing.B) {
	buf := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		b := buffPool.getBuffer(1024)
		b.Write(buf[0:1])
		for i := 0; i < 5<<10; i++ {
			b.Write(buf)
			b.Read(buf)
		}
		buffPool.putBuffer(b)
	}
}

func BenchmarkBufferFullSmallReads(b *testing.B) {
	buf := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		b := buffPool.getBuffer(1024)
		b.Write(buf)
		for b.Len()+20 < b.Cap() {
			b.Write(buf[:10])
		}
		for i := 0; i < 5<<10; i++ {
			b.Read(buf[:1])
			b.Write(buf[:1])
		}
		buffPool.putBuffer(b)
	}
}
