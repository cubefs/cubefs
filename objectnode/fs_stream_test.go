package objectnode

import (
	"context"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestNewStream_modify(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	stream := NewStream(ctx)
	var src = []byte("1234567890")

	// write to stream
	var err error
	if _, err = stream.Write(src); err != nil {
		t.Fatalf("write fail casue: %v", err)
	}
	_ = stream.Close()
	// modify source
	for i := 0; i < len(src); i++ {
		src[i] = 0
	}
	// read from stream
	var actual = make([]byte, len(src))
	if _, err = stream.Read(actual); err != nil && err != io.EOF {
		t.Fatalf("read fail cause: %v", err)
	}
	if string(actual) != "1234567890" {
		t.Fatalf("result mismatch")
	}
}

func TestStream_rw(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	stream := NewStream(ctx)
	var src = []byte("1234567890")

	asyncWG := new(sync.WaitGroup)

	// async write
	asyncWG.Add(1)
	go func() {
		defer asyncWG.Done()
		var err error
		var n int
		for _, b := range src {
			n, err = stream.Write([]byte{b})
			if err != nil {
				t.Fatalf("write fail cause: %v", err)
			}
			if n != 1 {
				t.Fatalf("write bytes count mismatch: expect 10 actual %v", n)
			}
			time.Sleep(100 * time.Millisecond)
		}
		_ = stream.Close()
	}()

	// async read
	asyncWG.Add(1)
	go func() {
		defer asyncWG.Done()
		var err error
		var n int
		tmp := make([]byte, 3)
		result := make([]byte, 0)
		for {
			n, err = stream.Read(tmp)
			if err != nil && err != io.EOF {
				t.Fatalf("read fail cause: %v", err)
			}
			if err == io.EOF {
				break
			}
			t.Logf("read: %v", tmp[:n])
			result = append(result, tmp[:n]...)
		}
		if !reflect.DeepEqual(result, src) {
			t.Fatalf("result mismatch: expect %v actual %v", src, result)
		}
	}()

	asyncWG.Wait()
}
