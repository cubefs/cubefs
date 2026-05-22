// Copyright 2026 The CubeFS Authors.
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

package executor

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/spec"
)

// benchS3Backend 是 bench 单元测试用的最小 backend 假实现：
// 记录每次 Put / Get / List 的入参（通过 *putCall / *getCall / *listCall 切片）
// 并提供线程安全的读取入口。
type benchS3Backend struct {
	mu sync.Mutex

	puts    []putCall
	gets    []getCall
	lists   []listCall
	deletes []string
	heads   []string

	// 当 listKeys 非空时，List 会按这些 key 一一推入 channel，再关闭。
	// 用于让 list 测试可断言"实际消费了 channel"。
	listKeys []string
}

type putCall struct {
	key  string
	size int64
	opts backend.PutOptions
}

type getCall struct {
	key  string
	off  int64
	size int64
}

type listCall struct {
	prefix    string
	recursive bool
}

func (f *benchS3Backend) Kind() string { return "s3" }

func (f *benchS3Backend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	f.mu.Lock()
	f.lists = append(f.lists, listCall{prefix: prefix, recursive: recursive})
	keys := append([]string(nil), f.listKeys...)
	f.mu.Unlock()

	ch := make(chan backend.Entry, len(keys)+1)
	go func() {
		defer close(ch)
		for _, k := range keys {
			select {
			case ch <- backend.Entry{Key: k, Size: 1}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (f *benchS3Backend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	f.mu.Lock()
	f.gets = append(f.gets, getCall{key: key, off: off, size: size})
	f.mu.Unlock()
	// 返回一个空 reader：bench 只关心 latency / 调用参数，不验证字节内容。
	return io.NopCloser(emptyReader{}), nil
}

func (f *benchS3Backend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	f.mu.Lock()
	f.heads = append(f.heads, key)
	f.mu.Unlock()
	return 0, "", time.Time{}, nil
}

func (f *benchS3Backend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	// 必须消费 body，否则 bench worker 的 bytes.Reader 行为与真实 backend 不一致；
	// 这里只 Drain 即可，不验证内容。
	_, _ = io.Copy(io.Discard, body)
	f.mu.Lock()
	f.puts = append(f.puts, putCall{key: key, size: size, opts: opts})
	f.mu.Unlock()
	return backend.PutResult{BytesPut: size}, nil
}

func (f *benchS3Backend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	return "", "", backend.ErrBackendUnsupported
}

func (f *benchS3Backend) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	f.deletes = append(f.deletes, key)
	f.mu.Unlock()
	return nil
}

func (f *benchS3Backend) Rename(ctx context.Context, oldKey, newKey string) error { return nil }

func (f *benchS3Backend) Capabilities() backend.Caps {
	return backend.Caps{RangeRead: true, Multipart: true}
}

func (f *benchS3Backend) SameInstance(o backend.Backend) bool {
	_, ok := o.(*benchS3Backend)
	return ok
}

func (f *benchS3Backend) Close() error { return nil }

// snapshotPuts/snapshotGets/snapshotLists 是测试侧用的读取入口，避免外部
// 直接拿 mu / 切片字段。返回拷贝，避免并发期间被覆盖。
func (f *benchS3Backend) snapshotPuts() []putCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]putCall, len(f.puts))
	copy(out, f.puts)
	return out
}

func (f *benchS3Backend) snapshotGets() []getCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]getCall, len(f.gets))
	copy(out, f.gets)
	return out
}

func (f *benchS3Backend) snapshotLists() []listCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]listCall, len(f.lists))
	copy(out, f.lists)
	return out
}

// emptyReader 让 Get 返回 EOF 而不分配实际 buffer。
type emptyReader struct{}

func (emptyReader) Read(p []byte) (int, error) { return 0, io.EOF }

// runShortStage 是测试 helper：用 NumObjects 限制 stage 一定能在毫秒内停下，
// 而不是依赖 Runtime 倒计时（让测试快、稳定）。返回 stage 结果与 err。
func runShortStage(t *testing.T, stage spec.ObjStage, b backend.Backend) *spec.BenchStageResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keyRing := make([]string, 0, 16)
	var mu sync.Mutex
	sr, err := runObjStage(ctx, stage, b, "test-prefix/", &keyRing, &mu, "tt", 0, 0)
	if err != nil {
		t.Fatalf("runObjStage: %v", err)
	}
	return sr
}

// TestBenchS3_PutMultipart_TransparentOptions: put_multipart 必须把
// PutOptions.Multipart=true 与 PartSizeMiB 透传到 backend.Put。
func TestBenchS3_PutMultipart_TransparentOptions(t *testing.T) {
	b := &benchS3Backend{}

	stage := spec.ObjStage{
		Name:       "wave-multipart",
		NumJobs:    1,
		NumObjects: 3,
		ObjectSize: spec.ObjSize{Fixed: 16 * 1024 * 1024}, // 16 MiB
		Ops: []spec.ObjOp{
			{Type: "put_multipart", Weight: 1, PartSizeMiB: 5},
		},
	}

	sr := runShortStage(t, stage, b)

	puts := b.snapshotPuts()
	if len(puts) == 0 {
		t.Fatalf("expected at least one Put, got 0")
	}
	for i, p := range puts {
		if !p.opts.Multipart {
			t.Errorf("put #%d: Multipart should be true, got false", i)
		}
		if p.opts.PartSizeMiB != 5 {
			t.Errorf("put #%d: PartSizeMiB want 5, got %d", i, p.opts.PartSizeMiB)
		}
		if p.size != 16*1024*1024 {
			t.Errorf("put #%d: size want 16MiB, got %d", i, p.size)
		}
	}
	if sr.TotalOps == 0 {
		t.Errorf("stage TotalOps should be > 0")
	}
}

// TestBenchS3_PutMultipart_DefaultPartSize: 当 ObjOp.PartSizeMiB 未配置时，
// 必须回落到 defaultMultipartPartMiB（8 MiB）。
func TestBenchS3_PutMultipart_DefaultPartSize(t *testing.T) {
	b := &benchS3Backend{}
	stage := spec.ObjStage{
		Name:       "wave-multipart-default",
		NumJobs:    1,
		NumObjects: 1,
		ObjectSize: spec.ObjSize{Fixed: 4096},
		Ops: []spec.ObjOp{
			{Type: "put_multipart", Weight: 1}, // 不设 PartSizeMiB
		},
	}

	_ = runShortStage(t, stage, b)

	puts := b.snapshotPuts()
	if len(puts) == 0 {
		t.Fatalf("expected at least one Put")
	}
	if puts[0].opts.PartSizeMiB != defaultMultipartPartMiB {
		t.Errorf("default PartSizeMiB want %d, got %d", defaultMultipartPartMiB, puts[0].opts.PartSizeMiB)
	}
	if !puts[0].opts.Multipart {
		t.Errorf("Multipart should remain true even with default part size")
	}
}

// 静态确认 benchS3Backend 实现了 backend.Backend 接口（编译期检查）。
var _ backend.Backend = (*benchS3Backend)(nil)
