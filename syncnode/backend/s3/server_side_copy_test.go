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

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// TestServerSideCopy_SingleObject covers the ≤5GiB CopyObject branch:
// upload a small object via Put, request ServerSideCopy to a new key, and
// verify the destination payload + ETag match.
func TestServerSideCopy_SingleObject(t *testing.T) {
	m := newMockS3("test-bucket")
	t.Cleanup(m.close)
	b := newTestBackend(t, m)
	ctx := context.Background()

	payload := bytes.Repeat([]byte("a"), 4096)
	if _, err := b.Put(ctx, "src/key", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put src: %v", err)
	}

	res, err := b.ServerSideCopy(ctx, "src/key", "dst/key", backend.PutOptions{})
	if err != nil {
		t.Fatalf("ServerSideCopy: %v", err)
	}
	if res.BytesPut != int64(len(payload)) {
		t.Errorf("BytesPut = %d, want %d", res.BytesPut, len(payload))
	}
	if res.ETag == "" {
		t.Errorf("expected non-empty ETag")
	}

	rc, err := b.Get(ctx, "dst/key", 0, 0)
	if err != nil {
		t.Fatalf("Get dst: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, payload) {
		t.Errorf("dst payload mismatch: len(got)=%d len(want)=%d", len(got), len(payload))
	}
}

// TestServerSideCopy_PreservesMtime exercises PutOptions.Mtime pass-through
// — after the copy, Head(dst) must report the supplied mtime, not the
// destination write-time.
func TestServerSideCopy_PreservesMtime(t *testing.T) {
	m := newMockS3("test-bucket")
	t.Cleanup(m.close)
	b := newTestBackend(t, m)
	ctx := context.Background()

	payload := []byte("mtime-payload")
	if _, err := b.Put(ctx, "src/m", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put src: %v", err)
	}

	want := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	if _, err := b.ServerSideCopy(ctx, "src/m", "dst/m", backend.PutOptions{Mtime: &want}); err != nil {
		t.Fatalf("ServerSideCopy: %v", err)
	}

	_, _, gotMtime, err := b.Head(ctx, "dst/m")
	if err != nil {
		t.Fatalf("Head dst: %v", err)
	}
	if !gotMtime.Equal(want) {
		t.Errorf("preserved mtime = %v, want %v", gotMtime, want)
	}
}

// TestServerSideCopy_Multipart covers the >5GiB UploadPartCopy branch. We
// shrink the part-size threshold and serverSideCopySingleMax via package
// override so the test runs over a tiny in-memory payload.
func TestServerSideCopy_Multipart(t *testing.T) {
	origMax := serverSideCopySingleMaxOverride
	origPart := serverSideCopyPartSize
	serverSideCopySingleMaxOverride = 1024 // any object > 1 KiB goes multipart
	serverSideCopyPartSize = 256
	t.Cleanup(func() {
		serverSideCopySingleMaxOverride = origMax
		serverSideCopyPartSize = origPart
	})

	m := newMockS3("test-bucket")
	t.Cleanup(m.close)
	b := newTestBackend(t, m)
	ctx := context.Background()

	payload := bytes.Repeat([]byte("x"), 5*1024) // 5 KiB → 20 parts of 256 B
	if _, err := b.Put(ctx, "src/big", bytes.NewReader(payload), int64(len(payload)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put src: %v", err)
	}

	res, err := b.ServerSideCopy(ctx, "src/big", "dst/big", backend.PutOptions{})
	if err != nil {
		t.Fatalf("ServerSideCopy multipart: %v", err)
	}
	if res.BytesPut != int64(len(payload)) {
		t.Errorf("BytesPut = %d, want %d", res.BytesPut, len(payload))
	}

	rc, err := b.Get(ctx, "dst/big", 0, 0)
	if err != nil {
		t.Fatalf("Get dst: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, payload) {
		t.Errorf("multipart-copied payload mismatch: len(got)=%d len(want)=%d", len(got), len(payload))
	}
}

// TestServerSideCopy_MissingSource asserts ErrKeyNotFound surfaces cleanly
// when src does not exist.
func TestServerSideCopy_MissingSource(t *testing.T) {
	m := newMockS3("test-bucket")
	t.Cleanup(m.close)
	b := newTestBackend(t, m)

	_, err := b.ServerSideCopy(context.Background(), "missing", "dst", backend.PutOptions{})
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Fatalf("got %v, want ErrKeyNotFound", err)
	}
}

// TestS3_SameInstance is the truth table for SameInstance: same triple
// (endpoint, region, credential) → true; any difference → false. Bucket
// MUST be ignored.
func TestS3_SameInstance(t *testing.T) {
	mk := func(endpoint, region, bucket, akEnv string) *Backend {
		return &Backend{
			cfg: &Config{
				Endpoint:     endpoint,
				Region:       region,
				Bucket:       bucket,
				AccessKeyEnv: akEnv,
			},
		}
	}
	base := mk("http://s3.local:9000", "us-east-1", "bucket-a", "AK1")

	cases := []struct {
		name string
		o    backend.Backend
		want bool
	}{
		{"identical", mk("http://s3.local:9000", "us-east-1", "bucket-a", "AK1"), true},
		{"different bucket only", mk("http://s3.local:9000", "us-east-1", "bucket-b", "AK1"), true},
		{"different endpoint", mk("http://s3.other:9000", "us-east-1", "bucket-a", "AK1"), false},
		{"different region", mk("http://s3.local:9000", "us-west-2", "bucket-a", "AK1"), false},
		{"different credential", mk("http://s3.local:9000", "us-east-1", "bucket-a", "AK2"), false},
		{"nil other", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := base.SameInstance(c.o); got != c.want {
				t.Errorf("SameInstance = %v, want %v", got, c.want)
			}
		})
	}
}

// TestS3_SameInstance_OtherKind covers the cross-kind path: an s3 backend
// against a non-s3 fake must always report false even if their internal
// fields would otherwise match.
func TestS3_SameInstance_OtherKind(t *testing.T) {
	b := &Backend{cfg: &Config{Endpoint: "x", Region: "y", AccessKeyEnv: "z"}}
	if b.SameInstance(&fakeNonS3{}) {
		t.Errorf("SameInstance against non-s3 backend should be false")
	}
}

type fakeNonS3 struct{}

func (fakeNonS3) Kind() string                                                            { return "fake" }
func (fakeNonS3) List(context.Context, string, bool) (<-chan backend.Entry, error)        { return nil, nil }
func (fakeNonS3) Get(context.Context, string, int64, int64) (io.ReadCloser, error)        { return nil, nil }
func (fakeNonS3) Head(context.Context, string) (int64, string, time.Time, error)          { return 0, "", time.Time{}, nil }
func (fakeNonS3) Put(context.Context, string, io.Reader, int64, backend.PutOptions) (backend.PutResult, error) {
	return backend.PutResult{}, nil
}
func (fakeNonS3) GetChecksum(context.Context, string) (string, string, error) { return "", "", nil }
func (fakeNonS3) Delete(context.Context, string) error                        { return nil }
func (fakeNonS3) Rename(context.Context, string, string) error                { return nil }
func (fakeNonS3) Capabilities() backend.Caps                                  { return backend.Caps{} }
func (fakeNonS3) SameInstance(backend.Backend) bool                           { return false }
func (fakeNonS3) Close() error                                                { return nil }

// Suppress unused-import lints that may surface as the file evolves.
var (
	_ = strings.TrimSpace
	_ = os.Getenv
)
