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
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// ---------- httptest-backed mock S3 server ----------
//
// mockS3 is a small in-memory S3-protocol implementation that covers the
// operations exercised by the Backend tests below. It is NOT a full S3
// server — only the methods used by the tests are wired up.

type mockObject struct {
	body     []byte
	etag     string
	modified time.Time
}

type mockUpload struct {
	key       string
	uploadID  string
	parts     map[int][]byte
	initiated time.Time
}

type mockS3 struct {
	mu      sync.Mutex
	bucket  string
	objects map[string]*mockObject
	uploads map[string]*mockUpload // uploadID -> upload
	srv     *httptest.Server
}

func newMockS3(bucket string) *mockS3 {
	m := &mockS3{
		bucket:  bucket,
		objects: make(map[string]*mockObject),
		uploads: make(map[string]*mockUpload),
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *mockS3) endpoint() string { return m.srv.URL }
func (m *mockS3) close()           { m.srv.Close() }

// keyFromPath extracts the object key from a path-style request like
// /<bucket>/<key...>.
func (m *mockS3) keyFromPath(p string) (string, bool) {
	p = strings.TrimPrefix(p, "/")
	prefix := m.bucket + "/"
	if !strings.HasPrefix(p, prefix) {
		return "", false
	}
	return strings.TrimPrefix(p, prefix), true
}

func (m *mockS3) handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// AbortMultipartUpload / CompleteMultipartUpload / UploadPart /
	// CreateMultipartUpload are all keyed by uploadId param.
	if uid := q.Get("uploadId"); uid != "" {
		key, ok := m.keyFromPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodPut:
			m.handleUploadPart(w, r, key, uid)
		case http.MethodPost:
			m.handleCompleteMultipart(w, r, key, uid)
		case http.MethodDelete:
			m.handleAbortMultipart(w, r, key, uid)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}

	// CreateMultipartUpload: POST ?uploads
	if _, ok := q["uploads"]; ok && r.Method == http.MethodPost {
		key, ok := m.keyFromPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		m.handleCreateMultipartUpload(w, r, key)
		return
	}

	// ListMultipartUploads: GET /<bucket>/?uploads
	if r.Method == http.MethodGet && strings.TrimPrefix(r.URL.Path, "/") == m.bucket {
		if _, ok := q["uploads"]; ok {
			m.handleListMultipartUploads(w, r)
			return
		}
		m.handleListObjectsV2(w, r)
		return
	}

	// GET /<bucket>/ (with optional list-type=2) for non-rooted variant
	if r.Method == http.MethodGet && strings.TrimPrefix(r.URL.Path, "/") == m.bucket+"/" {
		m.handleListObjectsV2(w, r)
		return
	}

	// Object-level operations.
	key, ok := m.keyFromPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	// CopyObject: PUT with x-amz-copy-source header
	if r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "" {
		m.handleCopyObject(w, r, key)
		return
	}

	switch r.Method {
	case http.MethodGet:
		m.handleGet(w, r, key)
	case http.MethodHead:
		m.handleHead(w, r, key)
	case http.MethodPut:
		m.handlePut(w, r, key)
	case http.MethodDelete:
		m.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (m *mockS3) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	m.mu.Lock()
	obj, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		writeS3Error(w, "NoSuchKey", "Key not found", http.StatusNotFound)
		return
	}
	body := obj.body
	start, end := int64(0), int64(len(body))
	if rng := r.Header.Get("Range"); rng != "" {
		// bytes=N-M or bytes=N-
		v := strings.TrimPrefix(rng, "bytes=")
		parts := strings.SplitN(v, "-", 2)
		if len(parts) == 2 {
			if parts[0] != "" {
				start, _ = strconv.ParseInt(parts[0], 10, 64)
			}
			if parts[1] != "" {
				e, _ := strconv.ParseInt(parts[1], 10, 64)
				end = e + 1
			}
		}
		if start < 0 {
			start = 0
		}
		if end > int64(len(body)) {
			end = int64(len(body))
		}
		if start > end {
			start = end
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(body)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start, 10))
		w.Header().Set("ETag", `"`+obj.etag+`"`)
		w.Header().Set("Last-Modified", obj.modified.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start:end])
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("ETag", `"`+obj.etag+`"`)
	w.Header().Set("Last-Modified", obj.modified.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (m *mockS3) handleHead(w http.ResponseWriter, _ *http.Request, key string) {
	m.mu.Lock()
	obj, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.body)))
	w.Header().Set("ETag", `"`+obj.etag+`"`)
	w.Header().Set("Last-Modified", obj.modified.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	sum := md5.Sum(body)
	etag := hex.EncodeToString(sum[:])
	m.mu.Lock()
	m.objects[key] = &mockObject{body: body, etag: etag, modified: time.Now()}
	m.mu.Unlock()
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3) handleDelete(w http.ResponseWriter, _ *http.Request, key string) {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockS3) handleCopyObject(w http.ResponseWriter, r *http.Request, key string) {
	src, err := url.PathUnescape(strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source"), "/"))
	if err != nil {
		http.Error(w, "bad copy-source", http.StatusBadRequest)
		return
	}
	// strip "bucket/" prefix
	parts := strings.SplitN(src, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad copy-source", http.StatusBadRequest)
		return
	}
	srcKey := parts[1]
	m.mu.Lock()
	srcObj, ok := m.objects[srcKey]
	if !ok {
		m.mu.Unlock()
		writeS3Error(w, "NoSuchKey", "Source key not found", http.StatusNotFound)
		return
	}
	bodyCopy := make([]byte, len(srcObj.body))
	copy(bodyCopy, srcObj.body)
	m.objects[key] = &mockObject{body: bodyCopy, etag: srcObj.etag, modified: time.Now()}
	m.mu.Unlock()
	type copyResult struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(copyResult{ETag: `"` + srcObj.etag + `"`, LastModified: time.Now().UTC().Format(time.RFC3339)})
}

func (m *mockS3) handleListObjectsV2(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delim := q.Get("delimiter")

	m.mu.Lock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	type content struct {
		Key          string    `xml:"Key"`
		Size         int64     `xml:"Size"`
		ETag         string    `xml:"ETag"`
		LastModified time.Time `xml:"LastModified"`
	}
	type commonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	type resp struct {
		XMLName        xml.Name       `xml:"ListBucketResult"`
		Name           string         `xml:"Name"`
		Prefix         string         `xml:"Prefix"`
		Delimiter      string         `xml:"Delimiter,omitempty"`
		KeyCount       int            `xml:"KeyCount"`
		MaxKeys        int            `xml:"MaxKeys"`
		IsTruncated    bool           `xml:"IsTruncated"`
		Contents       []content      `xml:"Contents"`
		CommonPrefixes []commonPrefix `xml:"CommonPrefixes"`
	}
	out := resp{Name: m.bucket, Prefix: prefix, Delimiter: delim, MaxKeys: 1000}
	seenPrefixes := map[string]bool{}
	for _, k := range keys {
		if delim != "" {
			rel := strings.TrimPrefix(k, prefix)
			if idx := strings.Index(rel, delim); idx >= 0 {
				cp := prefix + rel[:idx+1]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: cp})
				}
				continue
			}
		}
		obj := m.objects[k]
		out.Contents = append(out.Contents, content{
			Key:          k,
			Size:         int64(len(obj.body)),
			ETag:         `"` + obj.etag + `"`,
			LastModified: obj.modified,
		})
	}
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(out)
}

func (m *mockS3) handleCreateMultipartUpload(w http.ResponseWriter, _ *http.Request, key string) {
	uid := randomID()
	m.mu.Lock()
	m.uploads[uid] = &mockUpload{key: key, uploadID: uid, parts: map[int][]byte{}, initiated: time.Now()}
	m.mu.Unlock()
	type resp struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadId string   `xml:"UploadId"`
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(resp{Bucket: m.bucket, Key: key, UploadId: uid})
}

func (m *mockS3) handleUploadPart(w http.ResponseWriter, r *http.Request, _ string, uid string) {
	pn, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read part: "+err.Error(), http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	up, ok := m.uploads[uid]
	if !ok {
		m.mu.Unlock()
		writeS3Error(w, "NoSuchUpload", "no such upload", http.StatusNotFound)
		return
	}
	up.parts[pn] = body
	m.mu.Unlock()
	sum := md5.Sum(body)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3) handleCompleteMultipart(w http.ResponseWriter, _ *http.Request, key string, uid string) {
	m.mu.Lock()
	up, ok := m.uploads[uid]
	if !ok {
		m.mu.Unlock()
		writeS3Error(w, "NoSuchUpload", "no such upload", http.StatusNotFound)
		return
	}
	// Assemble parts in order.
	nums := make([]int, 0, len(up.parts))
	for n := range up.parts {
		nums = append(nums, n)
	}
	sort.Ints(nums)
	var all []byte
	for _, n := range nums {
		all = append(all, up.parts[n]...)
	}
	sum := md5.Sum(all)
	etag := fmt.Sprintf("%s-%d", hex.EncodeToString(sum[:]), len(nums))
	m.objects[key] = &mockObject{body: all, etag: etag, modified: time.Now()}
	delete(m.uploads, uid)
	m.mu.Unlock()
	type resp struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
		Location string   `xml:"Location"`
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(resp{Bucket: m.bucket, Key: key, ETag: `"` + etag + `"`})
}

func (m *mockS3) handleAbortMultipart(w http.ResponseWriter, _ *http.Request, _ string, uid string) {
	m.mu.Lock()
	_, ok := m.uploads[uid]
	if ok {
		delete(m.uploads, uid)
	}
	m.mu.Unlock()
	if !ok {
		writeS3Error(w, "NoSuchUpload", "no such upload", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockS3) handleListMultipartUploads(w http.ResponseWriter, _ *http.Request) {
	type upload struct {
		Key       string    `xml:"Key"`
		UploadID  string    `xml:"UploadId"`
		Initiated time.Time `xml:"Initiated"`
	}
	type resp struct {
		XMLName     xml.Name `xml:"ListMultipartUploadsResult"`
		Bucket      string   `xml:"Bucket"`
		IsTruncated bool     `xml:"IsTruncated"`
		Uploads     []upload `xml:"Upload"`
	}
	m.mu.Lock()
	out := resp{Bucket: m.bucket}
	for _, up := range m.uploads {
		out.Uploads = append(out.Uploads, upload{Key: up.key, UploadID: up.uploadID, Initiated: up.initiated})
	}
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(out)
}

func writeS3Error(w http.ResponseWriter, code, msg string, status int) {
	type errResp struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(errResp{Code: code, Message: msg})
}

func randomID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ---------- helpers ----------

func newTestBackend(t *testing.T, m *mockS3) *Backend {
	t.Helper()
	// Use per-test unique env-var names so concurrent t.Parallel tests
	// don't stomp on each other (and so we don't need t.Setenv which is
	// incompatible with t.Parallel).
	akEnv := "SYNCNODE_S3_TEST_AK_" + randomID()
	skEnv := "SYNCNODE_S3_TEST_SK_" + randomID()
	if err := os.Setenv(akEnv, "test-access-key"); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	if err := os.Setenv(skEnv, "test-secret-key"); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Unsetenv(akEnv)
		_ = os.Unsetenv(skEnv)
	})
	cfg := &Config{
		Endpoint:              m.endpoint(),
		Region:                "us-east-1",
		Bucket:                m.bucket,
		AccessKeyEnv:          akEnv,
		SecretKeyEnv:          skEnv,
		UsePathStyle:          true,
		MultipartThresholdMiB: 8,
		PartSizeMiB:           5,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New backend: %v", err)
	}
	return b.(*Backend)
}

// ---------- tests ----------

func TestConfigValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  *Config
	}{
		{"missing bucket", &Config{Endpoint: "http://s3", Region: "r", AccessKeyEnv: "A", SecretKeyEnv: "B"}},
		{"missing endpoint", &Config{Bucket: "b", Region: "r", AccessKeyEnv: "A", SecretKeyEnv: "B"}},
		{"missing region", &Config{Bucket: "b", Endpoint: "http://s3", AccessKeyEnv: "A", SecretKeyEnv: "B"}},
		{"missing access key env", &Config{Bucket: "b", Endpoint: "http://s3", Region: "r", SecretKeyEnv: "B"}},
		{"missing secret key env", &Config{Bucket: "b", Endpoint: "http://s3", Region: "r", AccessKeyEnv: "A"}},
		{"part size too small", &Config{Bucket: "b", Endpoint: "http://s3", Region: "r", AccessKeyEnv: "A", SecretKeyEnv: "B", PartSizeMiB: 1}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tc.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, backend.ErrConfigInvalid) {
				t.Errorf("expected ErrConfigInvalid, got %v", err)
			}
		})
	}
}

func TestConfigWrongType(t *testing.T) {
	t.Parallel()
	_, err := New("not a config")
	if err == nil || !errors.Is(err, backend.ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid, got %v", err)
	}
}

func TestRegistered(t *testing.T) {
	t.Parallel()
	kinds := backend.RegisteredKinds()
	found := false
	for _, k := range kinds {
		if k == "s3" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("'s3' not in RegisteredKinds(): %v", kinds)
	}
}

func TestCapabilities(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	caps := b.Capabilities()
	if !caps.RangeRead {
		t.Error("RangeRead should be true")
	}
	if !caps.Multipart {
		t.Error("Multipart should be true")
	}
	if caps.AtomicRename {
		t.Error("AtomicRename should be false for S3 (copy+delete)")
	}
	if !caps.StrongConsistency {
		t.Error("StrongConsistency should be true (S3 since Dec 2020)")
	}
	if caps.ListMaxKeys != defaultListMaxKeys {
		t.Errorf("ListMaxKeys = %d, want %d", caps.ListMaxKeys, defaultListMaxKeys)
	}
	if b.Kind() != "s3" {
		t.Errorf("Kind = %q, want s3", b.Kind())
	}
}

func TestPutGetRoundTrip_Small(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	key := "small.bin"
	body := make([]byte, 1024) // 1 KiB
	for i := range body {
		body[i] = byte(i)
	}
	etag, err := b.Put(ctx, key, strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if etag == "" {
		t.Error("expected non-empty etag")
	}

	rc, err := b.Get(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("body mismatch: got %d bytes, want %d bytes", len(got), len(body))
	}
}

func TestPutGetRoundTrip_Multipart(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	key := "big.bin"
	// 10 MiB > MultipartThresholdMiB=8, triggers multipart path
	size := 10 * mib
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i % 251)
	}
	etag, err := b.Put(ctx, key, strings.NewReader(string(body)), int64(size), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put multipart: %v", err)
	}
	if etag == "" {
		t.Error("expected non-empty etag")
	}
	// Multipart etag has "-N" suffix
	if !strings.Contains(etag, "-") {
		t.Errorf("expected multipart etag with -N suffix, got %q", etag)
	}

	rc, err := b.Get(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != size {
		t.Errorf("body size: got %d, want %d", len(got), size)
	}
	if string(got) != string(body) {
		t.Error("body mismatch on multipart round trip")
	}
}

func TestGetRange(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	body := []byte("0123456789abcdefghij")
	_, err := b.Put(ctx, "ranged", strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Read [5, 5+10) = "56789abcde"
	rc, err := b.Get(ctx, "ranged", 5, 10)
	if err != nil {
		t.Fatalf("Get range: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	want := "56789abcde"
	if string(got) != want {
		t.Errorf("range read: got %q, want %q", got, want)
	}
}

func TestHead(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	body := []byte("hello world")
	_, err := b.Put(ctx, "x.txt", strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	size, etag, mtime, err := b.Head(ctx, "x.txt")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if size != int64(len(body)) {
		t.Errorf("size: got %d, want %d", size, len(body))
	}
	if etag == "" {
		t.Error("etag empty")
	}
	if mtime.IsZero() {
		t.Error("mtime zero")
	}
}

func TestHeadNotFound(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	_, _, _, err := b.Head(context.Background(), "missing")
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestGetNotFound(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	_, err := b.Get(context.Background(), "missing", 0, 0)
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestList(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	for _, k := range []string{"a.txt", "b.txt", "dir/c.txt"} {
		body := []byte("body of " + k)
		if _, err := b.Put(ctx, k, strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{}); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	t.Run("recursive", func(t *testing.T) {
		ch, err := b.List(ctx, "", true)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		got := map[string]bool{}
		for e := range ch {
			if e.Err != nil {
				t.Fatalf("List entry err: %v", e.Err)
			}
			got[e.Key] = true
		}
		for _, want := range []string{"a.txt", "b.txt", "dir/c.txt"} {
			if !got[want] {
				t.Errorf("missing key %s in list (got %v)", want, got)
			}
		}
	})

	t.Run("non-recursive emits commonprefix", func(t *testing.T) {
		ch, err := b.List(ctx, "", false)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		dirSeen := false
		for e := range ch {
			if e.Err != nil {
				t.Fatalf("err: %v", e.Err)
			}
			if e.Key == "dir/" && e.IsDir {
				dirSeen = true
			}
		}
		if !dirSeen {
			t.Error("expected dir/ to be returned as IsDir entry when non-recursive")
		}
	})
}

func TestDelete(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	body := []byte("ephemeral")
	if _, err := b.Put(ctx, "tmp", strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := b.Delete(ctx, "tmp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, _, _, err := b.Head(ctx, "tmp")
	if !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("after Delete: expected ErrKeyNotFound, got %v", err)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	// Deleting a never-existed key is not an error.
	if err := b.Delete(context.Background(), "neverexisted"); err != nil {
		t.Errorf("Delete on missing key should be idempotent, got %v", err)
	}
}

func TestRename(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	ctx := context.Background()
	body := []byte("rename me")
	if _, err := b.Put(ctx, "old.txt", strings.NewReader(string(body)), int64(len(body)), backend.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := b.Rename(ctx, "old.txt", "new.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	// old should be gone, new should exist
	if _, _, _, err := b.Head(ctx, "old.txt"); !errors.Is(err, backend.ErrKeyNotFound) {
		t.Errorf("old key still exists after Rename: %v", err)
	}
	rc, err := b.Get(ctx, "new.txt", 0, 0)
	if err != nil {
		t.Fatalf("Get new.txt: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(body) {
		t.Errorf("renamed content: got %q, want %q", got, body)
	}
}

func TestAbortStaleMultipartUploads_None(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	n, err := b.AbortStaleMultipartUploads(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("AbortStaleMultipartUploads: %v", err)
	}
	if n != 0 {
		t.Errorf("aborted = %d, want 0", n)
	}
}

func TestAbortStaleMultipartUploads_WithStale(t *testing.T) {
	t.Parallel()
	m := newMockS3("test-bucket")
	defer m.close()
	b := newTestBackend(t, m)
	defer b.Close()

	// Inject 3 stale (1h old) + 1 fresh upload into the mock.
	stale := time.Now().Add(-time.Hour)
	fresh := time.Now()
	m.mu.Lock()
	m.uploads["upload-stale-1"] = &mockUpload{key: "a", uploadID: "upload-stale-1", parts: map[int][]byte{}, initiated: stale}
	m.uploads["upload-stale-2"] = &mockUpload{key: "b", uploadID: "upload-stale-2", parts: map[int][]byte{}, initiated: stale}
	m.uploads["upload-stale-3"] = &mockUpload{key: "c", uploadID: "upload-stale-3", parts: map[int][]byte{}, initiated: stale}
	m.uploads["upload-fresh"] = &mockUpload{key: "d", uploadID: "upload-fresh", parts: map[int][]byte{}, initiated: fresh}
	m.mu.Unlock()

	// Abort uploads older than 30 minutes.
	n, err := b.AbortStaleMultipartUploads(context.Background(), 30*time.Minute)
	if err != nil {
		t.Fatalf("AbortStaleMultipartUploads: %v", err)
	}
	if n != 3 {
		t.Errorf("aborted = %d, want 3", n)
	}
	// Verify the fresh upload remains.
	m.mu.Lock()
	_, freshStill := m.uploads["upload-fresh"]
	remaining := len(m.uploads)
	m.mu.Unlock()
	if !freshStill {
		t.Error("fresh upload should still exist after abort")
	}
	if remaining != 1 {
		t.Errorf("remaining uploads = %d, want 1 (only the fresh one)", remaining)
	}

	// Idempotent: second run with same cutoff finds nothing to abort.
	n2, err := b.AbortStaleMultipartUploads(context.Background(), 30*time.Minute)
	if err != nil {
		t.Fatalf("second AbortStaleMultipartUploads: %v", err)
	}
	if n2 != 0 {
		t.Errorf("second run aborted = %d, want 0", n2)
	}
}

// guard so a forgotten test env var doesn't accidentally hit real AWS.
func TestNoAccidentalRealAWS(t *testing.T) {
	if os.Getenv("AWS_PROFILE") != "" || os.Getenv("AWS_ACCESS_KEY_ID") != "" {
		t.Log("note: AWS_PROFILE/AWS_ACCESS_KEY_ID set in env — tests still use mock endpoint, but be cautious")
	}
}
