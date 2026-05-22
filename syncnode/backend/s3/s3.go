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

// Package s3 implements the syncnode Backend interface against S3 and
// S3-compatible object stores (MinIO, Ceph RGW, AWS S3, plus 3rd-party
// gateways exposing an S3 endpoint).
//
// SDK: aws-sdk-go-v2. Multipart upload uses feature/s3/manager.Uploader,
// which handles chunking, parallel parts, and AbortMultipartUpload on
// failure automatically.
//
// See design.md §3.4 (s3 kind), §9 C-2 (this implementation), §9 C-3
// (orphan multipart cleanup — see multipart_cleanup.go), and §10.6
// (Backend interface contract).
package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// Default thresholds — see design.md §9 C-2 for rationale.
const (
	defaultMultipartThresholdMiB = 64
	defaultPartSizeMiB           = 16
	defaultListMaxKeys           = 1000
	mib                          = 1 << 20
)

// Config is the per-Backend instance configuration. One Backend instance
// is bound to a single bucket; cross-bucket transfers go through two
// Backend instances.
type Config struct {
	// Endpoint is the S3 endpoint URL, e.g.
	// https://s3.cn-north-1.amazonaws.com.cn or http://minio:9000.
	// Required.
	Endpoint string

	// Region is the AWS region, e.g. cn-north-1. Required (use "us-east-1"
	// for MinIO if not otherwise meaningful).
	Region string

	// Bucket the single bucket this Backend talks to. Required.
	Bucket string

	// AccessKeyEnv / SecretKeyEnv are the names of the environment
	// variables holding the AK / SK. Reading from env (not from disk and
	// not from config) keeps credentials out of config files. Both
	// required unless AccessKey / SecretKey are set directly.
	AccessKeyEnv string
	SecretKeyEnv string

	// AccessKey / SecretKey are inline credentials injected by the dashboard
	// (Approach C). When non-empty, these take precedence over AccessKeyEnv /
	// SecretKeyEnv and no environment variable lookup is performed.
	AccessKey string
	SecretKey string

	// StorageClass is the default storage class applied to PutObject when
	// PutOptions.StorageClass is empty. May itself be empty (S3 picks
	// STANDARD).
	StorageClass string

	// UsePathStyle forces path-style addressing (bucket in URL path
	// instead of hostname). Required for MinIO, optional for AWS S3.
	UsePathStyle bool

	// MultipartThresholdMiB: PutObject larger than this triggers
	// multipart upload. Default 64.
	MultipartThresholdMiB int

	// PartSizeMiB: chunk size for multipart upload. Default 16. S3
	// minimum is 5 MiB.
	PartSizeMiB int

	// InsecureSkipVerify disables TLS certificate verification. Only
	// use in dev/test environments where the S3 endpoint's CA is not
	// trusted (e.g. containers without a ca-certificates bundle).
	InsecureSkipVerify bool
}

// validate checks required fields and applies defaults. Returns
// backend.ErrConfigInvalid (wrapped) on failure.
func (c *Config) validate() error {
	if c == nil {
		return fmt.Errorf("%w: nil Config", backend.ErrConfigInvalid)
	}
	if c.Bucket == "" {
		return fmt.Errorf("%w: missing Bucket", backend.ErrConfigInvalid)
	}
	if c.Endpoint == "" {
		return fmt.Errorf("%w: missing Endpoint", backend.ErrConfigInvalid)
	}
	// Normalize bare hostnames: AWS SDK v2 requires BaseEndpoint to be a
	// full URI. Add https:// when no scheme is present so callers don't have
	// to include the scheme explicitly.
	if !strings.HasPrefix(c.Endpoint, "http://") && !strings.HasPrefix(c.Endpoint, "https://") {
		c.Endpoint = "https://" + c.Endpoint
	}
	if c.Region == "" {
		return fmt.Errorf("%w: missing Region", backend.ErrConfigInvalid)
	}
	if c.AccessKeyEnv == "" && c.AccessKey == "" {
		return fmt.Errorf("%w: missing AccessKeyEnv (or inline AccessKey)", backend.ErrConfigInvalid)
	}
	if c.SecretKeyEnv == "" && c.SecretKey == "" {
		return fmt.Errorf("%w: missing SecretKeyEnv (or inline SecretKey)", backend.ErrConfigInvalid)
	}
	if c.MultipartThresholdMiB <= 0 {
		c.MultipartThresholdMiB = defaultMultipartThresholdMiB
	}
	if c.PartSizeMiB <= 0 {
		c.PartSizeMiB = defaultPartSizeMiB
	}
	if c.PartSizeMiB < 5 {
		return fmt.Errorf("%w: PartSizeMiB must be >= 5 (S3 minimum)", backend.ErrConfigInvalid)
	}
	return nil
}

// Backend is the S3 Backend implementation. Safe for concurrent use by
// multiple goroutines (the underlying *awss3.Client and *manager.Uploader
// are themselves goroutine-safe).
type Backend struct {
	client     *awss3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	bucket     string
	cfg        *Config
}

// New constructs a Backend from cfg (which must be *Config). Required by
// the registry pattern — see backend/registry.go.
//
// Credentials are loaded from env vars named by cfg.AccessKeyEnv /
// SecretKeyEnv. Empty values are permitted (anonymous / IAM-role auth)
// but the env var names themselves must be configured.
func New(cfg interface{}) (backend.Backend, error) {
	c, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("%w: s3 backend requires *s3.Config, got %T", backend.ErrConfigInvalid, cfg)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}

	// Prefer inline credentials; fall back to env var lookup.
	ak := c.AccessKey
	sk := c.SecretKey
	if ak == "" && c.AccessKeyEnv != "" {
		ak = os.Getenv(c.AccessKeyEnv)
	}
	if sk == "" && c.SecretKeyEnv != "" {
		sk = os.Getenv(c.SecretKeyEnv)
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(c.Region),
	}
	if ak != "" && sk != "" {
		loadOpts = append(loadOpts,
			awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(ak, sk, ""),
			),
		)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3 backend: load aws config: %w", err)
	}

	clientOpts := []func(*awss3.Options){
		func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(c.Endpoint)
			o.UsePathStyle = c.UsePathStyle
			if c.InsecureSkipVerify {
				o.HTTPClient = &http.Client{
					Transport: &http.Transport{
						TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
					},
				}
			}
		},
	}
	client := awss3.NewFromConfig(awsCfg, clientOpts...)

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = int64(c.PartSizeMiB) * mib
	})
	downloader := manager.NewDownloader(client, func(d *manager.Downloader) {
		d.PartSize = int64(c.PartSizeMiB) * mib
	})

	return &Backend{
		client:     client,
		uploader:   uploader,
		downloader: downloader,
		bucket:     c.Bucket,
		cfg:        c,
	}, nil
}

func init() {
	backend.Register("s3", New)
}

// Kind implements Backend.
func (b *Backend) Kind() string { return "s3" }

// Capabilities implements Backend. S3 has been strongly consistent for
// read-after-write since Dec 2020.
func (b *Backend) Capabilities() backend.Caps {
	return backend.Caps{
		RangeRead:         true,
		Multipart:         true,
		AtomicRename:      false, // copy+delete, not atomic
		ListMaxKeys:       defaultListMaxKeys,
		StrongConsistency: true,
		// S3 ETag is a server-side hash usable as a cheap integrity
		// signal (single-part: full-object MD5; multipart: documented
		// per-part hash). When the user-metadata sha256 we persist on
		// upload is present GetChecksum returns that with O(1) cost; even
		// when it isn't, the ETag fallback still lets the executor short-
		// circuit a redundant src-side hash. Reported true so the executor
		// can skip the `requireSrcChecksum` path when both ends are native.
		NativeChecksum: true,
		// Honored on Put via x-amz-meta-syncnode-mtime; Head re-reads it
		// and prefers it over LastModified. NOTE: ListObjectsV2 does NOT
		// return user metadata, so List still falls back to LastModified.
		NativeMtimeWrite: true,
		// S3 supports server-side copy via CopyObject (≤5 GiB) and via
		// UploadPartCopy (>5 GiB) inside the multipart-upload protocol.
		// See ServerSideCopy below. Executor checks SameInstance AND this
		// flag before invoking the fast path.
		ServerSideCopy: true,
	}
}

// SameInstance reports whether other points at the same S3 realm: same
// endpoint, same region, and same credential identity. Bucket is
// deliberately NOT part of the equality — cross-bucket server-side copy
// is supported by the S3 API as long as the credentials cover both
// buckets. AccessKey is compared via a stable hash so plaintext never
// leaves the Backend (caller could log mismatched values otherwise).
func (b *Backend) SameInstance(other backend.Backend) bool {
	o, ok := other.(*Backend)
	if !ok || o == nil || b.cfg == nil || o.cfg == nil {
		return false
	}
	if b.cfg.Endpoint != o.cfg.Endpoint {
		return false
	}
	if b.cfg.Region != o.cfg.Region {
		return false
	}
	if credentialFingerprint(b.cfg) != credentialFingerprint(o.cfg) {
		return false
	}
	return true
}

// credentialFingerprint returns a stable hash of the credential identity
// used by cfg. Prefers inline AccessKey when set, else falls back to the
// env-var NAME (NOT its value, which would force an env lookup and
// re-introduce races against secret rotation). We hash so a caller logging
// the value can't recover the AK.
func credentialFingerprint(c *Config) string {
	src := c.AccessKey
	if src == "" {
		src = c.AccessKeyEnv
	}
	sum := sha256.Sum256([]byte(src))
	return hex.EncodeToString(sum[:])
}

// Close implements Backend. The underlying SDK client uses Go's
// http.Client which doesn't need an explicit close — connections are
// returned to the transport pool. Safe to call multiple times.
func (b *Backend) Close() error { return nil }

// Get implements Backend. For size > 0, issues a ranged GET. For
// size == 0, reads to end of object. The returned reader streams from S3;
// callers must Close it to release the HTTP connection.
//
// Note: deliberately uses a plain GetObject (not manager.Downloader) so
// the caller gets a stream rather than having the whole object buffered
// in memory. Range coalescing via Downloader is only useful when the
// caller can WriteAt to a file; the executor handles its own
// chunked range fetches at a higher level.
func (b *Backend) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	input := &awss3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}
	if size > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", off, off+size-1))
	} else if off > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", off))
	}
	out, err := b.client.GetObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return nil, backend.ErrKeyNotFound
		}
		return nil, fmt.Errorf("s3 Get %s/%s: %w", b.bucket, key, err)
	}
	return out.Body, nil
}

// Head implements Backend. ETag is returned without surrounding quotes
// (S3 wraps it in quotes; callers expect bare). When the
// `x-amz-meta-syncnode-mtime` header is present its parsed RFC3339Nano value
// is returned in preference to LastModified — this lets callers see the
// source-side modification time stamped at upload (see PutOptions.Mtime).
func (b *Backend) Head(ctx context.Context, key string) (int64, string, time.Time, error) {
	out, err := b.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return 0, "", time.Time{}, backend.ErrKeyNotFound
		}
		return 0, "", time.Time{}, fmt.Errorf("s3 Head %s/%s: %w", b.bucket, key, err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	etag := strings.Trim(aws.ToString(out.ETag), `"`)
	mtime := parseSyncnodeMtime(out.Metadata)
	if mtime.IsZero() && out.LastModified != nil {
		mtime = *out.LastModified
	}
	return size, etag, mtime, nil
}

// parseSyncnodeMtime extracts and parses the syncnode-mtime user-metadata
// header. Returns the zero time on miss or parse error so callers can fall
// back to LastModified.
func parseSyncnodeMtime(md map[string]string) time.Time {
	if len(md) == 0 {
		return time.Time{}
	}
	v, ok := md[backend.MtimeMetadataKey]
	if !ok || v == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}
	}
	return t
}

// Put implements Backend. Routes between simple PutObject and multipart
// Upload based on size and opts.Multipart. Multipart path uses
// manager.Uploader which handles AbortMultipartUpload on error
// automatically (no orphan parts on transient failures).
//
// When opts.ComputeChecksum is true, the body stream is tee'd through a
// sha256 hasher so the digest is computed during the upload itself (no
// extra read). Because the manager.Uploader is a streaming multipart driver
// we cannot stamp the metadata header in the original PUT — the digest is
// only known once the body has been fully consumed. The implementation
// therefore performs a follow-up CopyObject with MetadataDirective=REPLACE
// to write `x-amz-meta-syncnode-sha256` onto the just-uploaded object.
// Should the metadata copy fail (e.g. permissions), we log and continue:
// the checksum still lives in PutResult so the caller's contract holds, and
// a subsequent GetChecksum will fall back to the ETag path.
func (b *Backend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	storageClass := opts.StorageClass
	if storageClass == "" {
		storageClass = b.cfg.StorageClass
	}

	threshold := int64(b.cfg.MultipartThresholdMiB) * mib
	useMultipart := opts.Multipart || (size > 0 && size > threshold) || size < 0

	// Optional sha256 sink: wraps body so the upload pulls bytes through
	// the hasher. sumFn() returns the hex digest after the upload finishes.
	src := body
	var sumFn func() string
	if opts.ComputeChecksum {
		h, fn := backend.NewSHA256Sink()
		src = io.TeeReader(body, h)
		sumFn = fn
	}

	if !useMultipart {
		// Buffer small bodies so we can pass an io.ReadSeeker that the
		// SDK signer can checksum (single-shot PutObject requires a
		// seekable body for v4 signing).
		buf, err := io.ReadAll(src)
		if err != nil {
			return backend.PutResult{}, fmt.Errorf("s3 Put %s/%s: read body: %w", b.bucket, key, err)
		}
		if size >= 0 && int64(len(buf)) != size {
			return backend.PutResult{}, fmt.Errorf("s3 Put %s/%s: body length %d != declared size %d",
				b.bucket, key, len(buf), size)
		}
		input := &awss3.PutObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(buf),
		}
		if storageClass != "" {
			input.StorageClass = s3types.StorageClass(storageClass)
		}
		if opts.ContentType != "" {
			input.ContentType = aws.String(opts.ContentType)
		}
		// Merge caller metadata + (optionally) sha256 + (optionally) source
		// mtime so the single-shot path can stamp everything in the original
		// PUT — no follow-up Copy needed, saves one round trip.
		md := mergeMetadata(opts.Metadata, sumFn, opts.Mtime)
		if len(md) > 0 {
			input.Metadata = md
		}
		out, err := b.client.PutObject(ctx, input)
		if err != nil {
			return backend.PutResult{}, fmt.Errorf("s3 PutObject %s/%s: %w", b.bucket, key, err)
		}
		res := backend.PutResult{
			ETag:     strings.Trim(aws.ToString(out.ETag), `"`),
			BytesPut: int64(len(buf)),
		}
		if sumFn != nil {
			// sumFn already invoked inside mergeMetadata; recompute from the
			// merged map's value to avoid double-Sum on the hash.
			if v, ok := md[backend.SHA256MetadataKey]; ok {
				res.Checksum = v
				res.Algorithm = backend.ChecksumAlgorithmSHA256
			}
		}
		return res, nil
	}

	// Multipart path.
	partSize := opts.PartSizeMiB
	if partSize <= 0 {
		partSize = b.cfg.PartSizeMiB
	}
	input := &awss3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   src,
	}
	if storageClass != "" {
		input.StorageClass = s3types.StorageClass(storageClass)
	}
	if opts.ContentType != "" {
		input.ContentType = aws.String(opts.ContentType)
	}
	// Mtime is known up-front (does not require the stream to finish), so we
	// stamp it on the original multipart Upload. sha256 still needs a post-
	// upload CopyObject because the digest is only known once the body is
	// drained.
	if md := mergeMetadata(opts.Metadata, nil, opts.Mtime); len(md) > 0 {
		input.Metadata = md
	}
	out, err := b.uploader.Upload(ctx, input, func(u *manager.Uploader) {
		u.PartSize = int64(partSize) * mib
	})
	if err != nil {
		return backend.PutResult{}, fmt.Errorf("s3 Upload %s/%s: %w", b.bucket, key, err)
	}

	res := backend.PutResult{
		ETag:     strings.Trim(aws.ToString(out.ETag), `"`),
		BytesPut: size,
	}
	if sumFn != nil {
		hexSum := sumFn()
		res.Checksum = hexSum
		res.Algorithm = backend.ChecksumAlgorithmSHA256
		// Stamp metadata via REPLACE-mode CopyObject. We must preserve
		// caller-supplied opts.Metadata AND the mtime entry that was already
		// on the original Upload — REPLACE overwrites the whole map.
		copyMeta := mergeMetadata(opts.Metadata, func() string { return hexSum }, opts.Mtime)
		if cerr := b.stampSHA256Metadata(ctx, key, storageClass, opts.ContentType, copyMeta); cerr != nil {
			// Best-effort: surface as a warning. Do NOT fail the Put — the
			// checksum is still in res.Checksum and the executor can act on
			// it. GetChecksum will degrade to the ETag fallback.
			fmt.Fprintf(os.Stderr, "s3 Put %s/%s: stamp sha256 metadata: %v\n", b.bucket, key, cerr)
		}
	}
	return res, nil
}

// mergeMetadata returns a new map with caller-supplied entries plus the
// sha256 entry sourced from sumFn (if any) and the syncnode-mtime entry
// derived from mtime (if non-nil). Returns nil when all sources are empty so
// callers can leave PutObjectInput.Metadata unset rather than passing an
// empty map (some servers treat empty-map differently).
func mergeMetadata(user map[string]string, sumFn func() string, mtime *time.Time) map[string]string {
	if sumFn == nil && len(user) == 0 && mtime == nil {
		return nil
	}
	out := make(map[string]string, len(user)+2)
	for k, v := range user {
		out[k] = v
	}
	if sumFn != nil {
		out[backend.SHA256MetadataKey] = sumFn()
	}
	if mtime != nil {
		out[backend.MtimeMetadataKey] = mtime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// stampSHA256Metadata performs a CopyObject onto key with
// MetadataDirective=REPLACE so the user-metadata map (containing the
// sha256) is written without re-uploading the data. CopySource is the
// just-written object itself.
func (b *Backend) stampSHA256Metadata(ctx context.Context, key, storageClass, contentType string, md map[string]string) error {
	if len(md) == 0 {
		return nil
	}
	input := &awss3.CopyObjectInput{
		Bucket:            aws.String(b.bucket),
		Key:               aws.String(key),
		CopySource:        aws.String(b.bucket + "/" + key),
		Metadata:          md,
		MetadataDirective: s3types.MetadataDirectiveReplace,
	}
	if storageClass != "" {
		input.StorageClass = s3types.StorageClass(storageClass)
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}
	_, err := b.client.CopyObject(ctx, input)
	return err
}

// GetChecksum implements Backend. Resolution order:
//
//  1. HeadObject → user metadata `syncnode-sha256`. Set on upload by Put
//     when ComputeChecksum=true. Returns (hex, "sha256", nil).
//  2. ETag fallback. Single-part ETags are an MD5 of the object body and
//     are useful for cross-endpoint comparison; we surface them as
//     algorithm "md5". Multipart ETags (suffixed with "-N") are NOT
//     comparable across servers — we return ErrChecksumMismatch so the
//     executor can decide to either skip the check or re-hash.
//  3. Missing key → ErrKeyNotFound.
func (b *Backend) GetChecksum(ctx context.Context, key string) (string, string, error) {
	out, err := b.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return "", "", backend.ErrKeyNotFound
		}
		return "", "", fmt.Errorf("s3 HeadObject %s/%s: %w", b.bucket, key, err)
	}
	// 1. user metadata (sdk strips x-amz-meta- prefix; lookups are
	//    case-insensitive per HTTP, but the SDK normalises to lowercase).
	if out.Metadata != nil {
		if v, ok := out.Metadata[backend.SHA256MetadataKey]; ok && v != "" {
			return v, backend.ChecksumAlgorithmSHA256, nil
		}
	}
	// 2. ETag fallback.
	etag := strings.Trim(aws.ToString(out.ETag), `"`)
	if etag != "" && !strings.Contains(etag, "-") && len(etag) == 32 {
		// Single-part: 32 hex chars, no dash. Standard MD5.
		return etag, backend.ChecksumAlgorithmMD5, nil
	}
	// Multipart ETag has the form `<hex>-<partCount>` and is NOT a
	// content hash compatible across endpoints. The caller has to fall
	// back to streaming compute.
	return "", "", backend.ErrChecksumMismatch
}

// Delete implements Backend. Idempotent: missing keys are not an error.
func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("s3 Delete %s/%s: %w", b.bucket, key, err)
	}
	return nil
}

// Rename implements Backend. S3 has no atomic rename; this is copy +
// delete. Capabilities().AtomicRename == false. The copy uses
// server-side CopyObject; for objects >5 GiB the caller would need to
// use UploadPartCopy — not implemented here because syncnode rules
// don't currently exercise that path (the executor does its own
// copy-via-stream when source/dest are different backends).
func (b *Backend) Rename(ctx context.Context, oldKey, newKey string) error {
	copySource := b.bucket + "/" + oldKey
	if _, err := b.client.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     aws.String(b.bucket),
		Key:        aws.String(newKey),
		CopySource: aws.String(copySource),
	}); err != nil {
		if isNotFound(err) {
			return backend.ErrKeyNotFound
		}
		return fmt.Errorf("s3 Rename copy %s/%s -> %s: %w", b.bucket, oldKey, newKey, err)
	}
	return b.Delete(ctx, oldKey)
}

// S3 server-side copy limits. CopyObject permits a single-shot copy up to
// 5 GiB; larger objects must use multipart-copy via UploadPartCopy. Both
// the threshold and the part size are package vars so tests can shrink
// them without inflating fixtures past the integration-test budget.
const serverSideCopySingleMaxDefault int64 = 5 * 1024 * 1024 * 1024

var (
	serverSideCopySingleMaxOverride int64 = serverSideCopySingleMaxDefault
	serverSideCopyPartSize          int64 = 100 * 1024 * 1024 // 100 MiB
)

// ServerSideCopy implements backend.ServerSideCopier. Caller MUST have
// asserted both SameInstance and Caps.ServerSideCopy beforehand.
//
// Strategy:
//   - srcSize ≤ 5 GiB: single CopyObject call (one round trip).
//   - srcSize > 5 GiB: multipart-copy protocol (CreateMultipartUpload +
//     loop of UploadPartCopy with byte-range copy-source + Complete).
//
// PutOptions semantics:
//   - StorageClass / ContentType / Metadata are passed through.
//   - Mtime, if set, is persisted as `x-amz-meta-syncnode-mtime` (RFC3339Nano)
//     just like a normal Put — both single and multipart paths respect it.
//   - ComputeChecksum is a hint only: server-side copy never restreams
//     bytes through the executor, so we cannot recompute sha256. The
//     returned PutResult carries the destination ETag and an empty
//     Checksum/Algorithm. The caller that asked for ComputeChecksum can
//     fall back to GetChecksum(dst) afterwards if needed.
func (b *Backend) ServerSideCopy(ctx context.Context, srcKey, dstKey string, opts backend.PutOptions) (backend.PutResult, error) {
	if srcKey == "" || dstKey == "" {
		return backend.PutResult{}, fmt.Errorf("s3 ServerSideCopy: empty src/dst key")
	}
	head, err := b.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(srcKey),
	})
	if err != nil {
		if isNotFound(err) {
			return backend.PutResult{}, backend.ErrKeyNotFound
		}
		return backend.PutResult{}, fmt.Errorf("s3 ServerSideCopy head %s/%s: %w", b.bucket, srcKey, err)
	}
	var size int64
	if head.ContentLength != nil {
		size = *head.ContentLength
	}
	if size <= serverSideCopySingleMaxOverride {
		return b.serverSideCopySingle(ctx, srcKey, dstKey, size, opts)
	}
	return b.serverSideCopyMultipart(ctx, srcKey, dstKey, size, opts)
}

// serverSideCopySingle issues a single CopyObject. Object stores honor
// MetadataDirective=REPLACE to overwrite metadata in the new copy; we set
// it only when the caller supplied at least one metadata directive (mtime,
// content type, custom metadata) so the default cheap path stays cheap.
func (b *Backend) serverSideCopySingle(ctx context.Context, srcKey, dstKey string, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	copySource := b.bucket + "/" + srcKey
	input := &awss3.CopyObjectInput{
		Bucket:     aws.String(b.bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(copySource),
	}
	if opts.StorageClass != "" {
		input.StorageClass = s3types.StorageClass(opts.StorageClass)
	}

	md, replace := buildCopyMetadata(opts)
	if replace {
		input.MetadataDirective = s3types.MetadataDirectiveReplace
		input.Metadata = md
		if opts.ContentType != "" {
			input.ContentType = aws.String(opts.ContentType)
		}
	}

	out, err := b.client.CopyObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return backend.PutResult{}, backend.ErrKeyNotFound
		}
		return backend.PutResult{}, fmt.Errorf("s3 ServerSideCopy single %s/%s -> %s: %w", b.bucket, srcKey, dstKey, err)
	}
	etag := ""
	if out.CopyObjectResult != nil && out.CopyObjectResult.ETag != nil {
		etag = strings.Trim(*out.CopyObjectResult.ETag, `"`)
	}
	return backend.PutResult{ETag: etag, BytesPut: size}, nil
}

// serverSideCopyMultipart drives the multipart-copy protocol for objects
// larger than 5 GiB. Aborts the upload on any error so we don't leave
// orphan parts (orphans are also cleaned by multipart_cleanup.go but
// best-effort abort here is cheaper and faster).
func (b *Backend) serverSideCopyMultipart(ctx context.Context, srcKey, dstKey string, size int64, opts backend.PutOptions) (backend.PutResult, error) {
	createIn := &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(dstKey),
	}
	if opts.StorageClass != "" {
		createIn.StorageClass = s3types.StorageClass(opts.StorageClass)
	}
	md, replace := buildCopyMetadata(opts)
	if replace {
		createIn.Metadata = md
		if opts.ContentType != "" {
			createIn.ContentType = aws.String(opts.ContentType)
		}
	}

	created, err := b.client.CreateMultipartUpload(ctx, createIn)
	if err != nil {
		return backend.PutResult{}, fmt.Errorf("s3 ServerSideCopy create multipart %s/%s: %w", b.bucket, dstKey, err)
	}
	uploadID := aws.ToString(created.UploadId)

	abort := func() {
		if uploadID == "" {
			return
		}
		_, _ = b.client.AbortMultipartUpload(context.Background(), &awss3.AbortMultipartUploadInput{
			Bucket:   aws.String(b.bucket),
			Key:      aws.String(dstKey),
			UploadId: aws.String(uploadID),
		})
	}

	partSize := serverSideCopyPartSize
	if partSize <= 0 {
		partSize = 100 * 1024 * 1024
	}
	copySource := b.bucket + "/" + srcKey
	parts := make([]s3types.CompletedPart, 0, (size/partSize)+1)
	var off int64
	var partNum int32 = 1
	for off < size {
		end := off + partSize - 1
		if end >= size {
			end = size - 1
		}
		rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)
		out, err := b.client.UploadPartCopy(ctx, &awss3.UploadPartCopyInput{
			Bucket:          aws.String(b.bucket),
			Key:             aws.String(dstKey),
			UploadId:        aws.String(uploadID),
			PartNumber:      aws.Int32(partNum),
			CopySource:      aws.String(copySource),
			CopySourceRange: aws.String(rangeHeader),
		})
		if err != nil {
			abort()
			return backend.PutResult{}, fmt.Errorf("s3 ServerSideCopy part %d %s/%s -> %s: %w", partNum, b.bucket, srcKey, dstKey, err)
		}
		etag := ""
		if out.CopyPartResult != nil && out.CopyPartResult.ETag != nil {
			etag = *out.CopyPartResult.ETag
		}
		parts = append(parts, s3types.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int32(partNum),
		})
		off = end + 1
		partNum++
	}

	done, err := b.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:          aws.String(b.bucket),
		Key:             aws.String(dstKey),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		abort()
		return backend.PutResult{}, fmt.Errorf("s3 ServerSideCopy complete %s/%s: %w", b.bucket, dstKey, err)
	}
	etag := ""
	if done.ETag != nil {
		etag = strings.Trim(*done.ETag, `"`)
	}
	return backend.PutResult{ETag: etag, BytesPut: size}, nil
}

// buildCopyMetadata assembles the user-metadata map to apply on the
// destination object, plus a boolean signaling whether the caller passed
// any directive that requires MetadataDirective=REPLACE. Mtime is encoded
// the same way Put does (RFC3339Nano under x-amz-meta-syncnode-mtime) so
// Head reads it back consistently.
func buildCopyMetadata(opts backend.PutOptions) (map[string]string, bool) {
	if len(opts.Metadata) == 0 && opts.Mtime == nil && opts.ContentType == "" {
		return nil, false
	}
	md := make(map[string]string, len(opts.Metadata)+1)
	for k, v := range opts.Metadata {
		md[k] = v
	}
	if opts.Mtime != nil {
		md[backend.MtimeMetadataKey] = opts.Mtime.UTC().Format(time.RFC3339Nano)
	}
	return md, true
}

// List implements Backend. Streams entries on the returned channel.
// When recursive == false, sets Delimiter="/" and emits CommonPrefixes
// as Entries with IsDir=true. The channel is closed when the listing
// completes or ctx is cancelled; an Entry with non-nil Err signals
// abnormal termination.
func (b *Backend) List(ctx context.Context, prefix string, recursive bool) (<-chan backend.Entry, error) {
	ch := make(chan backend.Entry, 256)
	go func() {
		defer close(ch)
		input := &awss3.ListObjectsV2Input{
			Bucket: aws.String(b.bucket),
			Prefix: aws.String(prefix),
		}
		if !recursive {
			input.Delimiter = aws.String("/")
		}
		paginator := awss3.NewListObjectsV2Paginator(b.client, input)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				select {
				case ch <- backend.Entry{Err: fmt.Errorf("s3 List %s/%s: %w", b.bucket, prefix, err)}:
				case <-ctx.Done():
				}
				return
			}
			// CommonPrefixes first (only when !recursive).
			for _, cp := range page.CommonPrefixes {
				e := backend.Entry{
					Key:   aws.ToString(cp.Prefix),
					IsDir: true,
				}
				select {
				case ch <- e:
				case <-ctx.Done():
					return
				}
			}
			for _, obj := range page.Contents {
				e := backend.Entry{
					Key:  aws.ToString(obj.Key),
					ETag: strings.Trim(aws.ToString(obj.ETag), `"`),
				}
				if obj.Size != nil {
					e.Size = *obj.Size
				}
				if obj.LastModified != nil {
					e.Mtime = *obj.LastModified
				}
				select {
				case ch <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}

// isNotFound recognises both the typed NoSuchKey error and 404 HTTP
// responses (HeadObject returns the latter — it has no typed
// NoSuchKey response from the SDK because HEAD has no body).
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var notFound *s3types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var httpErr *smithyhttp.ResponseError
	if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	return false
}
