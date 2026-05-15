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
	"crypto/tls"
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
	}
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
// (S3 wraps it in quotes; callers expect bare).
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
	var mtime time.Time
	if out.LastModified != nil {
		mtime = *out.LastModified
	}
	return size, etag, mtime, nil
}

// Put implements Backend. Routes between simple PutObject and multipart
// Upload based on size and opts.Multipart. Multipart path uses
// manager.Uploader which handles AbortMultipartUpload on error
// automatically (no orphan parts on transient failures).
func (b *Backend) Put(ctx context.Context, key string, body io.Reader, size int64, opts backend.PutOptions) (string, error) {
	storageClass := opts.StorageClass
	if storageClass == "" {
		storageClass = b.cfg.StorageClass
	}

	threshold := int64(b.cfg.MultipartThresholdMiB) * mib
	useMultipart := opts.Multipart || (size > 0 && size > threshold) || size < 0

	if !useMultipart {
		// Buffer small bodies so we can pass an io.ReadSeeker that the
		// SDK signer can checksum (single-shot PutObject requires a
		// seekable body for v4 signing).
		buf, err := io.ReadAll(body)
		if err != nil {
			return "", fmt.Errorf("s3 Put %s/%s: read body: %w", b.bucket, key, err)
		}
		if size >= 0 && int64(len(buf)) != size {
			return "", fmt.Errorf("s3 Put %s/%s: body length %d != declared size %d",
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
		if len(opts.Metadata) > 0 {
			input.Metadata = opts.Metadata
		}
		out, err := b.client.PutObject(ctx, input)
		if err != nil {
			return "", fmt.Errorf("s3 PutObject %s/%s: %w", b.bucket, key, err)
		}
		return strings.Trim(aws.ToString(out.ETag), `"`), nil
	}

	// Multipart path.
	partSize := opts.PartSizeMiB
	if partSize <= 0 {
		partSize = b.cfg.PartSizeMiB
	}
	input := &awss3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   body,
	}
	if storageClass != "" {
		input.StorageClass = s3types.StorageClass(storageClass)
	}
	if opts.ContentType != "" {
		input.ContentType = aws.String(opts.ContentType)
	}
	if len(opts.Metadata) > 0 {
		input.Metadata = opts.Metadata
	}
	out, err := b.uploader.Upload(ctx, input, func(u *manager.Uploader) {
		u.PartSize = int64(partSize) * mib
	})
	if err != nil {
		return "", fmt.Errorf("s3 Upload %s/%s: %w", b.bucket, key, err)
	}
	return strings.Trim(aws.ToString(out.ETag), `"`), nil
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
