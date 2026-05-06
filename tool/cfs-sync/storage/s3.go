package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Config holds connection parameters for an S3-compatible backend.
type S3Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
	Bucket    string
	NoSSL     bool
}

// S3Storage implements Storage for AWS S3 and compatible services.
type S3Storage struct {
	cfg    S3Config
	client *s3.S3
}

// NewS3 creates an S3Storage.
func NewS3(cfg S3Config) (*S3Storage, error) {
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	awsCfg := &aws.Config{
		Region: aws.String(cfg.Region),
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		awsCfg.Credentials = credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, "")
	}
	if cfg.Endpoint != "" {
		awsCfg.Endpoint = aws.String(cfg.Endpoint)
		awsCfg.S3ForcePathStyle = aws.Bool(true)
	}
	if cfg.NoSSL {
		awsCfg.DisableSSL = aws.Bool(true)
	}

	sess, err := session.NewSession(awsCfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 session: %w", err)
	}

	return &S3Storage{cfg: cfg, client: s3.New(sess)}, nil
}

func (s *S3Storage) String() string {
	return fmt.Sprintf("s3://%s", s.cfg.Bucket)
}

func (s *S3Storage) List(ctx context.Context, prefix string) (<-chan *Object, <-chan error) {
	objects := make(chan *Object, 256)
	errc := make(chan error, 1)

	prefix = strings.TrimPrefix(prefix, "/")

	go func() {
		defer close(objects)
		defer close(errc)

		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(s.cfg.Bucket),
			Prefix: aws.String(prefix),
		}
		err := s.client.ListObjectsV2PagesWithContext(ctx, input,
			func(page *s3.ListObjectsV2Output, _ bool) bool {
				for _, obj := range page.Contents {
					key := aws.StringValue(obj.Key)
					rel := strings.TrimPrefix(key, prefix)
					rel = strings.TrimPrefix(rel, "/")
					if rel == "" {
						continue
					}
					o := &Object{
						Key:  rel,
						Size: aws.Int64Value(obj.Size),
						ETag: strings.Trim(aws.StringValue(obj.ETag), `"`),
					}
					if obj.LastModified != nil {
						o.Mtime = *obj.LastModified
					}
					o.IsDir = strings.HasSuffix(rel, "/")
					select {
					case <-ctx.Done():
						return false
					case objects <- o:
					}
				}
				return true
			},
		)
		if err != nil {
			errc <- err
		}
	}()

	return objects, errc
}

func (s *S3Storage) Get(ctx context.Context, key string, off, size int64) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	}
	if off > 0 || size > 0 {
		var rangeStr string
		if size > 0 {
			rangeStr = fmt.Sprintf("bytes=%d-%d", off, off+size-1)
		} else {
			rangeStr = fmt.Sprintf("bytes=%d-", off)
		}
		input.Range = aws.String(rangeStr)
	}
	out, err := s.client.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", key, err)
	}
	return out.Body, nil
}

func (s *S3Storage) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return s.PutWithMtime(ctx, key, r, size, time.Time{})
}

// PutWithMtime uploads to S3; mtime is ignored since S3 does not support custom modification times.
func (s *S3Storage) PutWithMtime(ctx context.Context, key string, r io.Reader, size int64, _ time.Time) error {
	// Buffer into memory for PutObject (required for Content-Length).
	// For large files the caller should use multipart; here we keep it simple.
	var body io.ReadSeeker
	if size >= 0 {
		data, err := io.ReadAll(io.LimitReader(r, size+1))
		if err != nil {
			return fmt.Errorf("buffer s3 put: %w", err)
		}
		body = bytes.NewReader(data)
	} else {
		data, err := io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("buffer s3 put: %w", err)
		}
		body = bytes.NewReader(data)
	}

	_, err := s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

func (s *S3Storage) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	return err
}

// MkdirAll is a noop for S3.
func (s *S3Storage) MkdirAll(_ context.Context, _ string) error { return nil }
