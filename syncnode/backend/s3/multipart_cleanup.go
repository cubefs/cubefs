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
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// AbortStaleMultipartUploads scans the bucket for in-progress multipart
// uploads that were initiated more than olderThan ago and aborts them.
// It is intended to be called once at syncnode startup to clean up
// orphan parts left behind by previous crashes (the SDK already calls
// AbortMultipartUpload on Upload() error, so the only way an upload
// becomes orphaned is a hard crash mid-upload).
//
// See design.md §9 C-3.
//
// The function is idempotent: calling it twice in succession with no
// new in-flight uploads aborts nothing and returns aborted=0.
//
// Implementation notes:
//   - Uses ListMultipartUploadsPaginator to walk the full set rather
//     than capping at the first 1000 (the S3 default page size).
//   - Per-upload AbortMultipartUpload errors are collected; the
//     function continues with subsequent uploads rather than bailing on
//     the first error. The returned error is the join of all per-upload
//     errors (or nil if all succeed). Counts only successfully aborted
//     uploads.
//   - ctx cancellation aborts the scan; whatever was aborted up to that
//     point is returned in `aborted`.
func (b *Backend) AbortStaleMultipartUploads(ctx context.Context, olderThan time.Duration) (aborted int, err error) {
	cutoff := time.Now().Add(-olderThan)

	paginator := awss3.NewListMultipartUploadsPaginator(b.client, &awss3.ListMultipartUploadsInput{
		Bucket: aws.String(b.bucket),
	})

	var errs []error
	for paginator.HasMorePages() {
		page, perr := paginator.NextPage(ctx)
		if perr != nil {
			errs = append(errs, fmt.Errorf("list multipart uploads: %w", perr))
			break
		}
		for _, up := range page.Uploads {
			if up.Initiated == nil || up.Initiated.After(cutoff) {
				continue
			}
			if up.Key == nil || up.UploadId == nil {
				continue
			}
			if _, abortErr := b.client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
				Bucket:   aws.String(b.bucket),
				Key:      up.Key,
				UploadId: up.UploadId,
			}); abortErr != nil {
				// NoSuchUpload means another actor already aborted /
				// completed it — treat as success.
				if isNotFound(abortErr) {
					continue
				}
				errs = append(errs, fmt.Errorf("abort %s/%s upload=%s: %w",
					b.bucket, aws.ToString(up.Key), aws.ToString(up.UploadId), abortErr))
				continue
			}
			aborted++
		}
	}
	if len(errs) > 0 {
		return aborted, errors.Join(errs...)
	}
	return aborted, nil
}
