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

package backend

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

// ChecksumAlgorithmSHA256 is the algorithm string returned by Backend
// implementations when the checksum is a sha256 digest. Kept as a constant so
// callers (executor) can rely on equality rather than free-form strings.
const ChecksumAlgorithmSHA256 = "sha256"

// ChecksumAlgorithmMD5 is the algorithm string used when GetChecksum falls
// back to a server-side MD5 (e.g. S3 single-part ETag). The executor uses
// this to decide whether two values are comparable across endpoints.
const ChecksumAlgorithmMD5 = "md5"

// SHA256CompanionSuffix is the suffix appended to a key when a backend stores
// the sha256 in a companion object next to the data file (currently only the
// cfs backend uses this).
const SHA256CompanionSuffix = ".syncnode.sha256"

// SHA256MetadataKey is the user-metadata key under which object-store
// backends persist the sha256 hex string. SDKs strip the standard
// `x-amz-meta-` prefix from response Metadata maps so backends look up this
// bare key.
const SHA256MetadataKey = "syncnode-sha256"

// NewSHA256Sink returns a fresh sha256.Hash plus a closure that returns the
// hex-encoded digest. Backends typically call this once per Put, wrap the
// body with `io.TeeReader(body, h)`, then call sum() once the upload
// returns. Pulled out so all three backends compute the same way (less
// "did the s3 path forget to lowercase?" drift).
func NewSHA256Sink() (h hash.Hash, sum func() string) {
	h = sha256.New()
	return h, func() string { return hex.EncodeToString(h.Sum(nil)) }
}

// SHA256Stream computes the sha256 of r in a single streaming pass. Returns
// the hex digest and the byte count consumed. Used by GetChecksum
// implementations that have to compute on demand (POSIX, cfs cold path).
func SHA256Stream(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", n, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
