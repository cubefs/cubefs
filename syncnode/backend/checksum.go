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

// MtimeMetadataKey is the user-metadata key under which object-store backends
// persist the source modification time (RFC3339Nano) supplied via
// PutOptions.Mtime. The full HTTP header sent by the S3 SDK is
// `x-amz-meta-syncnode-mtime`; the SDK strips the `x-amz-meta-` prefix when
// surfacing Metadata maps, so backend code looks up the bare key.
const MtimeMetadataKey = "syncnode-mtime"

// ModeMetadataKey is the user-metadata key under which object-store backends
// persist the POSIX mode bits supplied via PutOptions.Mode. Encoded as an
// octal string (e.g. "0644", "04755"). Stat falls back to the rclone naked
// `mode` key when the syncnode-prefixed value is absent, so interop with
// rclone-written objects works out of the box.
const ModeMetadataKey = "syncnode-mode"

// UIDMetadataKey is the user-metadata key under which object-store backends
// persist the POSIX uid supplied via PutOptions.UID. Encoded as a decimal
// string (e.g. "1000"). Falls back to rclone naked `uid` on Stat.
const UIDMetadataKey = "syncnode-uid"

// GIDMetadataKey is the user-metadata key under which object-store backends
// persist the POSIX gid supplied via PutOptions.GID. Encoded as a decimal
// string. Falls back to rclone naked `gid` on Stat.
const GIDMetadataKey = "syncnode-gid"

// XattrsMetadataKey is the user-metadata key under which object-store
// backends persist extended attributes supplied via PutOptions.Xattrs.
// Encoding: base64(json.Marshal(map[string]string{name: base64(value)})) —
// xattr values are arbitrary bytes (zero-byte, non-UTF-8, etc.), so each
// value is base64-encoded inside the JSON object, then the whole map is
// base64-encoded one more time so the final value is HTTP-header safe. No
// rclone naked fallback — this layout is syncnode-specific.
const XattrsMetadataKey = "syncnode-xattrs"

// RcloneModeKey / RcloneUIDKey / RcloneGIDKey are the bare user-metadata
// keys rclone writes (no syncnode prefix). syncnode Stat reads these as
// fallback so objects written by rclone --metadata interop with syncnode.
const (
	RcloneModeKey = "mode"
	RcloneUIDKey  = "uid"
	RcloneGIDKey  = "gid"
)

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
