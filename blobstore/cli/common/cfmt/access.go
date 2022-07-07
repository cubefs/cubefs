// Copyright 2022 The CubeFS Authors.
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

package cfmt

import (
	"fmt"

	"github.com/dustin/go-humanize"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/cli/common"
)

// ParseLocation parst location from json or string
func ParseLocation(jsonORstr string) (access.Location, error) {
	var loc access.Location
	var err error
	if err = common.Unmarshal([]byte(jsonORstr), &loc); err == nil {
		return loc, nil
	}
	if loc, err = access.DecodeLocationFromHex(jsonORstr); err == nil {
		return loc, nil
	}
	if loc, err = access.DecodeLocationFromBase64(jsonORstr); err == nil {
		return loc, nil
	}
	return loc, fmt.Errorf("invalid (%s) %s", jsonORstr, err.Error())
}

// LocationJoin join line into string
func LocationJoin(loc *access.Location, prefix string) string {
	return joinWithPrefix(prefix, LocationF(loc))
}

// LocationF fmt pointer of Location
func LocationF(loc *access.Location) (vals []string) {
	if loc == nil {
		return []string{" <nil> "}
	}
	vals = make([]string, 0, 8)
	vals = append(vals, []string{
		fmt.Sprintf("Crc        : %-12d (0x%x)", loc.Crc, loc.Crc),
		fmt.Sprintf("ClusterID  : %d", loc.ClusterID),
		fmt.Sprintf("CodeMode   : %-12d (%s)", loc.CodeMode, loc.CodeMode.String()),
		fmt.Sprintf("Size       : %-12d (%s)", loc.Size, humanize.IBytes(loc.Size)),
		fmt.Sprintf("BlobSize   : %-12d (%s)", loc.BlobSize, humanize.IBytes(uint64(loc.BlobSize))),
		fmt.Sprintf("Blobs: (%d) [", len(loc.Blobs)),
	}...)
	for idx, blob := range loc.Blobs {
		vals = append(vals, fmt.Sprintf(" >:%3d| MinBid: %-20d Vid: %-10d Count: %-10d",
			idx, blob.MinBid, blob.Vid, blob.Count))
	}
	vals = append(vals, "]")
	vals = append(vals, fmt.Sprintf("--> Encode: %d of %d bytes", len(loc.Encode()), 21+16*len(loc.Blobs)))
	vals = append(vals, fmt.Sprintf("--> Hex   : %s", loc.HexString()))
	vals = append(vals, fmt.Sprintf("--> Base64: %s", loc.Base64String()))
	return
}

// HashSumMapJoin join line into string
func HashSumMapJoin(hashes access.HashSumMap, prefix string) string {
	return joinWithPrefix(prefix, HashSumMapF(hashes))
}

// HashSumMapF fmt HashSumMap
func HashSumMapF(hashes access.HashSumMap) (vals []string) {
	if v, ok := hashes.GetSum(access.HashAlgCRC32); ok {
		val := v.(uint32)
		vals = append(vals, fmt.Sprintf("CRC32 : %d (%x)", val, val))
	}
	if v, ok := hashes.GetSum(access.HashAlgMD5); ok {
		val := v.(string)
		vals = append(vals, fmt.Sprintf("MD5   : %s", val))
	}
	if v, ok := hashes.GetSum(access.HashAlgSHA1); ok {
		val := v.(string)
		vals = append(vals, fmt.Sprintf("SHA1  : %s", val))
	}
	if v, ok := hashes.GetSum(access.HashAlgSHA256); ok {
		val := v.(string)
		vals = append(vals, fmt.Sprintf("SHA256: %s", val))
	}
	return
}
