// Copyright 2015 The Go Authors. All rights reserved.
//
// Modified by 2020 The CubeFS Authors.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Minimal RFC 6724 address selection.

package iputil

import (
	"net"
)

// commonPrefixLen reports the length of the longest prefix (looking
// at the most significant, or leftmost, bits) that the
// two addresses have in common, up to the length of a's prefix (i.e.,
// the portion of the address not including the interface ID).
//
// If a or b is an IPv4 address as an IPv6 address, the IPv4 addresses
// are compared (with max common prefix length of 32).
// If a and b are different IP versions, 0 is returned.
//
// See https://tools.ietf.org/html/rfc6724#section-2.2
func commonPrefixLen(a, b net.IP) (cpl int) {
	if a4 := a.To4(); a4 != nil {
		a = a4
	}
	if b4 := b.To4(); b4 != nil {
		b = b4
	}
	if len(a) != len(b) {
		return 0
	}
	// If IPv6, only up to the prefix (first 64 bits)
	if len(a) > 8 {
		a = a[:8]
		b = b[:8]
	}
	for len(a) > 0 {
		if a[0] == b[0] {
			cpl += 8
			a = a[1:]
			b = b[1:]
			continue
		}
		bits := 8
		ab, bb := a[0], b[0]
		for {
			ab >>= 1
			bb >>= 1
			bits--
			if ab == bb {
				cpl += bits
				return
			}
		}
	}
	return
}
