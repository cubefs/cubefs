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

package clustermgr

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientIP(t *testing.T) {
	tests := []struct {
		name       string
		realIP     string
		forwarded  string
		remoteAddr string
		want       string
	}{
		{name: "remote IPv4 with port", remoteAddr: "192.0.2.10:52345", want: "192.0.2.10"},
		{name: "remote IPv6 with port", remoteAddr: "[2001:db8::1]:52345", want: "2001:db8::1"},
		{name: "X-Real-Ip IPv4", realIP: "192.0.2.20", remoteAddr: "10.0.0.1:80", want: "192.0.2.20"},
		{name: "X-Real-Ip IPv6", realIP: "2001:db8::2", remoteAddr: "10.0.0.1:80", want: "2001:db8::2"},
		{name: "X-Forwarded-For single", forwarded: "192.0.2.30", remoteAddr: "10.0.0.1:80", want: "192.0.2.30"},
		{name: "X-Forwarded-For chain", forwarded: "192.0.2.40, 10.0.0.2, 10.0.0.3", remoteAddr: "10.0.0.1:80", want: "192.0.2.40"},
		{name: "X-Forwarded-For IPv6 chain", forwarded: "2001:db8::3, 10.0.0.2", remoteAddr: "10.0.0.1:80", want: "2001:db8::3"},
		{name: "X-Real-Ip wins", realIP: "192.0.2.50", forwarded: "192.0.2.60", remoteAddr: "10.0.0.1:80", want: "192.0.2.50"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
			require.NoError(t, err)
			if tt.realIP != "" {
				r.Header.Set("X-Real-Ip", tt.realIP)
			}
			if tt.forwarded != "" {
				r.Header.Set("X-Forwarded-For", tt.forwarded)
			}
			r.RemoteAddr = tt.remoteAddr
			require.Equal(t, tt.want, clientIP(r))
		})
	}
}
