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

package objectnode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func newRequestWithIP(remoteAddr, realIP, forwardedFor string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	r.RemoteAddr = remoteAddr
	if realIP != "" {
		r.Header.Set("X-Real-Ip", realIP)
	}
	if forwardedFor != "" {
		r.Header.Set("X-Forwarded-For", forwardedFor)
	}
	return r
}

func TestGetRequestIP(t *testing.T) {
	testCases := []struct {
		name         string
		remoteAddr   string
		realIP       string
		forwardedFor string
		expect       string
	}{
		{name: "remote ipv4", remoteAddr: "192.0.2.1:4321", expect: "192.0.2.1"},
		{name: "remote ipv4 without port", remoteAddr: "192.0.2.1", expect: "192.0.2.1"},
		{name: "remote ipv6", remoteAddr: "[2001:db8::1]:4321", expect: "2001:db8::1"},
		{name: "real ip ipv4", remoteAddr: "10.0.0.1:80", realIP: "203.0.113.7", expect: "203.0.113.7"},
		{name: "real ip ipv4 with port", remoteAddr: "10.0.0.1:80", realIP: "203.0.113.7:5000", expect: "203.0.113.7"},
		{name: "real ip ipv6", remoteAddr: "10.0.0.1:80", realIP: "2001:db8::7", expect: "2001:db8::7"},
		{name: "real ip ipv6 with port", remoteAddr: "10.0.0.1:80", realIP: "[2001:db8::7]:5000", expect: "2001:db8::7"},
		{name: "forwarded for single", remoteAddr: "10.0.0.1:80", forwardedFor: "203.0.113.7", expect: "203.0.113.7"},
		{
			name: "forwarded for chain", remoteAddr: "10.0.0.1:80",
			forwardedFor: "203.0.113.7, 70.41.3.18, 150.172.238.178", expect: "203.0.113.7",
		},
		{
			name: "forwarded for ipv6 chain", remoteAddr: "10.0.0.1:80",
			forwardedFor: "2001:db8::7, 10.0.0.2", expect: "2001:db8::7",
		},
		{
			name: "real ip first", remoteAddr: "10.0.0.1:80",
			realIP: "203.0.113.7", forwardedFor: "198.51.100.1", expect: "203.0.113.7",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRequestWithIP(tc.remoteAddr, tc.realIP, tc.forwardedFor)
			require.Equal(t, tc.expect, getRequestIP(r))
		})
	}
}

// The request IP is the value of the aws:SourceIp condition key of a bucket policy.
func TestGetRequestIPSourceIPCondition(t *testing.T) {
	var condition Condition
	err := json.Unmarshal([]byte(`{"IpAddress":{"aws:SourceIp":["2001:db8::/32","203.0.113.0/24"]}}`), &condition)
	require.NoError(t, err)

	testCases := []struct {
		name         string
		remoteAddr   string
		forwardedFor string
		expect       bool
	}{
		{name: "ipv4 allowed", remoteAddr: "203.0.113.9:4321", expect: true},
		{name: "ipv4 denied", remoteAddr: "192.0.2.1:4321", expect: false},
		{name: "ipv6 allowed", remoteAddr: "[2001:db8::1]:4321", expect: true},
		{name: "ipv6 denied", remoteAddr: "[2001:db9::1]:4321", expect: false},
		{name: "proxy chain allowed", remoteAddr: "10.0.0.1:80", forwardedFor: "203.0.113.7, 10.0.0.2", expect: true},
		{name: "proxy chain denied", remoteAddr: "10.0.0.1:80", forwardedFor: "192.0.2.7, 10.0.0.2", expect: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRequestWithIP(tc.remoteAddr, "", tc.forwardedFor)
			values := map[string]string{SOURCEIP: getRequestIP(r)}
			require.NotPanics(t, func() {
				require.Equal(t, tc.expect, condition.Evaluate(values))
			})
		})
	}
}
