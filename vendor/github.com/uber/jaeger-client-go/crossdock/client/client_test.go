// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"net/url"
	"strings"
	"testing"

	"github.com/crossdock/crossdock-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/crossdock/common"
	"github.com/uber/jaeger-client-go/crossdock/log"
	"github.com/uber/jaeger-client-go/crossdock/server"
	jlog "github.com/uber/jaeger-client-go/log"
)

func TestCrossdock(t *testing.T) {
	log.Enabled = false // enable when debugging tests
	log.Printf("Starting crossdock test")

	var reporter jaeger.Reporter
	if log.Enabled {
		reporter = jaeger.NewLoggingReporter(jlog.StdLogger)
	} else {
		reporter = jaeger.NewNullReporter()
	}

	tracer, tCloser := jaeger.NewTracer(
		"crossdock",
		jaeger.NewConstSampler(false),
		reporter)
	defer tCloser.Close()

	s := &server.Server{
		HostPortHTTP: "127.0.0.1:0",
		Tracer:       tracer,
	}
	err := s.Start()
	require.NoError(t, err)
	defer s.Close()

	c := &Client{
		ClientHostPort: "127.0.0.1:0",
		ServerPortHTTP: s.GetPortHTTP(),
		hostMapper:     func(server string) string { return "localhost" },
	}
	err = c.AsyncStart()
	require.NoError(t, err)
	defer c.Close()

	crossdock.Wait(t, s.URL(), 10)
	crossdock.Wait(t, c.URL(), 10)

	behaviors := []struct {
		name string
		axes map[string][]string
	}{
		{
			name: behaviorTrace,
			axes: map[string][]string{
				server1NameParam:      {common.DefaultTracerServiceName},
				sampledParam:          {"true", "false"},
				server2NameParam:      {common.DefaultTracerServiceName},
				server2TransportParam: {transportHTTP, transportDummy},
				server3NameParam:      {common.DefaultTracerServiceName},
				server3TransportParam: {transportHTTP},
			},
		},
	}

	for _, bb := range behaviors {
		for _, entry := range crossdock.Combinations(bb.axes) {
			entryArgs := url.Values{}
			for k, v := range entry {
				entryArgs.Set(k, v)
			}
			name := strings.ReplaceAll(entryArgs.Encode(), "&", "/")
			t.Run(name, func(t *testing.T) {
				// test via real HTTP call
				crossdock.Call(t, c.URL(), bb.name, entryArgs)
			})
		}
	}
}

func TestHostMapper(t *testing.T) {
	c := &Client{
		ClientHostPort: "127.0.0.1:0",
		ServerPortHTTP: "8080",
	}
	assert.Equal(t, "go", c.mapServiceToHost("go"))
	c.hostMapper = func(server string) string { return "localhost" }
	assert.Equal(t, "localhost", c.mapServiceToHost("go"))
}
