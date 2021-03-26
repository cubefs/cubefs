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

package config

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/metricstest"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-client-go/transport"
)

func TestNewSamplerConst(t *testing.T) {
	constTests := []struct {
		param    float64
		decision bool
	}{{1, true}, {0, false}}
	for _, tst := range constTests {
		cfg := &SamplerConfig{Type: jaeger.SamplerTypeConst, Param: tst.param}
		s, err := cfg.NewSampler("x", nil)
		require.NoError(t, err)
		s1, ok := s.(*jaeger.ConstSampler)
		require.True(t, ok, "converted to constSampler")
		require.Equal(t, tst.decision, s1.Decision, "decision")
	}
}

func TestNewSamplerProbabilistic(t *testing.T) {
	constTests := []struct {
		param float64
		error bool
	}{{1.5, true}, {0.5, false}}
	for _, tst := range constTests {
		cfg := &SamplerConfig{Type: jaeger.SamplerTypeProbabilistic, Param: tst.param}
		s, err := cfg.NewSampler("x", nil)
		if tst.error {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			_, ok := s.(*jaeger.ProbabilisticSampler)
			require.True(t, ok, "converted to ProbabilisticSampler")
		}
	}
}

func TestDefaultSampler(t *testing.T) {
	cfg := Configuration{
		ServiceName: "test",
		Sampler:     &SamplerConfig{Type: "InvalidType"},
	}
	_, _, err := cfg.NewTracer()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidType")
}

func setEnv(t *testing.T, key, value string) {
	require.NoError(t, os.Setenv(key, value))
}

func unsetEnv(t *testing.T, key string) {
	require.NoError(t, os.Unsetenv(key))
}

func closeCloser(t *testing.T, c io.Closer) {
	require.NoError(t, c.Close())
}

func TestServiceNameFromEnv(t *testing.T) {
	setEnv(t, envServiceName, "my-service")

	cfg, err := FromEnv()
	assert.NoError(t, err)

	_, c, err := cfg.NewTracer()
	assert.NoError(t, err)
	defer closeCloser(t, c)
	unsetEnv(t, envServiceName)
}

func TestConfigFromEnv(t *testing.T) {
	cfg := &Configuration{
		ServiceName: "my-config-service",
		Disabled:    true,
		RPCMetrics:  false,
		Tags:        []opentracing.Tag{{Key: "KEY01", Value: "VALUE01"}},
	}

	// test
	cfg, err := cfg.FromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "my-config-service", cfg.ServiceName)
	assert.Equal(t, true, cfg.Disabled)
	assert.Equal(t, false, cfg.RPCMetrics)
	assert.Equal(t, "KEY01", cfg.Tags[0].Key)
	assert.Equal(t, "VALUE01", cfg.Tags[0].Value)

	// prepare
	setEnv(t, envServiceName, "my-service")
	setEnv(t, envDisabled, "false")
	setEnv(t, envRPCMetrics, "true")
	setEnv(t, env128bit, "true")
	setEnv(t, envTags, "KEY=VALUE")

	// test with env set
	cfg, err = cfg.FromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "my-service", cfg.ServiceName)
	assert.Equal(t, false, cfg.Disabled)
	assert.Equal(t, true, cfg.RPCMetrics)
	assert.Equal(t, true, cfg.Gen128Bit)
	assert.Equal(t, "KEY", cfg.Tags[0].Key)
	assert.Equal(t, "VALUE", cfg.Tags[0].Value)

	// cleanup
	unsetEnv(t, envServiceName)
	unsetEnv(t, envDisabled)
	unsetEnv(t, envRPCMetrics)
	unsetEnv(t, env128bit)
	unsetEnv(t, envTags)
}

func TestSamplerConfig(t *testing.T) {
	// prepare
	setEnv(t, envSamplerType, "const")
	setEnv(t, envSamplerParam, "1")
	setEnv(t, envSamplingEndpoint, "http://themaster:5778/sampling")
	setEnv(t, envSamplerMaxOperations, "10")
	setEnv(t, envSamplerRefreshInterval, "1m1s") // 61 seconds

	//existing SamplerConfig data
	sc := SamplerConfig{
		Type:                    "const-sample-config",
		Param:                   2,
		SamplingServerURL:       "http://themaster-sample-config",
		MaxOperations:           20,
		SamplingRefreshInterval: 2,
	}

	// test
	cfg, err := sc.samplerConfigFromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "const", cfg.Type)
	assert.Equal(t, float64(1), cfg.Param)
	assert.Equal(t, "http://themaster:5778/sampling", cfg.SamplingServerURL)
	assert.Equal(t, 10, cfg.MaxOperations)
	assert.Equal(t, 61000000000, int(cfg.SamplingRefreshInterval))

	// cleanup
	unsetEnv(t, envSamplerType)
	unsetEnv(t, envSamplerParam)
	unsetEnv(t, envSamplingEndpoint)
	unsetEnv(t, envSamplerMaxOperations)
	unsetEnv(t, envSamplerRefreshInterval)
}

func TestSamplerConfigOptions(t *testing.T) {
	initSampler := jaeger.NewRateLimitingSampler(1)
	cfg := SamplerConfig{
		// test passing options
		Options: []jaeger.SamplerOption{
			jaeger.SamplerOptions.InitialSampler(initSampler),
		},
	}
	sampler, err := cfg.NewSampler("service", jaeger.NewNullMetrics())
	require.NoError(t, err)
	defer sampler.Close()
	assert.Same(t, initSampler, sampler.(*jaeger.RemotelyControlledSampler).Sampler())
}

func TestReporter(t *testing.T) {
	// prepare
	setEnv(t, envReporterMaxQueueSize, "10")
	setEnv(t, envReporterFlushInterval, "1m1s") // 61 seconds
	setEnv(t, envReporterLogSpans, "true")
	setEnv(t, envAgentHost, "nonlocalhost")
	setEnv(t, envAgentPort, "6832")
	setEnv(t, envUser, "user")
	setEnv(t, envPassword, "password")
	setEnv(t, envReporterAttemptReconnectingDisabled, "false")
	setEnv(t, envReporterAttemptReconnectInterval, "40s")

	// Existing ReporterConfig data
	rc := ReporterConfig{
		QueueSize:           20,
		BufferFlushInterval: 2,
		LogSpans:            false,
		LocalAgentHostPort:  "localhost01",
		CollectorEndpoint:   "9999",
		User:                "user01",
		Password:            "password01",
	}

	// test
	cfg, err := rc.reporterConfigFromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, 10, cfg.QueueSize)
	assert.Equal(t, 61000000000, int(cfg.BufferFlushInterval))
	assert.Equal(t, true, cfg.LogSpans)
	assert.Equal(t, "nonlocalhost:6832", cfg.LocalAgentHostPort)
	assert.Equal(t, "user01", cfg.User)
	assert.Equal(t, "password01", cfg.Password)
	assert.Equal(t, false, cfg.DisableAttemptReconnecting)
	assert.Equal(t, time.Second*40, cfg.AttemptReconnectInterval)

	// Prepare
	setEnv(t, envEndpoint, "http://1.2.3.4:5678/api/traces")
	setEnv(t, envUser, "user")
	setEnv(t, envPassword, "password")

	// Existing ReprterConfig data for JAEGAR-ENDPOINT validation check
	rc = ReporterConfig{
		QueueSize:           20,
		BufferFlushInterval: 2,
		LogSpans:            false,
		LocalAgentHostPort:  "localhost",
		CollectorEndpoint:   "9999",
		User:                "user",
		Password:            "password",
	}

	// test
	cfg, err = rc.reporterConfigFromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "http://1.2.3.4:5678/api/traces", cfg.CollectorEndpoint)
	assert.Equal(t, "localhost", cfg.LocalAgentHostPort)
	assert.Equal(t, "user", cfg.User)
	assert.Equal(t, "password", cfg.Password)

	// cleanup
	unsetEnv(t, envReporterMaxQueueSize)
	unsetEnv(t, envReporterFlushInterval)
	unsetEnv(t, envReporterLogSpans)
	unsetEnv(t, envEndpoint)
	unsetEnv(t, envUser)
	unsetEnv(t, envPassword)
	unsetEnv(t, envAgentHost)
	unsetEnv(t, envAgentPort)
}

func TestFromEnv(t *testing.T) {
	setEnv(t, envServiceName, "my-service")
	setEnv(t, envDisabled, "false")
	setEnv(t, envRPCMetrics, "true")
	setEnv(t, envTags, "KEY=VALUE")

	cfg, err := FromEnv()
	assert.NoError(t, err)
	assert.Equal(t, "my-service", cfg.ServiceName)
	assert.Equal(t, false, cfg.Disabled)
	assert.Equal(t, true, cfg.RPCMetrics)
	assert.Equal(t, "KEY", cfg.Tags[0].Key)
	assert.Equal(t, "VALUE", cfg.Tags[0].Value)

	unsetEnv(t, envServiceName)
	unsetEnv(t, envDisabled)
	unsetEnv(t, envRPCMetrics)
	unsetEnv(t, envTags)
}

func TestNoServiceNameFromEnv(t *testing.T) {
	unsetEnv(t, envServiceName)

	cfg, err := FromEnv()
	assert.NoError(t, err)

	_, _, err = cfg.NewTracer()
	assert.Error(t, err)

	// However, if Disabled, then empty service name is irrelevant (issue #350)
	cfg.Disabled = true
	tr, _, err := cfg.NewTracer()
	assert.NoError(t, err)
	assert.Equal(t, &opentracing.NoopTracer{}, tr)
}

func TestSamplerConfigFromEnv(t *testing.T) {
	// prepare
	setEnv(t, envSamplerType, "remote")
	setEnv(t, envSamplerParam, "1")
	setEnv(t, envSamplingEndpoint, "http://themaster:5778/sampling")
	setEnv(t, envSamplerMaxOperations, "10")
	setEnv(t, envSamplerRefreshInterval, "1m1s") // 61 seconds

	// test
	cfg, err := FromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "remote", cfg.Sampler.Type)
	assert.Equal(t, float64(1), cfg.Sampler.Param)
	assert.Equal(t, "http://themaster:5778/sampling", cfg.Sampler.SamplingServerURL)
	assert.Equal(t, 10, cfg.Sampler.MaxOperations)
	assert.Equal(t, 61000000000, int(cfg.Sampler.SamplingRefreshInterval))

	// cleanup
	unsetEnv(t, envSamplerType)
	unsetEnv(t, envSamplerParam)
	unsetEnv(t, envSamplingEndpoint)
	unsetEnv(t, envSamplerMaxOperations)
	unsetEnv(t, envSamplerRefreshInterval)
}

func TestDeprecatedSamplerConfigFromEnv(t *testing.T) {
	// prepare
	setEnv(t, envSamplerManagerHostPort, "http://themaster")

	// test
	cfg, err := FromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "http://themaster", cfg.Sampler.SamplingServerURL)

	// cleanup
	unsetEnv(t, envSamplerManagerHostPort)
}

func TestSamplerConfigOnAgentFromEnv(t *testing.T) {
	// prepare
	setEnv(t, envAgentHost, "theagent")

	// test
	cfg, err := FromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "http://theagent:5778/sampling", cfg.Sampler.SamplingServerURL)

	// cleanup
	unsetEnv(t, envAgentHost)
}

func TestReporterConfigFromEnv(t *testing.T) {
	// prepare
	setEnv(t, envReporterMaxQueueSize, "10")
	setEnv(t, envReporterFlushInterval, "1m1s") // 61 seconds
	setEnv(t, envReporterLogSpans, "true")
	setEnv(t, envAgentHost, "nonlocalhost")
	setEnv(t, envAgentPort, "6832")

	// test
	cfg, err := FromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, 10, cfg.Reporter.QueueSize)
	assert.Equal(t, 61000000000, int(cfg.Reporter.BufferFlushInterval))
	assert.Equal(t, true, cfg.Reporter.LogSpans)
	assert.Equal(t, "nonlocalhost:6832", cfg.Reporter.LocalAgentHostPort)

	// Test HTTP transport
	setEnv(t, envEndpoint, "http://1.2.3.4:5678/api/traces")
	setEnv(t, envUser, "user")
	setEnv(t, envPassword, "password")

	// test
	cfg, err = FromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "http://1.2.3.4:5678/api/traces", cfg.Reporter.CollectorEndpoint)
	assert.Equal(t, "user", cfg.Reporter.User)
	assert.Equal(t, "password", cfg.Reporter.Password)
	assert.Equal(t, "", cfg.Reporter.LocalAgentHostPort)

	// cleanup
	unsetEnv(t, envReporterMaxQueueSize)
	unsetEnv(t, envReporterFlushInterval)
	unsetEnv(t, envReporterLogSpans)
	unsetEnv(t, envEndpoint)
	unsetEnv(t, envUser)
	unsetEnv(t, envPassword)
}

func TestReporterAgentConfigFromEnv(t *testing.T) {
	// prepare
	unsetEnv(t, envEndpoint)
	unsetEnv(t, envAgentHost)
	unsetEnv(t, envAgentPort)

	// No config and no env check
	rc := ReporterConfig{}

	// test
	cfg, err := rc.reporterConfigFromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "localhost:6831", cfg.LocalAgentHostPort)

	// No env check
	rc = ReporterConfig{
		LocalAgentHostPort: "localhost01:7777",
	}

	// test
	cfg, err = rc.reporterConfigFromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "localhost01:7777", cfg.LocalAgentHostPort)

	// Only host env check
	setEnv(t, envAgentHost, "localhost02")
	unsetEnv(t, envAgentPort)
	rc = ReporterConfig{
		LocalAgentHostPort: "localhost01:7777",
	}

	// test
	cfg, err = rc.reporterConfigFromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "localhost02:6831", cfg.LocalAgentHostPort)

	// Only port env check
	unsetEnv(t, envAgentHost)
	setEnv(t, envAgentPort, "8888")
	rc = ReporterConfig{
		LocalAgentHostPort: "localhost01:7777",
	}

	// test
	cfg, err = rc.reporterConfigFromEnv()
	assert.NoError(t, err)

	// verify
	assert.Equal(t, "localhost:8888", cfg.LocalAgentHostPort)

	// cleanup
	unsetEnv(t, envEndpoint)
	unsetEnv(t, envAgentHost)
	unsetEnv(t, envAgentPort)
}

func TestParsingErrorsFromEnv(t *testing.T) {
	setEnv(t, envAgentHost, "localhost") // we require this in order to test the parsing of the port

	tests := []struct {
		envVar string
		value  string
	}{
		{
			envVar: envRPCMetrics,
			value:  "NOT_A_BOOLEAN",
		},
		{
			envVar: envDisabled,
			value:  "NOT_A_BOOLEAN",
		},
		{
			envVar: env128bit,
			value:  "NOT_A_BOOLEAN",
		},
		{
			envVar: envSamplerParam,
			value:  "NOT_A_FLOAT",
		},
		{
			envVar: envSamplerMaxOperations,
			value:  "NOT_AN_INT",
		},
		{
			envVar: envSamplerRefreshInterval,
			value:  "NOT_A_DURATION",
		},
		{
			envVar: envReporterMaxQueueSize,
			value:  "NOT_AN_INT",
		},
		{
			envVar: envReporterFlushInterval,
			value:  "NOT_A_DURATION",
		},
		{
			envVar: envReporterLogSpans,
			value:  "NOT_A_BOOLEAN",
		},
		{
			envVar: envAgentPort,
			value:  "NOT_AN_INT",
		},
		{
			envVar: envEndpoint,
			value:  "NOT_A_URL",
		},
	}

	for _, test := range tests {
		setEnv(t, test.envVar, test.value)
		if test.envVar == envEndpoint {
			unsetEnv(t, envAgentHost)
			unsetEnv(t, envAgentPort)
		}
		_, err := FromEnv()
		require.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("cannot parse env var %s=%s", test.envVar, test.value))
		unsetEnv(t, test.envVar)
	}

}

func TestParsingUserPasswordErrorEnv(t *testing.T) {
	tests := []struct {
		envVar string
		value  string
	}{
		{
			envVar: envUser,
			value:  "user",
		},
		{
			envVar: envPassword,
			value:  "password",
		},
	}
	setEnv(t, envEndpoint, "http://localhost:8080")
	for _, test := range tests {
		setEnv(t, test.envVar, test.value)
		_, err := FromEnv()
		require.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("you must set %s and %s env vars together", envUser,
			envPassword))
		unsetEnv(t, test.envVar)
	}
	unsetEnv(t, envEndpoint)
}

func TestInvalidSamplerType(t *testing.T) {
	cfg := &SamplerConfig{MaxOperations: 10}
	s, err := cfg.NewSampler("x", jaeger.NewNullMetrics())
	require.NoError(t, err)
	rcs, ok := s.(*jaeger.RemotelyControlledSampler)
	require.True(t, ok, "converted to RemotelyControlledSampler")
	rcs.Close()
}

func TestUDPTransportType(t *testing.T) {
	rc := &ReporterConfig{LocalAgentHostPort: "localhost:1234"}
	expect, _ := jaeger.NewUDPTransport(rc.LocalAgentHostPort, 0)
	sender, err := rc.newTransport(log.NullLogger)
	require.NoError(t, err)
	require.IsType(t, expect, sender)
}

func TestHTTPTransportType(t *testing.T) {
	rc := &ReporterConfig{CollectorEndpoint: "http://1.2.3.4:5678/api/traces"}
	expect := transport.NewHTTPTransport(rc.CollectorEndpoint)
	sender, err := rc.newTransport(log.NullLogger)
	require.NoError(t, err)
	require.IsType(t, expect, sender)
}

func TestHTTPTransportTypeWithAuth(t *testing.T) {
	rc := &ReporterConfig{
		CollectorEndpoint: "http://1.2.3.4:5678/api/traces",
		User:              "auth_user",
		Password:          "auth_pass",
	}
	expect := transport.NewHTTPTransport(rc.CollectorEndpoint)
	sender, err := rc.newTransport(log.NullLogger)
	require.NoError(t, err)
	require.IsType(t, expect, sender)
}

func TestDefaultConfig(t *testing.T) {
	cfg := Configuration{}
	_, _, err := cfg.NewTracer(Metrics(metrics.NullFactory), Logger(log.NullLogger))
	require.EqualError(t, err, "no service name provided")

	cfg.ServiceName = "test"
	_, closer, err := cfg.NewTracer()
	require.NoError(t, err)
	defer closeCloser(t, closer)
}

func TestDisabledFlag(t *testing.T) {
	cfg := Configuration{ServiceName: "test", Disabled: true}
	_, closer, err := cfg.NewTracer()
	require.NoError(t, err)
	defer closeCloser(t, closer)
}

func TestNewReporterError(t *testing.T) {
	cfg := Configuration{
		ServiceName: "test",
		Reporter:    &ReporterConfig{LocalAgentHostPort: "bad_local_agent"},
	}
	_, _, err := cfg.NewTracer()
	require.Error(t, err)
}

func TestInitGlobalTracer(t *testing.T) {
	// Save the existing GlobalTracer and replace after finishing function
	prevTracer := opentracing.GlobalTracer()
	defer opentracing.SetGlobalTracer(prevTracer)
	noopTracer := opentracing.NoopTracer{}

	tests := []struct {
		cfg           Configuration
		shouldErr     bool
		tracerChanged bool
	}{
		{
			cfg:           Configuration{Disabled: true},
			shouldErr:     false,
			tracerChanged: false,
		},
		{
			cfg:           Configuration{Sampler: &SamplerConfig{Type: "InvalidType"}},
			shouldErr:     true,
			tracerChanged: false,
		},
		{
			cfg: Configuration{
				Sampler: &SamplerConfig{
					Type:                    "remote",
					SamplingRefreshInterval: 1,
				},
			},
			shouldErr:     false,
			tracerChanged: true,
		},
		{
			cfg:           Configuration{},
			shouldErr:     false,
			tracerChanged: true,
		},
	}
	for _, test := range tests {
		opentracing.SetGlobalTracer(noopTracer)
		_, err := test.cfg.InitGlobalTracer("testService")
		if test.shouldErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		if test.tracerChanged {
			require.NotEqual(t, noopTracer, opentracing.GlobalTracer())
		} else {
			require.Equal(t, noopTracer, opentracing.GlobalTracer())
		}
	}
}

func TestConfigWithReporter(t *testing.T) {
	c := Configuration{
		ServiceName: "test",
		Sampler: &SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	r := jaeger.NewInMemoryReporter()
	tracer, closer, err := c.NewTracer(Reporter(r))
	require.NoError(t, err)
	defer closeCloser(t, closer)

	tracer.StartSpan("test").Finish()
	assert.Len(t, r.GetSpans(), 1)
}

func TestConfigWithRPCMetrics(t *testing.T) {
	m := metricstest.NewFactory(0)
	c := Configuration{
		ServiceName: "test",
		Sampler: &SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		RPCMetrics: true,
	}
	r := jaeger.NewInMemoryReporter()
	tracer, closer, err := c.NewTracer(
		Reporter(r),
		Metrics(m),
		ContribObserver(fakeContribObserver{}),
	)
	require.NoError(t, err)
	defer closeCloser(t, closer)

	tracer.StartSpan("test", ext.SpanKindRPCServer).Finish()

	m.AssertCounterMetrics(t,
		metricstest.ExpectedMetric{
			Name:  "jaeger-rpc.requests",
			Tags:  map[string]string{"component": "jaeger", "endpoint": "test", "error": "false"},
			Value: 1,
		},
	)
}

func TestBaggageRestrictionsConfig(t *testing.T) {
	m := metricstest.NewFactory(0)
	c := Configuration{
		ServiceName: "test",
		BaggageRestrictions: &BaggageRestrictionsConfig{
			HostPort:        "not:1929213",
			RefreshInterval: time.Minute,
		},
	}
	_, closer, err := c.NewTracer(Metrics(m))
	require.NoError(t, err)
	defer closeCloser(t, closer)

	metricName := "jaeger.tracer.baggage_restrictions_updates"
	metricTags := map[string]string{"result": "err"}
	key := metrics.GetKey(metricName, metricTags, "|", "=")
	for i := 0; i < 100; i++ {
		// wait until the async initialization call is complete
		counters, _ := m.Snapshot()
		if _, ok := counters[key]; ok {
			break
		}
		time.Sleep(time.Millisecond)
	}

	m.AssertCounterMetrics(t,
		metricstest.ExpectedMetric{
			Name:  metricName,
			Tags:  metricTags,
			Value: 1,
		},
	)
}

func TestConfigWithGen128Bit(t *testing.T) {
	c := Configuration{
		ServiceName: "test",
		Sampler: &SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		RPCMetrics: true,
	}
	tracer, closer, err := c.NewTracer(Gen128Bit(true))
	require.NoError(t, err)
	defer closeCloser(t, closer)

	span := tracer.StartSpan("test")
	defer span.Finish()
	traceID := span.Context().(jaeger.SpanContext).TraceID()
	require.True(t, traceID.High != 0)
	require.True(t, traceID.Low != 0)
}

func TestConfigWithInjector(t *testing.T) {
	c := Configuration{ServiceName: "test"}
	tracer, closer, err := c.NewTracer(Injector("custom.format", fakeInjector{}))
	require.NoError(t, err)
	defer closeCloser(t, closer)

	span := tracer.StartSpan("test")
	defer span.Finish()

	err = tracer.Inject(span.Context(), "unknown.format", nil)
	require.Error(t, err)

	err = tracer.Inject(span.Context(), "custom.format", nil)
	require.NoError(t, err)
}

func TestConfigWithExtractor(t *testing.T) {
	c := Configuration{ServiceName: "test"}
	tracer, closer, err := c.NewTracer(Extractor("custom.format", fakeExtractor{}))
	require.NoError(t, err)
	defer closeCloser(t, closer)

	_, err = tracer.Extract("unknown.format", nil)
	require.Error(t, err)

	_, err = tracer.Extract("custom.format", nil)
	require.NoError(t, err)
}

func TestConfigWithSampler(t *testing.T) {
	c := Configuration{ServiceName: "test"}
	sampler := &fakeSampler{}

	tracer, closer, err := c.NewTracer(Sampler(sampler))
	require.NoError(t, err)
	defer closeCloser(t, closer)

	span := tracer.StartSpan("test")
	defer span.Finish()

	traceID := span.Context().(jaeger.SpanContext).TraceID()
	require.Equal(t, traceID, sampler.lastTraceID)
	require.Equal(t, "test", sampler.lastOperation)
}

func TestNewTracer(t *testing.T) {
	cfg := &Configuration{ServiceName: "my-service"}
	tracer, closer, err := cfg.NewTracer(Metrics(metrics.NullFactory), Logger(log.NullLogger))
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, closer)
	defer closeCloser(t, closer)
}

func TestNewTracerWithNoDebugFlagOnForcedSampling(t *testing.T) {
	cfg := &Configuration{ServiceName: "my-service"}
	tracer, closer, err := cfg.NewTracer(Metrics(metrics.NullFactory), Logger(log.NullLogger), NoDebugFlagOnForcedSampling(true))
	require.NoError(t, err)
	require.NotNil(t, tracer)
	require.NotNil(t, closer)
	defer closeCloser(t, closer)

	span := tracer.StartSpan("testSpan").(*jaeger.Span)
	ext.SamplingPriority.Set(span, 1)

	assert.NoError(t, err)
	assert.False(t, span.SpanContext().IsDebug())
	assert.True(t, span.SpanContext().IsSampled())
}

func TestNewTracerWithoutServiceName(t *testing.T) {
	cfg := &Configuration{}
	_, _, err := cfg.NewTracer(Metrics(metrics.NullFactory), Logger(log.NullLogger))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no service name provided")
}

func TestParseTags(t *testing.T) {
	setEnv(t, "existing", "not-default")
	tags := "key=value,k1=${nonExisting:default}, k2=${withSpace:default},k3=${existing:default}"
	ts := parseTags(tags)
	assert.Equal(t, 4, len(ts))

	assert.Equal(t, "key", ts[0].Key)
	assert.Equal(t, "value", ts[0].Value)

	assert.Equal(t, "k1", ts[1].Key)
	assert.Equal(t, "default", ts[1].Value)

	assert.Equal(t, "k2", ts[2].Key)
	assert.Equal(t, "default", ts[2].Value)

	assert.Equal(t, "k3", ts[3].Key)
	assert.Equal(t, "not-default", ts[3].Value)

	unsetEnv(t, "existing")
}

func TestServiceNameViaConfiguration(t *testing.T) {
	cfg := &Configuration{ServiceName: "my-service"}
	_, closer, err := cfg.New("")
	assert.NoError(t, err)
	defer closeCloser(t, closer)
}

func TestTracerTags(t *testing.T) {
	cfg := &Configuration{ServiceName: "test-service", Tags: []opentracing.Tag{{Key: "test", Value: 123}}}
	_, closer, err := cfg.NewTracer()
	assert.NoError(t, err)
	defer closeCloser(t, closer)
}

func TestThrottlerDefaultConfig(t *testing.T) {
	cfg := &Configuration{
		ServiceName: "test-service",
		Throttler:   &ThrottlerConfig{},
	}
	_, closer, err := cfg.NewTracer()
	assert.NoError(t, err)
	defer closeCloser(t, closer)
}
