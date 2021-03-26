package tracing

import (
	"fmt"
	"io"
	"runtime"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

var Tracing bool

type EmptyClose struct {
}

func (e EmptyClose) Close() error {
	return nil
}

func TraceInit(serviceName string, samplerType string, samplerParam float64, reportAddr string) io.Closer {
	if reportAddr == "" {
		Tracing = false
		return &EmptyClose{}
	}

	if samplerType == "" {
		samplerType = "const"
	}

	if samplerParam == 0 {
		samplerParam = 1
	}

	cfg := &config.Configuration{
		ServiceName: serviceName,
		Sampler: &config.SamplerConfig{
			Type:  samplerType,
			Param: samplerParam,
		},
		Reporter: &config.ReporterConfig{
			LocalAgentHostPort: reportAddr,
			LogSpans:           true,
		},
	}

	tracer, closer, err := cfg.NewTracer(config.Logger(jaeger.NullLogger))
	if err != nil {
		panic(fmt.Sprintf("Init failed: %v\n", err))
	}

	opentracing.SetGlobalTracer(tracer)

	return closer
}

func Stack() []byte {
	buf := make([]byte, 1024)
	for {
		n := runtime.Stack(buf, false)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
