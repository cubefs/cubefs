package proto

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPacketContext(t *testing.T) {
	t.Log(NewPacket().Span().TraceID())
	p1 := NewPacketReqID()
	p1.ReqID = RandomID()
	ctx1 := p1.Context()
	span1 := p1.Span()
	var nilCtx context.Context
	require.Panics(t, func() { p1.WithContext(nilCtx) })
	t.Log(p1)

	type userValue struct{}
	ctx2 := context.WithValue(ctx1, userValue{}, "user-context")
	p2 := p1.GetCopy().WithContext(ctx2)
	span2 := p2.Span()
	require.Equal(t, span1.TraceID(), span2.TraceID())

	ctx3 := context.WithValue(context.Background(), userValue{}, TraceID())
	p3 := p1.WithContext(ctx3)
	span3 := p3.Span()
	require.NotEqual(t, span1.TraceID(), span3.TraceID())

	ctx4 := ContextWithOperation(ctx3, "test")
	require.NotEqual(t, span3.TraceID(), SpanFromContext(ctx4).TraceID())
	require.NotEqual(t, SpanFromContext(ctx3).TraceID(), SpanFromContext(ctx4).TraceID())

	ctx5 := ContextWithOperationf(ctx3, "test %v", 1)
	require.NotEqual(t, span3.TraceID(), SpanFromContext(ctx5).TraceID())

	span6, ctx6 := SpanContext()
	require.Equal(t, span6.TraceID(), SpanFromContext(ctx6).TraceID())

	span7, ctx7 := SpanContextPrefix("test-")
	require.Equal(t, span7.TraceID(), SpanFromContext(ctx7).TraceID())
}

func BenchmarkPacketSpan(b *testing.B) {
	p := &Packet{}
	p.Context()
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		p.Span()
	}
}

func BenchmarkPacketContext(b *testing.B) {
	p := &Packet{}
	p.Context()
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		p.Context()
	}
}

func BenchmarkPacketWithContext(b *testing.B) {
	p := &Packet{}
	p.Context()
	b.ResetTimer()
	for ii := 0; ii < b.N; ii++ {
		p.WithContext(context.Background())
	}
}
