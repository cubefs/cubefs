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

package trace

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	ptlog "github.com/opentracing/opentracing-go/log"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	maxErrorLen = 32
)

// Span extends opentracing.Span
type Span interface {
	opentracing.Span

	// OperationName allows retrieving current operation name.
	OperationName() string

	// WithOperation recursively save span with operation.
	WithOperation(operation string) Span

	// Tags returns tags for span
	Tags() Tags

	// Logs returns micro logs for span
	Logs() []opentracing.LogRecord

	// String returns traceID:spanID.
	String() string

	// TraceID returns traceID
	TraceID() string

	// AppendRPCTrackLog appends RPC track logs to baggage with default key fieldTrackLogKey.
	AppendRPCTrackLog(logs []string)
	// AppendTrackLog records cost time with startTime (duration=time.Since(startTime)) for a calling to a module and
	// appends to baggage with default key fieldTrackLogKey.
	AppendTrackLog(module string, startTime time.Time, err error, opts ...SpanOption)
	// AppendTrackLogWithDuration records cost time with duration for a calling to a module and
	// appends to baggage with default key fieldTrackLogKey.
	AppendTrackLogWithDuration(module string, duration time.Duration, err error, opts ...SpanOption)
	// AppendTrackLogWithFunc records cost time for the function calling to a module and
	// appends to baggage with default key fieldTrackLogKey.
	AppendTrackLogWithFunc(module string, fn func() error, opts ...SpanOption)
	// TrackLog returns track log, calls BaggageItem with default key fieldTrackLogKey.
	TrackLog() []string

	// BaseLogger defines interface of application log apis.
	log.BaseLogger
}

// spanImpl implements Span
type spanImpl struct {
	operationName string

	tracer *Tracer

	context *SpanContext

	startTime time.Time
	duration  time.Duration

	tags Tags

	logs []opentracing.LogRecord

	// rootSpan, if true indicate that this span is the root of the (sub)tree
	// of spans and parentID is empty.
	rootSpan bool

	// references for this span
	references []opentracing.SpanReference

	rw sync.RWMutex
}

// Finish implements opentracing.Span API
func (s *spanImpl) Finish() {
	s.FinishWithOptions(opentracing.FinishOptions{})
}

// FinishWithOptions implements opentracing.Span API
func (s *spanImpl) FinishWithOptions(opts opentracing.FinishOptions) {
	finishTime := opts.FinishTime
	if finishTime.IsZero() {
		finishTime = time.Now()
	}
	s.duration = finishTime.Sub(s.startTime)

	s.rw.Lock()
	defer s.rw.Unlock()

	s.logs = append(s.logs, opts.LogRecords...)
	for _, ld := range opts.BulkLogData {
		s.logs = append(s.logs, ld.ToLogRecord())
	}

	// TODO report span
}

// Context implements opentracing.Span API
func (s *spanImpl) Context() opentracing.SpanContext {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.context
}

// OperationName returns operationName for span
func (s *spanImpl) OperationName() string {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.operationName
}

// SetOperationName implements opentracing.Span API
func (s *spanImpl) SetOperationName(operationName string) opentracing.Span {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.operationName = operationName
	return s
}

func (s *spanImpl) WithOperation(operation string) Span {
	op := s.OperationName()
	if len(op) > 0 {
		if len(operation) > 0 {
			op = fmt.Sprintf("%s:%s", op, operation)
		}
	} else {
		op = operation
	}
	return &operationSpan{
		Span:      s,
		operation: op,
	}
}

// LogFields implements opentracing.Span API
func (s *spanImpl) LogFields(fields ...ptlog.Field) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.logs = append(s.logs, opentracing.LogRecord{
		Fields:    fields,
		Timestamp: time.Now(),
	})
}

// LogKV implements opentracing.Span API
func (s *spanImpl) LogKV(keyValues ...interface{}) {
	fields, err := ptlog.InterleavedKVToFields(keyValues...)
	if err != nil {
		s.LogFields(ptlog.Error(err), ptlog.String("function", "LogKV"))
		return
	}
	s.LogFields(fields...)
}

// SetBaggageItem implements opentracing.Span API
func (s *spanImpl) SetBaggageItem(key, value string) opentracing.Span {
	for _, ref := range s.references {
		spanCtx, ok := ref.ReferencedContext.(*SpanContext)
		if !ok {
			continue
		}
		spanCtx.setBaggageItem(key, []string{value})
	}
	s.context.setBaggageItem(key, []string{value})
	return s
}

// BaggageItem implements opentracing.Span API
func (s *spanImpl) BaggageItem(key string) string {
	return strings.Join(s.context.baggageItem(key), ",")
}

// Tracer implements opentracing.Span API
func (s *spanImpl) Tracer() opentracing.Tracer {
	return s.tracer
}

// SetTag implements opentracing.Span API
func (s *spanImpl) SetTag(key string, value interface{}) opentracing.Span {
	s.rw.Lock()
	defer s.rw.Unlock()
	if s.tags == nil {
		s.tags = Tags{}
	}
	s.tags[key] = value
	return s
}

// Deprecated: use LogFields or LogKV (not implements)
func (s *spanImpl) LogEvent(event string) {
	// Deprecated: explaining why this function is empty.
}

// Deprecated: use LogFields or LogKV (not implements)
func (s *spanImpl) LogEventWithPayload(event string, payload interface{}) {
	// Deprecated: explaining why this function is empty.
}

// Deprecated: use LogFields or LogKV (not implements)
func (s *spanImpl) Log(data opentracing.LogData) {
	// Deprecated: explaining why this function is empty.
}

// Tags returns tags for span
func (s *spanImpl) Tags() Tags {
	s.rw.RLock()
	defer s.rw.RUnlock()
	// copy
	tags := make(map[string]interface{}, len(s.tags))
	for key, value := range s.tags {
		tags[key] = value
	}
	return tags
}

// Logs returns micro logs for span
func (s *spanImpl) Logs() []opentracing.LogRecord {
	s.rw.RLock()
	defer s.rw.RUnlock()
	return s.logs
}

// AppendTrackLog records cost time with startTime (duration=time.Since(startTime)) for a calling to a module and
// appends to baggage with default key fieldTrackLogKey.
func (s *spanImpl) AppendTrackLog(module string, startTime time.Time, err error, opts ...SpanOption) {
	s.AppendTrackLogWithDuration(module, time.Since(startTime), err, opts...)
}

// AppendTrackLogWithDuration records cost time with duration for a calling to a module and
// appends to baggage with default key fieldTrackLogKey.
func (s *spanImpl) AppendTrackLogWithDuration(module string, duration time.Duration, err error, opts ...SpanOption) {
	spanOpt := &spanOptions{duration: durationMs, errorLength: maxErrorLen} // compatibility
	for _, opt := range opts {
		opt(spanOpt)
	}

	if spanOpt.duration == durationAny {
		module += ":" + duration.String()
	} else if dur := spanOpt.duration.Value(duration); dur > 0 {
		module += ":" + strconv.FormatInt(dur, 10)
		if spanOpt.durationUnit {
			module += spanOpt.duration.Unit(duration)
		}
	}

	if err != nil {
		msg := err.Error()
		errLen := spanOpt.errorLength
		if len(msg) > errLen {
			msg = msg[:errLen]
		}
		module += "/" + msg
	}
	s.track(module)
}

// AppendTrackLogWithFunc records cost time for the function calling to a module.
func (s *spanImpl) AppendTrackLogWithFunc(module string, fn func() error, opts ...SpanOption) {
	startTime := time.Now()
	err := fn()
	s.AppendTrackLog(module, startTime, err, opts...)
}

// AppendRPCTrackLog appends RPC track logs to baggage with default key fieldTrackLogKey.
func (s *spanImpl) AppendRPCTrackLog(logs []string) {
	for _, trackLog := range logs {
		s.track(trackLog)
	}
}

// TrackLog returns track log, calls BaggageItem with default key fieldTrackLogKey.
func (s *spanImpl) TrackLog() []string {
	return s.context.trackLogs()
}

func (s *spanImpl) track(value string) {
	maxTracks := s.tracer.options.maxInternalTrack
	for _, ref := range s.references {
		spanCtx, ok := ref.ReferencedContext.(*SpanContext)
		if !ok {
			continue
		}
		spanCtx.append(maxTracks, value)
	}
	s.context.append(maxTracks, value)
}

// String returns traceID:spanID.
func (s *spanImpl) String() string {
	return fmt.Sprintf("%s:%s", s.context.traceID, s.context.spanID)
}

// TraceID return traceID
func (s *spanImpl) TraceID() string {
	return s.context.traceID
}

// -------------------------------------------------------------------
const (
	defaultCalldepth = 3
)

func (s *spanImpl) output(lvl log.Level, v []interface{}) {
	if log.DefaultLogger.GetOutputLevel() > lvl {
		return
	}
	log.DefaultLogger.Output(s.String(), lvl, defaultCalldepth, v...)
}

func (s *spanImpl) outputf(lvl log.Level, format string, v []interface{}) {
	if log.DefaultLogger.GetOutputLevel() > lvl {
		return
	}
	log.DefaultLogger.Outputf(s.String(), lvl, defaultCalldepth, format, v...)
}

func (s *spanImpl) Println(v ...interface{})               { s.output(log.Linfo, v) }
func (s *spanImpl) Printf(format string, v ...interface{}) { s.outputf(log.Linfo, format, v) }
func (s *spanImpl) Debug(v ...interface{})                 { s.output(log.Ldebug, v) }
func (s *spanImpl) Debugf(format string, v ...interface{}) { s.outputf(log.Ldebug, format, v) }
func (s *spanImpl) Info(v ...interface{})                  { s.output(log.Linfo, v) }
func (s *spanImpl) Infof(format string, v ...interface{})  { s.outputf(log.Linfo, format, v) }
func (s *spanImpl) Warn(v ...interface{})                  { s.output(log.Lwarn, v) }
func (s *spanImpl) Warnf(format string, v ...interface{})  { s.outputf(log.Lwarn, format, v) }
func (s *spanImpl) Error(v ...interface{})                 { s.output(log.Lerror, v) }
func (s *spanImpl) Errorf(format string, v ...interface{}) { s.outputf(log.Lerror, format, v) }

func (s *spanImpl) Panic(v ...interface{}) {
	str := fmt.Sprintln(v...)
	s.output(log.Lpanic, v)
	panic(s.String() + " -> " + str)
}

func (s *spanImpl) Panicf(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)
	s.outputf(log.Lpanic, format, v)
	panic(s.String() + " -> " + str)
}

func (s *spanImpl) Fatal(v ...interface{}) {
	s.output(log.Lfatal, v)
	os.Exit(1)
}

func (s *spanImpl) Fatalf(format string, v ...interface{}) {
	s.outputf(log.Lfatal, format, v)
	os.Exit(1)
}

// -------------------------------------------------------------------
type operationSpan struct {
	Span
	operation string
}

func (s *operationSpan) OperationName() string {
	return s.operation
}

func (s *operationSpan) SetOperationName(operation string) opentracing.Span {
	s.operation = operation
	return s
}

func (s *operationSpan) WithOperation(operation string) Span {
	op := s.OperationName()
	if len(op) > 0 {
		if len(operation) > 0 {
			op = fmt.Sprintf("%s:%s", op, operation)
		}
	} else {
		op = operation
	}
	return &operationSpan{
		Span:      s,
		operation: op,
	}
}

func (s *operationSpan) String() string {
	span := s.Span
	next := true
	for next {
		switch x := span.(type) {
		case *operationSpan:
			span = x.Span
		default:
			next = false
		}
	}
	if op := s.OperationName(); op != "" {
		return fmt.Sprintf("%s:%s", span.String(), op)
	}
	return span.String()
}

func (s *operationSpan) output(lvl log.Level, v []interface{}) {
	if log.DefaultLogger.GetOutputLevel() > lvl {
		return
	}
	log.DefaultLogger.Output(s.String(), lvl, defaultCalldepth, v...)
}

func (s *operationSpan) outputf(lvl log.Level, format string, v []interface{}) {
	if log.DefaultLogger.GetOutputLevel() > lvl {
		return
	}
	log.DefaultLogger.Outputf(s.String(), lvl, defaultCalldepth, format, v...)
}

func (s *operationSpan) Println(v ...interface{})               { s.output(log.Linfo, v) }
func (s *operationSpan) Printf(format string, v ...interface{}) { s.outputf(log.Linfo, format, v) }
func (s *operationSpan) Debug(v ...interface{})                 { s.output(log.Ldebug, v) }
func (s *operationSpan) Debugf(format string, v ...interface{}) { s.outputf(log.Ldebug, format, v) }
func (s *operationSpan) Info(v ...interface{})                  { s.output(log.Linfo, v) }
func (s *operationSpan) Infof(format string, v ...interface{})  { s.outputf(log.Linfo, format, v) }
func (s *operationSpan) Warn(v ...interface{})                  { s.output(log.Lwarn, v) }
func (s *operationSpan) Warnf(format string, v ...interface{})  { s.outputf(log.Lwarn, format, v) }
func (s *operationSpan) Error(v ...interface{})                 { s.output(log.Lerror, v) }
func (s *operationSpan) Errorf(format string, v ...interface{}) { s.outputf(log.Lerror, format, v) }

func (s *operationSpan) Panic(v ...interface{}) {
	str := fmt.Sprintln(v...)
	s.output(log.Lpanic, v)
	panic(s.String() + " -> " + str)
}

func (s *operationSpan) Panicf(format string, v ...interface{}) {
	str := fmt.Sprintf(format, v...)
	s.outputf(log.Lpanic, format, v)
	panic(s.String() + " -> " + str)
}

func (s *operationSpan) Fatal(v ...interface{}) {
	s.output(log.Lfatal, v)
	os.Exit(1)
}

func (s *operationSpan) Fatalf(format string, v ...interface{}) {
	s.outputf(log.Lfatal, format, v)
	os.Exit(1)
}
