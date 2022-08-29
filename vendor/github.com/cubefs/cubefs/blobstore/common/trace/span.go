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
	AppendTrackLog(module string, startTime time.Time, err error)
	// AppendTrackLogWithDuration records cost time with duration for a calling to a module and
	// appends to baggage with default key fieldTrackLogKey.
	AppendTrackLogWithDuration(module string, duration time.Duration, err error)
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

	sync.RWMutex
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

	s.Lock()
	defer s.Unlock()

	s.logs = append(s.logs, opts.LogRecords...)

	for _, ld := range opts.BulkLogData {
		s.logs = append(s.logs, ld.ToLogRecord())
	}

	// TODO report span
}

// Context implements opentracing.Span API
func (s *spanImpl) Context() opentracing.SpanContext {
	s.RLock()
	defer s.RUnlock()

	return s.context
}

// SetOperationName implements opentracing.Span API
func (s *spanImpl) SetOperationName(operationName string) opentracing.Span {
	s.Lock()
	defer s.Unlock()

	s.operationName = operationName
	return s
}

// LogFields implements opentracing.Span API
func (s *spanImpl) LogFields(fields ...ptlog.Field) {
	s.Lock()
	defer s.Unlock()

	lr := opentracing.LogRecord{
		Fields:    fields,
		Timestamp: time.Now(),
	}
	s.logs = append(s.logs, lr)
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
	s.Lock()
	defer s.Unlock()

	if s.tags == nil {
		s.tags = Tags{}
	}
	s.tags[key] = value
	return s
}

// Deprecated: use LogFields or LogKV (not implements)
func (s *spanImpl) LogEvent(event string) {}

// Deprecated: use LogFields or LogKV (not implements)
func (s *spanImpl) LogEventWithPayload(event string, payload interface{}) {}

// Deprecated: use LogFields or LogKV (not implements)
func (s *spanImpl) Log(data opentracing.LogData) {}

// OperationName returns operationName for span
func (s *spanImpl) OperationName() string {
	s.RLock()
	defer s.RUnlock()

	return s.operationName
}

// Tags returns tags for span
func (s *spanImpl) Tags() Tags {
	s.RLock()
	defer s.RUnlock()
	// copy
	tags := make(map[string]interface{}, len(s.tags))
	for key, value := range s.tags {
		tags[key] = value
	}
	return tags
}

// Logs returns micro logs for span
func (s *spanImpl) Logs() []opentracing.LogRecord {
	s.RLock()
	defer s.RUnlock()

	return s.logs
}

// AppendTrackLog records cost time with startTime (duration=time.Since(startTime)) for a calling to a module and
// appends to baggage with default key fieldTrackLogKey.
func (s *spanImpl) AppendTrackLog(module string, startTime time.Time, err error) {
	s.AppendTrackLogWithDuration(module, time.Since(startTime), err)
}

// AppendTrackLogWithDuration records cost time with duration for a calling to a module and
// appends to baggage with default key fieldTrackLogKey.
func (s *spanImpl) AppendTrackLogWithDuration(module string, duration time.Duration, err error) {
	durMs := duration.Nanoseconds() / 1e6
	if durMs > 0 {
		module += ":" + strconv.FormatInt(durMs, 10)
	}
	if err != nil {
		msg := err.Error()
		if len(msg) > maxErrorLen {
			msg = msg[:maxErrorLen]
		}
		module += "/" + msg
	}
	s.track(module)
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
	for _, ref := range s.references {
		spanCtx, ok := ref.ReferencedContext.(*SpanContext)
		if !ok {
			continue
		}
		spanCtx.append(value)
	}
	s.context.append(value)
}

// String returns traceID:spanID.
func (s *spanImpl) String() string {
	return fmt.Sprintf("%s:%s", s.context.traceID, s.context.spanID)
}

// TraceID return traceID
func (s *spanImpl) TraceID() string {
	return s.context.traceID
}

//-------------------------------------------------------------------
//
const (
	defaultCalldepth = 3
)

func (s *spanImpl) output(lvl log.Level, v []interface{}) {
	log.DefaultLogger.Output(s.String(), lvl, defaultCalldepth, fmt.Sprintln(v...))
}

func (s *spanImpl) outputf(lvl log.Level, format string, v []interface{}) {
	log.DefaultLogger.Output(s.String(), lvl, defaultCalldepth, fmt.Sprintf(format, v...))
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
