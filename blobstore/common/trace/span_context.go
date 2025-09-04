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
	"bytes"
	"hash/maphash"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	"github.com/cubefs/cubefs/blobstore/util"
)

const (
	internalTrackLogKey = "internal-baggage-key-tracklog"

	// | --- trace(47) --- | ':'(1) | -- span(16) -- |
	_lenContextID = 64
	_lenTraceID   = 47
	_indexColon   = 47
	_lenID        = 16
)

// ID used for spanID or traceID
type ID uint64

func (id ID) String() string {
	arr := id.bytes()
	return string(arr[:])
}

func (id ID) bytes() (arr [16]byte) {
	idx := 16
	ii := uint64(id)
	for ii > 0 {
		idx--
		arr[idx] = util.HexDigits[ii%16]
		ii = ii / 16
	}
	for idx > 0 {
		idx--
		arr[idx] = '0'
	}
	return
}

// RandomID generate ID for traceID or spanID
func RandomID() ID {
	return ID(new(maphash.Hash).Sum64())
}

// SpanContext implements opentracing.SpanContext
type SpanContext struct {
	// id is traceID + ":" + spanID.
	// traceID represents globally unique ID of the trace.
	id         [_lenContextID]byte
	traceIndex int

	// spanID represents span ID that must be unique within its trace.
	spanID ID

	// parentID refers to the ID of the parent span.
	// Should be 0 if the current span is a root span.
	parentID ID

	// Distributed Context baggage, write or read logItems's Buffer should with lock.
	baggage map[string]*logItems
	sync.RWMutex

	spanFromPool *spanImpl
	optsCarrier  []opentracing.StartSpanOption
}

func newCacheableSpanContext() *SpanContext {
	ctx := &SpanContext{}
	carrier := &startOptionCarrier{}
	carrier.opts.References = append(carrier.opts.References,
		opentracing.SpanReference{
			Type:              opentracing.SpanReferenceType(-1),
			ReferencedContext: ctx,
		})
	tag := ext.SpanKindRPCServer
	carrier.opts.Tags = map[string]interface{}{tag.Key: tag.Value}
	ctx.optsCarrier = append(ctx.optsCarrier, carrier)
	return ctx
}

// ForeachBaggageItem implements opentracing.SpanContext API
func (s *SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	panic("not implements")
}

// ForeachBaggageItems will called the handler function  for each baggage key/values pair.
func (s *SpanContext) ForeachBaggageItems(handler func(k string, buffers []*bytes.Buffer) bool) {
	s.RLock()
	for k, v := range s.baggage {
		if !handler(k, v.bufs) {
			break
		}
	}
	s.RUnlock()
}

func (s *SpanContext) clearID() {
	var arr [_lenContextID]byte
	copy(s.id[:], arr[:])
}

func (s *SpanContext) resetID(traceID string) {
	var traceBytes []byte
	if traceID == "" {
		arr := RandomID().bytes()
		traceBytes = arr[:]
	} else {
		traceBytes = []byte(traceID)
	}
	idx := _lenTraceID - len(traceBytes)
	if idx < 0 {
		idx = 0
	}
	s.traceIndex = idx
	copy(s.id[s.traceIndex:_lenTraceID], traceBytes)
	s.id[_indexColon] = ':'
	arr := s.spanID.bytes()
	copy(s.id[_indexColon+1:], arr[:])
}

func (s *SpanContext) traceID() string {
	return string(s.id[s.traceIndex:_lenTraceID])
}

// _getBaggage without lock
func (s *SpanContext) _getBaggage(key string) (item *logItems) {
	if s.baggage == nil {
		s.baggage = map[string]*logItems{key: {}}
	}
	item = s.baggage[key]
	if item == nil {
		item = &logItems{}
		s.baggage[key] = item
	}
	return
}

func (s *SpanContext) setBaggageItemBytes(key string, value []*bytes.Buffer) {
	s.Lock()
	s._getBaggage(key).setBytes(value)
	s.Unlock()
}

func (s *SpanContext) setBaggageItem(key string, value []string) {
	s.Lock()
	s._getBaggage(key).setString(value)
	s.Unlock()
}

func (s *SpanContext) traceLogs() (logs []string) {
	s.RLock()
	track := s.baggage[internalTrackLogKey]
	s.RUnlock()
	if track == nil {
		return nil
	}
	logs = s.baggageItem(internalTrackLogKey)
	return
}

func (s *SpanContext) trackLogsN() (n int) {
	s.RLock()
	if track := s.baggage[internalTrackLogKey]; track != nil {
		n = track.length()
	}
	s.RUnlock()
	return
}

func (s *SpanContext) trackLogsRange(f func(*bytes.Buffer) bool) {
	s.RLock()
	if track := s.baggage[internalTrackLogKey]; track != nil {
		for idx := range track.bufs {
			if !f(track.bufs[idx]) {
				break
			}
		}
	}
	s.RUnlock()
}

func (s *SpanContext) appendTrack(f func(nextBuffer func(maxTracks int) *bytes.Buffer)) {
	s.Lock()
	f(s._nextTrack)
	s.Unlock()
}

func (s *SpanContext) appendBaggage(f func(nextBuffer func(key string, maxTracks int) *bytes.Buffer)) {
	s.Lock()
	f(s._nextBuffer)
	s.Unlock()
}

// _nextTrack without lock
func (s *SpanContext) _nextTrack(maxTracks int) *bytes.Buffer {
	return s._nextBuffer(internalTrackLogKey, maxTracks)
}

// _nextBuffer without lock
func (s *SpanContext) _nextBuffer(key string, maxTracks int) (b *bytes.Buffer) {
	track := s._getBaggage(key)
	if track.length() < maxTracks {
		track.grow(1)
		b = track.bufs[track.length()-1]
		b.Reset()
	}
	return
}

func (s *SpanContext) baggageItem(key string) (item []string) {
	s.RLock()
	track := s.baggage[key]
	if track.length() > 0 {
		item = make([]string, 0, track.length())
		for idx := range track.bufs {
			item = append(item, track.bufs[idx].String())
		}
	}
	s.RUnlock()
	return
}

// IsValid returns true if SpanContext is valid
func (s *SpanContext) IsValid() bool {
	return s.traceID() != "" && s.spanID != 0
}

// IsEmpty returns true is span context is empty
func (s *SpanContext) IsEmpty() bool {
	s.RLock()
	l := len(s.baggage)
	s.RUnlock()
	return !s.IsValid() && l == 0
}

type logItems struct {
	bufs []*bytes.Buffer
}

func (b *logItems) length() int {
	return len(b.bufs)
}

func (b *logItems) grow(n int) {
	capacity := cap(b.bufs)
	length := len(b.bufs)
	if need := length + n - capacity; need > 0 {
		if need+capacity < 4 { // make 4 first time
			need = 4 - capacity
		}
		bufs := make([]*bytes.Buffer, need)
		b.bufs = append(b.bufs[:capacity], bufs...)
		b.bufs = b.bufs[:cap(b.bufs)]
		for idx := range b.bufs {
			if b.bufs[idx] == nil {
				b.bufs[idx] = bytes.NewBuffer(nil)
			}
		}
	}
	b.bufs = b.bufs[:length+n]
}

func (b *logItems) setString(v []string) {
	b.bufs = b.bufs[:0]
	b.grow(len(v))
	for idx := range b.bufs {
		b.bufs[idx].Reset()
		b.bufs[idx].WriteString(v[idx])
	}
}

func (b *logItems) setBytes(v []*bytes.Buffer) {
	b.bufs = b.bufs[:0]
	b.grow(len(v))
	for idx := range b.bufs {
		b.bufs[idx].Reset()
		b.bufs[idx].Write(v[idx].Bytes())
	}
}
