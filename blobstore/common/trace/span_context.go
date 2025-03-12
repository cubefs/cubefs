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

	"github.com/cubefs/cubefs/blobstore/util"
)

const (
	internalTrackLogKey = "internal-baggage-key-tracklog"
)

// ID used for spanID or traceID
type ID uint64

func (id ID) String() string {
	var arr [16]byte
	for idx := range arr {
		arr[idx] = '0'
	}
	idx := 16
	ii := uint64(id)
	for ii > 0 {
		idx -= 1
		arr[idx] = util.HexDigits[ii%16]
		ii = ii / 16
	}
	return string(arr[idx:])
}

// RandomID generate ID for traceID or spanID
func RandomID() ID {
	return ID(new(maphash.Hash).Sum64())
}

// SpanContext implements opentracing.SpanContext
type SpanContext struct {
	// id traceID + ":" + spanID
	id string

	// traceID represents globally unique ID of the trace.
	traceID string

	// spanID represents span ID that must be unique within its trace.
	spanID ID

	// parentID refers to the ID of the parent span.
	// Should be 0 if the current span is a root span.
	parentID ID

	// Distributed Context baggage.
	baggage map[string]*logItems
	sync.RWMutex

	spanFromPool *spanImpl
}

// ForeachBaggageItem implements opentracing.SpanContext API
func (s *SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	panic("not implements")
}

// ForeachBaggageItems will called the handler function  for each baggage key/values pair.
func (s *SpanContext) ForeachBaggageItems(handler func(k string, buffers []*bytes.Buffer) bool) {
	s.Lock()
	for k, v := range s.baggage {
		if !handler(k, v.bufs) {
			break
		}
	}
	s.Unlock()
}

func (s *SpanContext) resetID() {
	s.id = s.traceID + ":" + s.spanID.String()
}

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
	s._getBaggage(internalTrackLogKey)
	logs = s.baggageItem(internalTrackLogKey)
	s.RUnlock()
	return
}

func (s *SpanContext) trackLogsN() (n int) {
	s.RLock()
	n = s._getBaggage(internalTrackLogKey).length()
	s.RUnlock()
	return
}

func (s *SpanContext) trackLogsRange(f func(*bytes.Buffer) bool) {
	s.RLock()
	track := s._getBaggage(internalTrackLogKey)
	for idx := range track.bufs {
		if !f(track.bufs[idx]) {
			break
		}
	}
	s.RUnlock()
}

func (s *SpanContext) nextTrack(maxTracks int) (b *bytes.Buffer) {
	return s.nextBuffer(internalTrackLogKey, maxTracks)
}

func (s *SpanContext) nextBuffer(key string, maxTracks int) (b *bytes.Buffer) {
	s.Lock()
	track := s._getBaggage(key)
	if track.length() < maxTracks {
		track.grow(1)
		b = track.bufs[track.length()-1]
		b.Reset()
	}
	s.Unlock()
	return
}

func (s *SpanContext) baggageItem(key string) (item []string) {
	track := s.baggage[key]
	if track.length() > 0 {
		item = make([]string, 0, track.length())
		for idx := range track.bufs {
			item = append(item, track.bufs[idx].String())
		}
	}
	return
}

// IsValid returns true if SpanContext is valid
func (s *SpanContext) IsValid() bool {
	return s.traceID != "" && s.spanID != 0
}

// IsEmpty returns true is span context is empty
func (s *SpanContext) IsEmpty() bool {
	return !s.IsValid() && len(s.baggage) == 0
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
