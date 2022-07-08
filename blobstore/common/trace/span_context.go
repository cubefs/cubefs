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
	"math/rand"
	"sync"
	"time"
)

const (
	internalTrackLogKey = "internal-baggage-key-tracklog"
)

// ID used for spanID or traceID
type ID uint64

func (id ID) String() string {
	return fmt.Sprintf("%016x", uint64(id))
}

var (
	seededIDGen = rand.New(rand.NewSource(time.Now().UnixNano()))
	// The golang rand generators are *not* intrinsically thread-safe.
	seededIDLock sync.Mutex
)

// RandomID generate ID for traceID or spanID
func RandomID() ID {
	seededIDLock.Lock()
	defer seededIDLock.Unlock()
	return ID(seededIDGen.Int63())
}

// SpanContext implements opentracing.SpanContext
type SpanContext struct {
	// traceID represents globally unique ID of the trace.
	traceID string

	// spanID represents span ID that must be unique within its trace.
	spanID ID

	// parentID refers to the ID of the parent span.
	// Should be 0 if the current span is a root span.
	parentID ID

	// Distributed Context baggage.
	baggage map[string][]string
	sync.RWMutex
}

// ForeachBaggageItem implements opentracing.SpanContext API
func (s *SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	panic("not implements")
}

// ForeachBaggageItems will called the handler function  for each baggage key/values pair.
func (s *SpanContext) ForeachBaggageItems(handler func(k string, v []string) bool) {
	s.Lock()
	defer s.Unlock()

	for k, v := range s.baggage {
		if !handler(k, v) {
			break
		}
	}
}

func (s *SpanContext) setBaggageItem(key string, value []string) {
	s.Lock()
	defer s.Unlock()

	if s.baggage == nil {
		s.baggage = map[string][]string{key: value}
		return
	}
	s.baggage[key] = value
}

func (s *SpanContext) trackLogs() []string {
	return s.baggageItemDeepCopy(internalTrackLogKey)
}

func (s *SpanContext) append(value string) {
	s.Lock()
	defer s.Unlock()

	if s.baggage == nil {
		s.baggage = map[string][]string{internalTrackLogKey: {value}}
		return
	}

	if _, ok := s.baggage[internalTrackLogKey]; ok {
		s.baggage[internalTrackLogKey] = append(s.baggage[internalTrackLogKey], value)
		return
	}
	s.baggage[internalTrackLogKey] = []string{value}
}

func (s *SpanContext) baggageItem(key string) []string {
	s.RLock()
	defer s.RUnlock()

	return s.baggage[key]
}

func (s *SpanContext) baggageItemDeepCopy(key string) (item []string) {
	s.RLock()
	defer s.RUnlock()
	item = append(item, s.baggage[key]...)
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
