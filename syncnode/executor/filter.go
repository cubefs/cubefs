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

package executor

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
)

// Filter matches entries on glob include/exclude + size + age. The schema
// mirrors syncnode/config.go FilterConfig but pre-parsed into ready-to-
// match values (no string units, no glob recompilation per file).
type Filter struct {
	Include []string      // raw glob patterns, matched against entry.Key basename
	Exclude []string      // same
	MinSize int64         // bytes; 0 = no lower bound
	MaxSize int64         // bytes; 0 = no upper bound
	MinAge  time.Duration // 0 = no lower bound
	MaxAge  time.Duration // 0 = no upper bound
}

// Match returns true if entry should be processed by the task.
//   - entry.IsDir entries are always excluded (directories never sync as data)
//   - include patterns OR'd: any match → ok (empty include = match-all)
//   - exclude patterns OR'd: any match → reject
//   - size / age bounds applied inclusively
//   - now is the "current time" snapshot; pass time.Now() in production,
//     a fixed time in tests to make results deterministic
func (f *Filter) Match(entry backend.Entry, now time.Time) bool {
	if entry.IsDir {
		return false
	}
	base := filepath.Base(entry.Key)

	// Excludes win over includes (matches rsync semantics).
	for _, pat := range f.Exclude {
		if ok, _ := filepath.Match(pat, base); ok {
			return false
		}
	}
	if len(f.Include) > 0 {
		matched := false
		for _, pat := range f.Include {
			if ok, _ := filepath.Match(pat, base); ok {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	if f.MinSize > 0 && entry.Size < f.MinSize {
		return false
	}
	if f.MaxSize > 0 && entry.Size > f.MaxSize {
		return false
	}

	// Age = how long ago the entry was last modified. If we don't have a
	// useful Mtime (zero value), skip age checks rather than misclassify.
	if !entry.Mtime.IsZero() {
		age := now.Sub(entry.Mtime)
		if f.MinAge > 0 && age < f.MinAge {
			return false
		}
		if f.MaxAge > 0 && age > f.MaxAge {
			return false
		}
	}

	return true
}

// ParseFilter builds a Filter from the raw config strings ("1MB", "60s"
// etc.). Returns a typed error if any field is malformed; the syncnode
// config validator catches these earlier, but ParseFilter is also called
// by API trigger paths where input may bypass schema validation.
func ParseFilter(include, exclude []string, minSize, maxSize, minAge, maxAge string) (Filter, error) {
	var f Filter
	f.Include = include
	f.Exclude = exclude
	var err error
	if f.MinSize, err = parseSize(minSize); err != nil {
		return f, fmt.Errorf("minSize: %w", err)
	}
	if f.MaxSize, err = parseSize(maxSize); err != nil {
		return f, fmt.Errorf("maxSize: %w", err)
	}
	if f.MinAge, err = parseDuration(minAge); err != nil {
		return f, fmt.Errorf("minAge: %w", err)
	}
	if f.MaxAge, err = parseDuration(maxAge); err != nil {
		return f, fmt.Errorf("maxAge: %w", err)
	}
	return f, nil
}

// parseSize accepts "<N><unit>" where unit ∈ {B,KB,MB,GB,KiB,MiB,GiB}.
// Empty string returns 0.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	units := []struct {
		suffix string
		mult   int64
	}{
		{"GiB", 1 << 30},
		{"MiB", 1 << 20},
		{"KiB", 1 << 10},
		{"GB", 1000 * 1000 * 1000},
		{"MB", 1000 * 1000},
		{"KB", 1000},
		{"B", 1},
	}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			num := strings.TrimSuffix(s, u.suffix)
			n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse %q: %w", s, err)
			}
			return n * u.mult, nil
		}
	}
	return 0, fmt.Errorf("unknown size unit in %q (expected B/KB/MB/GB or KiB/MiB/GiB)", s)
}

// parseDuration accepts "<N><unit>" where unit ∈ {s,m,h,d,w}.
// Empty string returns 0.
func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	units := []struct {
		suffix string
		mult   time.Duration
	}{
		{"w", 7 * 24 * time.Hour},
		{"d", 24 * time.Hour},
		{"h", time.Hour},
		{"m", time.Minute},
		{"s", time.Second},
	}
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			num := strings.TrimSuffix(s, u.suffix)
			n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse %q: %w", s, err)
			}
			return time.Duration(n) * u.mult, nil
		}
	}
	return 0, fmt.Errorf("unknown duration unit in %q (expected s/m/h/d/w)", s)
}
