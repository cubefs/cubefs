package main

import (
	"path"
	"strings"
	"time"
)

// Filter decides whether a file should be transferred.
type Filter struct {
	includes   []string // glob patterns
	excludes   []string // glob patterns
	minSize    int64    // skip files smaller than this (0 = disabled)
	maxSize    int64    // skip files larger than this (0 = disabled)
	minAge     time.Duration // skip files newer than this (0 = disabled)
	maxAge     time.Duration // skip files older than this (0 = disabled)
}

// NewFilter creates a Filter from SyncOptions.
func NewFilter(opts *SyncOptions) *Filter {
	return &Filter{
		includes: opts.Include,
		excludes: opts.Exclude,
		minSize:  opts.MinSize,
		maxSize:  opts.MaxSize,
		minAge:   opts.MinAge,
		maxAge:   opts.MaxAge,
	}
}

// Allow returns true if the object passes all filter rules.
func (f *Filter) Allow(key string, size int64, mtime time.Time) bool {
	name := path.Base(key)

	// include rules: if any include rules exist, file must match at least one
	if len(f.includes) > 0 {
		matched := false
		for _, pat := range f.includes {
			if matchGlob(pat, name) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// exclude rules: file must not match any exclude rule
	for _, pat := range f.excludes {
		if matchGlob(pat, name) {
			return false
		}
	}

	// size filters
	if f.minSize > 0 && size < f.minSize {
		return false
	}
	if f.maxSize > 0 && size > f.maxSize {
		return false
	}

	// age filters
	if !mtime.IsZero() {
		age := time.Since(mtime)
		if f.minAge > 0 && age < f.minAge { // file is too new
			return false
		}
		if f.maxAge > 0 && age > f.maxAge { // file is too old
			return false
		}
	}

	return true
}

// matchGlob matches a simple glob pattern against a name.
// Supports * (any sequence except /) and ? (single char).
func matchGlob(pattern, name string) bool {
	// Use path.Match which implements shell glob semantics.
	// path.Match treats / specially; since name is already base, this is fine.
	ok, err := path.Match(pattern, name)
	if err != nil {
		// invalid pattern – treat as no-match
		return false
	}
	if ok {
		return true
	}
	// Also try matching the full relative path for patterns like "dir/*.go"
	ok, _ = path.Match(pattern, strings.TrimPrefix(name, "/"))
	return ok
}
