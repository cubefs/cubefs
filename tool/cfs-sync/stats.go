package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Stats tracks sync progress counters.
type Stats struct {
	FilesChecked   atomic.Int64
	FilesTransferred atomic.Int64
	FilesSkipped   atomic.Int64
	FilesDeleted   atomic.Int64
	FilesFailed    atomic.Int64
	BytesTransferred atomic.Int64
	start          time.Time
}

func newStats() *Stats {
	return &Stats{start: time.Now()}
}

func (s *Stats) elapsed() time.Duration {
	return time.Since(s.start)
}

func (s *Stats) throughputMBs() float64 {
	sec := s.elapsed().Seconds()
	if sec <= 0 {
		return 0
	}
	return float64(s.BytesTransferred.Load()) / 1024 / 1024 / sec
}

func (s *Stats) print() {
	elapsed := s.elapsed()
	fmt.Printf(
		"Transferred: %s, %s, %.2f MB/s\n"+
			"Checks: %d, Transferred: %d, Skipped: %d, Deleted: %d, Failed: %d\n"+
			"Elapsed: %s\n",
		humanBytes(s.BytesTransferred.Load()),
		humanCount(s.FilesTransferred.Load(), "file"),
		s.throughputMBs(),
		s.FilesChecked.Load(),
		s.FilesTransferred.Load(),
		s.FilesSkipped.Load(),
		s.FilesDeleted.Load(),
		s.FilesFailed.Load(),
		elapsed.Round(time.Millisecond),
	)
}

func humanBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.2f GiB", float64(b)/gb)
	case b >= mb:
		return fmt.Sprintf("%.2f MiB", float64(b)/mb)
	case b >= kb:
		return fmt.Sprintf("%.2f KiB", float64(b)/kb)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func humanCount(n int64, unit string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, unit)
	}
	return fmt.Sprintf("%d %ss", n, unit)
}
