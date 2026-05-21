package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/tool/cfs-sync/storage"
)

// SyncOptions holds all user-configurable sync parameters.
type SyncOptions struct {
	// Comparison
	SizeOnly       bool
	Checksum       bool
	IgnoreExisting bool

	// Concurrency
	Transfers   int
	Checkers    int
	ListWorkers int

	// Transfer control
	PartSize          int64
	MultiThreadCutoff int64

	// Delete / backup
	Delete    bool
	BackupDir string
	Suffix    string

	// Filters
	Include []string
	Exclude []string
	MinSize int64
	MaxSize int64
	MinAge  time.Duration
	MaxAge  time.Duration

	// Files-from (explicit list, no directory traversal)
	FilesFrom string

	// Limits
	MaxTransfer int64
	MaxDuration time.Duration

	// Retry
	Retries      int
	RetriesSleep time.Duration
	IgnoreErrors bool

	// Output
	DryRun   bool
	Progress bool
	Stats    time.Duration
	LogDir   string
	LogLevel string
}

// DefaultSyncOptions returns SyncOptions with sensible defaults.
func DefaultSyncOptions() SyncOptions {
	return SyncOptions{
		Transfers:         10,
		Checkers:          20,
		ListWorkers:       20,
		PartSize:          64 * 1024 * 1024,
		MultiThreadCutoff: 256 * 1024 * 1024,
		Retries:           3,
		RetriesSleep:      time.Second,
		Stats:             time.Minute,
		LogDir:            "/tmp/cfs-sync-logs",
		LogLevel:          "WARN",
	}
}

// Task is a unit of work produced by the diff phase.
type Task struct {
	SrcKey  string
	DstKey  string
	Size    int64
	DstSize int64
	Mtime   time.Time
	DstMtime time.Time
	Op      TaskOp
}

// TaskOp is the operation type for a sync task.
type TaskOp int

const (
	OpCopy   TaskOp = iota
	OpDelete TaskOp = iota
)

// Syncer orchestrates the 3-stage List → Check → Transfer pipeline.
type Syncer struct {
	src    storage.Storage
	dst    storage.Storage
	opts   SyncOptions
	filter *Filter
	stats  *Stats
}

// NewSyncer creates a Syncer.
func NewSyncer(src, dst storage.Storage, opts SyncOptions) *Syncer {
	return &Syncer{
		src:    src,
		dst:    dst,
		opts:   opts,
		filter: NewFilter(&opts),
		stats:  newStats(),
	}
}

// Run executes the sync and returns the number of failed files.
func (s *Syncer) Run(ctx context.Context) int64 {
	statsInterval := s.opts.Stats
	if s.opts.Progress && statsInterval <= 0 {
		statsInterval = 5 * time.Second
	}
	if statsInterval > 0 {
		go func() {
			ticker := time.NewTicker(statsInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					s.stats.print()
				}
			}
		}()
	}

	if s.opts.FilesFrom != "" {
		return s.runFilesFrom(ctx)
	}

	// Stage 1: start listing both sides concurrently.
	srcObjs, srcErr := s.src.List(ctx, "")
	dstObjs, dstErr := s.dst.List(ctx, "")

	// Stage 2: merge-diff → produce check tasks.
	checkQ := make(chan Task, 512)
	go func() {
		defer close(checkQ)
		s.mergeDiff(ctx, srcObjs, srcErr, dstObjs, dstErr, checkQ)
	}()

	// Stage 3: checker pool → transfer queue.
	transferQ := make(chan Task, 512)
	go func() {
		defer close(transferQ)
		s.runCheckers(ctx, checkQ, transferQ)
	}()

	failed := s.runWorkers(ctx, transferQ)
	s.stats.print()
	return failed
}

// mergeDiff does a streaming merge of sorted src and dst object lists and
// produces copy/delete tasks.
func (s *Syncer) mergeDiff(
	ctx context.Context,
	srcCh <-chan *storage.Object, srcErrCh <-chan error,
	dstCh <-chan *storage.Object, dstErrCh <-chan error,
	out chan<- Task,
) {
	srcObj, srcOk := <-srcCh
	dstObj, dstOk := <-dstCh

	emit := func(t Task) bool {
		select {
		case <-ctx.Done():
			return false
		case out <- t:
			return true
		}
	}

	for srcOk || dstOk {
		select {
		case <-ctx.Done():
			return
		default:
		}

		switch {
		case srcOk && (!dstOk || srcObj.Key < dstObj.Key):
			// src only → copy
			if !srcObj.IsDir && s.filter.Allow(srcObj.Key, srcObj.Size, srcObj.Mtime) {
				if !emit(Task{SrcKey: srcObj.Key, DstKey: srcObj.Key, Size: srcObj.Size, Mtime: srcObj.Mtime, Op: OpCopy}) {
					return
				}
			}
			srcObj, srcOk = <-srcCh

		case dstOk && (!srcOk || dstObj.Key < srcObj.Key):
			// dst only → delete (if --delete)
			if !dstObj.IsDir && s.opts.Delete {
				if !emit(Task{DstKey: dstObj.Key, Op: OpDelete}) {
					return
				}
			}
			dstObj, dstOk = <-dstCh

		default:
			// same key on both sides → checker decides
			if !srcObj.IsDir && s.filter.Allow(srcObj.Key, srcObj.Size, srcObj.Mtime) {
				if !emit(Task{SrcKey: srcObj.Key, DstKey: dstObj.Key, Size: srcObj.Size, DstSize: dstObj.Size, Mtime: srcObj.Mtime, DstMtime: dstObj.Mtime, Op: OpCopy}) {
					return
				}
			}
			srcObj, srcOk = <-srcCh
			dstObj, dstOk = <-dstCh
		}
	}

	if err := <-srcErrCh; err != nil {
		fmt.Fprintf(os.Stderr, "warn: src list error: %v\n", err)
	}
	if err := <-dstErrCh; err != nil {
		fmt.Fprintf(os.Stderr, "warn: dst list error: %v\n", err)
	}
}

// runCheckers decides which tasks actually need transferring.
func (s *Syncer) runCheckers(ctx context.Context, in <-chan Task, out chan<- Task) {
	sem := make(chan struct{}, s.opts.Checkers)
	var wg sync.WaitGroup

	for task := range in {
		task := task
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()

			if task.Op == OpDelete {
				select {
				case <-ctx.Done():
				case out <- task:
				}
				return
			}

			s.stats.FilesChecked.Add(1)

			if s.opts.IgnoreExisting {
				s.stats.FilesSkipped.Add(1)
				return
			}

			// Size-only comparison: skip if both sides report a positive equal size.
			if s.opts.SizeOnly && task.Size > 0 && task.DstSize > 0 && task.Size == task.DstSize {
				s.stats.FilesSkipped.Add(1)
				return
			}

			// Default: skip if size and mtime both match (within 1-second tolerance for
			// filesystems with coarse timestamp resolution).
			if !s.opts.Checksum && task.Size == task.DstSize &&
				!task.DstMtime.IsZero() && absDuration(task.Mtime.Sub(task.DstMtime)) < time.Second {
				s.stats.FilesSkipped.Add(1)
				return
			}

			// By default forward all copy tasks; checksum comparison would go here.
			select {
			case <-ctx.Done():
			case out <- task:
			}
		}()
	}
	wg.Wait()
}

// runWorkers executes transfer tasks concurrently.
func (s *Syncer) runWorkers(ctx context.Context, in <-chan Task) int64 {
	type result struct{ err error }
	results := make(chan result, 256)
	sem := make(chan struct{}, s.opts.Transfers)

	var wg sync.WaitGroup
	go func() {
		for task := range in {
			task := task
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				var err error
				if s.opts.DryRun {
					target := task.SrcKey
					if task.Op == OpDelete {
						target = task.DstKey
					}
					fmt.Printf("[dry-run] %s %s\n", opName(task.Op), target)
				} else {
					err = s.executeWithRetry(ctx, task)
				}
				results <- result{err: err}
			}()
		}
		wg.Wait()
		close(results)
	}()

	var failed int64
	for r := range results {
		if r.err != nil {
			if errors.Is(r.err, context.Canceled) || errors.Is(r.err, context.DeadlineExceeded) {
				s.stats.FilesSkipped.Add(1)
			} else {
				failed++
				s.stats.FilesFailed.Add(1)
				if !s.opts.IgnoreErrors {
					fmt.Fprintf(os.Stderr, "error: %v\n", r.err)
				}
			}
		}
	}
	return failed
}

func (s *Syncer) executeWithRetry(ctx context.Context, task Task) error {
	var err error
	for attempt := 0; attempt <= s.opts.Retries; attempt++ {
		if attempt > 0 {
			sleep := s.opts.RetriesSleep * time.Duration(1<<uint(attempt-1))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleep):
			}
		}
		if err = s.execute(ctx, task); err == nil {
			return nil
		}
	}
	return fmt.Errorf("%s %s: %w", opName(task.Op), task.SrcKey, err)
}

func (s *Syncer) execute(ctx context.Context, task Task) error {
	switch task.Op {
	case OpCopy:
		return s.copyFile(ctx, task)
	case OpDelete:
		err := s.dst.Delete(ctx, task.DstKey)
		if err == nil {
			s.stats.FilesDeleted.Add(1)
		}
		return err
	default:
		return fmt.Errorf("unknown op %d", task.Op)
	}
}

func (s *Syncer) copyFile(ctx context.Context, task Task) error {
	r, err := s.src.Get(ctx, task.SrcKey, 0, 0)
	if err != nil {
		return fmt.Errorf("get %s: %w", task.SrcKey, err)
	}
	defer r.Close()

	if err = s.dst.PutWithMtime(ctx, task.DstKey, r, task.Size, task.Mtime); err != nil {
		return fmt.Errorf("put %s: %w", task.DstKey, err)
	}
	s.stats.FilesTransferred.Add(1)
	s.stats.BytesTransferred.Add(task.Size)
	return nil
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func (s *Syncer) runFilesFrom(ctx context.Context) int64 {
	f, err := os.Open(s.opts.FilesFrom)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: open --files-from %s: %v\n", s.opts.FilesFrom, err)
		return 1
	}
	defer f.Close()

	tasks := make(chan Task, 256)
	go func() {
		defer close(tasks)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case tasks <- Task{SrcKey: line, DstKey: line, Op: OpCopy}:
			}
		}
	}()

	return s.runWorkers(ctx, tasks)
}

func opName(op TaskOp) string {
	switch op {
	case OpCopy:
		return "copy"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}
