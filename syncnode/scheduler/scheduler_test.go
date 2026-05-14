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

package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/spec"
	"github.com/cubefs/cubefs/syncnode/tasks"
)

// --- test doubles ---------------------------------------------------------

// stubTrigger counts Trigger calls per ruleID. errFor injects per-rule
// errors. Safe for concurrent use.
type stubTrigger struct {
	mu     sync.Mutex
	fires  map[string]int
	errFor map[string]error
}

func newStubTrigger() *stubTrigger {
	return &stubTrigger{
		fires:  make(map[string]int),
		errFor: make(map[string]error),
	}
}

func (s *stubTrigger) Trigger(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fires[ruleID]++
	n := s.fires[ruleID]
	if err, ok := s.errFor[ruleID]; ok {
		return nil, err
	}
	return &tasks.Record{TaskID: fmt.Sprintf("%s-%d", ruleID, n), RuleID: ruleID}, nil
}

func (s *stubTrigger) count(ruleID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fires[ruleID]
}

// stubStore is the smallest rules.Store the scheduler needs in tests. The
// scheduler does NOT call List itself (the admin layer does); we still
// implement enough to satisfy the interface so production wiring code can
// share the same shape.
type stubStore struct{ mu sync.Mutex }

func (s *stubStore) List(ctx context.Context) ([]*rules.Rule, error) {
	return nil, nil
}

func (s *stubStore) Get(ctx context.Context, id string) (*rules.Rule, error) {
	return nil, rules.ErrRuleNotFound
}
func (s *stubStore) Create(ctx context.Context, r *rules.Rule) error            { return nil }
func (s *stubStore) Update(ctx context.Context, r *rules.Rule) error            { return nil }
func (s *stubStore) Delete(ctx context.Context, id string) error                { return nil }
func (s *stubStore) SetState(ctx context.Context, id string, st rules.State) error { return nil }
func (s *stubStore) UpdateLastRun(ctx context.Context, id string, last rules.LastRunSummary) error {
	return nil
}
func (s *stubStore) Close() error { return nil }

// captureLogger buffers Printf output for assertion in tests that want to
// see the rejected-rule diagnostic.
type captureLogger struct {
	mu    sync.Mutex
	lines []string
}

func (c *captureLogger) Printf(format string, args ...interface{}) {
	c.mu.Lock()
	c.lines = append(c.lines, fmt.Sprintf(format, args...))
	c.mu.Unlock()
}

func (c *captureLogger) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

// --- helpers --------------------------------------------------------------

// mkRule constructs a minimal rules.Rule for scheduler tests. Only the
// fields the scheduler cares about (ID, Schedule, State) are exercised.
func mkRule(id, schedule string, state rules.State) *rules.Rule {
	return &rules.Rule{
		Config: spec.RuleConfig{ID: id, Schedule: schedule},
		State:  state,
	}
}

// waitForCount blocks until trigger.count(ruleID) >= want or timeout
// elapses. Returns the observed count.
func waitForCount(t *testing.T, trig *stubTrigger, ruleID string, want int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c := trig.count(ruleID); c >= want {
			return c
		}
		time.Sleep(20 * time.Millisecond)
	}
	return trig.count(ruleID)
}

// newStarted returns a Started scheduler wired to the supplied trigger.
// Caller is responsible for calling Stop in t.Cleanup.
func newStarted(t *testing.T, trig Trigger, opts ...Option) *Scheduler {
	t.Helper()
	s := New(&stubStore{}, trig, opts...)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })
	return s
}

// --- core behaviour -------------------------------------------------------

func TestNewSchedulerIsEmpty(t *testing.T) {
	s := New(&stubStore{}, newStubTrigger())
	if got := s.RegisteredCount(); got != 0 {
		t.Fatalf("RegisteredCount on fresh scheduler = %d, want 0", got)
	}
}

func TestApplyRulesRegistersAndFires(t *testing.T) {
	trig := newStubTrigger()
	s := newStarted(t, trig)

	// Every-second schedule (6 fields) so the test runs in seconds.
	rs := []*rules.Rule{mkRule("r1", "* * * * * *", rules.StateActive)}
	if err := s.ApplyRules(rs); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}
	if got := s.RegisteredCount(); got != 1 {
		t.Fatalf("RegisteredCount = %d, want 1", got)
	}

	// Allow up to 4 seconds to see at least 3 fires; the first fire happens
	// at the next boundary, so ~3 fires within 4s is the conservative floor.
	got := waitForCount(t, trig, "r1", 3, 4500*time.Millisecond)
	if got < 3 {
		t.Fatalf("expected >=3 fires in 4.5s, got %d", got)
	}
}

func TestApplyRulesSkipsPaused(t *testing.T) {
	trig := newStubTrigger()
	s := newStarted(t, trig)

	rs := []*rules.Rule{
		mkRule("active", "* * * * * *", rules.StateActive),
		mkRule("paused", "* * * * * *", rules.StatePaused),
		mkRule("degraded", "* * * * * *", rules.StateDegraded),
	}
	if err := s.ApplyRules(rs); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}
	if got := s.RegisteredCount(); got != 1 {
		t.Fatalf("RegisteredCount = %d, want 1 (only active)", got)
	}

	// Wait long enough for the active rule to fire at least once, then
	// confirm paused/degraded never fired.
	if got := waitForCount(t, trig, "active", 1, 2500*time.Millisecond); got < 1 {
		t.Fatalf("active rule didn't fire: got %d", got)
	}
	if got := trig.count("paused"); got != 0 {
		t.Fatalf("paused rule fired %d times, want 0", got)
	}
	if got := trig.count("degraded"); got != 0 {
		t.Fatalf("degraded rule fired %d times, want 0", got)
	}
}

func TestApplyRulesSkipsEmptySchedule(t *testing.T) {
	trig := newStubTrigger()
	s := newStarted(t, trig)

	rs := []*rules.Rule{
		mkRule("manual", "", rules.StateActive),
		mkRule("scheduled", "* * * * * *", rules.StateActive),
	}
	if err := s.ApplyRules(rs); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}
	if got := s.RegisteredCount(); got != 1 {
		t.Fatalf("RegisteredCount = %d, want 1 (only scheduled)", got)
	}
	if got := waitForCount(t, trig, "scheduled", 1, 2500*time.Millisecond); got < 1 {
		t.Fatalf("scheduled rule didn't fire: got %d", got)
	}
	if got := trig.count("manual"); got != 0 {
		t.Fatalf("manual rule fired %d times, want 0", got)
	}
}

func TestApplyRulesReplacesChangedSchedule(t *testing.T) {
	trig := newStubTrigger()
	s := newStarted(t, trig)

	// Start with a schedule that fires VERY infrequently so we can prove
	// the first entry is removed before the new schedule takes effect.
	if err := s.ApplyRules([]*rules.Rule{
		mkRule("r1", "0 0 1 1 *", rules.StateActive), // once per year
	}); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}
	if got := s.RegisteredCount(); got != 1 {
		t.Fatalf("RegisteredCount after first apply = %d, want 1", got)
	}

	// Re-apply with an every-second schedule. The old entry should be
	// removed and the new one registered.
	if err := s.ApplyRules([]*rules.Rule{
		mkRule("r1", "* * * * * *", rules.StateActive),
	}); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}
	if got := s.RegisteredCount(); got != 1 {
		t.Fatalf("RegisteredCount after schedule change = %d, want 1", got)
	}
	// The new schedule should produce fires within seconds.
	if got := waitForCount(t, trig, "r1", 2, 3*time.Second); got < 2 {
		t.Fatalf("expected >=2 fires after re-schedule, got %d", got)
	}
}

func TestApplyRulesRemovesAbsentRule(t *testing.T) {
	trig := newStubTrigger()
	s := newStarted(t, trig)

	if err := s.ApplyRules([]*rules.Rule{
		mkRule("r1", "* * * * * *", rules.StateActive),
	}); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	// Let it fire at least once so we know the entry was live.
	if got := waitForCount(t, trig, "r1", 1, 2500*time.Millisecond); got < 1 {
		t.Fatalf("r1 didn't fire before removal: %d", got)
	}

	// Re-apply with empty rule set; entry should be removed.
	if err := s.ApplyRules(nil); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if got := s.RegisteredCount(); got != 0 {
		t.Fatalf("RegisteredCount after removal = %d, want 0", got)
	}

	// Record the count and confirm it doesn't grow over the next 2s.
	before := trig.count("r1")
	time.Sleep(2200 * time.Millisecond)
	if after := trig.count("r1"); after != before {
		t.Fatalf("r1 fired after removal: before=%d after=%d", before, after)
	}
}

func TestApplyRulesOneBadOtherStillRegisters(t *testing.T) {
	trig := newStubTrigger()
	cap := &captureLogger{}
	s := newStarted(t, trig, WithLogger(cap))

	rs := []*rules.Rule{
		mkRule("good", "* * * * * *", rules.StateActive),
		mkRule("bad", "not a real cron", rules.StateActive),
	}
	err := s.ApplyRules(rs)
	if err == nil {
		t.Fatalf("expected non-nil error from bad schedule, got nil")
	}
	if !strings.Contains(err.Error(), "bad") {
		t.Fatalf("error doesn't mention bad rule: %v", err)
	}
	if !strings.Contains(cap.text(), "bad") {
		t.Fatalf("logger didn't capture bad rule: %q", cap.text())
	}
	if got := s.RegisteredCount(); got != 1 {
		t.Fatalf("RegisteredCount = %d, want 1 (good still armed)", got)
	}
	if got := waitForCount(t, trig, "good", 1, 2500*time.Millisecond); got < 1 {
		t.Fatalf("good rule didn't fire despite valid schedule: %d", got)
	}
}

func TestStopIsIdempotent(t *testing.T) {
	s := New(&stubStore{}, newStubTrigger())
	// Stop before Start: no-op.
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
	// Start and Stop normally; second Stop must be a no-op.
	s2 := New(&stubStore{}, newStubTrigger())
	if err := s2.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := s2.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := s2.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	// ApplyRules after Stop should error.
	if err := s2.ApplyRules(nil); err == nil {
		t.Fatalf("expected ApplyRules-after-Stop to error")
	}
}

func TestStartIsIdempotent(t *testing.T) {
	s := New(&stubStore{}, newStubTrigger())
	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := s.Start(ctx); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })
}

func TestApplyRulesBeforeStartErrors(t *testing.T) {
	s := New(&stubStore{}, newStubTrigger())
	if err := s.ApplyRules([]*rules.Rule{mkRule("r1", "* * * * * *", rules.StateActive)}); err == nil {
		t.Fatalf("expected ApplyRules-before-Start to error")
	}
}

// --- timing tests ---------------------------------------------------------

// TestTimingEverySecond is the fast version of the AC: 4-second window,
// every-second schedule, assert >= 3 fires. Runs in the standard suite.
func TestTimingEverySecond(t *testing.T) {
	trig := newStubTrigger()
	s := newStarted(t, trig)

	if err := s.ApplyRules([]*rules.Rule{
		mkRule("ticker", "* * * * * *", rules.StateActive),
	}); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}

	start := time.Now()
	got := waitForCount(t, trig, "ticker", 3, 4500*time.Millisecond)
	if got < 3 {
		t.Fatalf("got %d fires in %s, want >=3", got, time.Since(start))
	}
}

// TestTimingFiveMinutes is the spec's AC: 5 fires in 5 minutes for a
// */1 * * * * rule. Skipped in -short mode.
func TestTimingFiveMinutes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 5-minute AC test in short mode; see TestTimingEverySecond for the fast equivalent")
	}
	trig := newStubTrigger()
	s := newStarted(t, trig)

	if err := s.ApplyRules([]*rules.Rule{
		mkRule("once_a_minute", "*/1 * * * *", rules.StateActive),
	}); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}

	// 5 minutes + a 2s grace window for the final tick.
	got := waitForCount(t, trig, "once_a_minute", 5, 5*time.Minute+2*time.Second)
	if got < 5 {
		t.Fatalf("expected >=5 fires in 5min, got %d", got)
	}
}

// --- error / robustness ---------------------------------------------------

func TestTriggerErrorDoesNotStopScheduler(t *testing.T) {
	trig := newStubTrigger()
	trig.errFor["r1"] = fmt.Errorf("boom")
	cap := &captureLogger{}
	s := newStarted(t, trig, WithLogger(cap))

	if err := s.ApplyRules([]*rules.Rule{
		mkRule("r1", "* * * * * *", rules.StateActive),
	}); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}
	// Even though Trigger errors every time, the scheduler must keep firing.
	if got := waitForCount(t, trig, "r1", 2, 3*time.Second); got < 2 {
		t.Fatalf("expected >=2 fires despite errors, got %d", got)
	}
	if !strings.Contains(cap.text(), "boom") {
		t.Fatalf("logger didn't capture trigger error: %q", cap.text())
	}
}

func TestWithLoggerNilInstallsNoop(t *testing.T) {
	// Pass nil to WithLogger; the scheduler must not panic on the first
	// fire when a trigger error is produced (would otherwise nil-deref the
	// logger).
	trig := newStubTrigger()
	trig.errFor["r1"] = fmt.Errorf("boom")
	s := newStarted(t, trig, WithLogger(nil))

	if err := s.ApplyRules([]*rules.Rule{
		mkRule("r1", "* * * * * *", rules.StateActive),
	}); err != nil {
		t.Fatalf("ApplyRules: %v", err)
	}
	// Just wait for one fire to make sure we don't panic.
	if got := waitForCount(t, trig, "r1", 1, 2500*time.Millisecond); got < 1 {
		t.Fatalf("expected >=1 fire, got %d", got)
	}
}

func TestWithJobTimeoutIgnoresInvalid(t *testing.T) {
	s := New(&stubStore{}, newStubTrigger(), WithJobTimeout(0), WithJobTimeout(-1*time.Second))
	if s.opts.jobTimeout != defaultJobTimeout {
		t.Fatalf("jobTimeout = %v, want default %v", s.opts.jobTimeout, defaultJobTimeout)
	}
}

func TestWithJobTimeoutOverrides(t *testing.T) {
	s := New(&stubStore{}, newStubTrigger(), WithJobTimeout(5*time.Second))
	if s.opts.jobTimeout != 5*time.Second {
		t.Fatalf("jobTimeout = %v, want 5s", s.opts.jobTimeout)
	}
}

// --- concurrency ----------------------------------------------------------

func TestConcurrentApplyAndStop(t *testing.T) {
	trig := newStubTrigger()
	s := New(&stubStore{}, trig)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const N = 32
	var wg sync.WaitGroup
	var applyErrs atomic.Int32
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rs := []*rules.Rule{
				mkRule(fmt.Sprintf("r%d", i%4), "* * * * * *", rules.StateActive),
			}
			if err := s.ApplyRules(rs); err != nil {
				// "scheduler: stopped" is the only acceptable error here.
				if !strings.Contains(err.Error(), "stopped") {
					applyErrs.Add(1)
				}
			}
		}(i)
	}

	// Stop concurrently with the apply storm.
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		_ = s.Stop()
	}()

	wg.Wait()
	if n := applyErrs.Load(); n > 0 {
		t.Fatalf("unexpected ApplyRules errors: %d", n)
	}
}

// --- triggers.go adapter coverage ----------------------------------------

func TestTriggerFuncAdapter(t *testing.T) {
	var calls atomic.Int32
	adapter := TriggerFunc(func(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error) {
		calls.Add(1)
		if ruleID != "r1" || wait {
			t.Errorf("adapter saw ruleID=%q wait=%v, want r1/false", ruleID, wait)
		}
		return &tasks.Record{TaskID: "x", RuleID: ruleID}, nil
	})
	rec, err := adapter.Trigger(context.Background(), "r1", false)
	if err != nil {
		t.Fatalf("adapter.Trigger: %v", err)
	}
	if rec == nil || rec.TaskID != "x" {
		t.Fatalf("unexpected record: %+v", rec)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

// --- table-driven validity check on schedule strings --------------------

func TestApplyRulesScheduleValidityTable(t *testing.T) {
	tests := []struct {
		name      string
		schedule  string
		wantArmed bool
	}{
		{"5-field standard", "* * * * *", true},
		{"6-field with seconds", "* * * * * *", true},
		{"every descriptor", "@every 1s", true},
		{"hourly descriptor", "@hourly", true},
		{"empty", "", false},
		{"garbage", "not a cron", false},
		{"wrong count", "*", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trig := newStubTrigger()
			s := newStarted(t, trig)
			err := s.ApplyRules([]*rules.Rule{
				mkRule("r", tc.schedule, rules.StateActive),
			})
			armed := s.RegisteredCount() == 1
			if armed != tc.wantArmed {
				t.Fatalf("schedule=%q armed=%v want=%v (err=%v)",
					tc.schedule, armed, tc.wantArmed, err)
			}
		})
	}
}
