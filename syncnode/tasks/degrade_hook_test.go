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

package tasks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cubefs/cubefs/syncnode/backend"
	"github.com/cubefs/cubefs/syncnode/executor"
	"github.com/cubefs/cubefs/syncnode/rules"
	"github.com/cubefs/cubefs/syncnode/spec"
)

// realStoreHarness builds a Runner wired to the real rules.NewMemoryStore
// (not the stub used by the rest of runner_test.go). The Phase G-3 hook
// type-asserts r.rules.(rules.Store) — only this harness exercises that
// path. The stub harness leaves the hook silently no-op, which is exactly
// what those tests want.
func realStoreHarness(t *testing.T, build func(ep *spec.EndpointConfig) (backend.Backend, error)) (*Runner, rules.Store, Store) {
	t.Helper()
	exec := executor.New(executor.WithProgressInterval(20 * time.Millisecond))
	t.Cleanup(func() { _ = exec.Close() })
	taskStore := NewMemoryStore()
	t.Cleanup(func() { _ = taskStore.Close() })
	ruleStore := rules.NewMemoryStore()
	t.Cleanup(func() { _ = ruleStore.Close() })
	builder := &stubBackendBuilder{factory: build}
	runner := NewRunner(exec, taskStore, ruleStore, builder)
	return runner, ruleStore, taskStore
}

func newDegradeRule(id string) *rules.Rule {
	return rules.NewRule(spec.RuleConfig{
		ID:   id,
		Type: "sync",
		Src:  spec.EndpointConfig{Kind: "cfs", Vol: "v", Path: "/p"},
		Dst:  spec.EndpointConfig{Kind: "s3", Bucket: "b", Prefix: "pfx"},
	})
}

// waitForRuleState polls ruleStore.Get until the rule reaches want, or
// fails the test on timeout.
func waitForRuleState(t *testing.T, rs rules.Store, ruleID string, want rules.State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r, err := rs.Get(context.Background(), ruleID)
		if err == nil && r.State == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	r, _ := rs.Get(context.Background(), ruleID)
	t.Fatalf("rule %s never reached state %q within %s; current=%+v", ruleID, want, timeout, r)
}

// TestRunner_DegradesRuleOnVolNotFound is the Phase G-3 acceptance test:
// a task fails with a "vol not exists" error, and the Runner hook flips
// the rule to StateDegraded.
func TestRunner_DegradesRuleOnVolNotFound(t *testing.T) {
	r, ruleStore, taskStore := realStoreHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		// Source-side List returns the canonical "vol not exists" error so
		// the executor terminates with Status=Failed.
		return &listErrBackend{err: errors.New("cfs: get volume info: vol not exists")}, nil
	})

	rule := newDegradeRule("rule-vol-gone")
	if err := ruleStore.Create(context.Background(), rule); err != nil {
		t.Fatalf("Create rule: %v", err)
	}

	rec, err := r.Trigger(context.Background(), "rule-vol-gone", true)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rec.Status != executor.StatusFailed {
		t.Fatalf("Record.Status = %q, want failed", rec.Status)
	}

	// The Runner hook updates rule state on a background goroutine; wait
	// briefly for it to land.
	waitForRuleState(t, ruleStore, "rule-vol-gone", rules.StateDegraded, 2*time.Second)

	got, _ := ruleStore.Get(context.Background(), "rule-vol-gone")
	if got.LastRunStatus != "failed" {
		t.Errorf("rule.LastRunStatus = %q, want failed", got.LastRunStatus)
	}
	if got.LastRunError == "" {
		t.Errorf("rule.LastRunError empty, want classifier reason")
	}

	// Sanity: the task record still carries the executor result.
	rec2, _ := taskStore.Get(context.Background(), rec.TaskID)
	if rec2.Error == "" {
		t.Errorf("record.Error empty, want propagated failure")
	}
}

// TestRunner_DoesNotDegradeOnTransientError ensures the hook is selective:
// a transient network error does NOT flip the rule, because the executor
// will retry on the next scheduled run.
func TestRunner_DoesNotDegradeOnTransientError(t *testing.T) {
	r, ruleStore, _ := realStoreHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &listErrBackend{err: errors.New("dial tcp 10.0.0.1:443: i/o timeout")}, nil
	})

	rule := newDegradeRule("rule-transient")
	if err := ruleStore.Create(context.Background(), rule); err != nil {
		t.Fatalf("Create rule: %v", err)
	}

	rec, err := r.Trigger(context.Background(), "rule-transient", true)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rec.Status != executor.StatusFailed {
		t.Fatalf("Record.Status = %q, want failed", rec.Status)
	}

	// Give the hook goroutine a beat to potentially (wrongly) flip the
	// state; assert it did NOT happen.
	time.Sleep(50 * time.Millisecond)

	got, _ := ruleStore.Get(context.Background(), "rule-transient")
	if got.State != rules.StateActive {
		t.Fatalf("rule.State = %q, want still active (transient errors must not degrade)", got.State)
	}
}

// TestRunner_DoesNotDegradeOnSuccess confirms a successful task never
// touches rule state.
func TestRunner_DoesNotDegradeOnSuccess(t *testing.T) {
	r, ruleStore, _ := realStoreHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &emptyBackend{}, nil
	})

	rule := newDegradeRule("rule-ok")
	if err := ruleStore.Create(context.Background(), rule); err != nil {
		t.Fatalf("Create rule: %v", err)
	}

	rec, err := r.Trigger(context.Background(), "rule-ok", true)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if rec.Status != executor.StatusDone {
		t.Fatalf("Record.Status = %q, want done", rec.Status)
	}

	time.Sleep(50 * time.Millisecond)
	got, _ := ruleStore.Get(context.Background(), "rule-ok")
	if got.State != rules.StateActive {
		t.Errorf("rule.State = %q, want active", got.State)
	}
}

// TestRunner_DegradeIsResumable confirms the §9 G-3 acceptance criterion:
// after degradation, an operator can manually set state back to active
// (i.e. the auto-degrade does not lock the rule).
func TestRunner_DegradeIsResumable(t *testing.T) {
	r, ruleStore, _ := realStoreHarness(t, func(*spec.EndpointConfig) (backend.Backend, error) {
		return &listErrBackend{err: errors.New("cfs: get volume info: vol not exists")}, nil
	})

	rule := newDegradeRule("rule-resumable")
	if err := ruleStore.Create(context.Background(), rule); err != nil {
		t.Fatalf("Create rule: %v", err)
	}

	if _, err := r.Trigger(context.Background(), "rule-resumable", true); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	waitForRuleState(t, ruleStore, "rule-resumable", rules.StateDegraded, 2*time.Second)

	// Operator manually resumes via the store API used by the rule
	// handlers (PATCH state=active).
	if err := ruleStore.SetState(context.Background(), "rule-resumable", rules.StateActive); err != nil {
		t.Fatalf("SetState back to active: %v", err)
	}
	got, _ := ruleStore.Get(context.Background(), "rule-resumable")
	if got.State != rules.StateActive {
		t.Errorf("rule.State = %q, want active after resume", got.State)
	}
}
