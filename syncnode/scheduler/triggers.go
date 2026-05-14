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

	"github.com/cubefs/cubefs/syncnode/tasks"
)

// triggerFunc adapts a plain function to the Trigger interface. Used by
// tests and by callers that want to wrap the real *tasks.Runner with
// metrics / log enrichment without writing a new type.
type triggerFunc func(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error)

// Trigger satisfies the Trigger interface.
func (f triggerFunc) Trigger(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error) {
	return f(ctx, ruleID, wait)
}

// TriggerFunc lifts a function value into a Trigger. Handy for wrapping
// *tasks.Runner with instrumentation:
//
//	sched := scheduler.New(store, scheduler.TriggerFunc(func(ctx context.Context, id string, wait bool) (*tasks.Record, error) {
//	    metrics.SchedFires.Inc()
//	    return runner.Trigger(ctx, id, wait)
//	}))
//
// *tasks.Runner itself already satisfies Trigger directly, so the common
// path is `scheduler.New(store, runner, ...)` and this helper is only for
// the instrumented-wrapper case.
func TriggerFunc(f func(ctx context.Context, ruleID string, wait bool) (*tasks.Record, error)) Trigger {
	return triggerFunc(f)
}
