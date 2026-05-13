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

// Package tasks is the HTTP control plane for triggering and querying
// syncnode task runs. It owns:
//
//   - Record: the persisted view of one task run (one-to-one with executor.Run).
//   - Store: a small persistence contract (memory impl ships here; on-disk
//     impls land in Phase F).
//   - Runner: the glue that builds backend.Backend pairs from a Rule,
//     constructs executor.Task, hands it to the Executor, and records the
//     terminal Result.
//   - Handlers: the /admin/sync/task/* HTTP endpoints.
//
// See design.md §9 Phase E-3.
package tasks

import (
	"time"

	"github.com/cubefs/cubefs/syncnode/executor"
)

// Record is the persisted view of one task run. It is created with
// Status=Running by Runner.Trigger and updated to a terminal status when
// executor.Run returns.
//
// Records are intentionally a flat, JSON-friendly shape so they can be
// listed / searched without re-deriving runtime details from a Rule.
type Record struct {
	TaskID     string               `json:"taskID"`
	RuleID     string               `json:"ruleID,omitempty"` // empty for ad-hoc triggers
	Type       executor.TaskType    `json:"type"`
	Status     executor.Status      `json:"status"`
	StartedAt  time.Time            `json:"startedAt"`
	DoneAt     time.Time            `json:"doneAt,omitempty"` // zero while running
	Error      string               `json:"error,omitempty"`  // populated on failed
	Progress   executor.Progress    `json:"progress"`         // last snapshot
	Mismatches []executor.Mismatch  `json:"mismatches,omitempty"` // only for check task
}

// cloneRecord returns a deep copy of r. Mismatches is the only slice field —
// Progress is a value type and remaining fields are scalars.
func cloneRecord(r *Record) *Record {
	if r == nil {
		return nil
	}
	cp := *r
	if len(r.Mismatches) > 0 {
		cp.Mismatches = make([]executor.Mismatch, len(r.Mismatches))
		copy(cp.Mismatches, r.Mismatches)
	}
	return &cp
}
