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
	"errors"
)

// runSync, runLoad, runCheck are the entry points for each task type. The
// real implementations live in sync_task.go / load_task.go / check_task.go;
// this file provides linkable stubs so the framework compiles before those
// land (and so build errors during partial development are obvious).
//
// Each stub returns errTaskNotImplemented; tests that touch the real path
// will fail with this until the real impl is in place. Tests for the
// framework itself (Run dispatching, Cancel, validate, Reporter callbacks)
// use TaskTypeCheck with an empty src/dst pair → bypasses this stub via
// type-specific test helpers.

var errTaskNotImplemented = errors.New("executor: task implementation pending")

// runSync is implemented in sync_task.go.
// runLoad is implemented in load_task.go.
// runCheck is implemented in check_task.go.
