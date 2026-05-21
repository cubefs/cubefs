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

// Package atomicity carries the Phase G-4 verification harness for backend
// Rename atomicity. The actual tests live in rename_atomicity_test.go and
// are gated behind the `rename_atomicity` build tag — see the file header
// for the env-vars required and the operator-run command. This file exists
// only so `go list ./...` reports the package on the default build path.
package atomicity
