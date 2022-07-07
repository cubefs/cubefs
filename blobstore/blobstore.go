// Copyright 2022 The CubeFS Authors.
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

package blobstore

// go vet
//go:generate go vet ./...

// code formats with 'gofumpt' at version v0.2.1
// go install mvdan.cc/gofumpt@v0.2.1
//go:generate gofumpt -l -w .
//go:generate git diff --exit-code

// code golangci lint with 'golangci-lint' version v1.43.0
// go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.43.0
//go:generate golangci-lint run --issues-exit-code=1 -D errcheck -E bodyclose ./...
