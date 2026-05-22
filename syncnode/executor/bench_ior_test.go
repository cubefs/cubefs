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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cubefs/cubefs/syncnode/spec"
)

// sidecarStub captures POST /run requests and returns canned responses.
// Each call to /run records the decoded iorRunRequest so tests can assert
// the args / tool / mpi knobs were plumbed correctly.
type sidecarStub struct {
	mu       sync.Mutex
	requests []iorRunRequest

	// per-tool canned response. nil means "auto-zero exit + empty body".
	responses map[string]iorRunResponse
}

func newSidecarStub() *sidecarStub {
	return &sidecarStub{responses: map[string]iorRunResponse{}}
}

func (s *sidecarStub) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req iorRunRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.requests = append(s.requests, req)
		resp, ok := s.responses[req.Tool]
		s.mu.Unlock()
		if !ok {
			resp = iorRunResponse{ExitCode: 0}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
	return httptest.NewServer(mux)
}

// iorSampleJSON is a representative IOR 3.3 JSON summary excerpt covering
// both write and read operations.
const iorSampleJSON = `IOR-3.3.0: MPI Coordinated Test of Parallel I/O
Began: Thu May 22 12:00:00 2026
{
  "summary": [
    {
      "operation": "write",
      "API": "POSIX",
      "bwMaxMIB": 312.45,
      "bwMeanMIB": 280.10,
      "bwMinMIB": 240.00,
      "OPsMean": 71705.6,
      "latencyMin": 0.000020,
      "latencyMean": 0.000048,
      "latencyMax": 0.000310
    },
    {
      "operation": "read",
      "API": "POSIX",
      "bwMaxMIB": 600.00,
      "bwMeanMIB": 580.00,
      "bwMinMIB": 540.00,
      "OPsMean": 148480.0,
      "latencyMin": 0.000010,
      "latencyMean": 0.000022,
      "latencyMax": 0.000120
    }
  ]
}
Finished: Thu May 22 12:00:30 2026`

func TestRunBenchIOR_ParsesSummary(t *testing.T) {
	stub := newSidecarStub()
	stub.responses["ior"] = iorRunResponse{
		ExitCode:    0,
		Stdout:      iorSampleJSON,
		DurationSec: 30.0,
	}
	srv := stub.server(t)
	defer srv.Close()

	tmp := t.TempDir()
	rule := &spec.BenchRule{
		StorageType:     spec.BenchStorageIOR,
		MountPath:       tmp,
		SidecarEndpoint: srv.URL,
		IORStages: []spec.IORStage{
			{
				Name: "rw",
				Tool: "ior",
				Args: []string{"-a", "POSIX", "-w", "-r", "-t", "1m", "-b", "16m"},
			},
		},
	}

	res, err := runBenchIORWithClient(context.Background(), rule, "task-1", 0, 1, 0, http.DefaultClient)
	if err != nil {
		t.Fatalf("runBenchIOR: %v", err)
	}
	if res.Status != "done" {
		t.Fatalf("status=%q want done; error=%q", res.Status, res.Error)
	}
	if len(res.Stages) != 1 {
		t.Fatalf("stages=%d want 1", len(res.Stages))
	}
	sr := res.Stages[0]
	// Totals: write+read bwMeanMIB = 280.10 + 580.00 = 860.10
	if got, want := sr.ThroughputMBs, 860.10; !approxEq(got, want, 0.01) {
		t.Errorf("ThroughputMBs=%v want %v", got, want)
	}
	// Total ops/sec = 71705.6 + 148480.0
	if got, want := sr.OpsPerSec, 220185.6; !approxEq(got, want, 0.1) {
		t.Errorf("OpsPerSec=%v want %v", got, want)
	}
	// Dominant by bw is "read" (bwMeanMIB=580); latencyMean=22us
	if got, want := sr.Latency.Mean, 22.0; !approxEq(got, want, 0.01) {
		t.Errorf("Latency.Mean=%v want %v", got, want)
	}
	// P99 ≈ latencyMax = 120us (read)
	if got, want := sr.Latency.P99, 120.0; !approxEq(got, want, 0.01) {
		t.Errorf("Latency.P99=%v want %v", got, want)
	}
	if got, want := sr.DurationSec, 30.0; got != want {
		t.Errorf("DurationSec=%v want %v", got, want)
	}

	// Verify sidecar saw the args we expect + the forced summaryFormat=JSON
	if len(stub.requests) != 1 {
		t.Fatalf("sidecar saw %d requests, want 1", len(stub.requests))
	}
	req := stub.requests[0]
	if req.Tool != "ior" {
		t.Errorf("tool=%q want ior", req.Tool)
	}
	if !sliceContains(req.Args, "-w") || !sliceContains(req.Args, "-r") {
		t.Errorf("args missing -w/-r: %v", req.Args)
	}
	if !sliceContainsSeq(req.Args, "-O", "summaryFormat=JSON") {
		t.Errorf("args missing forced JSON summary: %v", req.Args)
	}
	wantWorkDir := filepath.Join(tmp, "bench-task-1-shard-0")
	if req.WorkDir != wantWorkDir {
		t.Errorf("workdir=%q want %q", req.WorkDir, wantWorkDir)
	}
}

func TestRunBenchIOR_SidecarFailure(t *testing.T) {
	stub := newSidecarStub()
	stub.responses["ior"] = iorRunResponse{
		ExitCode: 7,
		Stderr:   "IOR ERROR: cannot open file",
	}
	srv := stub.server(t)
	defer srv.Close()

	tmp := t.TempDir()
	rule := &spec.BenchRule{
		StorageType:     spec.BenchStorageIOR,
		MountPath:       tmp,
		SidecarEndpoint: srv.URL,
		IORStages: []spec.IORStage{
			{Name: "rw", Tool: "ior", Args: []string{"-w"}},
		},
	}

	res, err := runBenchIORWithClient(context.Background(), rule, "task-fail", 0, 1, 0, http.DefaultClient)
	if err == nil {
		t.Fatalf("expected error for exit=7")
	}
	if res.Status != "failed" {
		t.Errorf("status=%q want failed", res.Status)
	}
	if !strings.Contains(res.Error, "exit=7") {
		t.Errorf("error %q missing exit=7", res.Error)
	}
}

func TestRunBenchIOR_MdtestUsesMpi(t *testing.T) {
	stub := newSidecarStub()
	// minimal mdtest-style summary
	stub.responses["mdtest"] = iorRunResponse{
		ExitCode: 0,
		Stdout: `mdtest-1.9.3 was launched
{
  "summary": [
    { "operation": "File creation", "OPsMean": 12345.6, "latencyMean": 0.000080, "latencyMax": 0.000500 }
  ]
}`,
		DurationSec: 12.0,
	}
	srv := stub.server(t)
	defer srv.Close()

	tmp := t.TempDir()
	rule := &spec.BenchRule{
		StorageType:     spec.BenchStorageIOR,
		MountPath:       tmp,
		SidecarEndpoint: srv.URL,
		IORDefaults: &spec.IORConfig{
			UseMpi:   true,
			NumTasks: 4,
		},
		IORStages: []spec.IORStage{
			{Name: "meta", Tool: "mdtest", Args: []string{"-n", "10000", "-F"}},
		},
	}

	res, err := runBenchIORWithClient(context.Background(), rule, "task-md", 0, 1, 0, http.DefaultClient)
	if err != nil {
		t.Fatalf("runBenchIOR: %v", err)
	}
	if res.Status != "done" {
		t.Fatalf("status=%q want done; error=%q", res.Status, res.Error)
	}
	if got := res.Stages[0].OpsPerSec; !approxEq(got, 12345.6, 0.01) {
		t.Errorf("mdtest OpsPerSec=%v want 12345.6", got)
	}

	req := stub.requests[0]
	if !req.UseMpi {
		t.Errorf("UseMpi=false; want true (from defaults)")
	}
	if req.NumTasks != 4 {
		t.Errorf("NumTasks=%d want 4", req.NumTasks)
	}
	if !sliceContains(req.Args, "-F") {
		t.Errorf("mdtest args missing -F: %v", req.Args)
	}
	if !sliceContainsSeq(req.Args, "-d", filepath.Join(tmp, "bench-task-md-shard-0")) {
		t.Errorf("mdtest args missing default -d workdir: %v", req.Args)
	}
}

func TestRunBenchIOR_SkipStage(t *testing.T) {
	stub := newSidecarStub()
	stub.responses["ior"] = iorRunResponse{ExitCode: 0, Stdout: iorSampleJSON}
	srv := stub.server(t)
	defer srv.Close()

	tmp := t.TempDir()
	rule := &spec.BenchRule{
		StorageType:     spec.BenchStorageIOR,
		MountPath:       tmp,
		SidecarEndpoint: srv.URL,
		IORStages: []spec.IORStage{
			{Name: "skipped", Tool: "ior", Skip: true, Args: []string{"-w"}},
			{Name: "run", Tool: "ior", Args: []string{"-r"}},
		},
	}
	res, err := runBenchIORWithClient(context.Background(), rule, "task-skip", 0, 1, 0, http.DefaultClient)
	if err != nil {
		t.Fatalf("runBenchIOR: %v", err)
	}
	if len(res.Stages) != 2 {
		t.Fatalf("stages=%d want 2 (skipped + run)", len(res.Stages))
	}
	if res.Stages[0].OpsPerSec != 0 {
		t.Errorf("skipped stage should have zero metrics")
	}
	if len(stub.requests) != 1 {
		t.Errorf("sidecar should see 1 request (skipped stage suppressed); got %d", len(stub.requests))
	}
}

func TestBuildIORArgs_ForcesJSONSummary(t *testing.T) {
	args := buildIORArgs(spec.IORConfig{}, spec.IORStage{
		Tool: "ior",
		Args: []string{"-w"},
	}, "/wd", "ior")
	if !sliceContainsSeq(args, "-O", "summaryFormat=JSON") {
		t.Fatalf("missing forced -O summaryFormat=JSON: %v", args)
	}
}

func TestBuildIORArgs_RespectsCallerSummaryFormat(t *testing.T) {
	args := buildIORArgs(spec.IORConfig{}, spec.IORStage{
		Tool: "ior",
		Args: []string{"-w", "-O", "summaryFormat=CSV"},
	}, "/wd", "ior")
	// We should NOT have appended JSON when caller set CSV.
	if countOccurrences(args, "summaryFormat=") != 1 {
		t.Fatalf("buildIORArgs duplicated summaryFormat: %v", args)
	}
	if !sliceContainsSeq(args, "-O", "summaryFormat=CSV") {
		t.Fatalf("expected caller's CSV summary preserved: %v", args)
	}
}

func TestParseIORResult_GarbageReturnsZero(t *testing.T) {
	sr := parseIORResult("not a json line", "stage")
	if sr.ThroughputMBs != 0 || sr.OpsPerSec != 0 {
		t.Errorf("garbage input produced metrics: %+v", sr)
	}
	if sr.Name != "stage" {
		t.Errorf("name=%q want stage", sr.Name)
	}
}

// --- helpers ---------------------------------------------------------------

func approxEq(a, b, tol float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= tol
}

func sliceContains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func sliceContainsSeq(s []string, a, b string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == a && s[i+1] == b {
			return true
		}
	}
	return false
}

func countOccurrences(s []string, sub string) int {
	n := 0
	for _, x := range s {
		if strings.Contains(x, sub) {
			n++
		}
	}
	return n
}
