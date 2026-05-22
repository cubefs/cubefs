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

// bench-tools-runner 是 cubefs-bench-tools sidecar 镜像的 entrypoint。
//
// 与 syncnode 主容器共享 Pod network namespace，监听 127.0.0.1:18000（可通过
// BENCH_TOOLS_LISTEN 环境变量覆盖），接受 syncnode 通过 HTTP 调度的 ior /
// mdtest 子进程执行请求。Pod 内 loopback 通信，**不暴露在 Pod 外**。
//
// 路由：
//
//	GET  /healthz  → 200 OK
//	POST /run      → 执行子进程并把 stdout/stderr/exit/duration 以 JSON 返回
//
// 请求体：
//
//	{
//	  "tool":    "ior" | "mdtest",
//	  "args":    ["-a", "POSIX", "-w", "-r", ...],
//	  "workdir": "/cfs/posix-bench/bench-...",
//	  "useMpi":  true,            // 可选；true 时用 mpirun -n N 包一层
//	  "numTasks": 4,              // 可选；mpirun -n
//	  "mpiBin":  "mpirun",        // 可选；默认 mpirun
//	  "timeoutSec": 1800          // 可选；默认 0=不超时
//	}
//
// 响应体（HTTP 200 即使 exitCode != 0；只有协议错误才会返 4xx/5xx）：
//
//	{
//	  "exitCode":    0,
//	  "stdout":      "...",
//	  "stderr":      "...",
//	  "durationSec": 12.3
//	}
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// 允许执行的二进制白名单——避免 sidecar 沦为任意命令执行代理。
var allowedTools = map[string]string{
	"ior":    "/usr/local/bin/ior",
	"mdtest": "/usr/local/bin/mdtest",
}

type runRequest struct {
	Tool       string   `json:"tool"`
	Args       []string `json:"args"`
	WorkDir    string   `json:"workdir"`
	UseMpi     bool     `json:"useMpi"`
	NumTasks   int      `json:"numTasks"`
	MpiBin     string   `json:"mpiBin"`
	TimeoutSec int      `json:"timeoutSec"`
}

type runResponse struct {
	ExitCode    int     `json:"exitCode"`
	Stdout      string  `json:"stdout"`
	Stderr      string  `json:"stderr"`
	DurationSec float64 `json:"durationSec"`
}

func main() {
	addr := os.Getenv("BENCH_TOOLS_LISTEN")
	if addr == "" {
		addr = "127.0.0.1:18000"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/run", handleRun)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// /run 子进程可能跑很久 (IOR runtime + ramp 60s+)，所以不设 ReadTimeout
		// 也不设 WriteTimeout——靠请求中的 TimeoutSec 字段做上层超时。
	}

	log.Printf("bench-tools-runner listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}
}

func handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "decode request: "+err.Error(), http.StatusBadRequest)
		return
	}
	binPath, ok := allowedTools[req.Tool]
	if !ok {
		http.Error(w, "tool not allowed: "+req.Tool, http.StatusBadRequest)
		return
	}
	if req.WorkDir != "" {
		// 创建 workDir，确保 IOR 不会因目录不存在而报错。
		if err := os.MkdirAll(req.WorkDir, 0o755); err != nil {
			http.Error(w, "mkdir workdir: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	ctx := r.Context()
	if req.TimeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSec)*time.Second)
		defer cancel()
	}

	cmd := buildCmd(ctx, req, binPath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	t0 := time.Now()
	runErr := cmd.Run()
	dur := time.Since(t0).Seconds()

	exitCode := 0
	if runErr != nil {
		// ExitError 仅说明子进程返回非零；其他错误（如 start 失败）也要报。
		if ee, ok := runErr.(*exec.ExitError); ok {
			exitCode = ee.ExitCode()
		} else {
			// 启动失败等：以 -1 表达，stderr 里塞错误信息。
			exitCode = -1
			fmt.Fprintf(&stderr, "\nrunner error: %v\n", runErr)
		}
	}

	resp := runResponse{
		ExitCode:    exitCode,
		Stdout:      stdout.String(),
		Stderr:      stderr.String(),
		DurationSec: dur,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// buildCmd 根据请求拼接 *exec.Cmd。
//
//   - UseMpi=true：mpirun -n <N> --allow-run-as-root <binPath> <args...>
//   - UseMpi=false：<binPath> <args...>
//
// 工作目录由 WorkDir 指定；若为空则继承当前进程目录。
func buildCmd(ctx context.Context, req runRequest, binPath string) *exec.Cmd {
	if req.UseMpi {
		mpi := req.MpiBin
		if mpi == "" {
			mpi = "mpirun"
		}
		n := req.NumTasks
		if n <= 0 {
			n = 1
		}
		args := []string{
			"-n", strconv.Itoa(n),
			"--allow-run-as-root",
			binPath,
		}
		args = append(args, req.Args...)
		cmd := exec.CommandContext(ctx, mpi, args...)
		if req.WorkDir != "" {
			cmd.Dir = req.WorkDir
		}
		return cmd
	}
	cmd := exec.CommandContext(ctx, binPath, req.Args...)
	if req.WorkDir != "" {
		cmd.Dir = req.WorkDir
	}
	return cmd
}
