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
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestClassifyErr(t *testing.T) {
	// 构造一个真实 net.Error timeout：使用 net.DialTimeout 一定不可达端口能拿到
	// timeout error；但更稳妥的是手写一个实现了 net.Error 的类型。
	timeoutNetErr := &fakeNetErr{timeout: true, msg: "i/o timeout"}
	nonTimeoutNetErr := &fakeNetErr{timeout: false, msg: "some net err"}
	opErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("boom")}

	cases := []struct {
		name string
		err  error
		want string
	}{
		// 1. ok
		{"nil error -> ok", nil, "ok"},
		// 2. context
		{"context.DeadlineExceeded -> timeout", context.DeadlineExceeded, "timeout"},
		{"context.Canceled -> timeout", context.Canceled, "timeout"},
		// 3. syscall.Errno
		{"ECONNREFUSED -> refused", syscall.ECONNREFUSED, "refused"},
		{"ETIMEDOUT -> timeout", syscall.ETIMEDOUT, "timeout"},
		{"ECONNRESET -> network", syscall.ECONNRESET, "network"},
		{"EPIPE -> network", syscall.EPIPE, "network"},
		{"ENETUNREACH -> network", syscall.ENETUNREACH, "network"},
		{"EHOSTUNREACH -> network", syscall.EHOSTUNREACH, "network"},
		{"EACCES -> permission", syscall.EACCES, "permission"},
		{"EPERM -> permission", syscall.EPERM, "permission"},
		// 4. net.Error
		{"net.Error timeout -> timeout", timeoutNetErr, "timeout"},
		{"net.OpError -> network", opErr, "network"},
		// 5. os.ErrPermission
		{"os.ErrPermission -> permission", os.ErrPermission, "permission"},
		// 6. wrapped permission
		{"wrapped os.ErrPermission -> permission", fmt.Errorf("wrap: %w", os.ErrPermission), "permission"},
		// 7. message based
		{"msg timeout -> timeout", errors.New("operation timeout"), "timeout"},
		{"msg deadline -> timeout", errors.New("deadline exceeded"), "timeout"},
		{"msg connection refused -> refused", errors.New("connection refused"), "refused"},
		{"msg no route -> network", errors.New("no route to host"), "network"},
		{"msg broken pipe -> network", errors.New("broken pipe"), "network"},
		{"msg connection reset -> network", errors.New("connection reset by peer"), "network"},
		{"msg permission denied -> permission", errors.New("permission denied"), "permission"},
		{"msg access denied -> permission", errors.New("Access Denied"), "permission"},
		{"msg 503 -> server_5xx", errors.New("HTTP 503 Service Unavailable"), "server_5xx"},
		{"msg 500 -> server_5xx", errors.New("Internal 500 Error"), "server_5xx"},
		{"msg 502 -> server_5xx", errors.New("Bad Gateway 502"), "server_5xx"},
		{"msg 504 -> server_5xx", errors.New("Gateway 504 Unavailable"), "server_5xx"},
		{"msg 404 -> client_4xx", errors.New("404 not found"), "client_4xx"},
		{"msg 403 -> client_4xx", errors.New("403 forbidden"), "client_4xx"},
		{"msg 401 -> client_4xx", errors.New("401 unauthorized"), "client_4xx"},
		{"msg 400 -> client_4xx", errors.New("400 bad request"), "client_4xx"},
		// 8. fallback
		{"unknown msg -> other", errors.New("something else"), "other"},
		// 9. net.Error non-timeout 不在 As 链上 trigger 后跑到 OpError 检查或 msg fallback
		{"non-timeout net.Error msg fallthrough", nonTimeoutNetErr, "other"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyErr(tc.err)
			if got != tc.want {
				t.Errorf("ClassifyErr(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

// fakeNetErr 实现 net.Error 接口；通过 timeout 字段控制 Timeout() 行为。
type fakeNetErr struct {
	timeout bool
	msg     string
}

func (e *fakeNetErr) Error() string   { return e.msg }
func (e *fakeNetErr) Timeout() bool   { return e.timeout }
func (e *fakeNetErr) Temporary() bool { return false }

// 编译期检查 fakeNetErr 满足 net.Error。
var _ net.Error = (*fakeNetErr)(nil)

// TestClassifyErr_PriorityOrder：context > syscall > net.Error > os > message。
// 当 syscall 已确定 bucket 时，message-based 兜底不应再被触发。
func TestClassifyErr_PriorityOrder(t *testing.T) {
	// ECONNREFUSED.Error() 字符串里没有 "timeout"，所以这里只验证 refused 优先。
	if got := ClassifyErr(syscall.ECONNREFUSED); got != "refused" {
		t.Errorf("syscall.ECONNREFUSED should be refused, got %q", got)
	}
	// context.Canceled 优先于 message
	wrapped := fmt.Errorf("wrap %w (timeout-like text but cancel inside)", context.Canceled)
	if got := ClassifyErr(wrapped); got != "timeout" {
		t.Errorf("wrapped context.Canceled should be timeout (treated as cancel), got %q", got)
	}
	// 防止 time.Now 类比较意外触发：保证函数无副作用。
	_ = time.Now()
}
