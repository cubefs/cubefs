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
	"net"
	"os"
	"strings"
	"syscall"
)

// ClassifyErr 把任意 error 归类到 7 个固定 bucket，用于 cubefs_bench_error_attr_total。
// 调用方应自带 fallback（如 S3 4xx/5xx），ClassifyErr 主要服务 POSIX/IOR/通用 path。
//
// 返回值取值集合（恒定 7 类 + 1 类 ok）：
//
//	ok | timeout | refused | network | permission | server_5xx | client_4xx | other
func ClassifyErr(err error) string {
	if err == nil {
		return "ok"
	}
	// 1. context 超时/取消
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	// 2. 系统调用错误
	var sysErr syscall.Errno
	if errors.As(err, &sysErr) {
		switch sysErr {
		case syscall.ECONNREFUSED:
			return "refused"
		case syscall.ETIMEDOUT:
			return "timeout"
		case syscall.ECONNRESET, syscall.EPIPE, syscall.ENETUNREACH, syscall.EHOSTUNREACH:
			return "network"
		case syscall.EACCES, syscall.EPERM:
			return "permission"
		}
	}
	// 3. net 包错误
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return "network"
	}
	// 4. 文件系统错误
	if errors.Is(err, os.ErrPermission) {
		return "permission"
	}
	// 5. message-based 兜底（针对没有 wrap 的字符串错误）
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return "timeout"
	case strings.Contains(msg, "connection refused"):
		return "refused"
	case strings.Contains(msg, "no route to host") || strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection reset"):
		return "network"
	case strings.Contains(msg, "permission denied") || strings.Contains(msg, "access denied"):
		return "permission"
	case strings.Contains(msg, "5") && (strings.Contains(msg, "500") || strings.Contains(msg, "502") || strings.Contains(msg, "503") || strings.Contains(msg, "504")):
		return "server_5xx"
	case strings.Contains(msg, "400") || strings.Contains(msg, "401") || strings.Contains(msg, "403") || strings.Contains(msg, "404"):
		return "client_4xx"
	}
	return "other"
}
