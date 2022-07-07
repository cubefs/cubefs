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

package rpc

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"

	"github.com/cubefs/cubefs/blobstore/util/log"
)

// defaultRecovery logging panic info, then panic to next handler
func defaultRecovery(w http.ResponseWriter, req *http.Request, err interface{}) {
	var brokenPipe bool
	if ne, ok := err.(*net.OpError); ok {
		if se, ok := ne.Err.(*os.SyscallError); ok {
			if strings.Contains(strings.ToLower(se.Error()), "broken pipe") ||
				strings.Contains(strings.ToLower(se.Error()), "connection reset by peer") {
				brokenPipe = true
			}
		}
	}

	stack := stack(3)
	if brokenPipe {
		log.Warnf("handle panic: %s on broken pipe\n%s", err, stack)
	} else {
		log.Errorf("handle panic: %s\n%s", err, stack)
		panic(err)
	}
}

func stack(skip int) []byte {
	buf := new(bytes.Buffer)
	for i := skip; ; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fmt.Fprintf(buf, "%s:%d (0x%x:%s)\n", file, line, pc, funcname(pc))
	}
	return buf.Bytes()
}

// funcname returns the name of the function
func funcname(pc uintptr) []byte {
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return []byte("???")
	}
	name := []byte(fn.Name())

	if last := bytes.LastIndex(name, []byte("/")); last >= 0 {
		name = name[last+1:]
	}
	if first := bytes.Index(name, []byte(".")); first >= 0 {
		name = name[first+1:]
	}
	return name
}
