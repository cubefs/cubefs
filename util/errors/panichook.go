// Copyright 2023 The CubeFS Authors.
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

package errors

import (
	"runtime"
	"sync"
	_ "unsafe"

	"github.com/brahma-adshonor/gohook"
)

var ErrUnsupportedArch = New("Unsupported arch")

//go:linkname gopanic runtime.gopanic
func gopanic(e interface{})

var panicHook func()

// NOTE: trampoline don't works
var mu sync.Mutex

func hookedPanic(e interface{}) {
	mu.Lock()
	defer mu.Unlock()
	// NOTE: unhook before invoke hook function
	gohook.UnHook(gopanic)
	defer gohook.Hook(gopanic, hookedPanic, nil)
	panicHook()
	gopanic(e)
}

func AtPanic(hook func()) error {
	if !SupportPanicHook() {
		return ErrUnsupportedArch
	}
	panicHook = hook
	return gohook.Hook(gopanic, hookedPanic, nil)
}

var (
	oldToken = false
	newToken = false
)

//go:noinline
func setOldToken() {
	oldToken = true
}

//go:noinline
func setNewToken() {
	newToken = true
}

func supportTest() (ok bool) {
	err := gohook.Hook(setOldToken, setNewToken, nil)
	if err != nil {
		return
	}
	setOldToken()
	err = gohook.UnHook(setOldToken)
	if err != nil {
		return
	}
	setOldToken()
	ok = oldToken && newToken
	return
}

func SupportPanicHook() (ok bool) {
	switch runtime.GOARCH {
	case "amd64", "386":
		ok = supportTest()
	}
	return
}
