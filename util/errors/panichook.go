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
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"time"
	_ "unsafe"

	"github.com/brahma-adshonor/gohook"
)

var ErrUnsupportedArch = New("Unsupported arch")

// NOTE: export go language panic function address

//go:linkname gopanic runtime.gopanic
func gopanic(e interface{})

var panicHook func()

// NOTE: useless code for trampoline
func panicTrampoline(e interface{}) {
	_ = e
	const arrayCount = 1000
	randGen := rand.New(rand.NewSource(time.Now().Unix()))
	randTable := make([]int, 0)
	for i := 0; i < arrayCount; i++ {
		randTable = append(randTable, randGen.Int())
	}

	sort.Slice(randTable, func(i, j int) bool {
		return randTable[i] < randTable[j]
	})

	for _, v := range randTable {
		fmt.Printf("%v", v)
	}
}

func hookedPanic(e interface{}) {
	panicHook()
	panicTrampoline(e)
}

func AtPanic(hook func()) error {
	if !SupportPanicHook() {
		return ErrUnsupportedArch
	}
	panicHook = hook
	return gohook.Hook(gopanic, hookedPanic, panicTrampoline)
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
