// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fusetesting

import (
	"syscall"
	"time"
)

func extractMtime(sys interface{}) (mtime time.Time, ok bool) {
	mtime = time.Unix(sys.(*syscall.Stat_t).Mtim.Unix())
	ok = true
	return
}

func extractBirthtime(sys interface{}) (birthtime time.Time, ok bool) {
	return
}

func extractNlink(sys interface{}) (nlink uint64, ok bool) {
	nlink = sys.(*syscall.Stat_t).Nlink
	ok = true
	return
}

func getTimes(stat *syscall.Stat_t) (atime, ctime, mtime time.Time) {
	atime = time.Unix(stat.Atim.Unix())
	ctime = time.Unix(stat.Ctim.Unix())
	mtime = time.Unix(stat.Mtim.Unix())
	return
}
