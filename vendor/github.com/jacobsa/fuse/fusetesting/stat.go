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
	"fmt"
	"os"
	"reflect"
	"syscall"
	"time"

	"github.com/jacobsa/oglematchers"
)

// Match os.FileInfo values that specify an mtime equal to the given time.
func MtimeIs(expected time.Time) oglematchers.Matcher {
	return oglematchers.NewMatcher(
		func(c interface{}) error { return mtimeIsWithin(c, expected, 0) },
		fmt.Sprintf("mtime is %v", expected))
}

// Like MtimeIs, but allows for a tolerance.
func MtimeIsWithin(expected time.Time, d time.Duration) oglematchers.Matcher {
	return oglematchers.NewMatcher(
		func(c interface{}) error { return mtimeIsWithin(c, expected, d) },
		fmt.Sprintf("mtime is within %v of %v", d, expected))
}

func mtimeIsWithin(c interface{}, expected time.Time, d time.Duration) error {
	fi, ok := c.(os.FileInfo)
	if !ok {
		return fmt.Errorf("which is of type %v", reflect.TypeOf(c))
	}

	// Check ModTime().
	diff := fi.ModTime().Sub(expected)
	absDiff := diff
	if absDiff < 0 {
		absDiff = -absDiff
	}

	if !(absDiff < d) {
		return fmt.Errorf("which has mtime %v, off by %v", fi.ModTime(), diff)
	}

	return nil
}

// Match os.FileInfo values that specify a file birth time within the supplied
// radius of the given time. On platforms where there is no birth time
// available, match all os.FileInfo values.
func BirthtimeIsWithin(
	expected time.Time,
	d time.Duration) oglematchers.Matcher {
	return oglematchers.NewMatcher(
		func(c interface{}) error { return birthtimeIsWithin(c, expected, d) },
		fmt.Sprintf("birthtime is within %v of %v", d, expected))
}

func birthtimeIsWithin(
	c interface{},
	expected time.Time,
	d time.Duration) error {
	fi, ok := c.(os.FileInfo)
	if !ok {
		return fmt.Errorf("which is of type %v", reflect.TypeOf(c))
	}

	t, ok := extractBirthtime(fi.Sys())
	if !ok {
		return nil
	}

	diff := t.Sub(expected)
	absDiff := diff
	if absDiff < 0 {
		absDiff = -absDiff
	}

	if !(absDiff < d) {
		return fmt.Errorf("which has birth time %v, off by %v", t, diff)
	}

	return nil
}

// Extract time information from the supplied file info. Panic on platforms
// where this is not possible.
func GetTimes(fi os.FileInfo) (atime, ctime, mtime time.Time) {
	return getTimes(fi.Sys().(*syscall.Stat_t))
}

// Match os.FileInfo values that specify a number of links equal to the given
// number. On platforms where there is no nlink field available, match all
// os.FileInfo values.
func NlinkIs(expected uint64) oglematchers.Matcher {
	return oglematchers.NewMatcher(
		func(c interface{}) error { return nlinkIs(c, expected) },
		fmt.Sprintf("nlink is %v", expected))
}

func nlinkIs(c interface{}, expected uint64) error {
	fi, ok := c.(os.FileInfo)
	if !ok {
		return fmt.Errorf("which is of type %v", reflect.TypeOf(c))
	}

	if actual, ok := extractNlink(fi.Sys()); ok && actual != expected {
		return fmt.Errorf("which has nlink == %v", actual)
	}

	return nil
}
