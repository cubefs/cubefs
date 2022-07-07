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

package retry

import (
	"context"
	"time"
)

// Insist successfully on f.
// On error every duration if has error.
// Sleep duration time after f run.
func Insist(duration time.Duration, f func() error, onError func(error)) {
	err := f()
	if err == nil {
		return
	}
	onError(err)

	timer := time.NewTimer(duration)
	defer timer.Stop()
	<-timer.C

	for {
		err = f()
		if err == nil {
			return
		}
		onError(err)

		timer.Reset(duration)
		<-timer.C
	}
}

// InsistContext successfully on f or done with context.
// On error every duration if has error.
// Sleep duration time after f run.
func InsistContext(ctx context.Context, duration time.Duration, f func() error, onError func(error)) {
	err := f()
	if err == nil {
		return
	}
	onError(err)

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	for {
		err = f()
		if err == nil {
			return
		}
		onError(err)

		timer.Reset(duration)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}
