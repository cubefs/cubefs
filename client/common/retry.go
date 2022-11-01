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

package common

import "time"

type Retry struct {
	retryTimes int
	delayTime  uint32
}

func (r Retry) On(caller func() error) error {
	var lastErr error
	for i := 0; i < r.retryTimes; i++ {
		err := caller()
		if err == nil {
			return nil
		}

		i++
		time.Sleep(time.Duration(r.delayTime) * time.Millisecond)

	}
	return lastErr
}

// return a Retry
// delayTime is millisecond
func Timed(retryTimes int, delayTime uint32) Retry {
	return Retry{
		retryTimes: retryTimes,
		delayTime:  delayTime,
	}
}
