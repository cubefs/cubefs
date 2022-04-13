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

package samples

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jacobsa/fuse"
)

// Unmount the file system mounted at the supplied directory. Try again on
// "resource busy" errors, which happen from time to time on OS X (due to weird
// requests from the Finder) and when tests don't or can't synchronize all
// events.
func unmount(dir string) (err error) {
	delay := 10 * time.Millisecond
	for {
		err = fuse.Unmount(dir)
		if err == nil {
			return
		}

		if strings.Contains(err.Error(), "resource busy") {
			log.Println("Resource busy error while unmounting; trying again")
			time.Sleep(delay)
			delay = time.Duration(1.3 * float64(delay))
			continue
		}

		err = fmt.Errorf("Unmount: %v", err)
		return
	}
}
