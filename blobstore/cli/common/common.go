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

import (
	"context"
	sysfmt "fmt"
	"strings"

	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

// CmdContext returns context with special reqest id tag
func CmdContext() context.Context {
	_, ctx := trace.StartSpanFromContextWithTraceID(
		context.Background(), "from-cmd", trace.RandomID().String()+"-cmd")
	return ctx
}

// Confirm ask for confirmation
// case insensitive 'y', 'yes', means yes
// case insensitive 'n', 'no', means no
// otherwise ask again
func Confirm(s string) bool {
	for {
		fmt.Printf("%s [Y / N]: ", s)

		var response string
		_, err := sysfmt.Scanln(&response)
		if err != nil {
			fmt.Println("Error:", err)
			return false
		}

		response = strings.ToLower(strings.TrimSpace(response))
		switch response {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		default:
		}
	}
}
